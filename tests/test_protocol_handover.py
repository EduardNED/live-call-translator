import asyncio
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch
from google import genai
from google.genai import types
from google.oauth2.credentials import Credentials
import google.genai.live as live

spec=importlib.util.spec_from_file_location('checks',Path(__file__).with_name('handover_checks.py'))
checks=importlib.util.module_from_spec(spec);spec.loader.exec_module(checks)


def test_real_sdk_setup_and_receive_preserve_resumption_fields_without_network():
    class Wire:
        def __init__(self):
            self.sent=[]
            self.responses=[{'setupComplete':{'sessionId':'offline-example'}},
                {'sessionResumptionUpdate':{'newHandle':'offline-example','resumable':True,
                  'lastConsumedClientMessageIndex':7},'goAway':{'timeLeft':'60s'}}]
        async def send(self,data): self.sent.append(json.loads(data))
        async def recv(self,**kwargs): return json.dumps(self.responses.pop(0))
        async def __aenter__(self): return self
        async def __aexit__(self,*args): pass
    async def check():
        wire=Wire()
        client=genai.Client(enterprise=True,project='offline-audit-project',location='global',
                           credentials=Credentials('offline-example'),
                           http_options=types.HttpOptions(api_version='v1beta1'))
        try:
            cfg=types.LiveConnectConfig(response_modalities=['AUDIO'],
                translation_config=types.TranslationConfig(target_language_code='fr',echo_target_language=False),
                session_resumption=types.SessionResumptionConfig(transparent=True))
            with patch.object(live,'ws_connect',return_value=wire):
                async with client.aio.live.connect(model='gemini-3.5-live-translate-preview',config=cfg) as session:
                    setup=wire.sent[0]['setup']
                    assert setup['sessionResumption']=={'transparent':True}
                    assert setup['generationConfig']['translationConfig']=={
                        'targetLanguageCode':'fr','echoTargetLanguage':False}
                    msg=await session._receive()
                    assert msg.session_resumption_update.new_handle=='offline-example'
                    assert msg.session_resumption_update.resumable is True
                    assert msg.session_resumption_update.last_consumed_client_message_index==7
                    assert msg.go_away.time_left=='60s'
        finally: await client.aio.aclose()
    asyncio.run(check())


def test_negation_and_numeric_order_are_not_keyword_presence():
    right=checks.check_facts('outgoing',"N'annulez pas la réunion. Neuf, pas dix. Dix-huit, et non quatre-vingts.")
    wrong=checks.check_facts('outgoing','Annulez la réunion. Dix, pas neuf. Quatre-vingts, pas dix-huit.')
    for key in ('do_not_cancel','nine_not_ten','18_not_80'):
        assert right['fact_matches'][key]==1
        assert wrong['fact_matches'][key]==0


def test_incoming_negative_and_number_checks_reject_reversal():
    right=checks.check_facts('incoming','Do not delete the document. Fifteen, not sixteen. Thirteen, not thirty.')
    wrong=checks.check_facts('incoming','Delete the document. Sixteen, not fifteen. Thirty, not thirteen.')
    for key in ('do_not_delete','15_not_16','13_not_30'):
        assert right['fact_matches'][key]==1
        assert wrong['fact_matches'][key]==0


def test_duplicate_or_missing_boundary_content_is_reported():
    result=checks.check_facts('outgoing','Le dossier jaune. Cinq copies. Le dossier jaune.')
    assert 'boundary_yellow_folder' in result['repeated_transcript_facts']
    assert 'boundary_reception' in result['flagged_for_review']
    assert result['general_translation_accuracy_verified'] is False


def load_handover():
    import sys
    folder=str(Path(__file__).parent)
    if folder not in sys.path:sys.path.insert(0,folder)
    import probe_handover
    return probe_handover


def test_handover_does_not_drop_capture_or_misclassify_closed_old_session(monkeypatch):
    probe=load_handover()
    nodes=[]
    class FakeNode:
        def __init__(self,config,target,credentials,generation,stats,elapsed):
            from call_translator.audio import AudioBuffers
            self.closed=False;self.generation=generation;self.record={
                'generation':generation,'input_seconds':0.0,'output_seconds':0.0}
            self.buffers=AudioBuffers();self.text=[];self.complete=asyncio.Event()
            self.session=self;stats['nodes'].append(self.record);nodes.append(self)
        async def open(self):
            self.receiver=asyncio.create_task(asyncio.sleep(60));return self
        async def send_realtime_input(self,**kwargs):
            assert not self.closed
            if 'audio' in kwargs:
                size=len(kwargs['audio'].data)*3//2
                self.buffers.enqueue_output(bytes(size));self.record['output_seconds']+=size/48000
        async def end_input(self):pass  # Exact-model control: no turn_complete arrives.
        async def close(self):
            if self.closed:return
            self.closed=True;self.receiver.cancel()
            await asyncio.gather(self.receiver,return_exceptions=True)
    monkeypatch.setattr(probe,'Node',FakeNode)
    monkeypatch.setattr(probe,'PREPARE_AT',0.05)
    monkeypatch.setattr(probe,'SWITCH_AT',0.15)
    monkeypatch.setattr(probe,'INPUT_END',0.4)
    monkeypatch.setattr(probe,'SOURCE_QUIET_SECONDS',0.01)
    monkeypatch.setattr(probe,'QUIET_SECONDS',0.02)
    async def check():
        from types import SimpleNamespace
        shared=[]
        result=await probe.route(SimpleNamespace(model='fake'),'outgoing',bytes(12800),
                                 [0.1]*7,None,asyncio.Barrier(1),shared)
        assert result['status']=='sequence_completed'
        assert result['captured_chunks']==result['sent_chunks']==4
        assert result['input_sequence_gaps']==result['input_duplicate_chunks']==0
        assert result['handover_completed'] is True
        assert result['old_received_equals_played'] is True
        assert result['unplayed_received_seconds']==0
        assert len(nodes)==2 and all(n.closed and n.receiver.done() for n in nodes)
    asyncio.run(check())


def test_short_probe_preflight_refuses_stale_or_insufficient_credit():
    from datetime import date,timedelta
    import pytest
    probe=load_handover()
    today=date.today().isoformat();future=(date.today()+timedelta(days=3)).isoformat()
    assert probe.preflight(100,today,future)*1.2<0.5
    for balance in (0.49,float('nan'),float('inf')):
        with pytest.raises(ValueError):probe.preflight(balance,today,future)
    with pytest.raises(ValueError):probe.preflight(100,'2020-01-01',future)


def test_variants_share_one_time_and_cost_packet():
    import pytest
    probe=load_handover()
    probe.validate_combined_packet(66.156,0.079564,90,0.1225)
    for args in ((91,0.079564,90,0.1225),(66.156,0.49,90,0.1225),
                 (float('nan'),0,90,0.1),(0,float('inf'),90,0.1),(-1,0,90,0.1)):
        with pytest.raises(ValueError):probe.validate_combined_packet(*args)


def test_received_cost_counter_survives_playback_queue_rejection():
    from types import SimpleNamespace
    import queue
    import pytest
    from call_translator.audio import AudioBuffers
    probe=load_handover()
    node=object.__new__(probe.Node)
    node.record={'output_seconds':0.0,'output_activity_chunks':0,'usage_reports':0}
    node.elapsed=lambda:0
    node.buffers=AudioBuffers();node.buffers.enqueue_output(bytes(10*48000))
    node.text=[];node.ending=False
    message=SimpleNamespace(usage_metadata=None,server_content=SimpleNamespace(
        output_transcription=None,turn_complete=False,model_turn=SimpleNamespace(parts=[
            SimpleNamespace(inline_data=SimpleNamespace(mime_type='audio/pcm;rate=24000',data=bytes(4800)))])))
    class Session:
        async def receive(self):yield message
    node.session=Session()
    with pytest.raises(queue.Full):asyncio.run(node.receive())
    assert node.record['output_seconds']==0.1
