import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import pytest
from call_translator.audio import AudioBuffers

spec=importlib.util.spec_from_file_location('transition_drain',Path(__file__).with_name('transition_drain.py'))
drain=importlib.util.module_from_spec(spec);spec.loader.exec_module(drain)


def test_missing_turn_complete_can_drain_but_active_or_pending_audio_cannot():
    node=SimpleNamespace(buffers=AudioBuffers(),record={'last_activity_elapsed':4.5})
    node.buffers.enqueue_output(bytes(4800))
    assert not drain.drain_ready(node,6,4)
    assert drain.drain_ready(node,6.5,4)
    node.buffers.enqueue_output(b'\x01\x00'*2400)
    assert not drain.drain_ready(node,20,4)
    out=bytearray(9600);node.buffers.output_callback(out,4800,None,False)
    assert drain.drain_ready(node,20,4)


def test_discard_only_exact_zero_pcm_preserves_speech_and_accounting():
    buffers=AudioBuffers();buffers.pending=bytes(100)
    buffers.enqueue_output(bytes(4800)+b'\x01\x00'*2400+bytes(4800))
    assert drain.discard_leading_zeros(buffers)==4900
    assert buffers.played_bytes==0
    out=bytearray(4800);buffers.output_callback(out,2400,None,False)
    assert out==b'\x01\x00'*2400
    assert drain.discard_leading_zeros(buffers)==4800
    assert buffers.played_bytes==4800


def test_transition_buffer_is_bounded_and_counts_received_before_rejection():
    import sys
    sys.path.insert(0,str(Path(__file__).parent))
    from probe_handover import Node
    from google.oauth2.credentials import Credentials
    config=SimpleNamespace(project='offline-project',location='global')
    stats={'model':'offline-model','nodes':[]}
    async def check():
        node=Node(config,'fr',Credentials('offline-example'),1,stats,lambda:0)
        try:
            node.buffers.enqueue_output(bytes(12*48000))
            import queue
            with pytest.raises(queue.Full):node.buffers.enqueue_output(bytes(4800))
        finally:await node.close()
    asyncio.run(check())


def test_long_packet_preflight_fits_allocation_and_rejects_stale_credit():
    import sys
    from datetime import date,timedelta
    sys.path.insert(0,str(Path(__file__).parent))
    import probe_endurance
    today=date.today().isoformat();future=(date.today()+timedelta(days=30)).isoformat()
    assert probe_endurance.preflight(100,today,future)*1.2<2
    with pytest.raises(ValueError):probe_endurance.preflight(100,'2020-01-01',future)
    assert len(probe_endurance.IDS)==len(probe_endurance.SLOTS)==21
    assert probe_endurance.SLOTS[14]==535 and probe_endurance.IDS[14]==3


def test_no_speech_boundary_fails_bounded_and_closes_both_generations(monkeypatch):
    import sys
    sys.path.insert(0,str(Path(__file__).parent))
    import probe_handover as probe
    nodes=[]
    class FakeNode:
        def __init__(self,config,target,credentials,generation,stats,elapsed):
            self.generation=generation;self.closed=False;self.text=[]
            self.buffers=AudioBuffers();self.complete=asyncio.Event()
            self.record={'generation':generation,'input_seconds':0.,'output_seconds':0.}
            self.session=self;stats['nodes'].append(self.record);nodes.append(self)
        async def open(self):
            self.receiver=asyncio.create_task(asyncio.sleep(60));return self
        async def send_realtime_input(self,**kwargs):
            assert not self.closed
        async def end_input(self):pass
        async def close(self):
            self.closed=True;self.receiver.cancel()
            await asyncio.gather(self.receiver,return_exceptions=True)
    monkeypatch.setattr(probe,'Node',FakeNode)
    monkeypatch.setattr(probe,'PREPARE_AT',0.02)
    monkeypatch.setattr(probe,'SWITCH_AT',0.10)
    monkeypatch.setattr(probe,'BOUNDARY_WAIT_SECONDS',0.12)
    monkeypatch.setattr(probe,'INPUT_END',0.5)
    async def check():
        shared=[]
        with pytest.raises(TimeoutError):
            await probe.route(SimpleNamespace(model='offline'),'outgoing',
                b'\x00\x10'*8000,[0.1]*7,None,asyncio.Barrier(1),shared)
        assert shared[0]['status']=='error'
        assert shared[0].get('handover_completed') is not True
        assert shared[0]['input_sequence_gaps']==shared[0]['input_duplicate_chunks']==0
        assert len(nodes)==2 and all(n.closed and n.receiver.done() for n in nodes)
    asyncio.run(check())



def test_provider_failure_classification_never_logs_payload_or_auth():
    import sys
    from google.genai.errors import APIError
    sys.path.insert(0,str(Path(__file__).parent))
    import probe_handover
    error=APIError(1011,{'error':{'message':'Internal deadline exceeded secret-token-123 raw speech',
                               'status':'INTERNAL'}})
    result=probe_handover.diagnostic_error(error)
    assert result['error_code']==1011
    assert result['recognized_error_terms']==['internal','deadline']
    assert 'secret' not in str(result) and 'raw speech' not in str(result)
