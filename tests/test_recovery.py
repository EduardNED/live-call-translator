import asyncio
import sys
from datetime import date,timedelta
from pathlib import Path
from types import SimpleNamespace
import pytest
from google.genai.errors import APIError
from call_translator.audio import AudioBuffers

sys.path.insert(0,str(Path(__file__).parent))
import probe_recovery as probe


class FakeNode:
    def __init__(self,config,target,credentials,generation,stats,elapsed):
        self.generation=generation;self.elapsed=elapsed;self.closed=False;self.ending=False
        self.buffers=AudioBuffers();self.text=[];self.text_events=[];self.complete=asyncio.Event()
        self.record={'generation':generation,'input_seconds':0.,'output_seconds':0.,
                     'opened_elapsed':elapsed(),'discarded_exact_zero_seconds':0.}
        stats['nodes'].append(self.record);self.session=self;self.sent=[]
    async def open(self):
        self.receiver=asyncio.create_task(asyncio.sleep(60));return self
    async def send_realtime_input(self,**kwargs):
        if self.closed:raise APIError(1011,{'error':{'message':'Internal error'}})
        if 'audio' in kwargs:self.sent.append(kwargs['audio'].data)
    async def end_input(self):self.ending=True
    async def close(self):
        if self.closed:return
        self.closed=True;self.receiver.cancel();await asyncio.gather(self.receiver,return_exceptions=True)


def route(source=b'',name='outgoing',factory=FakeNode):
    r=probe.Route(SimpleNamespace(model='offline'),name,source,[],None,node_factory=factory)
    r.started=probe.time.monotonic()
    return r


def test_full_packet_reserve_and_no_stale_credit():
    today=date.today().isoformat();future=(date.today()+timedelta(days=2)).isoformat()
    assert probe.preflight(100,today,future)*1.2<3
    for balance in (2.99,float('nan'),float('inf')):
        with pytest.raises(ValueError):probe.preflight(balance,today,future)
    with pytest.raises(ValueError):probe.preflight(100,'2020-01-01',future)
    assert all(b[0]-a[0]>=20 for a,b in zip(probe.SCHEDULE,probe.SCHEDULE[1:]))


def test_auth_or_local_overflow_is_not_blindly_retried():
    import queue
    assert probe.recoverable(APIError(1011,{'error':{'message':'internal'}}))
    assert not probe.recoverable(APIError(403,{'error':{'message':'forbidden'}}))
    assert not probe.recoverable(queue.Full())


def test_recovery_preserves_queued_speech_and_does_not_touch_healthy_route():
    async def check():
        failed=route();healthy=route(name='incoming')
        failed.input_node=failed.output_node=await failed.new_node()
        healthy.input_node=healthy.output_node=await healthy.new_node()
        original=healthy.input_node
        voice=b'\x00\x10'*1600
        failed.queue.put_nowait((0,0.,voice))
        old=failed.input_node
        await failed.recover(old,APIError(1011,{'error':{'message':'internal'}}))
        assert healthy.input_node is original and not original.closed
        assert failed.queue.get_nowait()[2]==voice
        assert len(failed.stats['recoveries'])==1
        assert failed.stats['uncertain_fixture_slots']
        assert failed.input_node is not old
        await asyncio.gather(*failed.closing)
        for n in failed.nodes+healthy.nodes:await n.close()
    asyncio.run(check())


def test_recovery_timeout_includes_waiting_for_transition_lock(monkeypatch):
    monkeypatch.setattr(probe,'RECOVERY_LIMIT',0.03)
    async def check():
        r=route();r.input_node=r.output_node=await r.new_node()
        await r.transition_lock.acquire()
        try:
            with pytest.raises(TimeoutError):
                await r.recover(r.input_node,RuntimeError('forced_disconnect'))
        finally:
            r.transition_lock.release()
            for n in r.nodes:await n.close()
    asyncio.run(check())


def test_recovery_cap_is_enforced_before_another_connection():
    async def check():
        r=route();r.input_node=r.output_node=await r.new_node()
        r.stats['recoveries']=[{}]*probe.MAX_RECOVERIES
        with pytest.raises(RuntimeError,match='bounded_recovery_count_exhausted'):
            await r.recover(r.input_node,RuntimeError('forced_disconnect'))
        assert len(r.nodes)==1
        await r.input_node.close()
    asyncio.run(check())


def test_catchup_discards_exact_zero_only_and_keeps_voice_order():
    async def check():
        r=route();r.input_node=r.output_node=await r.new_node()
        first=b'\x00\x10'*1600;second=b'\x01\x00'*1600
        for item in ((0,-2.,bytes(3200)),(1,-1.9,first),(2,-1.8,second)):
            r.queue.put_nowait(item)
        r.catchup=True;r.capture_done=True
        await r.send()
        assert r.stats['skipped_exact_zero_input_chunks']==1
        assert r.input_node.sent==[first,second]
        assert r.stats['sequence_gaps']==r.stats['duplicate_chunks']==0
        assert r.stats['processed_chunks']==3 and r.stats['sent_chunks']==2
        await r.input_node.close()
    asyncio.run(check())


def test_short_complete_sequence_closes_every_node_without_turn_complete(monkeypatch):
    monkeypatch.setattr(probe,'SOURCE_SECONDS',0.4)
    monkeypatch.setattr(probe,'RENEW_AGE',60)
    monkeypatch.setattr(probe,'SCHEDULE',[(0,0)])
    monkeypatch.setattr(probe,'drain_ready',lambda n,now,end:now-end>=0.01)
    async def check():
        r=route(bytes(12800))
        result=await r.run(asyncio.Barrier(1))
        assert result['status']=='sequence_completed'
        assert result['captured_chunks']==result['sent_chunks']==4
        assert result['sequence_gaps']==result['duplicate_chunks']==0
        assert not result['cleanup_error_types']
        assert all(n.closed and n.receiver.done() for n in r.nodes)
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    asyncio.run(check())


def test_repeated_planned_transitions_and_one_forced_close_are_independent(monkeypatch):
    monkeypatch.setattr(probe,'SOURCE_SECONDS',1.8)
    monkeypatch.setattr(probe,'RENEW_AGE',0.6)
    monkeypatch.setattr(probe,'PREOPEN_LEAD',0.1)
    monkeypatch.setattr(probe,'FORCE_AT',1.05)
    monkeypatch.setattr(probe,'SOURCE_QUIET',0.01)
    monkeypatch.setattr(probe,'SCHEDULE',[(0,0)])
    monkeypatch.setattr(probe,'drain_ready',lambda n,now,end:now-end>=0.02)
    async def check():
        outgoing=route(bytes(57600),'outgoing');incoming=route(bytes(57600),'incoming')
        barrier=asyncio.Barrier(2)
        results=await asyncio.gather(outgoing.run(barrier),incoming.run(barrier))
        assert all(r['status']=='sequence_completed' for r in results)
        assert len(outgoing.stats['planned_transitions'])>=2
        assert len(incoming.stats['recoveries'])==1
        assert incoming.stats['recoveries'][0]['forced'] is True
        assert outgoing.stats['recoveries']==[]
        for r in (outgoing,incoming):
            assert r.stats['captured_chunks']==r.stats['processed_chunks']==18
            assert r.stats['sent_chunks']+r.stats['skipped_exact_zero_input_chunks']==18
            assert r.stats['sequence_gaps']==r.stats['duplicate_chunks']==0
            assert all(n.closed and n.receiver.done() for n in r.nodes)
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    asyncio.run(check())


def test_ambiguous_failed_send_is_flagged_and_not_replayed():
    class FailingNode(FakeNode):
        async def send_realtime_input(self,**kwargs):
            if self.generation==0:raise APIError(1011,{'error':{'message':'Internal error'}})
            await super().send_realtime_input(**kwargs)
    async def check():
        r=route(factory=FailingNode)
        r.input_node=r.output_node=await r.new_node()
        first=b'\x00\x10'*1600;second=b'\x01\x00'*1600
        r.queue.put_nowait((0,0.,first));r.queue.put_nowait((1,0.1,second));r.capture_done=True
        await r.send()
        assert r.stats['uncertain_send_chunks']==1
        assert r.stats['processed_chunks']==2 and r.stats['sent_chunks']==1
        assert r.nodes[1].sent==[second]
        assert r.stats['uncertain_fixture_slots']
        await asyncio.gather(*r.closing)
        for n in r.nodes:await n.close()
    asyncio.run(check())


def test_planned_end_input_cannot_hold_capture_indefinitely(monkeypatch):
    monkeypatch.setattr(probe,'RECOVERY_LIMIT',0.03)
    class HangingNode(FakeNode):
        async def end_input(self):await asyncio.sleep(60)
    async def check():
        r=route(factory=HangingNode)
        r.input_node=r.output_node=await r.new_node()
        r.input_node.record['opened_elapsed']=-probe.RENEW_AGE
        r.prepared=await r.new_node();r.last_source_activity=-2
        try:
            with pytest.raises(TimeoutError):await r.planned()
            assert not r.send_lock.locked() and not r.transition_lock.locked()
        finally:
            for n in r.nodes:await n.close()
    asyncio.run(check())


def test_retired_session_close_error_is_in_final_cleanup_report(monkeypatch):
    monkeypatch.setattr(probe,'SOURCE_SECONDS',0.2)
    monkeypatch.setattr(probe,'RENEW_AGE',60)
    monkeypatch.setattr(probe,'SCHEDULE',[(0,0)])
    async def check():
        r=route(bytes(6400))
        async def bad_close():raise RuntimeError('offline_retired_close_failure')
        r.closing.append(asyncio.create_task(bad_close()))
        with pytest.raises(RuntimeError,match='offline_retired_close_failure'):
            await r.run(asyncio.Barrier(1))
        assert r.stats['cleanup_error_types']==['RuntimeError']
        assert all(n.closed and n.receiver.done() for n in r.nodes)
    asyncio.run(check())
