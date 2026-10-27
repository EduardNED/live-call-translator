"""Production call/controller against callback, provider, clock and browser doubles."""
import asyncio
from dataclasses import replace
from functools import partial
from types import SimpleNamespace

import pytest
from google.genai.errors import APIError

from call_translator import continuous as call
from call_translator.call_budget import validate_continuous, FULL_RATE
from call_translator.meet import MeetObserver
from test_config_audio import valid_config

CONTINUOUS = dict(planned_minutes=30, allocation_usd=8., reserve_percent=20, auth_source='gcloud')


def config(**values):
    return replace(valid_config(), continuous={**CONTINUOUS, **values})


class Clock:
    def __init__(self):
        self.now = 0.
        self.waiters = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        future = asyncio.get_running_loop().create_future()
        self.waiters.append((self.now + seconds, future))
        await future

    def tick(self):
        self.now = round(self.now + 0.1, 5)
        remaining = []
        for due, future in self.waiters:
            if future.done():
                continue
            if due <= self.now + 0.00001:
                future.set_result(None)
            else:
                remaining.append((due, future))
        self.waiters = remaining


class FakeNode:
    registry = None
    fail_open = None

    def __init__(self, cfg, clock, credentials):
        self.config, self.clock = cfg, clock
        self.buffers = call.AudioBuffers(clock=clock)
        self.opened_at = clock()
        self.closed_at = float('inf')
        self.last_activity = clock()
        self.input_bytes = self.received_bytes = self.usage_reports = 0
        self.usage_last = {}
        self.closed = self.ending = False
        self.receiver = None
        self.session = self
        self.sent = []
        self.failure = None
        self.registry.append(self)

    async def open(self):
        await asyncio.sleep(0)
        if self.fail_open == self.config.source_language:
            raise OSError('private provider body')
        self.receiver = asyncio.create_task(self.wait())
        return self

    async def wait(self):
        await asyncio.Future()

    async def send_realtime_input(self, **kwargs):
        if self.failure:
            error, self.failure = self.failure, None
            raise error
        if 'audio' in kwargs:
            self.sent.append(kwargs['audio'].data)

    async def end_input(self):
        self.ending = True

    async def close(self):
        if self.closed:
            return
        self.closed = True
        self.closed_at = self.clock()
        if self.receiver:
            self.receiver.cancel()
            await asyncio.gather(self.receiver, return_exceptions=True)
        call.discard_buffers(self.buffers)


class Rig:
    def __init__(self, *, leave_at=1810, fail_open=None, fail_input=False, script=None,
                 observer_states=None, allocation=8., planned_minutes=30, input_pcm=None, node_base=FakeNode):
        self.clock = Clock()
        self.input_pcm = input_pcm or bytes(3200)
        self.nodes = []
        self.events = []
        self.streams = []
        self.open_count = 0
        self.ever_opened = 0
        self.played = bytearray()
        self.leave_at = leave_at
        self.script = script
        self.state = 'joined'
        self.capture_alive = True
        self.fail_input = fail_input
        self.fail_open = fail_open
        self.cfg = config(allocation_usd=allocation, planned_minutes=planned_minutes)
        self.observer_states = observer_states
        self.outcome = None
        rig = self

        class Node(node_base):
            registry = rig.nodes
            fail_open = rig.fail_open
        self.node_class = Node

        class Native:
            async def read(self):
                state = (rig.observer_states(rig.clock()) if rig.observer_states else
                         'left' if rig.clock() >= rig.leave_at else rig.state)
                return {'state': state, 'binding': 'a'*64}
        self.observer = MeetObserver(Native(), clock=self.clock, sleep=self.clock.sleep,
                                     notify=self.notify)

        class Stream:
            def __init__(self, kind, **kw):
                self.kind, self.callback = kind, kw['callback']
                self.latency = .1
                assert kw['latency'] == 'high'
                if kind == 'output':
                    assert kw['blocksize'] == 2400

            def __enter__(self):
                assert len([n for n in rig.nodes if n.receiver is not None]) >= 2
                if rig.fail_input and self.kind == 'input':
                    raise OSError('device failed')
                rig.streams.append(self)
                rig.open_count += 1
                rig.ever_opened += 1
                return self

            def __exit__(self, *args):
                rig.streams.remove(self)
                rig.open_count -= 1
        self.sd = SimpleNamespace(RawInputStream=partial(Stream, 'input'),
                                  RawOutputStream=partial(Stream, 'output'))

    def notify(self, event, **values):
        self.events.append((self.clock(), event, values))

    async def run(self, *, cancel_at=None):
        async def creds(_):
            return None
        factory = partial(call.ContinuousRoute, node_factory=self.node_class, credentials=creds)
        routes = {'outgoing': (self.cfg, 0, 1),
                  'incoming': (replace(self.cfg, source_language='fr', target_language='en'), 2, 3)}
        task = asyncio.create_task(call.run_call(routes, self.sd, self.observer,
            route_factory=factory, clock=self.clock, sleep=self.clock.sleep, notify=self.notify))
        try:
            for _ in range(40000):
                for _ in range(12):
                    await asyncio.sleep(0)
                if task.done():
                    return task.result()
                self.clock.tick()
                if self.script:
                    self.script(self)
                if cancel_at is not None and self.clock() >= cancel_at:
                    task.cancel()
                for stream in list(self.streams):
                    if stream.kind == 'input' and self.capture_alive:
                        chunk = self.input_pcm(self.clock()) if callable(self.input_pcm) else self.input_pcm
                        stream.callback(chunk, 1600, None, False)
                    elif stream.kind == 'output':
                        out = bytearray(4800)
                        stream.callback(out, 2400, None, False)
                        if out != bytes(4800):
                            self.played.extend(out)
            pytest.fail('Offline controller never stopped')
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            assert self.open_count == 0
            assert all(n.closed and (n.receiver is None or n.receiver.done()) for n in self.nodes)
            assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]


@pytest.mark.parametrize("duration,planned,allocation", [(1810, 30, 8.), (3610, 60, 10.)])
def test_full_production_call_survives_old_deadlines_silence_and_multiple_renewals(duration, planned, allocation):
    def states(now):
        if 20 <= now < 60:
            return 'unknown'  # temporary disconnection is not departure
        if 80 <= now < 160:
            return 'background'  # another tab is not departure, even beyond uncertainty grace
        return 'joined' if now < duration else 'left'
    rig = Rig(observer_states=states, planned_minutes=planned, allocation=allocation)
    outcome = asyncio.run(rig.run())
    assert outcome == 'selected_meet_left'
    assert any(e == 'planned_duration_exceeded' for _, e, _ in rig.events)
    assert len([e for _, e, _ in rig.events if e == 'session_renewed']) >= 6
    assert any(e == 'LIVE_BOTH' for _, e, _ in rig.events)
    assert rig.ever_opened == 4
    assert all(len(n.sent) for n in rig.nodes)


def test_last_received_reply_played_before_departure_and_queues_cleared():
    marker = b'\x00\x10' * 2400
    sent = False
    def script(rig):
        nonlocal sent
        if rig.clock() >= 2 and not sent and len(rig.nodes) == 2:
            for node in rig.nodes:
                node.buffers.enqueue_output(marker)
                node.received_bytes += len(marker)
                node.last_activity = rig.clock()
            sent = True
    rig = Rig(leave_at=4, script=script)
    assert asyncio.run(rig.run()) == 'selected_meet_left'
    assert marker in rig.played
    assert all(n.buffers.playback_idle() and n.buffers.capture.empty() for n in rig.nodes)


def test_emergency_and_partial_setup_close_everything():
    rig = Rig(leave_at=100)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(rig.run(cancel_at=3))
    assert rig.clock() < 4  # no final drain
    rig = Rig(fail_open='fr')
    with pytest.raises(ExceptionGroup):
        asyncio.run(rig.run())
    assert rig.ever_opened == 0
    rig = Rig(fail_input=True)
    with pytest.raises(OSError):
        asyncio.run(rig.run())
    assert rig.ever_opened == 1


def test_capture_stall_is_technical_failure_not_departure():
    rig = Rig(leave_at=100, script=lambda r: setattr(r, 'capture_alive', r.clock() < 3))
    with pytest.raises(RuntimeError, match='capture_callbacks_stalled'):
        asyncio.run(rig.run())
    assert rig.clock() < 7
    assert rig.events[-1][2]['reason'] == 'technical_failure'


def test_isolated_provider_failure_warns_repeat_and_keeps_healthy_session():
    triggered = False
    def script(rig):
        nonlocal triggered
        if rig.clock() > 3 and len(rig.nodes) == 2 and not triggered:
            rig.nodes[0].failure = APIError(1011, {'error': {'message': 'private payload'}})
            triggered = True
    rig = Rig(leave_at=8, script=script)
    assert asyncio.run(rig.run()) == 'selected_meet_left'
    assert len(rig.nodes) == 3
    assert len(rig.nodes[1].sent) > 60  # healthy incoming remained live
    assert [v['direction'] for _, e, v in rig.events if e == 'provider_recovered'] == ['outgoing']
    assert any(v.get('last_phrase_may_need_repeat') for _, e, v in rig.events)
    assert 'private payload' not in repr(rig.events)


def test_unknown_observer_keeps_translating_and_closed_requires_confirmation():
    # Unreadable Meet UI for minutes is not departure; the call ends on a positive signal.
    rig = Rig(observer_states=lambda t: 'joined' if t < 2 else 'unknown' if t < 200 else 'left')
    assert asyncio.run(rig.run()) == 'selected_meet_left'
    assert rig.clock() >= 200
    assert len([e for _, e, _ in rig.events if e == 'meet_observation_uncertain']) >= 3
    rig = Rig(observer_states=lambda t: 'closed' if t >= 3 else 'joined')
    assert asyncio.run(rig.run()) == 'selected_tab_closed'


def test_budget_reserve_and_stale_packet_cannot_be_used_for_call():
    assert validate_continuous(CONTINUOUS, config().credit) < 8
    assert validate_continuous({**CONTINUOUS, 'planned_minutes': 60}, config().credit) < 6
    assert 8 < validate_continuous({**CONTINUOUS, 'planned_minutes': 90, 'allocation_usd': 9}, config().credit) < 9
    for values in ({'planned_minutes': 91}, {'planned_minutes': 90}, {'allocation_usd': 2}, {'allocation_usd': float('nan')},
                   {'reserve_percent': 0}, {'planned_minutes': 0}, {'auth_source': 'fallback'}):
        with pytest.raises(ValueError):
            config(**values).validate()
    with pytest.raises(ValueError):
        replace(config(), call={}).validate()


def test_nonzero_fifo_is_preserved_during_technical_catchup_and_cap_is_bounded():
    async def check():
        clock = Clock()
        async def creds(_): return None
        nodes = []
        class Node(FakeNode): registry = nodes
        route = call.ContinuousRoute(config(), 'outgoing', clock=clock, sleep=clock.sleep,
                                     node_factory=Node, credentials=creds, notify=lambda *a, **k: None)
        route.current = await route.new_node()
        first, second = b'\x00\x10'*1600, b'\x01\x00'*1600
        for chunk in (bytes(3200), first, second):
            route.input_callback(chunk, 1600, None, False)
        clock.now = 2
        route.catchup = True
        task = asyncio.create_task(route.send())
        for _ in range(50):
            await asyncio.sleep(0)
            clock.tick()
        assert route.current.sent == [first, second]
        assert route.skipped_zero == 1
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        route.recoveries = call.MAX_RECOVERIES
        with pytest.raises(RuntimeError, match='bounded_recovery_count_exhausted'):
            await route.recover(route.current, ConnectionError('private'))
        assert len(nodes) == 1
        await route.close()
    asyncio.run(check())


def test_auth_fault_does_not_retry_and_device_status_stops_both():
    triggered = False
    def script(rig):
        nonlocal triggered
        if rig.clock() > 2 and rig.nodes and not triggered:
            rig.nodes[0].failure = APIError(403, {'error': {'message': 'private auth'}})
            triggered = True
    rig = Rig(leave_at=100, script=script)
    with pytest.raises(APIError):
        asyncio.run(rig.run())
    assert len(rig.nodes) == 2
    assert not any(e == 'provider_recovered' for _, e, _ in rig.events)
    def fault(rig):
        if rig.clock() > 2 and rig.streams:
            rig.streams[0].callback(bytearray(4800), 2400, None, True)
    rig = Rig(leave_at=10, script=fault)
    asyncio.run(rig.run())
    assert not any(e == 'direction_status' and k.get('state') == 'stopped'
                   and 'underflow' in str(k.get('reason')) for _, e, k in rig.events)


def test_financial_warnings_are_cumulative_and_distinct_from_normal_meet_end():
    from call_translator.call_budget import CallBudget
    events = []
    budget = CallBudget(config(), lambda event, **kw: events.append((event, kw)))
    node = SimpleNamespace(opened_at=0., closed_at=float('inf'), input_bytes=0, received_bytes=0)
    budget.check([node], 6.5 / FULL_RATE)
    budget.check([node], 7.3 / FULL_RATE)
    assert [v['fraction'] for _, v in events] == [.8, .9]
    with pytest.raises(RuntimeError, match='financial_safety_stop'):
        budget.check([node], 7.96 / FULL_RATE)
    node.closed_at = 10
    assert budget.check([node], 1000) == 10 * FULL_RATE
    node.received_bytes = 48000 * 200
    assert budget.check([node], 1000) > 10 * FULL_RATE


def test_uncertain_send_is_not_replayed_and_nonzero_queue_survives_recovery():
    async def check():
        clock = Clock()
        async def creds(_): return None
        nodes = []
        class Node(FakeNode): registry = nodes
        route = call.ContinuousRoute(config(), 'outgoing', clock=clock, sleep=clock.sleep,
            node_factory=Node, credentials=creds, notify=lambda *a, **k: None)
        route.current = await route.new_node()
        route.current.failure = ConnectionError('private')
        voice1, voice2 = b'\x00\x10'*1600, b'\x01\x00'*1600
        route.input_callback(voice1, 1600, None, False)
        route.input_callback(voice2, 1600, None, False)
        task = asyncio.create_task(route.send())
        for _ in range(100):
            await asyncio.sleep(0)
            clock.tick()
        assert len(nodes) == 2
        assert nodes[0].sent == [] and nodes[1].sent == [voice2]
        assert route.uncertain_sends == 1 and route.recoveries == 1
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await route.close()
    asyncio.run(check())


def test_recovery_timeout_bounds_lock_wait_without_dropping_queued_voice(monkeypatch):
    monkeypatch.setattr(call, 'TRANSITION_LIMIT', .02)
    async def check():
        nodes = []
        class Node(FakeNode): registry = nodes
        async def creds(_): return None
        route = call.ContinuousRoute(config(), 'outgoing', node_factory=Node,
                                     credentials=creds, notify=lambda *a, **kw: None)
        route.current = await route.new_node()
        route.input_callback(b'\x00\x10'*1600, 1600, None, False)
        await route.transition_lock.acquire()
        try:
            with pytest.raises(TimeoutError):
                await route.recover(route.current, ConnectionError())
            assert route.capture.capture.qsize() == 1
            assert len(nodes) == 1
        finally:
            route.transition_lock.release()
            await route.close()
    asyncio.run(check())


def test_continuous_config_does_not_disable_legacy_finite_commands():
    valid_config().validate()
    assert call.live_config(config()).input_audio_transcription is None
    assert call.live_config(config()).output_audio_transcription is None


def test_recovery_can_interrupt_natural_boundary_wait_without_replacing_fresh_session():
    async def check():
        clock = Clock()
        clock.now = 500
        nodes = []
        class Node(FakeNode): registry = nodes
        async def creds(_): return None
        route = call.ContinuousRoute(config(), 'outgoing', clock=clock, sleep=clock.sleep,
            node_factory=Node, credentials=creds, notify=lambda *a, **k: None)
        route.current = await route.new_node()
        old = route.current
        old.opened_at = 0
        route.last_source_activity = clock()
        transition = asyncio.create_task(route.planned())
        await asyncio.sleep(0)
        assert not route.transition_lock.locked()
        await route.recover(old, ConnectionError('private'))
        for _ in range(10):
            clock.tick()
            await asyncio.sleep(0)
        await transition
        assert route.current is nodes[1] and len(nodes) == 2
        assert route.transitions == 0 and route.recoveries == 1
        await route.close()
    asyncio.run(check())


def test_real_session_adapter_keeps_only_numeric_usage_and_closes_client_after_close_error(monkeypatch):
    from test_lifecycle import audio_message
    state = {'closed': False, 'sent_config': None}
    class Session:
        delivered = False
        async def receive(self):
            if self.delivered:
                await asyncio.Future()
            self.delivered = True
            message = audio_message(b'\x00\x10'*2400)
            message.usage_metadata = SimpleNamespace(model_dump=lambda: {
                'total_token_count': 100, 'private_text': 'not retained', 'invalid': float('nan')})
            yield message
    class Context:
        async def __aenter__(self): return Session()
        async def __aexit__(self, *args): raise RuntimeError('private close payload')
    class Client:
        def __init__(self, **kwargs):
            self.aio = SimpleNamespace(live=self, aclose=self.aclose)
        def connect(self, **kwargs):
            state['sent_config'] = kwargs['config']
            return Context()
        async def aclose(self): state['closed'] = True
    monkeypatch.setattr(call.genai, 'Client', Client)
    async def check():
        node = call.SessionNode(config(), lambda: 1., None)
        await node.open()
        for _ in range(5): await asyncio.sleep(0)
        assert node.received_bytes == 4800
        assert node.usage_last == {'total_token_count': 100}
        assert state['sent_config'].input_audio_transcription is None
        assert state['sent_config'].output_audio_transcription is None
        with pytest.raises(RuntimeError, match='^session_cleanup_failed$'):
            await node.close()
        assert state['closed'] and node.buffers.playback_idle() and node.receiver.done()
    asyncio.run(check())


def test_continuous_nonzero_background_does_not_accumulate_at_planned_renewals():
    # Real microphones are not byte-exact zero. Preserve all such input, including quiet
    # samples, rather than hide the technical backlog by silently dropping potential speech.
    rig = Rig(leave_at=3610, planned_minutes=60, allocation=10., input_pcm=b'\x01\x00'*1600)
    assert asyncio.run(rig.run()) == 'selected_meet_left'
    assert rig.clock() >= 3610
    assert len([e for _, e, _ in rig.events if e == 'session_renewed']) >= 14
    assert sum(len(n.sent) for n in rig.nodes) >= 72000


def test_new_input_continues_while_old_output_drains_without_reordering_playback():
    async def check():
        clock = Clock()
        nodes = []
        class Node(FakeNode): registry = nodes
        async def creds(_): return None
        route = call.ContinuousRoute(config(), 'outgoing', clock=clock, sleep=clock.sleep,
            node_factory=Node, credentials=creds, notify=lambda *a, **k: None)
        old = route.current = route.output_node = await route.new_node()
        old.opened_at = -call.RENEW_AGE
        old.last_activity = route.last_source_activity = -3
        replacement = route.prepared = await route.new_node()
        old_pcm = b'\xe8\x03'*2400
        new_pcm = b'\xd0\x07'*2400
        old.buffers.enqueue_output(old_pcm)
        transition = asyncio.create_task(route.planned())
        for _ in range(5): await asyncio.sleep(0)
        assert route.current is replacement and route.output_node is old
        source_pcm = b'\x00\x10'*1600
        route.input_callback(source_pcm, 1600, None, False)
        sender = asyncio.create_task(route.send())
        replacement.buffers.enqueue_output(new_pcm)
        for _ in range(5): await asyncio.sleep(0)
        assert replacement.sent == [source_pcm] and not transition.done()
        nonzero = bytearray()
        for _ in range(35):
            out = bytearray(4800)
            route.output_callback(out, 2400, None, False)
            if out != bytes(4800): nonzero.extend(out)
            clock.tick()
            for _ in range(5): await asyncio.sleep(0)
        await transition
        assert bytes(nonzero) == old_pcm + new_pcm
        sender.cancel()
        await asyncio.gather(sender, return_exceptions=True)
        await route.close()
    asyncio.run(check())


def test_partial_handover_timeout_is_explicit_and_does_not_suppress_current_output(monkeypatch):
    monkeypatch.setattr(call, 'TRANSITION_LIMIT', .02)
    async def check():
        nodes = []
        class Node(FakeNode): registry = nodes
        async def creds(_): return None
        route = call.ContinuousRoute(config(), 'outgoing', node_factory=Node,
            credentials=creds, notify=lambda *a, **k: None)
        old = route.current = route.output_node = await route.new_node()
        old.opened_at -= call.RENEW_AGE
        route.last_source_activity -= 2
        replacement = route.prepared = await route.new_node()
        old.buffers.enqueue_output(b'\x00\x10'*2400)
        with pytest.raises(RuntimeError, match='renewal_transition_failed'):
            await route.planned()
        assert route.output_node is old
        assert not old.buffers.playback_idle()
        assert route.current is replacement
        await route.close()
        assert all(n.closed for n in nodes)
    asyncio.run(check())


def test_repeated_handover_with_delayed_continuous_provider_output_has_bounded_backlog(monkeypatch):
    from collections import deque
    monkeypatch.setattr(call, 'RENEW_AGE', 15)
    monkeypatch.setattr(call, 'PREOPEN_LEAD', 3)
    class DelayedNode(FakeNode):
        def __init__(self, *args):
            super().__init__(*args)
            self.jobs = deque()
            self.max_backlog = 0
        async def send_realtime_input(self, **kwargs):
            await super().send_realtime_input(**kwargs)
            if 'audio' in kwargs:
                chunk = kwargs['audio'].data
                pcm = chunk[:2]*2400 if call.active(chunk) else bytes(4800)
                self.jobs.append((self.clock()+3.5, pcm))
        async def wait(self):
            while True:
                while self.jobs and self.jobs[0][0] <= self.clock()+.00001:
                    _, pcm = self.jobs.popleft()
                    self.buffers.enqueue_output(pcm)
                    self.received_bytes += len(pcm)
                    if call.active(pcm): self.last_activity = self.clock()
                    self.max_backlog = max(self.max_backlog,
                        (self.buffers.output_bytes-self.buffers.played_bytes)/48000)
                await self.clock.sleep(.05)
    def source(now):
        # Only a 0.9 s quiet boundary every six seconds, with non-zero microphone noise.
        return (b'\xe8\x03' if now%6 < 5.1 else b'\x01\x00')*1600
    rig = Rig(leave_at=125, input_pcm=source, node_base=DelayedNode)
    assert asyncio.run(rig.run()) == 'selected_meet_left'
    assert len([e for _, e, _ in rig.events if e == 'session_renewed']) >= 12
    assert max(n.max_backlog for n in rig.nodes) < 2.5
    assert sum(len(n.sent) for n in rig.nodes) > 2400


def test_renewal_without_quiet_boundary_hands_over_instead_of_stopping_call():
    # Continuous non-zero source above the activity marker never offers a 0.8 s boundary.
    rig = Rig(leave_at=1100, input_pcm=b'\xe8\x03'*1600)
    assert asyncio.run(rig.run()) == 'selected_meet_left'
    renewed = [v['direction'] for _, e, v in rig.events if e == 'session_renewed']
    assert sorted(renewed) == ['incoming', 'incoming', 'outgoing', 'outgoing']
    forced = [v for _, e, v in rig.events if e == 'renewal_without_quiet_boundary']
    assert len(forced) == 4 and all(v['last_phrase_may_need_repeat'] for v in forced)


def test_token_refresh_timeout_or_failure_is_bounded_recoverable():
    import subprocess
    assert call.recoverable(subprocess.TimeoutExpired(['gcloud'], 5))
    assert call.recoverable(subprocess.CalledProcessError(1, ['gcloud']))
    assert not call.recoverable(RuntimeError('capture_queue_overflow'))


def test_session_node_enables_resumption_and_forwards_handle_and_go_away_without_leaking_handle(monkeypatch):
    from test_lifecycle import audio_message
    state = {'configs': []}
    class Session:
        delivered = False
        async def receive(self):
            if self.delivered:
                await asyncio.Future()
            self.delivered = True
            message = audio_message(b'\x00\x10'*2400)
            message.session_resumption_update = SimpleNamespace(
                new_handle='secret-handle-abc-999', resumable=True,
                last_consumed_client_message_index=12)
            message.go_away = SimpleNamespace(time_left='45s')
            yield message
    class Context:
        async def __aenter__(self): return Session()
        async def __aexit__(self, *args): return None
    class Client:
        def __init__(self, **kwargs):
            self.aio = SimpleNamespace(live=self, aclose=self._close)
        def connect(self, **kwargs):
            state['configs'].append(kwargs['config'])
            return Context()
        async def _close(self): return None
    monkeypatch.setattr(call.genai, 'Client', Client)
    async def check():
        events = []
        async def creds(_): return None
        route = call.ContinuousRoute(config(), 'outgoing', credentials=creds,
                                     notify=lambda e, **v: events.append((e, v)))
        first = route.current = route.output_node = await route.new_node()
        for _ in range(5): await asyncio.sleep(0)
        assert state['configs'][0].session_resumption.transparent is True
        assert state['configs'][0].session_resumption.handle is None
        assert route.resumable is True
        assert route.resumption_handle == 'secret-handle-abc-999'
        assert route.last_consumed_index == 12
        assert first.go_away_requested is True
        assert first.go_away_time_left == 45.0
        assert any(e == 'server_go_away' and v['time_left_seconds'] == 45.0 and v['resumable'] is True
                   for e, v in events)
        second = await route.new_node()
        assert state['configs'][1].session_resumption.transparent is True
        assert state['configs'][1].session_resumption.handle == 'secret-handle-abc-999'
        assert second.resumed is True
        assert route.resumed_sessions == 1
        assert 'secret-handle-abc-999' not in repr(events)
        await route.close()
        assert route.resumption_handle is None and route.resumable is False
    asyncio.run(check())


def test_resumption_handle_updates_clear_when_not_resumable_and_reject_stale_prepared():
    async def check():
        clock = Clock()
        nodes = []
        class Node(FakeNode):
            registry = nodes
        async def creds(_): return None
        events = []
        route = call.ContinuousRoute(config(), 'incoming', clock=clock, sleep=clock.sleep,
                                     node_factory=Node, credentials=creds,
                                     notify=lambda e, **v: events.append((e, v)))
        route.current = route.output_node = await route.new_node()
        route._on_resumption(route.current, SimpleNamespace(
            new_handle='handle-1', resumable=True, last_consumed_client_message_index=3))
        route.prepared = await route.new_node()
        assert route.prepared.resumption_handle == 'handle-1'
        # Newer handle arrives after pre-open: stale prepared connection must be retired and replaced.
        route._on_resumption(route.current, SimpleNamespace(
            new_handle='handle-2', resumable=True, last_consumed_client_message_index=9))
        replacement = await route._take_replacement()
        assert replacement.resumption_handle == 'handle-2'
        assert replacement.resumed is True
        # When server marks session non-resumable, stored handle is cleared.
        route._on_resumption(route.current, SimpleNamespace(
            new_handle='handle-3', resumable=False, last_consumed_client_message_index=10))
        assert route.resumable is False and route.resumption_handle is None
        fresh = await route.new_node()
        assert fresh.resumption_handle is None and fresh.resumed is False
        assert 'handle-' not in repr(events)
        await route.close()
    asyncio.run(check())


def test_rejected_resumption_handle_falls_back_to_fresh_session_without_killing_sibling():
    class RejectHandleNode(FakeNode):
        async def open(self):
            await asyncio.sleep(0)
            if getattr(self, 'resumption_handle', None):
                raise APIError(400, {'error': {'message': 'invalid session_resumption handle secret-h'}})
            self.receiver = asyncio.create_task(self.wait())
            return self
    triggered = False
    def script(rig):
        nonlocal triggered
        if rig.clock() > 2 and len(rig.nodes) == 2 and not triggered:
            out_node = rig.nodes[0]
            out_node.on_resumption(out_node, SimpleNamespace(
                new_handle='secret-h', resumable=True, last_consumed_client_message_index=4))
            out_node.failure = APIError(1011, {'error': {'message': 'internal close'}})
            triggered = True
    rig = Rig(leave_at=7, script=script, node_base=RejectHandleNode)
    assert asyncio.run(rig.run()) == 'selected_meet_left'
    rejected = [v for _, e, v in rig.events if e == 'resumption_handle_rejected']
    assert len(rejected) == 1 and rejected[0]['direction'] == 'outgoing'
    assert rejected[0]['fallback'] == 'fresh_session'
    recovered = [v for _, e, v in rig.events if e == 'provider_recovered']
    assert len(recovered) == 1 and recovered[0]['direction'] == 'outgoing'
    assert recovered[0]['mode'] == 'fresh'
    assert 'secret-h' not in repr(rig.events)


def test_go_away_triggers_early_renewal_and_reports_per_direction_states():
    triggered = False
    def script(rig):
        nonlocal triggered
        if rig.clock() >= 5 and len(rig.nodes) == 2 and not triggered:
            inc = rig.nodes[1]
            inc.on_resumption(inc, SimpleNamespace(
                new_handle='secret-inc-handle', resumable=True, last_consumed_client_message_index=15))
            inc.go_away_requested = True
            inc.go_away_time_left = 30.0
            inc.on_go_away(inc, 30.0)
            triggered = True
    rig = Rig(leave_at=12, script=script)
    assert asyncio.run(rig.run()) == 'selected_meet_left'
    renewed = [v for _, e, v in rig.events if e == 'session_renewed']
    assert len(renewed) == 1
    assert renewed[0]['direction'] == 'incoming'
    assert renewed[0]['reason'] == 'go_away'
    assert renewed[0]['mode'] == 'resumed'
    statuses = [(v['direction'], v['state']) for _, e, v in rig.events if e == 'direction_status']
    assert ('outgoing', 'starting') in statuses and ('outgoing', 'ready') in statuses
    assert ('incoming', 'starting') in statuses and ('incoming', 'ready') in statuses
    assert ('incoming', 'renewing') in statuses
    assert ('outgoing', 'stopped') in statuses and ('incoming', 'stopped') in statuses
    assert 'secret-inc-handle' not in repr(rig.events)



def test_recovery_backlog_is_dropped_so_nonzero_input_returns_to_real_time():
    async def check():
        clock = Clock()
        nodes = []
        class Node(FakeNode):
            registry = nodes
            async def open(self):
                if len(nodes) > 1:
                    await clock.sleep(3)  # slow reconnect while the microphone keeps capturing
                return await super().open()
        async def creds(_): return None
        events = []
        route = call.ContinuousRoute(config(), 'outgoing', clock=clock, sleep=clock.sleep,
            node_factory=Node, credentials=creds, notify=lambda e, **v: events.append((e, v)))
        route.current = route.output_node = await route.new_node()
        task = asyncio.create_task(route.send())
        voice = b'\x00\x10'*1600
        for step in range(120):
            if step == 10:
                route.current.failure = ConnectionError('private')
            route.input_callback(voice, 1600, None, False)
            for _ in range(8):
                await asyncio.sleep(0)
            clock.tick()
        assert route.recoveries == 1 and len(nodes) == 2
        # Owner accepts recovery audio loss: stale backlog must not become permanent delay.
        assert route.capture.capture.qsize() * 0.1 <= call.RECOVERY_STALE + 0.1  # was 2.8 s
        assert route.dropped_stale > 0
        dropped = [v for e, v in events if e == 'recovery_backlog_dropped']
        assert len(dropped) == 1 and dropped[0]['seconds'] >= 2
        assert dropped[0]['last_phrase_may_need_repeat'] is True
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await route.close()
    asyncio.run(check())


def test_prepared_node_resumption_updates_do_not_replace_current_lineage_handle():
    async def check():
        nodes = []
        class Node(FakeNode): registry = nodes
        async def creds(_): return None
        route = call.ContinuousRoute(config(), 'incoming', node_factory=Node,
                                     credentials=creds, notify=lambda *a, **k: None)
        route.current = route.output_node = await route.new_node()
        route._on_resumption(route.current, SimpleNamespace(
            new_handle='current-1', resumable=True, last_consumed_client_message_index=3))
        prepared = route.prepared = await route.new_node()
        # The idle pre-opened session must not redirect the live direction's handle.
        route._on_resumption(prepared, SimpleNamespace(
            new_handle='prepared-1', resumable=True, last_consumed_client_message_index=0))
        route._on_resumption(prepared, SimpleNamespace(
            new_handle=None, resumable=False, last_consumed_client_message_index=0))
        assert route.resumption_handle == 'current-1' and route.resumable is True
        assert await route._take_replacement() is prepared
        await route.close()
    asyncio.run(check())
