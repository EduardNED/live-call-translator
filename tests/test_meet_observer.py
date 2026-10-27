import asyncio
import json
import sys

import pytest

from call_translator.meet import MeetObserver, NativeMeet


class Native:
    def __init__(self, values):
        self.values = iter(values)
    async def read(self):
        value = next(self.values)
        if isinstance(value, Exception):
            raise value
        return value


def signal(state, binding='a'*64):
    return {'state': state, 'binding': binding}


def observe(values, prepare=True):
    async def run():
        now = [0]
        async def sleep(seconds):
            now[0] += seconds
        observer = MeetObserver(Native(values), clock=lambda: now[0], sleep=sleep)
        if prepare:
            await observer.require_joined()
        return await observer.wait_departure()
    return asyncio.run(run())


@pytest.mark.parametrize('state,result', [('left', 'selected_meet_left'), ('ended', 'selected_meet_ended'),
                                         ('closed', 'selected_tab_closed')])
def test_positive_departure_requires_same_signal_twice(state, result):
    assert observe([signal('joined'), signal(state), signal('joined'), signal(state), signal(state)]) == result


def test_background_silence_other_people_disconnect_and_read_failure_are_not_exit():
    values = [signal('joined')] + [signal('background')]*90 + [signal('unknown')]*20
    values += [OSError('private'), signal('joined'), signal('left'), signal('left')]
    assert observe(values) == 'selected_meet_left'


def test_reused_tab_id_or_wrong_meeting_is_never_followed():
    values = [signal('joined')] + [signal('left', 'b'*64)]*70
    with pytest.raises(RuntimeError, match='meet_observer_timeout'):
        observe(values)


@pytest.mark.parametrize('state', ['left', 'closed', 'background', 'unknown', 'unavailable', 'not_meet'])
def test_start_requires_observable_joined_selected_tab(state):
    with pytest.raises(RuntimeError, match='selected_meet_not_observable_and_joined'):
        observe([signal(state)])


def test_native_boundary_ignores_extra_payload_and_invalid_state(tmp_path):
    # A subprocess double exercises parsing/cleanup, not another browser implementation.
    async def run():
        from unittest.mock import patch
        class Process:
            returncode = 0
            async def communicate(self):
                return json.dumps(signal('joined') | {'speech': 'must not escape'}).encode(), b''
        async def create(*args, **kwargs): return Process()
        native = NativeMeet(sys.executable, 5)
        with patch('asyncio.create_subprocess_exec', create):
            assert await native.read() == signal('joined')
        async def bad(*args, **kwargs):
            class Bad(Process):
                async def communicate(self): return b'{"state":"participant_left"}', b''
            return Bad()
        with patch('asyncio.create_subprocess_exec', bad):
            with pytest.raises(ValueError): await native.read()
    asyncio.run(run())
