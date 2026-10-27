"""Explicit tab binding and conservative native observer; never read speech or captions."""
import asyncio
import json
from pathlib import Path
import time

STATES = {'joined', 'left', 'ended', 'closed', 'background', 'unknown', 'unavailable', 'not_meet'}


class NativeMeet:
    def __init__(self, helper, tab_id=None):
        self.helper = str(Path(helper).resolve(strict=True))
        self.tab_id = tab_id
        if tab_id is not None and (type(tab_id) is not int or tab_id <= 0):
            raise ValueError('Select a positive Chrome tab ID from meet-tabs')

    async def read(self):
        args = ['--list'] if self.tab_id is None else ['--tab', str(self.tab_id)]
        process = await asyncio.create_subprocess_exec(self.helper, *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            async with asyncio.timeout(4):
                stdout, _ = await process.communicate()
            if process.returncode or len(stdout) > 16384:
                raise RuntimeError('observer_unavailable')
            value = json.loads(stdout)
            if self.tab_id is not None:
                if value.get('state') not in STATES:
                    raise ValueError('Invalid observer state')
                binding = value.get('binding')
                if binding is not None and (not isinstance(binding, str) or len(binding) != 64
                        or any(c not in '0123456789abcdef' for c in binding)):
                    raise ValueError('Invalid observer binding')
                return {'state': value['state'], 'binding': binding}
            tabs = value['tabs']
            if not isinstance(tabs, list) or any(type(x.get('tab_id')) is not int
                    or type(x.get('active')) is not bool for x in tabs):
                raise ValueError('Invalid tab inventory')
            return [{'tab_id': x['tab_id'], 'active': x['active']} for x in tabs]
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()


class MeetObserver:
    def __init__(self, native, *, clock=time.monotonic, sleep=asyncio.sleep, notify=None):
        self.native, self.clock, self.sleep = native, clock, sleep
        self.notify = notify or (lambda *a, **kw: None)
        self.binding = None

    async def require_joined(self):
        value = await self.native.read()
        if value['state'] != 'joined' or not value.get('binding'):
            raise RuntimeError('selected_meet_not_observable_and_joined')
        if self.binding is not None and self.binding != value['binding']:
            raise RuntimeError('selected_meet_changed')
        self.binding = value['binding']

    async def wait_departure(self):
        unknown_since = moved_since = None
        candidate = None
        while True:
            try:
                value = await self.native.read()
            except (OSError, ValueError, RuntimeError, TimeoutError):
                value = {'state': 'unknown'}
            state, binding = value['state'], value.get('binding')
            if state == 'not_meet' or binding not in (None, self.binding):
                state = 'moved'
            elif state != 'closed' and binding is None:
                state = 'unknown'
            if state in ('left', 'ended', 'closed'):
                if candidate == state:
                    return 'selected_tab_closed' if state == 'closed' else 'selected_meet_' + state
                candidate = state
                unknown_since = None
            else:
                candidate = None
                if state in ('joined', 'background'):
                    unknown_since = moved_since = None
                elif state == 'moved':
                    # The selected tab no longer shows this meeting. It is never followed, and
                    # a persistent change is a technical stop rather than a guessed departure.
                    if moved_since is None:
                        moved_since = self.clock()
                        self.notify('selected_meet_changed', call_continues=True)
                    if self.clock() - moved_since >= 60:
                        raise RuntimeError('meet_observer_timeout')
                elif unknown_since is None or self.clock() - unknown_since >= 60:
                    # Unreadable UI on the same meeting is not departure: ending translation
                    # mid-call is worse than relying on Ctrl-C if departure goes unseen.
                    unknown_since = self.clock()
                    self.notify('meet_observation_uncertain', call_continues=True,
                                stop_with_ctrl_c_after_leaving=True)
            await self.sleep(1)
