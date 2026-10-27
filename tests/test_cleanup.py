import asyncio
from types import SimpleNamespace

import pytest

from call_translator import stream
from test_config_audio import valid_config


@pytest.mark.parametrize('failure', ['cancel', 'server_error'])
def test_streams_tasks_and_client_close(monkeypatch, capsys, failure):
    class AudioStream:
        active = 0

        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            AudioStream.active += 1
            return self

        def __exit__(self, *args):
            AudioStream.active -= 1

    class Session:
        async def send_realtime_input(self, **kwargs):
            pass

        async def receive(self):
            if failure == 'server_error':
                raise RuntimeError('private API body must not be logged')
            await asyncio.Future()
            yield None

    class Connection:
        closed = False

        async def __aenter__(self):
            return Session()

        async def __aexit__(self, *args):
            self.closed = True

    connection = Connection()

    class Client:
        closed = False

        def __init__(self, **kwargs):
            self.aio = SimpleNamespace(live=self, aclose=self.aclose)

        def connect(self, **kwargs):
            return connection

        async def aclose(self):
            Client.closed = True

    monkeypatch.setattr(stream.genai, 'Client', Client)
    sd = SimpleNamespace(RawInputStream=AudioStream, RawOutputStream=AudioStream)

    async def scenario():
        task = asyncio.create_task(stream.translate(valid_config(), 0, 1, sd))
        if failure == 'cancel':
            while AudioStream.active != 2:
                await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(RuntimeError):
                await task
        assert AudioStream.active == 0
        assert connection.closed
        assert Client.closed
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(scenario())
    output = capsys.readouterr().out
    assert 'private API body' not in output
    assert 'usage_last_counters' in output
