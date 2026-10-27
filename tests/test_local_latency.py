"""Local scheduling only: fake devices/model, synthetic PCM in memory, no API or mic."""
import asyncio
from dataclasses import replace
import threading
from types import SimpleNamespace

import pytest

from call_translator import stream
from call_translator.audio import AudioBuffers, INPUT_FRAMES
from test_config_audio import valid_config


@pytest.mark.parametrize('profile', ['low', 'high'])
def test_latency_profiles_are_explicit_and_reversible(profile):
    replace(valid_config(), audio_latency=profile).validate()
    assert valid_config().audio_latency == 'high'


@pytest.mark.parametrize('profile', [None, '', 'lowest', 0.01, True, []])
def test_invalid_latency_profiles_fail_closed(profile):
    with pytest.raises(ValueError, match='audio_latency'):
        replace(valid_config(), audio_latency=profile).validate()


def test_adaptive_output_preserves_pcm_across_varying_callback_sizes():
    buffers = AudioBuffers()
    pcm = b''.join(i.to_bytes(2, 'little', signed=True) for i in range(7237))
    buffers.enqueue_output(pcm)
    parts = []
    for frames in [127, 512, 2048, 73, 4096, 8192]:
        out = bytearray(frames * 2)
        buffers.output_callback(out, frames, None, None)
        parts.append(bytes(out))
    played = b''.join(parts)
    assert played == pcm + bytes(len(played) - len(pcm))
    assert buffers.played_bytes == len(pcm)
    assert buffers.playback_idle() and not buffers.fault


def test_playback_does_not_wait_for_queue_prefill():
    buffers = AudioBuffers()
    out = bytearray(14)
    buffers.output_callback(out, 7, None, None)
    assert out == bytes(14)
    buffers.enqueue_output(b'\x01\x00\x02\x00')
    buffers.output_callback(out, 7, None, None)
    assert out == b'\x01\x00\x02\x00' + bytes(10)


def test_capture_notifications_do_not_weaken_the_full_queue_guard():
    notifications = []
    buffers = AudioBuffers(on_capture=lambda: notifications.append(1))
    for _ in range(11):
        buffers.input_callback(bytes(8), 4, None, None)
    assert len(notifications) == buffers.capture.qsize() == 10
    assert buffers.fault == 'capture_queue_overflow'


def test_idle_reader_wakes_on_normal_finish_and_drains_accepted_pcm():
    async def scenario():
        ready = asyncio.Event()
        buffers = AudioBuffers(on_capture=ready.set)
        waiting = asyncio.create_task(buffers.next_capture(ready))
        await asyncio.sleep(0)
        assert not waiting.done()
        buffers.input_callback(b'\x01\x00', 1, None, None)
        buffers.input_callback(b'\x02\x00', 1, None, None)
        buffers.admitting = False
        ready.set()
        assert await asyncio.wait_for(waiting, 1) == b'\x01\x00'
        assert await buffers.next_capture(ready) == b'\x02\x00'
        assert await buffers.next_capture(ready) is None

        buffers = AudioBuffers()
        waiting = asyncio.create_task(buffers.next_capture(ready))
        await asyncio.sleep(0)
        buffers.admitting = False
        ready.set()
        assert await asyncio.wait_for(waiting, 1) is None
    asyncio.run(scenario())


@pytest.mark.parametrize('profile', ['low', 'high'])
def test_real_audio_thread_wakes_sender_preserves_fifo_and_cancels_cleanly(monkeypatch, profile):
    async def scenario():
        opened, sent, metrics = {}, [], []
        all_sent = asyncio.Event()
        gate = threading.Event()
        packets = [i.to_bytes(2, 'little') * INPUT_FRAMES for i in range(6)]
        thread = None
        closed = []

        class Device:
            def __init__(self, side, **kwargs):
                self.side, self.kwargs = side, kwargs
                self.latency = 0.012 if side == 'input' else 0.023

            def __enter__(self):
                nonlocal thread
                opened[self.side] = self.kwargs
                if self.side == 'input':
                    def produce():
                        assert gate.wait(1)
                        for packet in packets:
                            self.kwargs['callback'](packet, INPUT_FRAMES, None, None)
                    thread = threading.Thread(target=produce, daemon=True)
                    thread.start()
                return self

            def __exit__(self, *args):
                closed.append(self.side)

        class Session:
            async def send_realtime_input(self, *, audio):
                sent.append(audio.data)
                if len(sent) == len(packets):
                    all_sent.set()

            async def receive(self):
                await asyncio.Future()
                yield None

        class Connection:
            async def __aenter__(self):
                return Session()

            async def __aexit__(self, *args):
                closed.append('session')

        class Client:
            def __init__(self, **kwargs):
                self.aio = SimpleNamespace(live=self, aclose=self.aclose)

            def connect(self, **kwargs):
                return Connection()

            async def aclose(self):
                closed.append('client')

        monkeypatch.setattr(stream.genai, 'Client', Client)
        sd = SimpleNamespace(RawInputStream=lambda **kw: Device('input', **kw),
                             RawOutputStream=lambda **kw: Device('output', **kw))
        task = asyncio.create_task(stream.translate(replace(valid_config(), audio_latency=profile),
                                                   0, 1, sd, on_metrics=metrics.append))
        try:
            while 'input' not in opened:
                await asyncio.sleep(0)
            # Give the sender a turn to become idle, then trigger a genuine other-thread callback.
            await asyncio.sleep(0)
            gate.set()
            await asyncio.wait_for(all_sent.wait(), 1)
            assert sent == packets  # includes all-zero PCM; no speech gating or batching loss
        finally:
            gate.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if thread is not None:
                thread.join(1)
                assert not thread.is_alive()
        assert opened['output']['blocksize'] == 2400
        assert opened['input']['blocksize'] == INPUT_FRAMES
        assert opened['input']['latency'] == opened['output']['latency'] == profile
        assert metrics[0]['input_device_latency_seconds'] == 0.012
        assert metrics[0]['output_device_latency_seconds'] == 0.023
        assert metrics[0]['audio_latency_profile'] == profile
        assert metrics[0]['audio_fault'] is None
        assert sorted(closed) == ['client', 'input', 'output', 'session']
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    asyncio.run(scenario())
