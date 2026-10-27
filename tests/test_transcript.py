"""Synthetic text only; no provider, microphone or retained human speech."""
import asyncio
import os
import threading
import time
from types import SimpleNamespace as NS

from google.genai import types

from call_translator.transcript import Transcript, MAX_TEXT
from call_translator.continuous import SessionNode, ContinuousRoute
from call_translator.stream import live_config
from test_continuous import config

LANGUAGES = {'outgoing': ('en', 'fr'), 'incoming': ('fr', 'en')}


def test_fragments_interleaving_generations_missing_completion_and_permissions(tmp_path):
    async def run():
        events = []
        writer = Transcript(LANGUAGES, lambda *a, **kw: events.append((a, kw)),
                            directory=tmp_path / 'private')
        writer.submit('outgoing', 1, 'original', 'synthetic source')
        writer.submit('incoming', 1, 'translation', '# synthetic target', True)
        writer.submit('outgoing', 1, 'translation', 'synthetic translation', True)
        writer.gap('outgoing')
        writer.submit('outgoing', 2, 'original', 'new synthetic fragment', True)
        # Independent incoming completion must never complete outgoing generation 1.
        await writer.close('selected_meet_left')
        text = writer.path.read_text()
        assert 'synthetic source' in text and '    # synthetic target' in text
        assert 'Partial tail: outgoing generation 1 original' in text
        assert 'Partial tail: incoming' not in text
        assert 'recovery gap' in text and 'partial=true' in text
        assert 'remote participants' in text and '/ me /' in text
        assert os.stat(writer.path).st_mode & 0o777 == 0o600
        assert os.stat(writer.directory).st_mode & 0o777 == 0o700
        assert 'synthetic' not in repr(events)
    asyncio.run(run())


def test_incremental_persistence_and_clean_shutdown(tmp_path):
    async def run():
        writer = Transcript(LANGUAGES, lambda *a, **k: None, directory=tmp_path / 'private')
        writer.submit('incoming', 1, 'original', 'synthetic incremental', True)
        for _ in range(100):
            if writer.path.exists() and 'synthetic incremental' in writer.path.read_text():
                break
            await asyncio.sleep(.01)
        else:
            assert False, 'fragment was not flushed before stop'
        await writer.close('selected_meet_left')
        assert not writer.partial and 'partial=false' in writer.path.read_text()
    asyncio.run(run())


def test_write_failure_preserves_prefix_without_leaking_exception(tmp_path, monkeypatch):
    events = []
    original = Transcript._open
    class FailingFile:
        def __init__(self, file):
            self.file, self.writes = file, 0
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.file.close()
        def flush(self):
            self.file.flush()
        def write(self, value):
            self.writes += 1
            if self.writes >= 5:
                raise OSError('SENSITIVE exception')
            return self.file.write(value)
    monkeypatch.setattr(Transcript, '_open', lambda self: FailingFile(original(self)))
    async def run():
        writer = Transcript(LANGUAGES, lambda *a, **kw: events.append((a, kw)), directory=tmp_path / 'private')
        writer.submit('outgoing', 1, 'original', 'synthetic')
        await writer.close('selected_meet_left')
        assert writer.disabled and writer.partial
        assert writer.path.read_text().startswith('# Machine call transcript')
        assert 'SENSITIVE' not in repr(events)
        assert any(a[0] == 'transcript_warning' for a, _ in events)
    asyncio.run(run())


def test_slow_disk_queue_overflow_and_bounded_stop(tmp_path, monkeypatch):
    blocked, release = threading.Event(), threading.Event()
    original = Transcript._open
    def slow_open(self):
        blocked.set()
        release.wait(2)
        return original(self)
    monkeypatch.setattr(Transcript, '_open', slow_open)
    async def run():
        events = []
        writer = Transcript(LANGUAGES, lambda *a, **kw: events.append((a, kw)),
                            directory=tmp_path / 'private', capacity=1)
        while not blocked.is_set():
            await asyncio.sleep(.001)
        began = time.monotonic()
        writer.submit('outgoing', 1, 'original', 'synthetic')
        writer.submit('outgoing', 1, 'original', 'synthetic lost')
        assert writer.disabled and time.monotonic() - began < .1
        await writer.close('emergency_stop', timeout=.03)
        assert time.monotonic() - began < .2
        assert events[-1][1]['writer_finished'] is False
        release.set()
        while writer.thread.is_alive():
            await asyncio.sleep(.01)
        assert 'partial=true' in writer.path.read_text()
    asyncio.run(run())


def test_unwritable_directory_and_large_fragment_degrade_safely(tmp_path):
    async def run():
        bad = tmp_path / 'file'
        bad.write_text('not a directory')
        writer = Transcript(LANGUAGES, lambda *a, **k: None, directory=bad / 'private')
        await writer.close('selected_meet_left')
        assert writer.disabled and not writer.path.exists()
        writer = Transcript(LANGUAGES, lambda *a, **k: None, directory=tmp_path / 'private')
        writer.submit('outgoing', 1, 'original', 'x' * (MAX_TEXT + 1))
        await writer.close('selected_meet_left')
        assert writer.disabled and writer.partial
    asyncio.run(run())


def test_node_requests_and_dispatches_text_only_with_opt_in():
    cfg = config()
    assert live_config(cfg).input_audio_transcription is None
    enabled = live_config(cfg, diagnostics=True)
    assert enabled.input_audio_transcription is not None and enabled.output_audio_transcription is not None
    async def run(enabled):
        seen = []
        node = SessionNode(cfg, time.monotonic, None)
        node.ending = True
        node.on_text = (lambda *args: seen.append(args)) if enabled else None
        message = types.LiveServerMessage(server_content=types.LiveServerContent(
            input_transcription=types.Transcription(text='synthetic source'),
            output_transcription=types.Transcription(finished=True)))
        class Session:
            count = 0
            async def receive(self):
                self.count += 1
                if self.count == 1:
                    yield message
        node.session = Session()
        await node.receive()
        assert seen == ([('original', 'synthetic source', False), ('translation', '', True)] if enabled else [])
    asyncio.run(run(False))
    asyncio.run(run(True))


def test_route_generations_and_recovery_gap_are_independent():
    async def run():
        route = ContinuousRoute(config(), 'outgoing', node_factory=SessionNode, notify=lambda *a, **k: None)
        assert route.transcript is None
        off = route._create_node(None, None)
        assert off.on_text is None
        events = []
        route.transcript = NS(submit=lambda *args: events.append(args), gap=lambda *args: events.append(args))
        first, second = route._create_node(None, None), route._create_node(None, None)
        second.on_text('translation', 'synthetic', False)
        first.on_text('original', 'synthetic', True)
        route.event('provider_recovery')
        assert events[0][1] == 2 and events[1][1] == 1 and events[2] == ('outgoing',)
    asyncio.run(run())


def test_voice_receives_audio_when_text_queue_has_failed(tmp_path, monkeypatch):
    release = threading.Event()
    original = Transcript._open
    def slow_open(self):
        release.wait(2)
        return original(self)
    monkeypatch.setattr(Transcript, '_open', slow_open)
    async def run():
        writer = Transcript(LANGUAGES, lambda *a, **k: None, directory=tmp_path / 'private', capacity=1)
        node = SessionNode(config(), time.monotonic, None)
        node.ending = True
        node.on_text = lambda *args: writer.submit('outgoing', 1, *args)
        message = types.LiveServerMessage(server_content=types.LiveServerContent(
            input_transcription=types.Transcription(text='synthetic'),
            output_transcription=types.Transcription(text='synthetic'),
            model_turn=types.Content(parts=[types.Part(inline_data=types.Blob(
                data=b'\x01\x00' * 2400, mime_type='audio/pcm;rate=24000'))])))
        class Session:
            count = 0
            async def receive(self):
                self.count += 1
                if self.count == 1:
                    yield message
        node.session = Session()
        try:
            await asyncio.wait_for(node.receive(), timeout=.2)
            assert writer.disabled
            assert node.received_bytes == 4800 and node.buffers.output_bytes == 4800
        finally:
            release.set()
            await writer.close('selected_meet_left')
    asyncio.run(run())


def test_symlinked_private_directory_is_refused(tmp_path):
    async def run():
        target = tmp_path / 'target'
        target.mkdir()
        link = tmp_path / 'private'
        link.symlink_to(target, target_is_directory=True)
        writer = Transcript(LANGUAGES, lambda *a, **k: None, directory=link)
        await writer.close('selected_meet_left')
        assert writer.disabled and not list(target.iterdir())
    asyncio.run(run())
