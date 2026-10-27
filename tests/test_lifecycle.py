"""Call lifecycle against injected clock, audio and client doubles: no microphone, Meet or API."""
import asyncio
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pytest

from call_translator import stream
from call_translator.audio import AudioBuffers
from call_translator.bridge import run_bridge
from test_config_audio import valid_config

CALL = dict(setup_seconds=15, conversation_seconds=300, drain_seconds=20,
            packet_remaining_seconds=600, packet_verified_on=date.today().isoformat())
TICK = 0.5  # slack for the supervisor and pump polling in simulated seconds


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.now += seconds
        await asyncio.sleep(0)


def audio_message(data, *, interrupted=False):
    parts = [SimpleNamespace(inline_data=SimpleNamespace(data=data, mime_type="audio/pcm;rate=24000"))]
    return SimpleNamespace(usage_metadata=None, server_content=SimpleNamespace(
        input_transcription=None, output_transcription=None, interrupted=interrupted,
        model_turn=None if interrupted else SimpleNamespace(parts=parts)))


class Rig:
    """Everything outside the translator: clock, device callbacks, server and cleanup state."""

    def __init__(self, monkeypatch, *, call=CALL, setup_delay=0, connect="ok", input_stream="ok",
                 capture_alive=True):
        self.config = replace(valid_config(), call=call)
        self.clock = FakeClock()
        self.start = self.clock()
        self.inbox = asyncio.Queue()
        self.metrics = []
        self.sent_times = []
        self.open_streams = 0
        self.ever_opened = 0
        self.session_closed = self.client_closed = False
        self.capture_alive = capture_alive
        self.server_closed = False
        self.speaking = False
        self.input_status = None
        self.speaker_consumes = True
        self.input_cb = self.output_cb = None
        self.translate_task = None
        rig = self

        class Stream:
            def __init__(self, kind, callback, fail=False):
                self.kind, self.callback, self.fail = kind, callback, fail

            def __enter__(self):
                if self.fail:
                    raise OSError("device unavailable")
                rig.open_streams += 1
                rig.ever_opened += 1
                setattr(rig, f"{self.kind}_cb", self.callback)
                return self

            def __exit__(self, *args):
                rig.open_streams -= 1

        class Session:
            async def send_realtime_input(self, **kwargs):
                rig.sent_times.append(rig.t())

            async def receive(self):
                while not rig.server_closed:
                    message = await rig.inbox.get()
                    if message is None:
                        rig.server_closed = True  # a closed stream stays closed on re-entry
                        return
                    yield message

        class Connection:
            async def __aenter__(self):
                if connect == "hang":
                    await asyncio.Future()
                if setup_delay:
                    await rig.clock.sleep(setup_delay)
                if connect == "fail":
                    raise ConnectionError("private API body must not be logged")
                return Session()

            async def __aexit__(self, *args):
                rig.session_closed = True

        class Client:
            def __init__(self, **kwargs):
                self.aio = SimpleNamespace(live=self, aclose=self.aclose)

            def connect(self, **kwargs):
                return Connection()

            async def aclose(self):
                rig.client_closed = True

        monkeypatch.setattr(stream.genai, "Client", Client)
        self.sd = SimpleNamespace(
            RawOutputStream=lambda **kw: Stream("output", kw["callback"]),
            RawInputStream=lambda **kw: Stream("input", kw["callback"], fail=input_stream == "fail"))

    def t(self):
        return self.clock() - self.start

    async def until(self, seconds):
        while self.t() < seconds:
            await self.clock.sleep(0.01)

    def say(self, seconds):
        self.inbox.put_nowait(audio_message(b"\x01\x00" * int(24000 * seconds)))

    async def pump(self):
        """Delivers 100 ms device callbacks the way PortAudio would while both streams are open."""
        next_tick = self.clock()
        while True:
            while self.open_streams == 2 and self.clock() >= next_tick:
                next_tick += 0.1
                if self.capture_alive:
                    self.input_cb((b"\xe8\x03" if self.speaking else b"\x00\x00") * 1600,
                                  1600, None, self.input_status)
                if self.speaker_consumes:
                    self.output_cb(bytearray(4800), 2400, None, None)
            await self.clock.sleep(0.01)

    async def run(self, script=None):
        self.translate_task = asyncio.create_task(stream.translate(
            self.config, 0, 1, self.sd, direction="outgoing", on_metrics=self.metrics.append,
            clock=self.clock, sleep=self.clock.sleep))
        helpers = [asyncio.create_task(self.pump())]
        if script:
            helpers.append(asyncio.create_task(script(self)))
        try:
            outcome = await self.translate_task
        except (Exception, asyncio.CancelledError) as exc:
            outcome = exc
        for helper in helpers:
            helper.cancel()
        await asyncio.gather(*helpers, return_exceptions=True)
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
        return outcome

    def assert_closed(self):
        assert self.open_streams == 0 and self.session_closed and self.client_closed


def play(monkeypatch, script=None, **options):
    rig = Rig(monkeypatch, **options)
    return rig, asyncio.run(rig.run(script))


def test_call_window_longer_than_120_seconds_with_bounded_final_playback(monkeypatch):
    async def script(rig):
        await rig.until(100)
        rig.say(0.5)
        await rig.until(303 + 2)  # window ended at 303 (3 s setup + 300 s); this is the tail
        rig.say(0.5)
    rig, metrics = play(monkeypatch, script, setup_delay=3)
    assert metrics["status"] == "call_completed" and metrics["drain_end_reason"] == "quiet"
    assert metrics["elapsed_seconds"] > 300
    assert metrics["elapsed_seconds"] <= 335 + TICK
    assert 3 <= metrics["setup_seconds_used"] < 3 + TICK
    assert 6 <= metrics["drain_seconds_used"] < 20
    # The translation that arrived after the window closed was still played out, not dropped.
    assert metrics["received_audio_seconds"] == metrics["played_audio_seconds"] == 1.0
    assert not metrics["audio_fault"]
    rig.assert_closed()


def test_window_end_stops_admitting_speech_but_keeps_accepted_audio():
    buffers = AudioBuffers()
    buffers.input_callback(b"\x01\x00" * 4, 4, None, None)
    buffers.admitting = False
    buffers.input_callback(b"\x01\x00" * 4, 4, None, None)
    assert buffers.capture.qsize() == 1 and not buffers.fault


def test_ordinary_silence_with_live_callbacks_is_not_a_fault(monkeypatch):
    rig, metrics = play(monkeypatch)
    assert metrics["status"] == "call_completed" and not metrics["audio_fault"]
    assert metrics["input_audio_seconds"] > 290  # silent callbacks were still captured and sent
    rig.assert_closed()


def test_drain_never_outlives_its_reserve_while_audio_keeps_arriving(monkeypatch):
    async def script(rig):
        await rig.until(300)
        while True:
            rig.say(0.2)
            await rig.clock.sleep(0.2)
    rig, metrics = play(monkeypatch, script)
    assert metrics["status"] == "time_limit_reached" and metrics["drain_end_reason"] == "drain_limit"
    assert 300 + 20 <= metrics["elapsed_seconds"] <= 335 + TICK
    assert metrics["elapsed_seconds"] < 300 + 20 + TICK + 15  # setup had no slack to spend
    rig.assert_closed()


def test_server_going_idle_after_the_window_ends_the_drain_once_audio_has_played(monkeypatch):
    async def script(rig):
        await rig.until(301)
        rig.say(0.3)
        rig.inbox.put_nowait(None)
    rig, metrics = play(monkeypatch, script)
    assert metrics["status"] == "call_completed" and metrics["drain_end_reason"] == "server_closed"
    assert metrics["received_audio_seconds"] == metrics["played_audio_seconds"] == 0.3
    rig.assert_closed()


@pytest.mark.parametrize("fault, fault_at", [
    # Device status flags are counted, not fatal, since the long-call change (see audio.py).
    (fault, at) for fault in ("interrupted", "backlog", "server_closed")
    for at in (10, 301) if (fault, at) != ("server_closed", 301)])  # idle server after the window is normal
def test_faults_stop_at_once_and_never_drain(monkeypatch, fault, fault_at):
    async def script(rig):
        await rig.until(fault_at)
        if fault == "interrupted":
            rig.inbox.put_nowait(audio_message(b"", interrupted=True))
        elif fault == "backlog":
            rig.speaker_consumes = False
            rig.say(11)  # more than the ten-second playback queue holds
        elif fault == "device_status":
            rig.input_status = "overflow"
        else:
            rig.inbox.put_nowait(None)
    rig, outcome = play(monkeypatch, script)
    assert isinstance(outcome, Exception)
    metrics = rig.metrics[0]
    assert metrics["status"] == "error"
    assert metrics["elapsed_seconds"] <= fault_at + TICK
    assert metrics["drain_end_reason"] == (None if fault_at < 300 else "fault")
    rig.assert_closed()


def test_stalled_capture_callbacks_are_a_fault_with_direction_and_timing(monkeypatch, capsys):
    async def script(rig):
        await rig.until(50)
        rig.capture_alive = False
    rig, outcome = play(monkeypatch, script)
    assert isinstance(outcome, RuntimeError)
    metrics = rig.metrics[0]
    assert metrics["direction"] == "outgoing" and metrics["audio_fault"] == "capture_callbacks_stalled"
    assert 2 < metrics["capture_stall_seconds"] <= 2 + TICK
    assert metrics["elapsed_seconds"] <= 52 + TICK  # no waiting out the call window
    assert rig.config.project not in capsys.readouterr().out
    rig.assert_closed()


def test_capture_that_never_starts_is_a_fault(monkeypatch):
    rig, outcome = play(monkeypatch, capture_alive=False)
    assert isinstance(outcome, RuntimeError)
    assert 5 < rig.metrics[0]["capture_stall_seconds"] <= 5 + TICK
    rig.assert_closed()


def test_capture_stall_during_the_drain_stops_it(monkeypatch):
    async def script(rig):
        await rig.until(301)
        rig.capture_alive = False
    rig, outcome = play(monkeypatch, script)
    assert isinstance(outcome, RuntimeError)
    assert rig.metrics[0]["drain_end_reason"] == "fault"
    assert rig.metrics[0]["elapsed_seconds"] <= 301 + 2 + TICK
    rig.assert_closed()


@pytest.mark.parametrize("stop_at", [50, 301])
def test_emergency_stop_closes_immediately_without_draining(monkeypatch, stop_at):
    async def script(rig):
        await rig.until(stop_at)
        rig.say(0.5)  # accepted audio is abandoned, not played, on an emergency stop
        rig.translate_task.cancel()
    rig, outcome = play(monkeypatch, script)
    assert isinstance(outcome, asyncio.CancelledError)
    metrics = rig.metrics[0]
    assert metrics["status"] == "interrupted"
    assert metrics["drain_end_reason"] == (None if stop_at < 300 else "interrupted")
    assert metrics["elapsed_seconds"] <= stop_at + TICK
    rig.assert_closed()


def test_partial_setup_closes_whatever_was_opened(monkeypatch):
    rig, outcome = play(monkeypatch, input_stream="fail")
    assert isinstance(outcome, OSError)
    assert rig.ever_opened == 1  # output stream opened, then input failed
    rig.assert_closed()

    rig, outcome = play(monkeypatch, connect="fail", setup_delay=2)
    assert isinstance(outcome, ConnectionError) and rig.ever_opened == 0
    assert rig.metrics[0]["status"] == "error" and rig.metrics[0]["error_type"] == "ConnectionError"
    assert rig.client_closed and not rig.session_closed


def test_setup_that_never_completes_is_a_fault_not_a_longer_call(monkeypatch):
    # asyncio.timeout runs on the loop clock, so this one test spends one real second.
    rig, outcome = play(monkeypatch, connect="hang", call={**CALL, "setup_seconds": 1})
    assert isinstance(outcome, RuntimeError) and str(outcome) == "setup_timeout"
    assert rig.ever_opened == 0 and rig.client_closed


def test_diagnostic_harness_keeps_the_short_window_unless_the_call_window_is_requested():
    from diagnose_live_audio import limited_routes
    routes = {"outgoing": (replace(valid_config(), call=CALL), 0, 1),
              "incoming": (replace(valid_config(), call=CALL), 2, 3)}
    short = limited_routes(routes, language_pair="en-ru", direction="both")
    assert all(item.call is None and item.max_seconds == 60 for item, _, _ in short.values())
    assert short["outgoing"][0].target_language == "ru" and short["incoming"][0].source_language == "ru"
    long = limited_routes(routes, language_pair="en-ru", direction="both", call_window=True)
    assert all(item.call == CALL for item, _, _ in long.values())
    assert long["incoming"][0].target_language == "en"
