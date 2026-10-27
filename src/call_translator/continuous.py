"""Continuous paired device streams with bounded session renewal/recovery and optional private text."""
import asyncio
from contextlib import ExitStack
import json
import math
import queue
import subprocess
import time

from google import genai
from google.genai import types
from google.genai.errors import APIError
from google.oauth2.credentials import Credentials

from .audio import AudioBuffers, INPUT_RATE, OUTPUT_RATE, INPUT_FRAMES, OUTPUT_CALLBACK_FRAMES
from .call_budget import CallBudget, estimate_nodes
from .stream import live_config

RENEW_AGE = 480
PREOPEN_LEAD = 15
BOUNDARY_WAIT = 20
SOURCE_QUIET = 0.8
TRANSITION_LIMIT = 8
SEND_TIMEOUT = 2
# Per call, not per window: 3 stopped a 60-minute call after a few transient drops.
MAX_RECOVERIES = 50
# Owner-accepted recovery loss (2026-10-25): capture older than this after a recovery is
# dropped, because real-time pacing would otherwise keep the backlog as permanent delay.
RECOVERY_STALE = 0.5
HANDLE_TERMS = ('session_resumption', 'sessionresumption', 'resumption', 'handle',
                'transparent', 'invalid_argument', 'not_found')


def emit(event, **values):
    print(json.dumps({'event': event, **values}), flush=True)


def parse_time_left(value):
    if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith('s'):
            text = text[:-1]
        try:
            parsed = float(text)
            if math.isfinite(parsed) and parsed >= 0:
                return parsed
        except ValueError:
            return None
    return None


def is_handle_rejection(exc):
    text = f'{type(exc).__name__} {exc}'.lower()
    if isinstance(exc, APIError) and exc.code in (400, 404, 1003, 1007, 1008):
        return True
    return any(term in text for term in HANDLE_TERMS)


def active(pcm):
    samples = memoryview(pcm).cast('h')
    return bool(samples) and sum(v*v for v in samples) / len(samples) >= 200**2


def only_zero_tail(buffers):
    with buffers.playback.mutex:
        return not any(buffers.pending) and all(not any(x) for x in buffers.playback.queue)


def discard_zeros(buffers):
    # Caller holds the output selection lock, so the device cannot mutate pending here.
    if not only_zero_tail(buffers):
        raise RuntimeError('nonzero_output_pending')
    buffers.pending = b''
    with buffers.playback.mutex:
        buffers.playback.queue.clear()


def recoverable(exc):
    if isinstance(exc, APIError):
        return exc.code in (1000, 1001, 1011, 429, 500, 502, 503, 504)
    return (type(exc).__name__ in ('ConnectionClosed', 'ConnectionClosedError', 'ConnectionClosedOK')
            or isinstance(exc, (ConnectionError, OSError, TimeoutError))
            # A slow/failed token refresh for a new connection is retried within the
            # bounded recovery count; a persistent auth failure still stops explicitly.
            or isinstance(exc, (subprocess.TimeoutExpired, subprocess.CalledProcessError))
            or isinstance(exc, RuntimeError) and str(exc) == 'unexpected_server_close')


async def existing_credentials(source):
    if source == 'adc':
        # Let the SDK use the existing ADC. There is no fallback to another identity.
        return None
    result = await asyncio.to_thread(subprocess.run,
        ['gcloud', 'auth', 'print-access-token'], capture_output=True, text=True,
        check=True, timeout=5)
    token = result.stdout.strip()
    if not token:
        raise RuntimeError('existing_cli_token_unavailable')
    return Credentials(token)


class SessionNode:
    def __init__(self, config, clock, credentials, *, resumption_handle=None,
                 on_resumption=None, on_go_away=None):
        self.config, self.clock, self.credentials = config, clock, credentials
        self.resumption_handle = resumption_handle
        self.resumed = bool(resumption_handle)
        self.on_resumption = on_resumption
        self.on_go_away = on_go_away
        self.on_text = None
        self.buffers = AudioBuffers(clock=clock)
        self.client = self.context = self.receiver = self.session = None
        self.opened_at = clock()
        self.closed_at = math.inf
        self.last_activity = self.opened_at
        self.input_bytes = self.received_bytes = 0
        self.usage_last = {}
        self.usage_reports = 0
        self.resumption_updates = 0
        self.go_away_count = 0
        self.interruptions = 0
        self.go_away_requested = False
        self.go_away_time_left = None
        self.ending = False
        self.closed = False

    async def open(self):
        self.client = genai.Client(enterprise=True, project=self.config.project,
            location=self.config.location, credentials=self.credentials,
            http_options=types.HttpOptions(api_version='v1beta1'))
        resumption = types.SessionResumptionConfig(
            handle=self.resumption_handle, transparent=True)
        self.context = self.client.aio.live.connect(
            model=self.config.model,
            config=live_config(self.config, diagnostics=self.on_text is not None, session_resumption=resumption))
        self.session = await self.context.__aenter__()
        self.receiver = asyncio.create_task(self.receive())
        return self

    async def receive(self):
        while True:
            received = False
            async for message in self.session.receive():
                received = True
                if message.usage_metadata:
                    self.usage_last = {k: v for k, v in message.usage_metadata.model_dump().items()
                                       if type(v) in (int, float) and math.isfinite(v)}
                    self.usage_reports += 1
                update = getattr(message, 'session_resumption_update', None)
                if update is not None:
                    self.resumption_updates += 1
                    if self.on_resumption is not None:
                        self.on_resumption(self, update)
                go_away = getattr(message, 'go_away', None)
                if go_away is not None:
                    self.go_away_count += 1
                    self.go_away_requested = True
                    self.go_away_time_left = parse_time_left(getattr(go_away, 'time_left', None))
                    if self.on_go_away is not None:
                        self.on_go_away(self, self.go_away_time_left)
                content = message.server_content
                if content and self.on_text is not None:
                    for kind, item in (('original', content.input_transcription),
                                       ('translation', content.output_transcription)):
                        if item is not None and (item.text or item.finished):
                            self.on_text(kind, item.text or '', bool(item.finished))
                if content and content.interrupted:
                    # Overlapping remote speakers can trigger it; stopping the call is worse
                    # than letting the model carry on with the next phrase.
                    self.interruptions += 1
                if content and content.model_turn:
                    for part in content.model_turn.parts or []:
                        blob = part.inline_data
                        if blob and blob.data:
                            if not (blob.mime_type or '').startswith('audio/pcm'):
                                raise RuntimeError('unexpected_audio_format')
                            self.received_bytes += len(blob.data)
                            if active(blob.data):
                                self.last_activity = self.clock()
                            self.buffers.enqueue_output(blob.data)
            if not received:
                if self.ending:
                    return
                raise RuntimeError('unexpected_server_close')

    async def end_input(self):
        self.ending = True
        await self.session.send_realtime_input(audio_stream_end=True)

    async def close(self):
        if self.closed:
            return
        self.closed = True
        errors = []
        if self.receiver:
            self.receiver.cancel()
            await asyncio.gather(self.receiver, return_exceptions=True)
        try:
            if self.session is not None:
                async with asyncio.timeout(3):
                    await self.context.__aexit__(None, None, None)
        except Exception as exc:
            errors.append(exc)
        finally:
            try:
                if self.client is not None:
                    async with asyncio.timeout(3):
                        await self.client.aio.aclose()
            except Exception as exc:
                errors.append(exc)
            self.closed_at = self.clock()
            discard_buffers(self.buffers)
        if errors:
            raise RuntimeError('session_cleanup_failed') from None


def discard_buffers(buffers):
    buffers.admitting = False
    buffers.pending = b''
    for q in (buffers.capture, buffers.playback):
        with q.mutex:
            q.queue.clear()


class ContinuousRoute:
    def __init__(self, config, name, *, clock=time.monotonic, sleep=asyncio.sleep,
                 node_factory=SessionNode, credentials=existing_credentials, notify=emit):
        import threading
        self.config, self.name, self.clock, self.sleep = config, name, clock, sleep
        self.node_factory, self.credentials, self.notify = node_factory, credentials, notify
        self.nodes, self.tasks, self.closing = [], [], []
        self.current = self.output_node = self.prepared = None
        self.send_lock = asyncio.Lock()
        self.transition_lock = asyncio.Lock()
        self.output_lock = threading.Lock()
        self.ready = asyncio.Event()
        loop = asyncio.get_running_loop()
        self.capture = AudioBuffers(clock=clock,
            on_capture=lambda: loop.call_soon_threadsafe(self.ready.set))
        self.capture.capture = queue.Queue(maxsize=120)  # 12 s, bounded technical transition only
        self.last_source_activity = clock()
        self.catchup = False
        self.drop_stale = False
        self.dropped_stale = 0
        self.dropped_stale_seconds = 0.
        self.capture_times = queue.Queue(maxsize=120)
        self.recoveries = self.transitions = self.skipped_zero = self.uncertain_sends = 0
        self.resumed_sessions = self.fresh_fallbacks = self.go_away_renewals = 0
        self.resumption_handle = None
        self.resumable = False
        self.last_consumed_index = None
        self.state = 'starting'
        self.transcript = None
        self.text_generation = 0

    def event(self, event, **values):
        if self.transcript is not None and event in ('provider_recovery', 'renewal_without_quiet_boundary'):
            self.transcript.gap(self.name)
        self.notify(event, direction=self.name, **values)

    def set_state(self, state, **values):
        self.state = state
        self.event('direction_status', state=state, **values)

    def _on_resumption(self, node, update):
        # Only the session receiving live input defines this direction's resumable state;
        # an idle pre-opened session must not replace or clear that handle.
        if node is not self.current:
            return
        resumable = bool(getattr(update, 'resumable', False))
        new_handle = getattr(update, 'new_handle', None)
        idx = getattr(update, 'last_consumed_client_message_index', None)
        if isinstance(idx, int):
            self.last_consumed_index = idx
        self.resumable = resumable
        if resumable and new_handle:
            self.resumption_handle = new_handle
        elif not resumable:
            self.resumption_handle = None

    def _on_go_away(self, node, time_left):
        if node is not self.current and node is not self.prepared:
            return
        self.event('server_go_away', time_left_seconds=time_left,
                   resumable=bool(self.resumable and self.resumption_handle))

    def _create_node(self, credential, handle):
        node = self._make_node(credential, handle)
        if self.transcript is not None:
            self.text_generation += 1
            generation = self.text_generation
            node.on_text = lambda kind, text, finished: self.transcript.submit(
                self.name, generation, kind, text, finished)
        return node

    def _make_node(self, credential, handle):
        try:
            return self.node_factory(
                self.config, self.clock, credential, resumption_handle=handle,
                on_resumption=self._on_resumption, on_go_away=self._on_go_away)
        except TypeError:
            node = self.node_factory(self.config, self.clock, credential)
            node.resumption_handle = handle
            node.resumed = bool(handle)
            node.on_resumption = self._on_resumption
            node.on_go_away = self._on_go_away
            if not hasattr(node, 'resumption_updates'):
                node.resumption_updates = 0
            if not hasattr(node, 'go_away_count'):
                node.go_away_count = 0
            if not hasattr(node, 'go_away_requested'):
                node.go_away_requested = False
            if not hasattr(node, 'go_away_time_left'):
                node.go_away_time_left = None
            return node

    async def new_node(self, *, allow_resumption=True):
        credential = await self.credentials(self.config.continuous['auth_source'])
        handle = self.resumption_handle if (allow_resumption and self.resumable and self.resumption_handle) else None
        node = self._create_node(credential, handle)
        self.nodes.append(node)  # includes partial setup, so cancellation still cleans it
        try:
            await node.open()
            if handle:
                self.resumed_sessions += 1
            return node
        except Exception as exc:
            if handle and (is_handle_rejection(exc) or recoverable(exc)):
                self.resumption_handle = None
                self.resumable = False
                self.fresh_fallbacks += 1
                self.event('resumption_handle_rejected', fallback='fresh_session',
                           error_type=type(exc).__name__)
                await self.retire(node)
                fresh = self._create_node(credential, None)
                self.nodes.append(fresh)
                await fresh.open()
                return fresh
            raise

    def input_callback(self, data, frames, timing, status):
        if not self.capture.admitting:
            return
        # Both queues are written by this one PortAudio callback and read in FIFO order.
        try:
            self.capture_times.put_nowait(self.clock())
        except queue.Full:
            self.capture.fault = 'capture_queue_overflow'
            return
        if active(data):
            self.last_source_activity = self.clock()
        self.capture.input_callback(data, frames, timing, status)

    def output_callback(self, out, frames, timing, status):
        with self.output_lock:
            (self.output_node or self.current).buffers.output_callback(out, frames, timing, status)

    async def send(self):
        next_at = self.clock()
        while True:
            chunk = await self.capture.next_capture(self.ready)
            if chunk is None:
                return
            captured_at = self.capture_times.get_nowait()
            await self.sleep(max(0, next_at - self.clock()))
            error = None
            async with self.send_lock:
                age = self.clock() - captured_at
                if self.drop_stale:
                    if age > RECOVERY_STALE:
                        self.dropped_stale += 1
                        self.dropped_stale_seconds += len(chunk)/32000
                        continue
                    self.drop_stale = False
                    if self.dropped_stale_seconds:
                        self.event('recovery_backlog_dropped',
                                   seconds=round(self.dropped_stale_seconds, 1),
                                   last_phrase_may_need_repeat=True)
                        self.dropped_stale_seconds = 0.
                if self.catchup and age > 0.15 and not any(chunk):
                    self.skipped_zero += 1
                    continue
                if age <= 0.15:
                    self.catchup = False
                failed = self.current
                try:
                    async with asyncio.timeout(SEND_TIMEOUT):
                        await failed.session.send_realtime_input(audio=types.Blob(
                            data=chunk, mime_type='audio/pcm;rate=16000'))
                    failed.input_bytes += len(chunk)
                except Exception as exc:
                    error = exc
                    self.uncertain_sends += 1
                    # Even an uncertain send may have been billable; count it conservatively.
                    failed.input_bytes += len(chunk)
            if error is not None:
                await self.recover(failed, error)
            next_at = max(next_at + len(chunk)/32000, self.clock())

    def select(self, replacement):
        with self.output_lock:
            discard_zeros((self.output_node or self.current).buffers)
            self.current = self.output_node = replacement
        self.prepared = None
        self.catchup = True

    async def retire(self, old):
        self.closing.append(asyncio.create_task(old.close()))

    async def _take_replacement(self):
        if self.prepared is not None:
            prepared = self.prepared
            self.prepared = None
            want_handle = self.resumption_handle if (self.resumable and self.resumption_handle) else None
            if getattr(prepared, 'resumption_handle', None) == want_handle and not prepared.receiver.done():
                return prepared
            await self.retire(prepared)
        return await self.new_node()

    async def planned(self, *, reason='age'):
        old = self.current
        began_wait = self.clock()
        time_left = getattr(old, 'go_away_time_left', None)
        wait_limit = BOUNDARY_WAIT if time_left is None else min(BOUNDARY_WAIT, max(0.0, time_left - TRANSITION_LIMIT - 1.0))
        # Do not hold the recovery lock while waiting for a natural speech boundary.
        while self.clock() - self.last_source_activity < SOURCE_QUIET:
            if self.current is not old:
                return
            if old.receiver.done():
                old.receiver.result()
                raise RuntimeError('unexpected_server_close')
            if self.clock() - began_wait >= wait_limit:
                # Continuous speech or a noise floor above the activity marker must not end
                # the whole call. Hand over mid-phrase within the transition limit instead.
                self.event('renewal_without_quiet_boundary', last_phrase_may_need_repeat=True,
                           reason=reason)
                break
            await self.sleep(0.05)
        async with self.transition_lock:
            if self.current is not old or (
                    self.clock() - old.opened_at < RENEW_AGE and not getattr(old, 'go_away_requested', False)):
                return
            self.set_state('renewing', reason=reason,
                           resumable=bool(self.resumable and self.resumption_handle))
            replacement = await self._take_replacement()
            try:
                async with asyncio.timeout(TRANSITION_LIMIT):
                    if replacement.receiver.done():
                        replacement.receiver.result()
                        raise RuntimeError('unexpected_server_close')
                    async with self.send_lock:
                        began = self.clock()
                        # Continue sending captured PCM while old output drains. Pausing the
                        # sender made real non-zero microphone noise accumulate every renewal.
                        self.output_node = old
                        self.current = replacement
                    await old.end_input()
                    while (self.clock() - began < 2 or self.clock() - old.last_activity < 2
                           or not only_zero_tail(old.buffers)):
                        if old.receiver.done():
                            old.receiver.result()
                        if replacement.receiver.done():
                            replacement.receiver.result()
                            raise RuntimeError('unexpected_server_close')
                        await self.sleep(0.05)
                    self.select(replacement)
                    self.transitions += 1
                    if reason == 'go_away':
                        self.go_away_renewals += 1
                    mode = 'resumed' if getattr(replacement, 'resumed', False) else 'fresh'
                    duration = round(self.clock() - began, 3)
                    self.event('session_renewed', output_transition_seconds=duration,
                               mode=mode, reason=reason)
                    self.set_state('ready', mode=mode, reason=reason,
                                   transition_seconds=duration)
            except Exception:
                self.set_state('stopped', reason='renewal_transition_failed')
                # A partially completed handover is not an ordinary current-session fault.
                # Do not silently suppress it by retrying the no-longer-current old input.
                raise RuntimeError('renewal_transition_failed') from None
            await self.retire(old)

    async def recover(self, failed, error):
        if not recoverable(error):
            self.set_state('stopped', reason=type(error).__name__)
            raise error
        began = self.clock()
        async with asyncio.timeout(TRANSITION_LIMIT):
            async with self.transition_lock:
                if failed is not self.current:
                    return
                if self.recoveries >= MAX_RECOVERIES:
                    self.set_state('stopped', reason='bounded_recovery_count_exhausted')
                    raise RuntimeError('bounded_recovery_count_exhausted')
                self.recoveries += 1
                self.set_state('recovering', error_type=type(error).__name__,
                               resumable=bool(self.resumable and self.resumption_handle),
                               last_phrase_may_need_repeat=True)
                self.event('provider_recovery', last_phrase_may_need_repeat=True,
                           error_type=type(error).__name__,
                           resumable=bool(self.resumable and self.resumption_handle))
                async with self.send_lock:
                    replacement = await self._take_replacement()
                    if replacement.receiver.done():
                        replacement.receiver.result()
                        raise RuntimeError('unexpected_server_close')
                    while not only_zero_tail(failed.buffers):
                        await self.sleep(0.05)
                    self.select(replacement)
                    self.drop_stale = True
                duration = round(self.clock() - began, 3)
                mode = 'resumed' if getattr(replacement, 'resumed', False) else 'fresh'
                self.event('provider_recovered', seconds=duration, mode=mode,
                           last_phrase_may_need_repeat=True)
                self.set_state('ready', mode=mode, recovery_seconds=duration,
                               last_phrase_may_need_repeat=True)
                await self.retire(failed)

    async def monitor(self):
        while True:
            self.check_audio()
            node = self.current
            if node.receiver.done():
                try:
                    node.receiver.result()
                except Exception as exc:
                    await self.recover(node, exc)
                else:
                    await self.recover(node, RuntimeError('unexpected_server_close'))
                continue
            try:
                age = self.clock() - node.opened_at
                go_away = getattr(node, 'go_away_requested', False)
                if self.prepared is None and (age >= RENEW_AGE - PREOPEN_LEAD or go_away):
                    async with self.transition_lock:
                        if (self.prepared is None and self.current is node
                                and (self.clock() - node.opened_at >= RENEW_AGE - PREOPEN_LEAD
                                     or getattr(node, 'go_away_requested', False))):
                            async with asyncio.timeout(8):
                                self.prepared = await self.new_node()
                if age >= RENEW_AGE or go_away:
                    await self.planned(reason='go_away' if go_away else 'age')
            except Exception as exc:
                if not recoverable(exc):
                    self.set_state('stopped', reason=str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__)
                    raise
                await self.recover(node, exc)
            await self.sleep(0.05)

    def check_audio(self):
        if self.capture.fault:
            self.set_state('stopped', reason=self.capture.fault)
            raise RuntimeError(self.capture.fault)
        if self.capture.capture_stall() is not None:
            self.set_state('stopped', reason='capture_callbacks_stalled')
            raise RuntimeError('capture_callbacks_stalled')
        for node in (self.current, self.output_node):
            if node and node.buffers.fault:
                self.set_state('stopped', reason=node.buffers.fault)
                raise RuntimeError(node.buffers.fault)
        for task in self.closing:
            if task.done():
                task.result()

    async def close(self):
        self.capture.admitting = False
        self.ready.set()
        self.resumption_handle = None
        self.resumable = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        retired = await asyncio.gather(*self.closing, return_exceptions=True)
        results = await asyncio.gather(*(n.close() for n in self.nodes), return_exceptions=True)
        discard_buffers(self.capture)
        with self.capture_times.mutex:
            self.capture_times.queue.clear()
        if any(isinstance(x, BaseException) for x in retired + results):
            raise RuntimeError('session_cleanup_failed')


async def run_call(routes, sd, observer, *, route_factory=ContinuousRoute, clock=time.monotonic,
                   sleep=asyncio.sleep, notify=emit, save_transcript=False):
    config = next(iter(routes.values()))[0]
    if set(routes) != {'outgoing', 'incoming'} or not config.continuous:
        raise ValueError('Continuous calls require both routes and [continuous]')
    # Confirm the selected, joined Meet before any credentials, model or audio stream.
    await observer.require_joined()
    workers = [route_factory(cfg, name, clock=clock, sleep=sleep, notify=notify)
               for name, (cfg, _, _) in routes.items()]
    budget = CallBudget(config, notify)
    started = clock()
    tasks = []
    outcome = 'technical_failure'
    transcript = None
    if save_transcript:
        from .transcript import Transcript
        try:
            transcript = Transcript({name: (cfg.source_language, cfg.target_language)
                                     for name, (cfg, _, _) in routes.items()}, notify)
        except Exception:
            notify('transcript_warning', reason='saving_unavailable', voice_continues=True)
        for worker in workers:
            worker.transcript = transcript
    try:
        with ExitStack() as stack:
            # No microphone is open during provider setup, and partial setup is bounded.
            async with asyncio.timeout(15):
                async with asyncio.TaskGroup() as group:
                    for worker in workers:
                        group.create_task(setup_route(worker))
            await observer.require_joined()
            for worker, (_, source, target) in zip(workers, routes.values()):
                stack.enter_context(sd.RawOutputStream(device=target, samplerate=OUTPUT_RATE,
                    channels=1, dtype='int16', blocksize=OUTPUT_CALLBACK_FRAMES,
                    latency=worker.config.audio_latency, callback=worker.output_callback))
                stack.enter_context(sd.RawInputStream(device=source, samplerate=INPUT_RATE,
                    channels=1, dtype='int16', blocksize=INPUT_FRAMES,
                    latency=worker.config.audio_latency, callback=worker.input_callback))
                worker.capture.watch_capture()
                worker.tasks = [asyncio.create_task(worker.send()), asyncio.create_task(worker.monitor())]
                tasks.extend(worker.tasks)
            lifecycle = asyncio.create_task(observer.wait_departure())
            tasks.append(lifecycle)
            notify('LIVE_BOTH', normal_end='selected_meet_departure', emergency_stop='Ctrl-C',
                   directions={w.name: w.state for w in workers})
            next_report = 0
            planned_warning = False
            while True:
                if lifecycle.done():
                    reason = lifecycle.result()
                    outcome = reason
                    # No post-departure drain: the final translation must be heard first.
                    break
                for task in tasks[:-1]:
                    if task.done():
                        task.result()
                        raise RuntimeError('audio_worker_stopped')
                for worker in workers:
                    worker.check_audio()
                estimate = budget.check([n for w in workers for n in w.nodes], clock())
                elapsed = clock() - started
                if elapsed >= config.continuous['planned_minutes'] * 60 and not planned_warning:
                    planned_warning = True
                    notify('planned_duration_exceeded', call_continues=True)
                if elapsed >= next_report:
                    notify('call_usage', elapsed_seconds=round(elapsed, 1),
                           conservative_estimate_usd=round(estimate, 6),
                           routes=[{'direction': w.name, 'state': w.state,
                                    'recoveries': w.recoveries,
                                    'renewals': w.transitions,
                                    'resumed_sessions': w.resumed_sessions,
                                    'fresh_fallbacks': w.fresh_fallbacks,
                                    'go_away_renewals': w.go_away_renewals,
                                    'resumable': bool(w.resumable and w.resumption_handle),
                                    'skipped_exact_zero_chunks': w.skipped_zero,
                                    'dropped_recovery_backlog_chunks': w.dropped_stale,
                                    'uncertain_send_chunks': w.uncertain_sends,
                                    'usage_last_counters': w.current.usage_last,
                                    'usage_report_count': sum(n.usage_reports for n in w.nodes)} for w in workers])
                    next_report = elapsed + 30
                await sleep(0.05)
    except asyncio.CancelledError:
        outcome = 'emergency_stop'
        raise
    finally:
        # Device context exits precede awaited network cleanup. Stop capture/playback at once.
        for worker in workers:
            worker.capture.admitting = False
            if worker.state != 'stopped':
                worker.set_state('stopped', reason=outcome)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        results = await asyncio.gather(*(worker.close() for worker in workers), return_exceptions=True)
        if transcript is not None:
            await transcript.close(outcome)
        notify('call_closed', reason=outcome, settings_changed=False,
               conservative_estimate_usd=round(estimate_nodes([n for w in workers for n in w.nodes], clock()), 6),
               routes=[{'direction': w.name,
                        'state': w.state,
                        'recoveries': w.recoveries,
                        'renewals': w.transitions,
                        'resumed_sessions': w.resumed_sessions,
                        'fresh_fallbacks': w.fresh_fallbacks,
                        'go_away_renewals': w.go_away_renewals,
                        'sent_seconds': round(sum(n.input_bytes for n in w.nodes)/32000, 3),
                        'received_seconds': round(sum(n.received_bytes for n in w.nodes)/48000, 3),
                        'played_seconds': round(sum(n.buffers.played_bytes for n in w.nodes)/48000, 3),
                        'dropped_recovery_backlog_chunks': w.dropped_stale,
                        'uncertain_send_chunks': w.uncertain_sends} for w in workers],
               cleanup_error_types=[type(x).__name__ for x in results if isinstance(x, BaseException)])
        if any(isinstance(x, BaseException) for x in results):
            raise RuntimeError('session_cleanup_failed')
    return outcome


async def setup_route(worker):
    worker.set_state('starting')
    worker.current = worker.output_node = await worker.new_node()
    worker.set_state('ready', mode='resumed' if getattr(worker.current, 'resumed', False) else 'fresh')
