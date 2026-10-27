import asyncio
import json
import math
import time
from contextlib import AsyncExitStack, suppress

from google import genai
from google.genai import types

from .audio import AudioBuffers, INPUT_RATE, OUTPUT_RATE, INPUT_FRAMES, OUTPUT_CALLBACK_FRAMES, ACTIVITY_RMS_THRESHOLD

# Output silence that ends a normal drain early. Longer than the ~3.5 s activity-to-playback
# delay seen in early live tests, because a turn-complete event is not guaranteed to arrive.
DRAIN_QUIET_SECONDS = 6.0
SUPERVISOR_TICK_SECONDS = 0.05


def live_config(config, *, diagnostics=False, session_resumption=None):
    # Text requires an explicit diagnostic observer or continuous-call transcript opt-in.
    return types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        session_resumption=session_resumption,
        input_audio_transcription=types.AudioTranscriptionConfig() if diagnostics else None,
        output_audio_transcription=types.AudioTranscriptionConfig() if diagnostics else None,
        translation_config=types.TranslationConfig(
            target_language_code=config.target_language, echo_target_language=False),
    )


async def translate(config, input_index, output_index, sd, *, direction=None, on_metrics=None,
                    on_text=None, clock=time.monotonic, sleep=asyncio.sleep):
    started = clock()
    loop = asyncio.get_running_loop()
    capture_ready = asyncio.Event()
    buffers = AudioBuffers(activity_started=started if on_text is not None else None, clock=clock,
                           on_capture=lambda: loop.call_soon_threadsafe(capture_ready.set))
    metrics = {"status": "starting", "usage_last_counters": {}, "usage_report_count": 0,
               "first_input_to_first_output_seconds": None, "drain_end_reason": None}
    if direction is not None:
        metrics["direction"] = direction
    call = config.call
    # Without [call] this is the short diagnostic window: setup and capture share one hard limit.
    setup_limit = call["setup_seconds"] if call else None
    conversation = call["conversation_seconds"] if call else config.max_seconds
    drain_limit = call["drain_seconds"] if call else 0
    total = (setup_limit + conversation + drain_limit) if call else config.max_seconds
    api_deadline = started + total
    live_at = drain_started = drain_ended = None
    first_input = None
    client = genai.Client(enterprise=True, project=config.project, location=config.location,
                         http_options=types.HttpOptions(api_version="v1beta1"))
    tasks = []
    try:
        # Includes setup, capture, playback and drain: no unbounded paid session.
        async with asyncio.timeout(total):
            async with AsyncExitStack() as stack:
                try:
                    async with asyncio.timeout(setup_limit):
                        session = await stack.enter_async_context(client.aio.live.connect(
                            model=config.model,
                            config=live_config(config, diagnostics=on_text is not None)))
                except TimeoutError:
                    raise RuntimeError("setup_timeout") from None

                async def send():
                    nonlocal first_input
                    while True:
                        if buffers.fault:
                            raise RuntimeError(buffers.fault)
                        chunk = await buffers.next_capture(capture_ready)
                        if chunk is None:
                            return
                        if first_input is None:
                            first_input = clock()
                        await session.send_realtime_input(audio=types.Blob(
                            data=chunk, mime_type="audio/pcm;rate=16000"))
                        buffers.input_bytes += len(chunk)

                async def receive():
                    while True:
                        received = False
                        async for message in session.receive():
                            received = True
                            if message.usage_metadata:
                                # Keep numeric counters only; never retain text/server payloads.
                                usage = message.usage_metadata.model_dump()
                                counters = {k: v for k, v in usage.items()
                                            if isinstance(v, (int, float))}
                                metrics["usage_last_counters"] = counters
                                metrics["usage_report_count"] += 1
                            content = message.server_content
                            if content and on_text is not None:
                                for kind, item in (("source", content.input_transcription),
                                                   ("translation", content.output_transcription)):
                                    if item and item.text:
                                        on_text(kind, item.text, round(clock() - started, 3))
                            if content and content.interrupted:
                                raise RuntimeError("translation_interrupted")
                            if content and content.model_turn:
                                for part in content.model_turn.parts or []:
                                    blob = part.inline_data
                                    if blob and blob.data:
                                        if not (blob.mime_type or "").startswith("audio/pcm"):
                                            raise RuntimeError("unexpected_audio_format")
                                        if (metrics["first_input_to_first_output_seconds"] is None
                                                and first_input is not None):
                                            metrics["first_input_to_first_output_seconds"] = round(
                                                clock() - first_input, 3)
                                        buffers.enqueue_output(blob.data)
                        if not received:
                            # After the window closes the server may legitimately go idle.
                            if not buffers.admitting:
                                return
                            raise RuntimeError("server_stream_closed")

                def check():
                    if buffers.fault:
                        raise RuntimeError(buffers.fault)
                    for task in tasks:
                        if task.done():
                            task.result()
                    stall = buffers.capture_stall()
                    if stall is not None:
                        metrics["capture_stall_seconds"] = round(stall, 3)
                        buffers.fault = "capture_callbacks_stalled"
                        raise RuntimeError(buffers.fault)

                def drain_end_reason(now):
                    if now >= drain_started + drain_limit:
                        return "drain_limit"
                    # Early exit only once everything accepted has been played out.
                    if not tasks[0].done() or not buffers.playback_idle():
                        return None
                    if tasks[1].done():
                        return "server_closed"
                    last_output = max(drain_started, buffers.last_output_at or 0)
                    return "quiet" if now - last_output >= DRAIN_QUIET_SECONDS else None

                with sd.RawOutputStream(device=output_index, samplerate=OUTPUT_RATE,
                                        channels=1, dtype="int16", blocksize=OUTPUT_CALLBACK_FRAMES,
                                        latency=config.audio_latency,
                                        callback=buffers.output_callback) as output_stream, sd.RawInputStream(
                        device=input_index, samplerate=INPUT_RATE, channels=1, dtype="int16",
                        blocksize=INPUT_FRAMES, latency=config.audio_latency,
                        callback=buffers.input_callback) as input_stream:
                    # These are device/driver estimates, not measured speech-to-translation delay.
                    for side, audio_stream in (("input", input_stream), ("output", output_stream)):
                        estimate = getattr(audio_stream, "latency", None)
                        metrics[f"{side}_device_latency_seconds"] = (
                            float(estimate) if isinstance(estimate, (int, float))
                            and math.isfinite(estimate) and estimate >= 0 else None)
                    metrics["audio_latency_profile"] = config.audio_latency
                    metrics["output_callback_blocksize"] = OUTPUT_CALLBACK_FRAMES
                    buffers.watch_capture()
                    live_at = clock()
                    conversation_end = min(live_at + conversation, api_deadline) if call else api_deadline
                    window = f"{conversation}s + up to {drain_limit}s drain; " if call else ""
                    print(f"LIVE {direction or 'single'}: speak now; {window}Ctrl-C stops at once. No recording.", flush=True)
                    tasks = [asyncio.create_task(send()), asyncio.create_task(receive())]
                    while True:
                        check()
                        now = clock()
                        if now >= api_deadline:
                            metrics["status"] = "time_limit_reached"
                            if drain_started is not None:
                                metrics["drain_end_reason"] = "api_deadline"
                            break
                        if drain_started is None and now >= conversation_end:
                            if not call:
                                metrics["status"] = "time_limit_reached"
                                break
                            # Normal finish: stop admitting speech, let accepted audio play out.
                            drain_started = now
                            buffers.admitting = False
                            capture_ready.set()  # release an idle sender so normal drain can finish
                            print(f"CALL_WINDOW_ENDED {direction or 'single'}: draining.", flush=True)
                        if drain_started is not None:
                            reason = drain_end_reason(now)
                            if reason:
                                metrics["drain_end_reason"] = reason
                                metrics["status"] = ("call_completed" if reason in ("quiet", "server_closed")
                                                     else "time_limit_reached")
                                break
                        await sleep(SUPERVISOR_TICK_SECONDS)
                    drain_ended = clock()
    except TimeoutError:
        metrics["status"] = "time_limit_reached"
        if drain_started is not None:
            metrics["drain_end_reason"] = "api_deadline"
    except asyncio.CancelledError:
        # Emergency stop: never drain.
        metrics["status"] = "interrupted"
        if drain_started is not None:
            metrics["drain_end_reason"] = "interrupted"
        raise
    except Exception as exc:
        metrics["status"] = "error"
        if drain_started is not None:
            metrics["drain_end_reason"] = "fault"
        # API exceptions can contain request data. Do not print their bodies.
        metrics["error_type"] = type(exc).__name__
        raise
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        with suppress(Exception):
            async with asyncio.timeout(5):
                await client.aio.aclose()
        now = clock()
        metrics.update(elapsed_seconds=round(now - started, 3),
                       input_audio_seconds=round(buffers.input_bytes / (INPUT_RATE * 2), 3),
                       received_audio_seconds=round(buffers.output_bytes / (OUTPUT_RATE * 2), 3),
                       played_audio_seconds=round(buffers.played_bytes / (OUTPUT_RATE * 2), 3),
                       audio_fault=buffers.fault,
                       input_clipped_samples=buffers.input_clipped_samples,
                       output_clipped_samples=buffers.output_clipped_samples,
                       received_pcm_zero_fraction=round(buffers.output_zero_samples / buffers.output_samples, 3) if buffers.output_samples else None,
                       audio_durations_include_silence=True,
                       max_playback_backlog_seconds=buffers.max_playback_chunks / 10,
                       setup_seconds_used=round(live_at - started, 3) if live_at is not None else None,
                       drain_seconds_used=(round((drain_ended or now) - drain_started, 3)
                                           if drain_started is not None else None))
        if on_text is not None:
            metrics["pcm_activity_rms_threshold"] = ACTIVITY_RMS_THRESHOLD
            metrics["pcm_activity"] = buffers.activity
        print(json.dumps(metrics, sort_keys=True), flush=True)
        if on_metrics is not None:
            on_metrics(metrics)
    return metrics
