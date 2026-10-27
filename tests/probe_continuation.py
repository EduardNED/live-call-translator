"""Explicitly approved, bounded synthetic-audio Cloud continuation probe.

No sounddevice, microphone, Meet, playback, audio files or transcripts. Resumption
handles remain in memory. This observes transport behavior, not semantic continuity.
"""
import argparse
import asyncio
from contextlib import suppress
from dataclasses import replace
from datetime import date
import json
import math
from pathlib import Path
import subprocess
import time
import tomllib

from google import genai
from google.genai import types
from google.oauth2.credentials import Credentials
from call_translator.config import Config

MAX_SECONDS = 900
MAX_ESTIMATE_USD = 2.0
RATE_INPUT = 3.5 * 25 / 1_000_000
RATE_OUTPUT = 21 * 25 / 1_000_000
FIXTURES = {
    'outgoing': ('en-GB', 'fr', 'The blue document is ready. We need two tickets for Friday.'),
    'incoming': ('fr-FR', 'en', 'Le document bleu est prêt. Nous avons besoin de deux billets pour vendredi.'),
}


def check_packet(seconds, balance, verified_on, expires_on):
    if type(seconds) is not int or not 1 <= seconds <= MAX_SECONDS:
        raise ValueError('Probe must fit the single approved 15-minute packet')
    if (not math.isfinite(balance) or balance < MAX_ESTIMATE_USD
            or date.fromisoformat(verified_on) != date.today()
            or date.fromisoformat(expires_on) <= date.today()):
        raise ValueError('Fresh eligible credit evidence required')
    estimate = 2 * seconds * (RATE_INPUT + RATE_OUTPUT)
    if estimate * 1.2 > MAX_ESTIMATE_USD:
        raise ValueError('Planned probe plus reserve exceeds approval')
    return estimate


def safe_error(exc):
    # The API body may contain payloads/tokens. Never print it.
    code = getattr(exc, 'code', None)
    return {'error_type': type(exc).__name__,
            'error_code': code if isinstance(code, (int, float)) else None,
            'mentions_resumption': 'resum' in str(exc).lower(),
            'mentions_transparent': 'transparent' in str(exc).lower()}


async def route(config, name, pcm, duration, totals, credentials, *, force_at=60):
    voice, target, _ = FIXTURES[name]
    del voice
    client = genai.Client(enterprise=True, project=config.project, location=config.location,
                         credentials=credentials,
                         http_options=types.HttpOptions(api_version='v1beta1'))
    started = time.monotonic()
    result = dict(direction=name, connections=0, successful_resumes=0,
                  resumption_updates=0, resumable_updates=0, indexed_updates=0,
                  go_away_count=0, input_chunks=0, input_audio_seconds=0.0,
                  output_audio_seconds=0.0, output_chunks=0, usage_reports=0,
                  status='starting', input_replay_implemented=False,
                  maximum_send_gap_seconds=0.0, maximum_output_gap_seconds=0.0,
                  transitions=[], numeric_usage_last={})
    handle = None
    resumable = False
    source_offset = 0
    last_send = last_output = None
    force_done = False
    # A bounded fixture followed by 6 s of actual silent PCM; keep streaming silence.
    source = pcm + bytes(6 * 32000)
    tasks = []
    try:
        async with asyncio.timeout(duration):
            while True:
                if result['connections'] >= 3:
                    raise RuntimeError('bounded_transition_count_reached')
                transition_at = time.monotonic()
                resuming = handle is not None
                cfg = types.LiveConnectConfig(
                    response_modalities=['AUDIO'],
                    translation_config=types.TranslationConfig(
                        target_language_code=target, echo_target_language=False),
                    session_resumption=types.SessionResumptionConfig(
                        handle=handle, transparent=True))
                async with asyncio.timeout(20):
                    context = client.aio.live.connect(model=config.model, config=cfg)
                    session = await context.__aenter__()
                try:
                    result['connections'] += 1
                    if resuming:
                        result['successful_resumes'] += 1
                    opened_at = time.monotonic()
                    result['transitions'].append(dict(
                        elapsed_seconds=round(opened_at-started, 3), resumed=resuming,
                        setup_seconds=round(opened_at-transition_at, 3)))
                    print(json.dumps({'event':'connected','direction':name,
                                      'elapsed':round(opened_at-started,3),
                                      'resumed':resuming}), flush=True)
                    renew = asyncio.Event()

                    async def send():
                        nonlocal source_offset, last_send
                        next_at = time.monotonic()
                        while True:
                            chunk = source[source_offset:source_offset+3200]
                            source_offset += len(chunk)
                            if source_offset == len(source):
                                source_offset = 0
                            now = time.monotonic()
                            if last_send is not None:
                                result['maximum_send_gap_seconds'] = max(
                                    result['maximum_send_gap_seconds'], now-last_send)
                            await session.send_realtime_input(audio=types.Blob(
                                data=chunk, mime_type='audio/pcm;rate=16000'))
                            last_send = now
                            result['input_chunks'] += 1
                            seconds = len(chunk)/32000
                            result['input_audio_seconds'] += seconds
                            totals['input'] += seconds
                            next_at += seconds
                            await asyncio.sleep(max(0, next_at-time.monotonic()))

                    async def receive():
                        nonlocal handle, resumable, last_output
                        while True:
                            any_message = False
                            async for msg in session.receive():
                                any_message = True
                                if msg.session_resumption_update is not None:
                                    update = msg.session_resumption_update
                                    result['resumption_updates'] += 1
                                    resumable = bool(update.resumable)
                                    if resumable and update.new_handle:
                                        handle = update.new_handle
                                        result['resumable_updates'] += 1
                                    if update.last_consumed_client_message_index is not None:
                                        result['indexed_updates'] += 1
                                if msg.go_away is not None:
                                    result['go_away_count'] += 1
                                    renew.set()
                                if msg.usage_metadata is not None:
                                    result['usage_reports'] += 1
                                    result['numeric_usage_last'] = {
                                        k:v for k,v in msg.usage_metadata.model_dump().items()
                                        if type(v) in (int,float)}
                                content = msg.server_content
                                if content is None or content.model_turn is None:
                                    continue
                                for part in content.model_turn.parts or []:
                                    blob = part.inline_data
                                    if blob is None or not blob.data:
                                        continue
                                    if not (blob.mime_type or '').startswith('audio/pcm'):
                                        raise RuntimeError('unexpected_audio_format')
                                    now = time.monotonic()
                                    if last_output is not None:
                                        result['maximum_output_gap_seconds'] = max(
                                            result['maximum_output_gap_seconds'], now-last_output)
                                    last_output = now
                                    seconds = len(blob.data)/48000
                                    result['output_chunks'] += 1
                                    result['output_audio_seconds'] += seconds
                                    totals['output'] += seconds
                                    # Only duration counters survive; audio discarded immediately.
                            if not any_message:
                                raise RuntimeError('server_stream_closed')

                    tasks = [asyncio.create_task(send()),asyncio.create_task(receive())]
                    while True:
                        for task in tasks:
                            if task.done():
                                task.result()
                        elapsed = time.monotonic()-started
                        if not force_done and elapsed >= force_at and resumable and handle:
                            force_done = True
                            renew.set()
                        if renew.is_set():
                            if not resumable or not handle:
                                # No blind reconnect or retries without current server support.
                                raise RuntimeError('no_current_resumable_state')
                            print(json.dumps({'event':'renew','direction':name,
                                              'elapsed':round(elapsed,3),
                                              'go_away_count':result['go_away_count']}),flush=True)
                            break
                        await asyncio.sleep(0.05)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
                    async with asyncio.timeout(5):
                        await context.__aexit__(None,None,None)
    except TimeoutError:
        result['status'] = ('packet_complete' if time.monotonic()-started >= duration-0.1
                            else 'setup_or_cleanup_timeout')
    except asyncio.CancelledError:
        result['status'] = 'cancelled'
        raise
    except Exception as exc:
        result['status'] = 'error'
        result.update(safe_error(exc))
        raise
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        with suppress(Exception):
            async with asyncio.timeout(5):
                await client.aio.aclose()
        result['elapsed_seconds'] = round(time.monotonic()-started,3)
        for k,v in result.items():
            if isinstance(v,float): result[k] = round(v,4)
        print(json.dumps({'event':'route_final',**result}),flush=True)
    return result


async def run(config, fixtures, seconds, credentials):
    totals = {'input':0.0,'output':0.0}
    tasks = [asyncio.create_task(route(config,name,pcm,seconds,totals,credentials))
             for name,pcm in fixtures.items()]
    started = time.monotonic()
    results = None
    outcome = 'stopped'
    try:
        async with asyncio.timeout(seconds+10):
            while not all(t.done() for t in tasks):
                # A failure in either route immediately stops the other route.
                for task in tasks:
                    if task.done(): task.result()
                estimate = totals['input']*RATE_INPUT + totals['output']*RATE_OUTPUT
                if estimate >= 1.75:
                    raise RuntimeError('estimate_reserve_reached')
                print(json.dumps({'event':'progress',
                                  'elapsed':round(time.monotonic()-started,1),
                                  'input_seconds':round(totals['input'],1),
                                  'output_seconds':round(totals['output'],1),
                                  'transport_estimate_usd':round(estimate,6)}),flush=True)
                done,_ = await asyncio.wait(tasks,timeout=30,return_when=asyncio.FIRST_EXCEPTION)
                for task in done: task.result()
            results = [t.result() for t in tasks]
            outcome = 'packet_complete'
    except Exception as exc:
        print(json.dumps({'event':'probe_fault',**safe_error(exc)}),flush=True)
    finally:
        for task in tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        print(json.dumps({'event':'probe_final','status':outcome,
                          'elapsed':round(time.monotonic()-started,3),
                          'input_seconds':round(totals['input'],3),
                          'output_seconds':round(totals['output'],3),
                          'transport_estimate_usd':round(
                              totals['input']*RATE_INPUT+totals['output']*RATE_OUTPUT,6),
                          'semantics_and_lossless_continuity_verified':False}),flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--synthesizer',required=True)
    parser.add_argument('--seconds',type=int,default=780)
    parser.add_argument('--verified-on',required=True)
    parser.add_argument('--expires-on',required=True)
    parser.add_argument('--remaining-usd',type=float,required=True)
    parser.add_argument('--owner-approved-continuation-test',action='store_true',required=True)
    args = parser.parse_args()
    estimate = check_packet(args.seconds,args.remaining_usd,args.verified_on,args.expires_on)
    raw = tomllib.loads(Path(args.config).read_text())
    config = replace(Config(**raw),call=None,bridge=None,credit=dict(
        eligible=True,verified_on=args.verified_on,expires_on=args.expires_on,
        remaining_usd=args.remaining_usd))
    config.validate()
    fixtures = {}
    for name,(voice,_,text) in FIXTURES.items():
        pcm = subprocess.run([args.synthesizer,voice,text],capture_output=True,
                             check=True,timeout=25).stdout
        if len(pcm)%2 or not 1 <= len(pcm)/32000 <= 20 or not any(pcm):
            raise ValueError('Invalid synthetic PCM fixture')
        fixtures[name] = pcm
    print(json.dumps({'event':'approved_probe_start','seconds':args.seconds,
                      'planned_full_audio_estimate_usd':round(estimate,6),
                      'allocation_usd':MAX_ESTIMATE_USD,'physical_devices_opened':False,
                      'transcription_requested':False}),flush=True)
    # Use the already authorized CLI identity verified by read-only IAM/Billing checks.
    # Do not repair shared ADC, run login, widen IAM, or print/store this token.
    token = subprocess.run(['/opt/homebrew/bin/gcloud','auth','print-access-token'],
                           capture_output=True,text=True,check=True,timeout=20).stdout.strip()
    if not token:
        raise RuntimeError('Existing CLI token unavailable')
    results = asyncio.run(run(config,fixtures,args.seconds,Credentials(token)))
    return 0 if results is not None else 2


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print('STOPPED',flush=True)
        raise SystemExit(130)
