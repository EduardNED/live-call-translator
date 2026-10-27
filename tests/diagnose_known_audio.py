"""Owner-approved diagnostic only: fixed stock TTS -> API, no live devices/files.

Prints only neutral fixture transcripts transiently. Never feed private/human audio.
Production language validation remains EN/FR. Compile synthesize_pcm.swift first.
"""
import argparse
import array
import asyncio
from contextlib import suppress
import json
import math
from pathlib import Path
import subprocess
import time

from google import genai
from google.genai import types
from call_translator.config import Config

FIXTURES = {
    'en-es': ('en-GB', 'es', 'The meeting starts tomorrow at ten. Please send the blue document. We need two tickets for Friday.'),
    'fr-en': ('fr-FR', 'en', 'La réunion commence demain à dix heures. Veuillez envoyer le document bleu. Nous avons besoin de deux billets pour vendredi.'),
    'es-en': ('es-ES', 'en', 'La reunión empieza mañana a las diez. Por favor, envía el documento azul. Necesitamos dos entradas para el viernes.'),
}


async def probe(config, direction, pcm):
    _, target, _ = FIXTURES[direction]
    client = genai.Client(enterprise=True, project=config.project, location=config.location,
                         http_options=types.HttpOptions(api_version='v1beta1'))
    metrics = dict(direction=direction, status='starting', source_text='', translated_text='',
                   input_audio_seconds=0, output_audio_seconds=0, usage_report_count=0)
    tasks = []
    started = time.monotonic()
    ended = asyncio.Event()
    first_output = None
    output_samples = 0
    output_zeros = 0
    output_square_sum = 0
    output_peak = 0
    output_formats = set()
    try:
        async with asyncio.timeout(30):
            async with client.aio.live.connect(model=config.model, config=types.LiveConnectConfig(
                    response_modalities=['AUDIO'],
                    input_audio_transcription=types.AudioTranscriptionConfig(),
                    output_audio_transcription=types.AudioTranscriptionConfig(),
                    translation_config=types.TranslationConfig(target_language_code=target,
                                                               echo_target_language=False))) as session:
                async def send():
                    # Append exactly three seconds of silence, then explicitly end input.
                    source = pcm + bytes(3 * 32000)
                    metrics["input_start_elapsed"] = round(time.monotonic() - started, 3)
                    for offset in range(0, len(source), 3200):
                        chunk = source[offset:offset + 3200]
                        await session.send_realtime_input(audio=types.Blob(
                            data=chunk, mime_type='audio/pcm;rate=16000'))
                        metrics['input_audio_seconds'] += len(chunk) / 32000
                        await asyncio.sleep(len(chunk) / 32000)
                    metrics["input_end_elapsed"] = round(time.monotonic() - started, 3)
                    ended.set()
                    await session.send_realtime_input(audio_stream_end=True)

                async def receive():
                    nonlocal first_output, output_samples, output_zeros, output_square_sum, output_peak
                    while True:
                        any_message = False
                        async for message in session.receive():
                            any_message = True
                            if message.usage_metadata:
                                metrics['usage_report_count'] += 1
                            content = message.server_content
                            if not content:
                                continue
                            if content.input_transcription and content.input_transcription.text:
                                metrics['source_text'] += content.input_transcription.text
                                metrics['source_text_last_elapsed'] = round(time.monotonic() - started, 3)
                            if content.output_transcription and content.output_transcription.text:
                                metrics['translated_text'] += content.output_transcription.text
                                metrics['translation_text_last_elapsed'] = round(time.monotonic() - started, 3)
                            if content.model_turn:
                                for part in content.model_turn.parts or []:
                                    blob = part.inline_data
                                    if blob and blob.data:
                                        if not (blob.mime_type or '').startswith('audio/pcm'):
                                            raise ValueError('Unexpected audio format')
                                        if first_output is None:
                                            first_output = time.monotonic() - started
                                        output_formats.add(blob.mime_type)
                                        values = array.array('h', blob.data)
                                        output_samples += len(values)
                                        output_zeros += values.count(0)
                                        output_square_sum += sum(v*v for v in values)
                                        output_peak = max(output_peak, max(map(abs, values), default=0))
                                        metrics['output_audio_seconds'] += len(blob.data) / 48000
                            if content.turn_complete and ended.is_set():
                                metrics['status'] = 'completed'
                                return
                        if not any_message:
                            if ended.is_set():
                                metrics['status'] = 'server_closed_after_input'
                                return
                            raise RuntimeError('Server closed before input finished')

                sender = asyncio.create_task(send())
                receiver = asyncio.create_task(receive())
                tasks = [sender, receiver]
                await asyncio.gather(*tasks)
    except TimeoutError:
        metrics['status'] = 'time_limit_reached'
    except Exception as exc:
        metrics['status'] = 'error'
        metrics['error_type'] = type(exc).__name__
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        with suppress(Exception):
            async with asyncio.timeout(5):
                await client.aio.aclose()
        metrics['output_formats'] = sorted(output_formats)
        metrics['output_zero_fraction'] = round(output_zeros / output_samples, 3) if output_samples else None
        metrics['output_rms'] = round(math.sqrt(output_square_sum / output_samples), 2) if output_samples else None
        metrics['output_peak'] = output_peak
        metrics['elapsed_seconds'] = round(time.monotonic() - started, 3)
        metrics['first_output_seconds_from_connection_start'] = round(first_output, 3) if first_output else None
        for key in ('input_audio_seconds', 'output_audio_seconds'):
            metrics[key] = round(metrics[key], 3)
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('direction', choices=FIXTURES)
    parser.add_argument('--config', required=True)
    parser.add_argument('--owner-approved-en-es', action='store_true', required=True)
    args = parser.parse_args()
    config = Config.load(args.config)  # Retain current same-day credit/model checks.
    voice, _, text = FIXTURES[args.direction]
    result = subprocess.run([str(Path('.local/synthesize_pcm').resolve()), voice, text],
                            capture_output=True, timeout=25, check=True)
    pcm = result.stdout
    if len(pcm) % 2 or not 1 <= len(pcm) / 32000 <= 20:
        raise ValueError('Invalid stock TTS duration/PCM')
    samples = array.array('h', pcm)
    if max(map(abs, samples), default=0) < 100:
        raise ValueError('Stock TTS is silent')
    print('DIRECT_API_CONTROL: no microphone, playback device or recording', flush=True)
    print(json.dumps(asyncio.run(probe(config, args.direction, pcm)), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
