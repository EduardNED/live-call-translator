"""Owner-started virtual return probe; no AI, local physical devices or audio files.

Compare explicitly owner-started remote-microphone conditions using a weak tone or
fixed stock Apple speech generated in memory. The stimulus tests the configured
outgoing route while only the incoming virtual route is measured. A negative result
is limited: the remote endpoint is not instrumented, and background/echo processing
may suppress the stimulus. Energy-envelope correlation alone is not causal proof.
"""
import argparse
import array
from contextlib import ExitStack
import json
import math
from pathlib import Path
import subprocess
import time
import tomllib

import sounddevice as sd
from call_translator.audio import select_device

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--config', required=True)
parser.add_argument('--speech', action='store_true')
parser.add_argument('--seconds', type=int, choices=[6, 8], default=8)
state = parser.add_mutually_exclusive_group(required=True)
state.add_argument('--remote-mic-muted', action='store_true')
state.add_argument('--remote-mic-unmuted', action='store_true')
args = parser.parse_args()
with open(args.config, 'rb') as handle:
    config = tomllib.load(handle)
devices = sd.query_devices()
bridge = config['bridge']
if bridge['incoming_device'] == bridge['outgoing_device']:
    raise ValueError('Distinct virtual routes required')
source = select_device(devices, bridge['incoming_device'], 'input', virtual=True)
target = select_device(devices, bridge['outgoing_device'], 'output', virtual=True)
rate_in, rate_out, frequency = 16000, 24000, 1200
frames_in = rate_in // 10
sine = [math.sin(2 * math.pi * frequency * i / rate_in) for i in range(frames_in)]
cosine = [math.cos(2 * math.pi * frequency * i / rate_in) for i in range(frames_in)]
received = 0
sent = 0
energy = 0
matched_energy = 0
peak = 0
last_capture = time.monotonic()
faults = []
sent_envelope = []
captured_envelope = []
speech = None
if args.speech:
    result = subprocess.run([str(Path('.local/synthesize_pcm').resolve()), 'en-GB',
                             'Blue document.'], capture_output=True, check=True, timeout=25)
    original = array.array('h', result.stdout)
    if not 0.5 <= len(original) / 16000 <= 3 or max(map(abs, original), default=0) < 100:
        raise ValueError('Invalid stock speech control')
    scale = 5000 / max(map(abs, original))
    speech = [0] * (rate_out // 2)
    for index in range(len(original) * 3 // 2):
        position, remainder = divmod(index * 2, 3)
        value = ((3 - remainder) * original[min(position, len(original) - 1)]
                 + remainder * original[min(position + 1, len(original) - 1)]) / 3
        speech.append(int(value * scale))


def output(out, frames, timing, status):
    global sent
    if status:
        faults.append('output:' + str(status))
    if speech is not None:
        values = speech[sent:sent + frames]
        values += [0] * (frames - len(values))
    else:
        values = [int(700 * math.sin(2 * math.pi * frequency * (sent + i) / rate_out))
                 if rate_out <= sent + i < 7 * rate_out else 0
                 for i in range(frames)]
    out[:] = array.array('h', values).tobytes()
    sent_envelope.append(math.sqrt(sum(v * v for v in values) / frames))
    sent += frames


def capture(data, frames, timing, status):
    global received, energy, matched_energy, peak, last_capture
    last_capture = time.monotonic()
    if status:
        faults.append('input:' + str(status))
    if frames != frames_in:
        faults.append('unexpected_capture_block')
        return
    values = array.array('h', bytes(data))
    received += frames
    chunk_energy = sum(v * v for v in values)
    energy += chunk_energy
    captured_envelope.append(math.sqrt(chunk_energy / frames))
    peak = max(peak, max(map(abs, values), default=0))
    real = sum(v * s for v, s in zip(values, sine))
    imaginary = sum(v * c for v, c in zip(values, cosine))
    matched_energy += 2 * (real * real + imaginary * imaginary) / frames


try:
    with ExitStack() as stack:
        stack.enter_context(sd.RawOutputStream(device=target, samplerate=rate_out, channels=1,
            dtype='int16', blocksize=rate_out // 10, callback=output))
        stack.enter_context(sd.RawInputStream(device=source, samplerate=rate_in, channels=1,
            dtype='int16', blocksize=frames_in, callback=capture))
        last_capture = time.monotonic()
        print(f'RETURN_PROBE_LIVE: {args.seconds} seconds, stock speech={args.speech}, remote mic muted={args.remote_mic_muted}; no AI/recording', flush=True)
        deadline = time.monotonic() + args.seconds
        while time.monotonic() < deadline:
            if faults or time.monotonic() - last_capture > 2:
                raise RuntimeError('Audio fault or stalled capture; result inconclusive')
            time.sleep(0.05)
finally:
    # Numeric energy envelopes only; no incoming sample buffer is retained.
    best = None
    for lag in range(-10, 31):
        left = sent_envelope[max(0, -lag):]
        right = captured_envelope[max(0, lag):]
        n = min(len(left), len(right))
        if n < 20:
            continue
        left, right = left[:n], right[:n]
        ml, mr = sum(left) / n, sum(right) / n
        denominator = math.sqrt(sum((v - ml) ** 2 for v in left)
                                * sum((v - mr) ** 2 for v in right))
        if denominator:
            correlation = sum((a - ml) * (b - mr) for a, b in zip(left, right)) / denominator
            if best is None or correlation > best[0]:
                best = (correlation, lag)
    print(json.dumps(dict(captured_seconds=round(received / rate_in, 3),
        output_seconds=round(sent / rate_out, 3), rms=round(math.sqrt(energy / received), 3)
        if received else None, peak=peak,
        tone_energy_fraction=round(matched_energy / energy, 4) if energy else None,
        stimulus='stock_speech' if args.speech else 'tone',
        max_envelope_correlation=round(best[0], 3) if best else None,
        envelope_lag_blocks=best[1] if best else None,
        expected_tone_hz=None if args.speech else frequency, output_peak=5000 if args.speech else 700, input_rate=rate_in, output_rate=rate_out,
        faults=faults, status='probe_closed', remote_mic_muted_asserted=args.remote_mic_muted)), flush=True)
