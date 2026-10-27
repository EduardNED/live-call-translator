"""Manual virtual-only rate probe: no physical devices, AI, speech or files.

Run from repository root: .venv/bin/python tests/manual_audio_rates.py matched48
Alternatives reproduce mixed-rate probes; close calls first. Numeric results only.
"""
import argparse
import array
from contextlib import ExitStack
import json
import math
import time

import sounddevice as sd

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('mode', choices=['matched48', 'mixed-input-first', 'mixed-output-first'])
args = parser.parse_args()
routes = [('outgoing', 'BlackHole 2ch', 24000, 48000, 440),
          ('incoming', 'Microsoft Teams Audio', 48000, 16000, 660)]
if args.mode == 'matched48':
    routes = [(name, device, 48000, 48000, freq) for name, device, _, _, freq in routes]
devices = sd.query_devices()
captured = {name: array.array('h') for name, *_ in routes}
phases = {name: 0 for name, *_ in routes}
faults = []


def output(name, rate, freq):
    def callback(out, frames, timing, status):
        if status:
            faults.append(name + ':output:' + str(status))
        start = phases[name]
        phases[name] += frames
        out[:] = array.array('h', (int(1000 * math.sin(2 * math.pi * freq * (start + i) / rate))
                                  for i in range(frames))).tobytes()
    return callback


def capture(name):
    def callback(data, frames, timing, status):
        if status:
            faults.append(name + ':input:' + str(status))
        captured[name].frombytes(bytes(data))
    return callback


with ExitStack() as stack:
    for name, device, send_rate, rate, freq in routes:
        indexes = [i for i, d in enumerate(devices) if d['name'] == device
                   and d['max_input_channels'] and d['max_output_channels']]
        if len(indexes) != 1:
            raise ValueError('Exactly one of each named virtual device is required')
        builders = [(sd.RawInputStream, rate, capture(name)),
                    (sd.RawOutputStream, send_rate, output(name, send_rate, freq))]
        if args.mode != 'mixed-input-first':
            builders.reverse()
        for constructor, sample_rate, callback in builders:
            stack.enter_context(constructor(device=indexes[0], samplerate=sample_rate,
                                            channels=1, dtype='int16',
                                            blocksize=sample_rate // 10, callback=callback))
    time.sleep(8)

results = []
for name, device, send_rate, rate, freq in routes:
    values = captured[name][rate:-rate]
    crossings = sum(a <= 0 < b for a, b in zip(values, values[1:]))
    results.append(dict(route=name, mode=args.mode, output_rate=send_rate, capture_rate=rate,
                        expected_hz=freq,
                        measured_hz=round(crossings * rate / len(values), 2) if values else None,
                        rms=round(math.sqrt(sum(v * v for v in values) / len(values)), 2)
                        if values else None,
                        captured_seconds=round(len(captured[name]) / rate, 2),
                        output_seconds=round(phases[name] / send_rate, 2),
                        faults=[f for f in faults if f.startswith(name)]))
print(json.dumps(results, indent=2))
