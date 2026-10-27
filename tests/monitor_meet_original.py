"""Listen to incoming Meet original without AI; no physical microphone or recording."""
import argparse
import array
from contextlib import ExitStack
import json
import math
import time
import tomllib

import sounddevice as sd
from call_translator.audio import AudioBuffers, select_device

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--config', required=True)
parser.add_argument('--seconds', type=int, choices=range(1, 61), default=20)
parser.add_argument('--input-device', help='Explicit alternative virtual route for comparison')
args = parser.parse_args()
with open(args.config, 'rb') as handle:
    config = tomllib.load(handle)
name = config['output_device']
if not any(word in name.lower() for word in ('airpods', 'headphone', 'headset')):
    raise ValueError('Explicit physical headphones required')
devices = sd.query_devices()
source_name = args.input_device or config['bridge']['incoming_device']
source = select_device(devices, source_name, 'input', virtual=True)
target = select_device(devices, name, 'output')
buffers = AudioBuffers()
last_capture = time.monotonic()
frames_received = 0
energy = 0
peak = 0
faults = []


def capture(data, frames, timing, status):
    global last_capture, frames_received, energy, peak
    last_capture = time.monotonic()
    frames_received += frames
    values = array.array('h', bytes(data))
    energy += sum(v*v for v in values)
    peak = max(peak, max(map(abs, values), default=0))
    if status:
        faults.append(str(status))
    try:
        buffers.enqueue_output(bytes(data))
    except Exception as exc:
        faults.append(type(exc).__name__)


try:
    with ExitStack() as stack:
        stack.enter_context(sd.RawOutputStream(device=target, samplerate=48000, channels=1,
            dtype='int16', blocksize=4800, callback=buffers.output_callback))
        stack.enter_context(sd.RawInputStream(device=source, samplerate=48000, channels=1,
            dtype='int16', blocksize=4800, callback=capture))
        print(f'ORIGINAL_MONITOR_LIVE: {args.seconds} seconds, no AI or microphone; Ctrl-C stops', flush=True)
        deadline = time.monotonic() + args.seconds
        while time.monotonic() < deadline:
            if time.monotonic() - last_capture > 2:
                raise RuntimeError('Virtual capture stalled')
            if faults or buffers.fault:
                raise RuntimeError('Audio callback fault')
            time.sleep(0.05)
finally:
    print(json.dumps(dict(captured_seconds=round(frames_received / 48000, 3),
                          rms=round(math.sqrt(energy / frames_received), 2) if frames_received else None,
                          peak=peak, faults=faults, audio_fault=buffers.fault,
                          status='monitor_closed')), flush=True)
