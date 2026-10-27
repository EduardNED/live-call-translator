import queue
import time

INPUT_RATE = 16000
OUTPUT_RATE = 24000
INPUT_FRAMES = 1600
OUTPUT_FRAMES = 2400
# Fixed 100 ms callbacks: adaptive sizes made outgoing speech garbled through the
# virtual Meet microphone in the 2026-10-27 live EN/RU call; 2400 restored clean sound.
OUTPUT_CALLBACK_FRAMES = OUTPUT_FRAMES
ACTIVITY_RMS_THRESHOLD = 200
# Callback cadence, not audio content: silence with live callbacks is a valid call state.
# The first callback may lag while a Bluetooth route switches profile.
CAPTURE_START_STALL_SECONDS = 5.0
CAPTURE_STALL_SECONDS = 2.0


def select_device(devices, name, direction, *, virtual=False):
    key = f"max_{direction}_channels"
    matches = [i for i, item in enumerate(devices)
               if item["name"] == name and item[key] > 0]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one {direction} device matching {name!r}")
    index = matches[0]
    is_virtual = name.startswith("BlackHole ") or name == "Microsoft Teams Audio"
    if virtual:
        if not is_virtual:
            raise ValueError("Select a separate BlackHole or Teams virtual route")
    elif is_virtual or any(term in name.lower() for term in ("aggregate", "multi-output")):
        raise ValueError("Select physical microphone/headphones for this task")
    elif direction == "output" and "speaker" in name.lower():
        raise ValueError("Select physical headphones rather than loudspeakers")
    return index


class AudioBuffers:
    def __init__(self, *, activity_started=None, clock=time.monotonic, on_capture=None):
        self.on_capture = on_capture
        self.activity_started = activity_started
        self.clock = clock
        self.admitting = True
        self.watch_started = None
        self.last_capture_callback = None
        self.last_output_at = None
        self.activity = {side: {"first_seconds": None, "last_seconds": None, "chunks": 0}
                         for side in ("capture", "playback")}
        self.capture = queue.Queue(maxsize=10)
        # 10 s of 100 ms chunks: headroom for renewal handovers during nonstop speech;
        # normal backlog stays under a second, so this adds no delay.
        self.playback = queue.Queue(maxsize=100)
        self.fault = None
        self.input_status_flags = 0
        self.output_status_flags = 0
        self.input_bytes = 0
        self.output_bytes = 0
        self.played_bytes = 0
        self.pending = b""
        self.input_clipped_samples = 0
        self.output_clipped_samples = 0
        self.max_playback_chunks = 0
        self.output_zero_samples = 0
        self.output_samples = 0

    def observe_activity(self, data, side):
        # Diagnostic amplitude marker, not speech recognition or sentence alignment.
        if self.activity_started is None or not data:
            return
        samples = memoryview(data).cast("h")
        if sum(v * v for v in samples) / len(samples) < ACTIVITY_RMS_THRESHOLD ** 2:
            return
        elapsed = round(time.monotonic() - self.activity_started, 3)
        marker = self.activity[side]
        if marker["first_seconds"] is None:
            marker["first_seconds"] = elapsed
        marker["last_seconds"] = elapsed
        marker["chunks"] += 1

    def watch_capture(self):
        self.watch_started = self.clock()

    def capture_stall(self):
        """Seconds without a capture callback when that exceeds the allowance, else None."""
        if self.last_capture_callback is not None:
            gap, limit = self.clock() - self.last_capture_callback, CAPTURE_STALL_SECONDS
        elif self.watch_started is not None:
            gap, limit = self.clock() - self.watch_started, CAPTURE_START_STALL_SECONDS
        else:
            return None
        return gap if gap > limit else None

    def input_callback(self, data, frames, timing, status):
        self.last_capture_callback = self.clock()
        if status:
            # A single Bluetooth xrun must not end a long call; real stalls are caught by
            # the capture watchdog and output queue limits.
            self.input_status_flags += 1
        if not self.admitting:
            # Call window is over: the stream stays open only so the watchdog keeps ticking.
            return
        self.observe_activity(data, "capture")
        self.input_clipped_samples += sum(abs(v) >= 32760 for v in memoryview(data).cast("h"))
        try:
            self.capture.put_nowait(bytes(data))
            if self.on_capture is not None:
                self.on_capture()
        except queue.Full:
            self.fault = "capture_queue_overflow"

    async def next_capture(self, ready):
        while True:
            # Clear before checking the queue: a callback between empty/get and wait
            # must wake this reader. The audio thread schedules ready.set on the loop.
            ready.clear()
            try:
                return self.capture.get_nowait()
            except queue.Empty:
                if not self.admitting:
                    return None
                await ready.wait()

    def output_callback(self, out, frames, timing, status):
        if status:
            self.output_status_flags += 1
        size = len(out)
        chunks = []
        remaining = size
        while remaining:
            if not self.pending:
                try:
                    self.pending = self.playback.get_nowait()
                except queue.Empty:
                    break
            chunk, self.pending = self.pending[:remaining], self.pending[remaining:]
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        out[:] = data + bytes(remaining)
        self.observe_activity(data, "playback")
        self.played_bytes += len(data)

    def enqueue_output(self, data):
        if len(data) % 2:
            raise ValueError("Invalid PCM sample length")
        # Bound memory even if a server message contains a long audio segment.
        for start in range(0, len(data), OUTPUT_FRAMES * 2):
            self.playback.put_nowait(data[start:start + OUTPUT_FRAMES * 2])
        samples = memoryview(data).cast("h")
        self.output_clipped_samples += sum(abs(v) >= 32760 for v in samples)
        self.output_zero_samples += sum(v == 0 for v in samples)
        self.output_samples += len(samples)
        self.max_playback_chunks = max(self.max_playback_chunks, self.playback.qsize())
        self.output_bytes += len(data)
        self.last_output_at = self.clock()

    def playback_idle(self):
        return self.playback.empty() and not self.pending
