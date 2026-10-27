"""Bounded offline callback-to-consumer comparison; no devices, speech or model connection."""
import asyncio
import json
import queue
import statistics
import threading
import time

from call_translator.audio import AudioBuffers


async def compare(mode, count=64):
    loop = asyncio.get_running_loop()
    ready = asyncio.Event()
    buffers = AudioBuffers(on_capture=(lambda: loop.call_soon_threadsafe(ready.set))
                           if mode == 'notified' else None)
    requests = queue.Queue()
    produced = {}
    delays = []
    failures = []

    def producer():
        try:
            while True:
                index = requests.get()
                if index is None:
                    return
                # Vary arrival phase relative to the old 10 ms polling interval.
                time.sleep((1 + index * 7 % 9) / 1000)
                produced[index] = time.perf_counter()
                buffers.input_callback(index.to_bytes(2, 'little'), 1, None, None)
        except Exception as exc:
            failures.append(type(exc).__name__)

    thread = threading.Thread(target=producer, daemon=True)
    thread.start()
    try:
        for index in range(count):
            requests.put(index)
            if mode == 'notified':
                packet = await buffers.next_capture(ready)
            else:
                while True:
                    try:
                        packet = buffers.capture.get_nowait()
                        break
                    except queue.Empty:
                        await asyncio.sleep(0.01)
            received = time.perf_counter()
            assert packet == index.to_bytes(2, 'little')
            delays.append((received - produced[index]) * 1000)
    finally:
        requests.put(None)
        thread.join(1)
    assert not thread.is_alive() and not failures and not buffers.fault
    ordered = sorted(delays)
    return dict(samples=len(delays), mean_ms=round(statistics.mean(delays), 3),
                median_ms=round(statistics.median(delays), 3),
                p95_ms=round(ordered[int(0.95 * (len(ordered) - 1))], 3),
                max_ms=round(max(delays), 3))


async def main():
    async with asyncio.timeout(10):
        result = {mode: await compare(mode) for mode in ('polling', 'notified')}
    print(json.dumps({'scope': 'synthetic local handoff only, not audible call latency', **result}, sort_keys=True))


if __name__ == '__main__':
    asyncio.run(main())
