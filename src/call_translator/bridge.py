"""Two separately buffered sessions; no writes to macOS or Meet defaults."""
import asyncio
from dataclasses import replace

from .config import LANGUAGE_PAIRS
from .audio import INPUT_RATE, OUTPUT_RATE, select_device
from .stream import translate


def check_bridge(config, sd):
    config.validate()
    if (config.source_language, config.target_language) not in LANGUAGE_PAIRS:
        raise ValueError("Bridge requires a supported outgoing language pair")
    bridge = config.bridge or {}
    devices = sd.query_devices()
    mic = select_device(devices, config.input_device, "input")
    headphones = select_device(devices, config.output_device, "output")
    # Speaker fallback creates an acoustic feedback path even with virtual isolation.
    if not any(word in config.output_device.lower() for word in ("headphone", "airpods", "headset")):
        raise ValueError("Bridge requires explicitly selected headphones")
    outgoing_name = bridge.get("outgoing_device", "")
    incoming_name = bridge.get("incoming_device", "")
    if outgoing_name == incoming_name:
        raise ValueError("Meet microphone and speaker must use distinct virtual routes")
    outgoing = select_device(devices, outgoing_name, "output", virtual=True)
    incoming = select_device(devices, incoming_name, "input", virtual=True)
    if incoming_name == "Microsoft Teams Audio":
        raise ValueError(
            "Use BlackHole 2ch for incoming_device and Microsoft Teams Audio for outgoing_device"
        )
    if len({mic, headphones, outgoing, incoming}) != 4:
        raise ValueError("Bridge requires four distinct endpoints without local crossover")
    # Check both ends, including Meet's side, before opening either AI session.
    select_device(devices, outgoing_name, "input", virtual=True)
    select_device(devices, incoming_name, "output", virtual=True)
    routes = {
        "outgoing": (replace(config, output_device=outgoing_name, bridge=None), mic, outgoing),
        "incoming": (replace(config, source_language=config.target_language,
                             target_language=config.source_language,
                             input_device=incoming_name, bridge=None), incoming, headphones),
    }
    for _, source, target in routes.values():
        sd.check_input_settings(device=source, channels=1, dtype="int16", samplerate=INPUT_RATE)
        sd.check_output_settings(device=target, channels=1, dtype="int16", samplerate=OUTPUT_RATE)
    return routes


async def run_bridge(routes, sd, direction="both", translator=translate):
    selected = routes if direction == "both" else {direction: routes[direction]}
    metrics = {}
    tasks = {name: asyncio.create_task(translator(
        config, source, target, sd, direction=name,
        on_metrics=lambda value, name=name: metrics.update({name: value})))
             for name, (config, source, target) in selected.items()}
    call = getattr(next(iter(selected.values()))[0], "call", None)
    pending = set(tasks.values())
    grace = None
    try:
        # Completion, timeout or error of either route closes the other as well.
        while pending:
            done, pending = await asyncio.wait(pending, timeout=grace, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                break
            for task in done:
                task.result()
            if not call:
                break
            # A call window that ended cleanly leaves the sibling its own bounded drain; its
            # API deadline still caps it, and a fault in either route (raised above) does not wait.
            grace = call["setup_seconds"] + call["drain_seconds"]
    finally:
        for task in tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
    return metrics
