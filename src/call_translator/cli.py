import argparse
import asyncio
import sys

from .config import Config
from .audio import INPUT_RATE, OUTPUT_RATE, select_device


def check_devices(config, sd):
    devices = sd.query_devices()
    source = select_device(devices, config.input_device, "input")
    target = select_device(devices, config.output_device, "output")
    sd.check_input_settings(device=source, channels=1, dtype="int16", samplerate=INPUT_RATE)
    sd.check_output_settings(device=target, channels=1, dtype="int16", samplerate=OUTPUT_RATE)
    return source, target


def main():
    parser = argparse.ArgumentParser(description="Explicitly started local English/French Meet translator")
    parser.add_argument("command", choices=["devices", "check", "run", "check-bridge", "bridge", "snapshot", "meet-tabs", "check-call", "call", "prepare"])
    parser.add_argument("--config", default="config.local.toml")
    parser.add_argument("--direction", choices=["both", "outgoing", "incoming"], default="both")
    parser.add_argument("--snapshot-path", default=".local/audio-before.json")
    parser.add_argument("--meet-microphone")
    parser.add_argument("--meet-speaker")
    parser.add_argument("--meet-tab-id", type=int)
    parser.add_argument("--meet-helper", default=".local/meet-observer")
    parser.add_argument("--save-transcript", action="store_true",
                        help="Opt in to private local original/translation Markdown (call only)")
    args = parser.parse_args()
    if args.save_transcript and args.command != "call":
        parser.error("--save-transcript requires call")
    try:
        if args.command == "prepare":
            from .native_build import prepare
            prepare(args.meet_helper)
            print("NATIVE_HELPER_PREPARED (no browser, audio or AI connection)")
            return 0
        if args.command == "snapshot":
            from .routing import snapshot
            snapshot(args.snapshot_path, args.meet_microphone, args.meet_speaker)
            print("LOCAL_RESTORATION_SNAPSHOT_SAVED (no settings changed)")
            return 0
        if args.command == "meet-tabs":
            from .meet import NativeMeet
            import json
            print(json.dumps(asyncio.run(NativeMeet(args.meet_helper).read())))
            return 0
        import sounddevice as sd
        if args.command == "devices":
            print(sd.query_devices())
            return 0
        config = Config.load(args.config)
        if args.command in ("check-call", "call"):
            from .bridge import check_bridge
            from .meet import NativeMeet, MeetObserver
            from .continuous import run_call, emit
            if args.direction != "both" or not config.continuous or args.meet_tab_id is None:
                raise ValueError("call requires [continuous], both directions and --meet-tab-id")
            routes = check_bridge(config, sd)
            observer = MeetObserver(NativeMeet(args.meet_helper, args.meet_tab_id), notify=emit)
            if args.command == "check-call":
                asyncio.run(observer.require_joined())
                print("CALL_PREFLIGHT_OK (no capture, playback or AI; credit is operator-attested)")
                return 0
            asyncio.run(run_call(routes, sd, observer, save_transcript=args.save_transcript))
            return 0
        if config.continuous is not None:
            raise ValueError("Use call/check-call for [continuous]; remove it for finite diagnostics")
        if args.command in ("check-bridge", "bridge"):
            from .bridge import check_bridge, run_bridge
            routes = check_bridge(config, sd)
            if args.command == "check-bridge":
                print("BRIDGE_CONFIG_AND_FORMATS_OK (no capture, no AI connection)")
                return 0
            results = asyncio.run(run_bridge(routes, sd, args.direction))
            # Silence is not evidence of a successful route.
            if len(results) != (2 if args.direction == "both" else 1) or any(
                    not m["played_audio_seconds"] or m["audio_fault"] for m in results.values()):
                print("BRIDGE_NOT_VERIFIED: inspect per-direction metrics", file=sys.stderr)
                return 2
            return 0
        source, target = check_devices(config, sd)
        if args.command == "check":
            print("CONFIG_AND_DEVICE_FORMATS_OK (no capture, no AI connection)")
            return 0
        from .stream import translate
        metrics = asyncio.run(translate(config, source, target, sd))
        if not metrics["played_audio_seconds"] or metrics["audio_fault"]:
            print("NO_VERIFIED_PLAYBACK: no translated audio played or device fault", file=sys.stderr)
            return 2
        return 0
    except KeyboardInterrupt:
        print("STOPPED", file=sys.stderr)
        return 130
    except Exception as exc:
        # No credential, speech, API body or transcript in diagnostics.
        safe = {"financial_safety_stop", "meet_observer_timeout", "selected_meet_changed",
                "selected_meet_not_observable_and_joined", "bounded_recovery_count_exhausted",
                "capture_queue_overflow", "capture_callbacks_stalled", "session_cleanup_failed",
                "renewal_transition_failed", "translation_interrupted", "audio_worker_stopped",
                "unexpected_server_close", "unexpected_audio_format"}
        category = str(exc) if isinstance(exc, RuntimeError) and str(exc) in safe else type(exc).__name__
        print(f"STOPPED: {category}; check config, audio and Cloud access", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
