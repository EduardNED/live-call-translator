"""Local restoration snapshot; never changes system or browser settings."""
import json
from pathlib import Path
import subprocess


def snapshot(path, meet_microphone, meet_speaker):
    path = Path(path)
    if path.parent != Path(".local") or path.exists():
        raise ValueError("Use a new snapshot filename directly under ignored .local/")
    report = json.loads(subprocess.check_output(
        ["system_profiler", "SPAudioDataType", "-json"], timeout=15))
    devices = [item for group in report.get("SPAudioDataType", []) for item in group.get("_items", [])]
    defaults = {key: next((d["_name"] for d in devices if d.get(prop) == "spaudio_yes"), None)
                for key, prop in [("input", "coreaudio_default_audio_input_device"),
                                  ("output", "coreaudio_default_audio_output_device"),
                                  ("system_output", "coreaudio_default_audio_system_device")]}
    if not all(defaults.values()) or not meet_microphone or not meet_speaker:
        raise ValueError("Real macOS defaults and prior Meet labels are required")
    path.parent.mkdir(exist_ok=True)
    with path.open("x") as handle:
        json.dump({"macos": defaults, "meet": {"microphone": meet_microphone, "speaker": meet_speaker}},
                  handle, indent=2)
    path.chmod(0o600)
