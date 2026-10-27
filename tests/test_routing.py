import json
from pathlib import Path

import pytest
from call_translator import routing


def test_snapshot_requires_real_defaults_and_never_overwrites(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    devices = [{'_name': 'Mic', 'coreaudio_default_audio_input_device': 'spaudio_yes'},
               {'_name': 'Headphones', 'coreaudio_default_audio_output_device': 'spaudio_yes',
                'coreaudio_default_audio_system_device': 'spaudio_yes'}]
    monkeypatch.setattr(routing.subprocess, 'check_output',
                        lambda *a, **kw: json.dumps({'SPAudioDataType': [{'_items': devices}]}))
    routing.snapshot('.local/before.json', 'Prior mic', 'Prior speaker')
    saved = Path('.local/before.json').read_bytes()
    assert json.loads(saved)['macos']['output'] == 'Headphones'
    assert Path('.local/before.json').stat().st_mode & 0o777 == 0o600
    with pytest.raises(ValueError):
        routing.snapshot('.local/before.json', 'Other mic', 'Other speaker')
    assert Path('.local/before.json').read_bytes() == saved
    monkeypatch.setattr(routing.subprocess, 'check_output',
                        lambda *a, **kw: '{"SPAudioDataType": []}')
    with pytest.raises(ValueError):
        routing.snapshot('.local/empty.json', 'Mic', 'Speaker')
    assert not Path('.local/empty.json').exists()
