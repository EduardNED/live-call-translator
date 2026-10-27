from dataclasses import replace
from datetime import date, timedelta
import queue

import pytest

from call_translator.config import Config
from call_translator.audio import AudioBuffers, select_device
from call_translator.stream import live_config


def valid_config():
    return Config('existing-project', 'global', 'gemini-3.5-live-translate-preview',
                  'en', 'fr', 'Microphone', 'Headphones', 30,
                  {'eligible': True, 'verified_on': date.today().isoformat(),
                   'expires_on': (date.today() + timedelta(days=5)).isoformat(),
                   'remaining_usd': 10})


def test_credit_and_duration_fail_closed():
    config = valid_config()
    config.validate()
    for change in [{'max_seconds': 121}, {'max_seconds': True}, {'target_language': 'en'},
                   {'credit': {**config.credit, 'eligible': False}},
                   {'credit': {**config.credit, 'remaining_usd': float('nan')}},
                   {'credit': {**config.credit, 'remaining_usd': 4.99}},
                   {'credit': {**config.credit, 'verified_on': '2000-01-01'}}]:
        with pytest.raises(ValueError):
            replace(config, **change).validate()


def test_airpods_disambiguated_and_missing_rejected():
    devices = [{'name': 'AirPods', 'max_input_channels': 1, 'max_output_channels': 0},
               {'name': 'AirPods', 'max_input_channels': 0, 'max_output_channels': 2}]
    assert select_device(devices, 'AirPods', 'output') == 1
    assert select_device(devices, 'AirPods', 'input') == 0
    with pytest.raises(ValueError):
        select_device(devices, 'Headphones', 'output')
    with pytest.raises(ValueError):
        select_device(devices + [devices[1]], 'AirPods', 'output')
    with pytest.raises(ValueError):
        select_device([{'name': 'BlackHole 2ch', 'max_input_channels': 2}],
                      'BlackHole 2ch', 'input')
    with pytest.raises(ValueError):
        select_device([{'name': 'MacBook Air Speakers', 'max_input_channels': 0, 'max_output_channels': 2}],
                      'MacBook Air Speakers', 'output')


def test_bounded_capture_and_playback_overflow():
    buffers = AudioBuffers()
    for _ in range(11):
        buffers.input_callback(b'\x01\x00' * 1600, 1600, None, None)
    assert buffers.capture.qsize() == 10
    assert buffers.fault == 'capture_queue_overflow'
    with pytest.raises(queue.Full):
        buffers.enqueue_output(bytes(101 * 4800))
    assert buffers.playback.qsize() == 100


def test_output_partial_silence_and_pcm_integrity():
    buffers = AudioBuffers()
    buffers.enqueue_output(b'\x01\x00' * 10)
    out = bytearray(30)
    buffers.output_callback(out, 15, None, None)
    assert out == b'\x01\x00' * 10 + bytes(10)
    assert buffers.played_bytes == 20
    buffers.output_callback(out, 15, None, None)
    assert out == bytes(30)
    with pytest.raises(ValueError):
        buffers.enqueue_output(b'x')


def test_no_transcription_or_target_echo():
    config = live_config(valid_config())
    assert config.translation_config.target_language_code == 'fr'
    assert config.translation_config.echo_target_language is False
    assert config.input_audio_transcription is None
    assert config.output_audio_transcription is None


def test_output_fills_from_multiple_server_chunks():
    buffers = AudioBuffers()
    for _ in range(10):
        buffers.enqueue_output(b"\x01\x00" * 240)
    out = bytearray(4800)
    buffers.output_callback(out, 2400, None, None)
    assert out == b"\x01\x00" * 2400
    assert buffers.playback.empty()


def test_transcription_is_explicit_and_pcm_durations_include_silence():
    config = valid_config()
    assert live_config(config).input_audio_transcription is None
    assert live_config(config).output_audio_transcription is None
    diagnostic = live_config(config, diagnostics=True)
    assert diagnostic.input_audio_transcription is not None
    assert diagnostic.output_audio_transcription is not None
    buffers = AudioBuffers()
    buffers.enqueue_output(bytes(8) + b"\x01\x00" * 4)
    assert buffers.output_samples == 8
    assert buffers.output_zero_samples == 4


def test_diagnostic_activity_ignores_silence_and_preserves_first_marker(monkeypatch):
    buffers = AudioBuffers(activity_started=100.0)
    monkeypatch.setattr("call_translator.audio.time.monotonic", lambda: 101.0)
    buffers.input_callback(bytes(8), 4, None, None)
    assert buffers.activity["capture"]["first_seconds"] is None
    buffers.input_callback(b"\xe8\x03" * 4, 4, None, None)
    buffers.enqueue_output(b"\xe8\x03" * 4)
    monkeypatch.setattr("call_translator.audio.time.monotonic", lambda: 104.0)
    buffers.output_callback(bytearray(8), 4, None, None)
    buffers.input_callback(b"\xe8\x03" * 4, 4, None, None)
    assert buffers.activity["capture"] == dict(first_seconds=1.0, last_seconds=4.0, chunks=2)
    assert buffers.activity["playback"] == dict(first_seconds=4.0, last_seconds=4.0, chunks=1)
    normal = AudioBuffers()
    normal.input_callback(b"\xe8\x03" * 4, 4, None, None)
    assert normal.activity["capture"]["first_seconds"] is None


def test_call_window_is_finite_explicit_and_inside_the_packet():
    from test_lifecycle import CALL
    config = replace(valid_config(), call=CALL)
    config.validate()
    assert CALL['conversation_seconds'] > 120  # the 1-120 s ceiling only bounds the diagnostic run
    with pytest.raises(ValueError):
        replace(config, max_seconds=121).validate()
    bad = [{'conversation_seconds': 0}, {'conversation_seconds': 601}, {'conversation_seconds': True},
           {'drain_seconds': 0}, {'drain_seconds': 61}, {'setup_seconds': 0}, {'setup_seconds': 1.5},
           {'packet_remaining_seconds': 601}, {'packet_remaining_seconds': 334},
           {'packet_verified_on': '2000-01-01'}]
    for change in bad:
        with pytest.raises(ValueError):
            replace(config, call={**CALL, **change}).validate()
    for call in [{k: v for k, v in CALL.items() if k != 'drain_seconds'}, {**CALL, 'unlimited': True}, {}]:
        with pytest.raises(ValueError):
            replace(config, call=call).validate()


def test_example_config_keeps_the_call_window_off_until_configured(tmp_path):
    import tomllib
    from pathlib import Path
    text = (Path(__file__).parent.parent / 'config.example.toml').read_text()
    assert 'call' not in tomllib.loads(text)
    assert '# [call]' in text


def test_standing_owner_credit_confirmation_keeps_original_date_and_expiry():
    config = valid_config()
    credit = {**config.credit, 'owner_confirmed': True,
              'verified_on': (date.today() - timedelta(days=30)).isoformat()}
    replace(config, credit=credit).validate()
    assert credit['verified_on'] != date.today().isoformat()
    for change in ({'owner_confirmed': 'true'}, {'eligible': False},
                   {'expires_on': date.today().isoformat()},
                   {'verified_on': (date.today() + timedelta(days=1)).isoformat()},
                   {'remaining_usd': float('inf')}):
        with pytest.raises(ValueError):
            replace(config, credit={**credit, **change}).validate()
    with pytest.raises(ValueError):
        replace(config, credit={**credit, 'owner_confirmed': False}).validate()
