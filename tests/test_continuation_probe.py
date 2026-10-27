"""Financial preflight and sanitized failures before any paid probe connection."""
from datetime import date,timedelta
import importlib.util
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location('probe_continuation',Path(__file__).with_name('probe_continuation.py'))
probe=importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def check(seconds=900,balance=1931.20,verified=None,expires=None):
    return probe.check_packet(seconds,balance,verified or date.today().isoformat(),
                              expires or (date.today()+timedelta(days=10)).isoformat())


def test_packet_estimate_reserves_headroom():
    assert check() == pytest.approx(1.1025)
    assert check()*1.2 < probe.MAX_ESTIMATE_USD


@pytest.mark.parametrize('seconds',[0,901,True,1.2])
def test_packet_cannot_expand_authority(seconds):
    with pytest.raises(ValueError): check(seconds=seconds)


@pytest.mark.parametrize('balance',[0,1.99,float('nan'),float('inf')])
def test_unknown_or_insufficient_credit_stops_before_connect(balance):
    with pytest.raises(ValueError): check(balance=balance)


def test_stale_credit_or_expiry_stops_before_connect():
    yesterday=(date.today()-timedelta(days=1)).isoformat()
    with pytest.raises(ValueError): check(verified=yesterday)
    with pytest.raises(ValueError): check(expires=date.today().isoformat())


def test_error_payload_and_handles_never_escape():
    class Failure(Exception): code=400
    result=probe.safe_error(Failure('secret-token-123 invalid session_resumption transparent'))
    assert result == dict(error_type='Failure',error_code=400,
                          mentions_resumption=True,mentions_transparent=True)
    assert 'secret' not in str(result)
