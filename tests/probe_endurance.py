"""One owner-approved 13-minute dual-route synthetic transition test, no devices."""
import argparse
import asyncio
from dataclasses import replace
from datetime import date
import json
import subprocess
from pathlib import Path
import tomllib

from google.oauth2.credentials import Credentials
from call_translator.config import Config
import probe_handover as probe
from probe_continuation import check_packet, RATE_INPUT, RATE_OUTPUT, safe_error
from handover_checks import FIXTURES, RULES

SLOTS=tuple(range(0,521,40))+(535,565,600,640,680,720,760)
IDS=tuple(i%7 for i in range(14))+(3,4,5,6,0,1,2)


def configure():
    probe.ENDURANCE=True
    probe.BASELINE=False
    probe.INPUT_END=780
    probe.HARD_SECONDS=800
    probe.PREPARE_AT=525
    probe.SWITCH_AT=540
    probe.ALLOCATION=2.0
    probe.SLOTS=SLOTS
    probe.FIXTURE_IDS=IDS
    probe.FACT_GROUPS={name:tuple(tuple(RULES[name])[start:end] for start,end in
        ((0,3),(3,5),(5,7),(7,11),(11,13),(13,14),(14,16))) for name in FIXTURES}
    probe.FIXTURES={name:(voice,target,tuple(texts[i] for i in IDS))
                    for name,(voice,target,texts) in FIXTURES.items()}


def preflight(balance,verified_on,expires_on):
    estimate=check_packet(825,balance,verified_on,expires_on)
    # Includes full-rate input/output reserve for two replacement streams for 75 s.
    estimate+=150*(RATE_INPUT+RATE_OUTPUT)
    if estimate*1.2>2:raise ValueError('Test plus reserve exceeds allocation')
    return estimate


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--synthesizer',required=True)
    parser.add_argument('--remaining-usd',type=float,required=True)
    parser.add_argument('--verified-on',required=True)
    parser.add_argument('--expires-on',required=True)
    parser.add_argument('--owner-approved-endurance-test',action='store_true',required=True)
    args=parser.parse_args()
    estimate=preflight(args.remaining_usd,args.verified_on,args.expires_on)
    configure()
    config=replace(Config(**tomllib.loads(Path(args.config).read_text())),call=None,bridge=None,
        credit={'eligible':True,'verified_on':args.verified_on,'expires_on':args.expires_on,
                'remaining_usd':args.remaining_usd})
    config.validate()
    sources={name:probe.make_source(args.synthesizer,name) for name in probe.FIXTURES}
    # The renewal timer must fire inside a known utterance, and the boundary wait
    # must actually preserve that utterance rather than switch in convenient silence.
    for source,durations in sources.values():
        if not 535+durations[14]>540:raise ValueError('Boundary fixture does not straddle timer')
    token=subprocess.run(['/opt/homebrew/bin/gcloud','auth','print-access-token'],
        capture_output=True,text=True,check=True,timeout=20).stdout.strip()
    if not token:raise RuntimeError('Existing authorized token unavailable')
    print(json.dumps({'event':'approved_start','source_seconds':780,'hard_route_seconds':800,
        'process_supervisor_seconds':825,'switch_timer_seconds':540,'preopen_seconds':525,
        'boundary_wait_limit_seconds':30,'drain_limit_seconds':8,'transition_buffer_seconds':12,
        'allocation_usd':2,'full_audio_with_overlap_estimate_usd':round(estimate,6),
        'reserve_estimate_usd':round(estimate*1.2,6),'physical_devices_opened':False,
        'synthetic_target_text_in_memory_only':True}),flush=True)
    return asyncio.run(probe.run(config,sources,Credentials(token)))


if __name__=='__main__':
    try:raise SystemExit(main())
    except KeyboardInterrupt:print('STOPPED',flush=True);raise SystemExit(130)
    except Exception as exc:
        print(json.dumps({'event':'preflight_fault',**safe_error(exc)}),flush=True)
        raise SystemExit(2)
