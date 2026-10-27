"""Owner-started synthetic handover/quality probe; no physical devices or saved speech."""
import argparse
import asyncio
from contextlib import suppress
from dataclasses import replace
from datetime import date
import json
import math
import queue as sync_queue
from pathlib import Path
import statistics
import subprocess
import time
import tomllib

from google import genai
from google.genai import types
from google.oauth2.credentials import Credentials
from call_translator.audio import AudioBuffers
from call_translator.config import Config
from handover_checks import FIXTURES,SLOTS,check_facts
from transition_drain import drain_ready, discard_leading_zeros
from probe_continuation import safe_error,RATE_INPUT,RATE_OUTPUT

INPUT_END=140
PREPARE_AT=45
SWITCH_AT=60
HARD_SECONDS=170
ALLOCATION=0.50
BASELINE=False
ENDURANCE=False
QUIET_SECONDS=2.0
SOURCE_QUIET_SECONDS=0.8
BOUNDARY_WAIT_SECONDS=30
DRAIN_SECONDS=8
TRANSITION_BUFFER_SECONDS=12
FIXTURE_IDS=None
FACT_GROUPS=None


def preflight(balance,verified_on,expires_on):
    if (not math.isfinite(balance) or balance<ALLOCATION
        or date.fromisoformat(verified_on)!=date.today()
        or date.fromisoformat(expires_on)<=date.today()):
        raise ValueError('Fresh eligible credit evidence required')
    # 2 inputs/outputs for the hard window + 2 overlapping tail streams for 10 s.
    estimate=(2*HARD_SECONDS+20)*(RATE_INPUT+RATE_OUTPUT)
    if estimate*1.2>ALLOCATION: raise ValueError('Allocation exceeded')
    return estimate



def validate_combined_packet(prior_seconds,prior_estimate,hard_seconds,estimate):
    if (not math.isfinite(prior_seconds) or prior_seconds<0
        or not math.isfinite(prior_estimate) or prior_estimate<0
        or prior_seconds+hard_seconds+10>180
        or prior_estimate+estimate*1.2>ALLOCATION):
        raise ValueError('Combined variants exceed the single approved time/cost packet')



def diagnostic_error(exc):
    result=safe_error(exc)
    message=str(exc).lower()
    # Canonical categories only: provider payloads may contain audio/text or secrets.
    result['recognized_error_terms']=[term for term in (
        'internal','deadline','timeout','quota','resource_exhausted','rate limit','unavailable')
        if term in message]
    return result


def active(pcm):
    values=memoryview(pcm).cast('h')
    return bool(values) and sum(v*v for v in values)/len(values)>=200**2


def fixture_index(elapsed):
    return max((i for i,s in enumerate(SLOTS) if elapsed>=s),default=0)


class Node:
    def __init__(self,config,target,credentials,generation,stats,elapsed):
        self.client=genai.Client(enterprise=True,project=config.project,location=config.location,
            credentials=credentials,http_options=types.HttpOptions(api_version='v1beta1'))
        self.cfg=types.LiveConnectConfig(response_modalities=['AUDIO'],
            translation_config=types.TranslationConfig(target_language_code=target,echo_target_language=False),
            output_audio_transcription=types.AudioTranscriptionConfig())
        self.generation=generation;self.stats=stats;self.elapsed=elapsed;self.closed=False
        self.buffers=AudioBuffers();self.text=[];self.text_events=[];self.ending=False
        if generation:
            self.buffers.playback=sync_queue.Queue(maxsize=TRANSITION_BUFFER_SECONDS*10)
        self.complete=asyncio.Event();self.receiver=None;self.context=None
        self.record={'generation':generation,'input_seconds':0.0,'output_seconds':0.0,
                     'max_backlog_seconds':0.0,'output_activity_chunks':0,
                     'usage_reports':0,'turn_complete_after_end':False,'discarded_exact_zero_seconds':0.0}
        self.stats['nodes'].append(self.record)

    async def open(self):
        started=time.monotonic()
        self.context=self.client.aio.live.connect(model=self.stats['model'],config=self.cfg)
        async with asyncio.timeout(15): self.session=await self.context.__aenter__()
        self.record['setup_seconds']=round(time.monotonic()-started,3)
        self.record['opened_elapsed']=round(self.elapsed(),3)
        self.receiver=asyncio.create_task(self.receive())
        return self

    async def receive(self):
        while True:
            any_message=False
            async for message in self.session.receive():
                any_message=True
                if message.usage_metadata is not None:self.record['usage_reports']+=1
                content=message.server_content
                if content is None:continue
                if content.output_transcription and content.output_transcription.text:
                    self.text.append(content.output_transcription.text)
                    self.text_events.append((self.elapsed(),content.output_transcription.text))
                if content.model_turn:
                    for part in content.model_turn.parts or []:
                        blob=part.inline_data
                        if blob is None or not blob.data:continue
                        if not (blob.mime_type or '').startswith('audio/pcm'):
                            raise RuntimeError('unexpected_audio_format')
                        # Received audio can be billable even if the local queue rejects it.
                        self.record['output_seconds']+=len(blob.data)/48000
                        if active(blob.data):
                            self.record['output_activity_chunks']+=1
                            self.record['last_activity_elapsed']=self.elapsed()
                        self.buffers.enqueue_output(blob.data)
                        self.record['max_backlog_seconds']=max(self.record['max_backlog_seconds'],
                            (self.buffers.output_bytes-self.buffers.played_bytes)/48000)
                if self.ending and content.turn_complete:
                    self.record['turn_complete_after_end']=True
                    self.complete.set();return
            if not any_message:
                if self.ending:
                    self.complete.set();return
                raise RuntimeError('unexpected_server_close')

    async def end_input(self):
        self.ending=True
        await self.session.send_realtime_input(audio_stream_end=True)

    async def close(self):
        if self.closed:return
        self.closed=True
        if self.receiver is not None:
            self.receiver.cancel()
            await asyncio.gather(self.receiver,return_exceptions=True)
        try:
            if self.context is not None:
                async with asyncio.timeout(5):await self.context.__aexit__(None,None,None)
        finally:
            async with asyncio.timeout(5):await self.client.aio.aclose()


def make_source(synthesizer,name):
    voice,_,texts=FIXTURES[name]
    source=bytearray(INPUT_END*32000)
    durations=[]
    for i,(slot,text) in enumerate(zip(SLOTS,texts)):
        pcm=subprocess.run([synthesizer,voice,text],capture_output=True,check=True,timeout=25).stdout
        duration=len(pcm)/32000
        if len(pcm)%2 or not 1<=duration<=20 or not active(pcm):
            raise ValueError('Invalid synthetic source')
        if slot+duration>(SLOTS[i+1] if i+1<len(SLOTS) else INPUT_END):
            raise ValueError('Fixture overlaps next slot')
        source[slot*32000:slot*32000+len(pcm)]=pcm
        durations.append(round(duration,4))
    if not BASELINE and not ENDURANCE and not SLOTS[3]+durations[3]>SWITCH_AT:
        raise ValueError('Control must actually straddle handover')
    return bytes(source),durations


async def route(config,name,source,durations,credentials,barrier,shared):
    started=None
    def elapsed():return 0.0 if started is None else time.monotonic()-started
    stats={'model':config.model,'direction':name,'status':'starting','nodes':[],
           'fixture_durations':durations,'captured_chunks':0,'sent_chunks':0,
           'input_sequence_gaps':0,'input_duplicate_chunks':0,'max_send_gap_seconds':0.0,
           'source_activity_onsets':[None]*len(SLOTS),'virtual_activity_onsets':[None]*len(SLOTS),
           'virtual_callback_block_seconds':0.1,'activity_rms_threshold':200,
           'physical_devices_opened':False,'audio_transcript_saved':False}
    nodes=[];tasks=[];queue=asyncio.Queue(maxsize=10);lock=asyncio.Lock()
    input_node=output_node=None
    source_onsets=stats['source_activity_onsets'];played_onsets=stats['virtual_activity_onsets']
    first_new_activity=None;last_old_activity=None;last_seq=-1;last_sent=None
    last_source_activity=0.0;input_end_elapsed=None
    try:
        node=Node(config,FIXTURES[name][1],credentials,0,stats,elapsed);nodes.append(node)
        await node.open();input_node=output_node=node
        await barrier.wait();started=time.monotonic()
        stats['status']='running'

        async def capture():
            nonlocal last_source_activity
            for seq,offset in enumerate(range(0,len(source),3200)):
                await asyncio.sleep(max(0,started+offset/32000-time.monotonic()))
                chunk=source[offset:offset+3200]
                i=fixture_index(offset/32000)
                if active(chunk):
                    last_source_activity=elapsed()
                    if source_onsets[i] is None:source_onsets[i]=elapsed()
                queue.put_nowait((seq,chunk))
                stats['captured_chunks']+=1
            await asyncio.sleep(max(0,started+INPUT_END-time.monotonic()))
            await queue.put(None)

        async def send():
            nonlocal last_seq,last_sent,input_end_elapsed
            while True:
                item=await queue.get()
                if item is None:
                    async with lock:await input_node.end_input()
                    input_end_elapsed=elapsed()
                    return
                seq,chunk=item
                if seq<=last_seq:stats['input_duplicate_chunks']+=1
                if seq!=last_seq+1:stats['input_sequence_gaps']+=1
                async with lock:
                    await input_node.session.send_realtime_input(audio=types.Blob(
                        data=chunk,mime_type='audio/pcm;rate=16000'))
                    input_node.record['input_seconds']+=len(chunk)/32000
                now=time.monotonic()
                if last_sent is not None:stats['max_send_gap_seconds']=max(
                    stats['max_send_gap_seconds'],now-last_sent)
                last_seq=seq;last_sent=now;stats['sent_chunks']+=1

        async def sink():
            nonlocal first_new_activity,last_old_activity
            next_at=time.monotonic()
            while True:
                await asyncio.sleep(max(0,next_at-time.monotonic()))
                next_at+=0.1
                out=bytearray(4800)
                output_node.buffers.output_callback(out,2400,None,False)
                if active(out):
                    t=elapsed();i=fixture_index(t)
                    if played_onsets[i] is None:played_onsets[i]=t
                    if output_node.generation==0:last_old_activity=t+0.1
                    elif first_new_activity is None:first_new_activity=t

        async def handover():
            nonlocal input_node,output_node
            if BASELINE:return
            await asyncio.sleep(max(0,started+PREPARE_AT-time.monotonic()))
            replacement=Node(config,FIXTURES[name][1],credentials,1,stats,elapsed)
            nodes.append(replacement);await replacement.open()
            await asyncio.sleep(max(0,started+SWITCH_AT-time.monotonic()))
            # Do not split an utterance just because the renewal timer fired.
            # Capture and both sessions remain live; no cost-saving pause.
            async with asyncio.timeout(BOUNDARY_WAIT_SECONDS):
                while elapsed()-last_source_activity < SOURCE_QUIET_SECONDS:
                    await asyncio.sleep(0.05)
            old=input_node
            async with lock:
                input_node=replacement
                stats['input_switch_elapsed']=elapsed()
                await old.end_input()
            async with asyncio.timeout(DRAIN_SECONDS):
                while not drain_ready(old,elapsed(),stats['input_switch_elapsed'],QUIET_SECONDS):
                    await asyncio.sleep(0.05)
            old.record['discarded_exact_zero_seconds']=discard_leading_zeros(old.buffers)/48000
            replacement.record['discarded_exact_zero_seconds']=discard_leading_zeros(replacement.buffers)/48000
            output_node=replacement
            stats['output_switch_elapsed']=elapsed()
            stats['old_received_equals_played']=(old.buffers.output_bytes==old.buffers.played_bytes)
            stats['old_received_equals_played_or_exact_zero_discarded']=(
                old.buffers.output_bytes==old.buffers.played_bytes+
                round(old.record['discarded_exact_zero_seconds']*48000))
            await old.close()
            stats['handover_completed']=True
            print(json.dumps({'event':'handover_completed','direction':name,
                'input_switch_elapsed':round(stats['input_switch_elapsed'],3),
                'output_switch_elapsed':round(stats['output_switch_elapsed'],3)}),flush=True)

        tasks=[asyncio.create_task(capture()),asyncio.create_task(send()),
               asyncio.create_task(sink()),asyncio.create_task(handover())]
        async with asyncio.timeout(HARD_SECONDS):
            while True:
                for task in tasks:
                    if task.done():task.result()
                for n in nodes:
                    if not n.closed and n.receiver is not None and n.receiver.done():n.receiver.result()
                sequence_done=(tasks[1].done() and input_end_elapsed is not None
                    and drain_ready(output_node,elapsed(),input_end_elapsed,QUIET_SECONDS)
                    and (BASELINE or stats.get('handover_completed',False)))
                if sequence_done or (tasks[1].done() and input_node.complete.is_set()
                    and output_node.buffers.playback_idle()
                    and (BASELINE or stats.get('handover_completed',False))):
                    stats['status']='sequence_completed';break
                await asyncio.sleep(0.05)
    except asyncio.CancelledError:
        stats['status']='cancelled';raise
    except Exception as exc:
        stats['status']='error';stats.update(diagnostic_error(exc));raise
    finally:
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        cleanup_errors=0
        for n in nodes:
            try:await n.close()
            except Exception as exc:
                stats['cleanup_error_type']=type(exc).__name__
                cleanup_errors+=1
        stats['local_cleanup_completed_without_error']=(cleanup_errors==0)
        # Transient target text for fixed synthetic fixtures only; no snippets survive.
        target=''.join(''.join(n.text) for n in nodes)
        stats['received_target_checks']=check_facts(name,target)
        if FIXTURE_IDS is not None:
            events=sorted(event for n in nodes for event in n.text_events)
            stats['fixture_target_checks']=[]
            for i,slot in enumerate(SLOTS):
                end=SLOTS[i+1] if i+1<len(SLOTS) else HARD_SECONDS
                text=''.join(text for t,text in events if slot<=t<end)
                result=check_facts(name,text)
                expected=FACT_GROUPS[name][FIXTURE_IDS[i]]
                counts={key:result['fact_matches'][key] for key in expected}
                stats['fixture_target_checks'].append({'slot':slot,'source_fixture_index':FIXTURE_IDS[i],
                    'fact_matches':counts,'flagged_for_review':[k for k,v in counts.items() if v==0],
                    'repeated_facts':[k for k,v in counts.items() if v>1]})
        target=''
        for n in nodes:
            n.text.clear()
            if hasattr(n,'text_events'):n.text_events.clear()
        stats['variant']='endurance' if ENDURANCE else ('baseline' if BASELINE else 'handover')
        stats['fixture_slots']=SLOTS
        stats['elapsed_seconds']=round(elapsed(),3)
        stats['discarded_exact_zero_seconds']=round(sum(
            n.record.get('discarded_exact_zero_seconds',0) for n in nodes),4)
        stats.pop('model',None)
        stats['virtual_played_seconds']=round(sum(n.buffers.played_bytes for n in nodes)/48000,4)
        stats['received_seconds']=round(sum(n.record['output_seconds'] for n in nodes),4)
        stats['unplayed_received_seconds']=round(stats['received_seconds']-stats['virtual_played_seconds']-
            stats['discarded_exact_zero_seconds'],4)
        stats['handover_activity_gap_seconds']=(round(first_new_activity-last_old_activity,3)
            if first_new_activity is not None and last_old_activity is not None else None)
        latencies=[round(b-a,3) if a is not None and b is not None else None
                   for a,b in zip(source_onsets,played_onsets)]
        stats['source_to_virtual_activity_seconds']=latencies
        valid=[x for x in latencies if x is not None]
        stats['activity_latency_median_seconds']=statistics.median(valid) if valid else None
        stats['activity_latency_max_seconds']=max(valid) if valid else None
        stats['source_activity_onsets']=[round(x,3) if x is not None else None for x in source_onsets]
        stats['virtual_activity_onsets']=[round(x,3) if x is not None else None for x in played_onsets]
        for n in stats['nodes']:
            for k,v in n.items():
                if isinstance(v,float):n[k]=round(v,4)
        stats['max_send_gap_seconds']=round(stats['max_send_gap_seconds'],3)
        shared.append(stats)
        print(json.dumps({'event':'route_final',**stats},sort_keys=True),flush=True)
    return stats


async def run(config,sources,credentials):
    shared=[];barrier=asyncio.Barrier(2)
    tasks=[asyncio.create_task(route(config,name,*source,credentials,barrier,shared))
           for name,source in sources.items()]
    started=time.monotonic();next_progress=0;outcome='stopped'
    try:
        async with asyncio.timeout(HARD_SECONDS+25):
            while not all(t.done() for t in tasks):
                for t in tasks:
                    if t.done():t.result()
                elapsed=time.monotonic()-started
                if elapsed>=next_progress:
                    print(json.dumps({'event':'progress','elapsed':round(elapsed,1)}),flush=True)
                    next_progress+=15
                await asyncio.wait(tasks,timeout=0.25,return_when=asyncio.FIRST_EXCEPTION)
            for t in tasks:t.result()
            outcome='sequence_completed'
    except Exception as exc:
        print(json.dumps({'event':'probe_fault',**diagnostic_error(exc)}),flush=True)
    finally:
        for t in tasks:
            if not t.done():t.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        total_in=sum(n['input_seconds'] for r in shared for n in r['nodes'])
        total_out=sum(n['output_seconds'] for r in shared for n in r['nodes'])
        print(json.dumps({'event':'probe_final','status':outcome,'elapsed_seconds':round(time.monotonic()-started,3),
            'input_seconds':round(total_in,4),'output_seconds':round(total_out,4),
            'transport_estimate_usd':round(total_in*RATE_INPUT+total_out*RATE_OUTPUT,6),
            'allocation_usd':ALLOCATION,'acoustic_or_meet_latency_verified':False,
            'general_translation_accuracy_verified':False}),flush=True)
    return 0 if outcome=='sequence_completed' else 2


def main():
    global BASELINE,INPUT_END,HARD_SECONDS,SLOTS
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',action='store_true')
    parser.add_argument('--prior-packet-seconds',type=float,default=0)
    parser.add_argument('--prior-estimate-usd',type=float,default=0)
    parser.add_argument('--config',required=True);parser.add_argument('--synthesizer',required=True)
    parser.add_argument('--verified-on',required=True);parser.add_argument('--expires-on',required=True)
    parser.add_argument('--remaining-usd',type=float,required=True)
    parser.add_argument('--owner-approved-handover-quality-test',action='store_true',required=True)
    args=parser.parse_args()
    if args.baseline:
        BASELINE=True;INPUT_END=80;HARD_SECONDS=90;SLOTS=(0,10,20,30,44,54,64)
    estimate=preflight(args.remaining_usd,args.verified_on,args.expires_on)
    validate_combined_packet(args.prior_packet_seconds,args.prior_estimate_usd,
                             HARD_SECONDS,estimate)
    config=replace(Config(**tomllib.loads(Path(args.config).read_text())),call=None,bridge=None,
        credit={'eligible':True,'verified_on':args.verified_on,'expires_on':args.expires_on,
                'remaining_usd':args.remaining_usd})
    config.validate()
    sources={name:make_source(args.synthesizer,name) for name in FIXTURES}
    token=subprocess.run(['/opt/homebrew/bin/gcloud','auth','print-access-token'],
                         capture_output=True,text=True,check=True,timeout=20).stdout.strip()
    if not token:raise RuntimeError('Existing authorized token unavailable')
    print(json.dumps({'event':'approved_start','hard_seconds':HARD_SECONDS,
        'allocation_usd':ALLOCATION,'planned_full_audio_with_tail_estimate_usd':round(estimate,6),
        'physical_devices_opened':False,'synthetic_target_text_in_memory_only':True}),flush=True)
    return asyncio.run(run(config,sources,Credentials(token)))


if __name__=='__main__':
    try:raise SystemExit(main())
    except KeyboardInterrupt:print('STOPPED',flush=True);raise SystemExit(130)
