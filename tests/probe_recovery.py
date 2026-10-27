"""Owner-started 30-minute synthetic recovery probe; no physical audio or saved text."""
import argparse
import asyncio
from bisect import bisect_right
from contextlib import suppress
from dataclasses import replace
from datetime import date
import json
import math
from pathlib import Path
import statistics
import subprocess
import time
import tomllib

from google.genai.errors import APIError
from google.genai import types
from google.oauth2.credentials import Credentials
from call_translator.audio import AudioBuffers
from call_translator.config import Config
from handover_checks import FIXTURES, check_facts
from fixture_semantics import check as check_semantics
from probe_handover import Node, active, diagnostic_error
from probe_continuation import RATE_INPUT, RATE_OUTPUT
from transition_drain import drain_ready, discard_leading_zeros, only_zero_tail

SOURCE_SECONDS = 1800
HARD_SECONDS = 1840
RENEW_AGE = 480
PREOPEN_LEAD = 15
BOUNDARY_WAIT = 20
SOURCE_QUIET = 0.8
DRAIN_LIMIT = 8
RECOVERY_LIMIT = 8
SEND_TIMEOUT = 2
MAX_RECOVERIES = 3
FORCE_AT = 720
ALLOCATION = 3.0
SPECIAL = {477:3, 717:3, 742:3, 942:3, 1197:3, 1407:3, 1662:3, 1782:6}
SCHEDULE = sorted([(slot, i%7) for i,slot in enumerate(range(2,1783,40))
                   if all(abs(slot-special)>=20 for special in SPECIAL)] + list(SPECIAL.items()))
FACT_GROUPS = ((0,3),(3,5),(5,7),(7,11),(11,13),(13,14),(14,16))


def preflight(balance, verified_on, expires_on):
    if (not math.isfinite(balance) or balance < ALLOCATION
        or date.fromisoformat(verified_on) != date.today()
        or date.fromisoformat(expires_on) <= date.today()):
        raise ValueError('Fresh eligible credit evidence required')
    # Full-rate paired hard window plus conservative overlapping-session reserve.
    estimate = (2*HARD_SECONDS+180)*(RATE_INPUT+RATE_OUTPUT)
    if estimate*1.2 > ALLOCATION:
        raise ValueError('Allocation exceeded')
    return estimate


def recoverable(exc):
    if isinstance(exc, APIError):
        return exc.code in (1000,1001,1011,429,500,502,503,504)
    return (type(exc).__name__ in ('ConnectionClosed','ConnectionClosedError','ConnectionClosedOK')
            or isinstance(exc, (ConnectionError, OSError))
            or isinstance(exc, RuntimeError) and str(exc) in
               ('unexpected_server_close','forced_disconnect'))


def make_source(synthesizer, name):
    voice, _, texts = FIXTURES[name]
    cache = []
    for text in texts:
        pcm = subprocess.run([synthesizer,voice,text],capture_output=True,check=True,timeout=25).stdout
        if len(pcm)%2 or not 1 <= len(pcm)/32000 <= 20 or not active(pcm):
            raise ValueError('Invalid synthetic source')
        cache.append(pcm)
    source = bytearray(SOURCE_SECONDS*32000)
    durations = []
    for i,(slot,fixture) in enumerate(SCHEDULE):
        pcm = cache[fixture]
        end = SCHEDULE[i+1][0] if i+1<len(SCHEDULE) else SOURCE_SECONDS
        if slot+len(pcm)/32000 > end:
            raise ValueError('Overlapping synthetic fixture')
        source[slot*32000:slot*32000+len(pcm)] = pcm
        durations.append(len(pcm)/32000)
    return bytes(source), durations


class Route:
    def __init__(self, config, name, source, durations, credentials, *, node_factory=Node):
        self.config=config; self.name=name; self.source=source; self.durations=durations
        self.credentials=credentials; self.node_factory=node_factory; self.started=None
        self.nodes=[]; self.tasks=[]; self.closing=[]; self.capture_done=False
        self.queue=asyncio.Queue(maxsize=120)
        self.send_lock=asyncio.Lock(); self.transition_lock=asyncio.Lock()
        self.input_node=self.output_node=self.prepared=None
        self.last_source_activity=0.; self.catchup=False; self.last_processed=-1
        self.last_send=None; self.last_send_at=None; self.forced=False; self.ending=False
        self.stats={'model':config.model,'direction':name,'status':'starting','nodes':[],
            'captured_chunks':0,'processed_chunks':0,'sent_chunks':0,
            'sequence_gaps':0,'duplicate_chunks':0,'skipped_exact_zero_input_chunks':0,
            'uncertain_send_chunks':0,'uncertain_fixture_slots':[],
            'max_send_gap_seconds':0.,'max_capture_backlog_seconds':0.,
            'planned_transitions':[],'recoveries':[],'forced_disconnects':0,
            'source_activity_onsets':[None]*len(SCHEDULE),
            'virtual_activity_onsets':[None]*len(SCHEDULE),
            'physical_devices_opened':False,'audio_or_transcript_saved':False}

    def elapsed(self):
        return 0. if self.started is None else time.monotonic()-self.started

    def index(self, elapsed):
        return max(0,bisect_right([slot for slot,_ in SCHEDULE],elapsed)-1)

    async def new_node(self):
        credentials=await self.credentials() if callable(self.credentials) else self.credentials
        node=self.node_factory(self.config,FIXTURES[self.name][1],credentials,
                              len(self.nodes),self.stats,self.elapsed)
        # New input begins only after output selection: no held replacement audio queue.
        node.buffers=AudioBuffers()
        self.nodes.append(node)
        await node.open()
        return node

    def emit(self, event, **values):
        print(json.dumps({'event':event,'direction':self.name,**values}),flush=True)

    async def capture(self):
        for seq,offset in enumerate(range(0,len(self.source),3200)):
            await asyncio.sleep(max(0,self.started+offset/32000-time.monotonic()))
            chunk=self.source[offset:offset+3200]; now=self.elapsed()
            if active(chunk):
                self.last_source_activity=now
                i=self.index(offset/32000)
                if self.stats['source_activity_onsets'][i] is None:
                    self.stats['source_activity_onsets'][i]=now
            self.queue.put_nowait((seq,offset/32000,chunk))
            self.stats['captured_chunks']+=1
            self.stats['max_capture_backlog_seconds']=max(
                self.stats['max_capture_backlog_seconds'],self.queue.qsize()/10)
        await asyncio.sleep(max(0,self.started+SOURCE_SECONDS-time.monotonic()))
        self.capture_done=True

    async def send(self):
        next_at=time.monotonic()
        while not self.capture_done or not self.queue.empty():
            try:item=await asyncio.wait_for(self.queue.get(),0.25)
            except TimeoutError:continue
            seq,source_at,chunk=item
            await asyncio.sleep(max(0,next_at-time.monotonic()))
            error=None; failed=None
            async with self.send_lock:
                if seq<=self.last_processed:self.stats['duplicate_chunks']+=1
                if seq!=self.last_processed+1:self.stats['sequence_gaps']+=1
                self.last_processed=seq;self.stats['processed_chunks']+=1
                age=self.elapsed()-source_at
                if self.catchup and age>0.15 and not any(chunk):
                    self.stats['skipped_exact_zero_input_chunks']+=1
                    continue
                if age<=0.15:self.catchup=False
                failed=self.input_node
                try:
                    async with asyncio.timeout(SEND_TIMEOUT):
                        await failed.session.send_realtime_input(audio=types.Blob(
                            data=chunk,mime_type='audio/pcm;rate=16000'))
                    failed.record['input_seconds']+=len(chunk)/32000
                    self.stats['sent_chunks']+=1
                    now=time.monotonic()
                    if self.last_send_at is not None:
                        self.stats['max_send_gap_seconds']=max(
                            self.stats['max_send_gap_seconds'],now-self.last_send_at)
                    self.last_send_at=now;self.last_send=seq
                except Exception as exc:
                    error=exc;self.stats['uncertain_send_chunks']+=1
            if error is not None:
                # A failed await does not prove the server rejected the input. Do not
                # replay that ambiguous chunk; flag the last phrase for human repeat.
                await self.recover(failed,error)
            next_at=max(next_at+0.1,time.monotonic())
        async with self.send_lock:
            self.ending=True
            await self.input_node.end_input()
            self.stats['input_end_elapsed']=self.elapsed()

    async def sink(self):
        next_at=time.monotonic()
        while True:
            await asyncio.sleep(max(0,next_at-time.monotonic()));next_at+=0.1
            out=bytearray(4800)
            self.output_node.buffers.output_callback(out,2400,None,False)
            if active(out):
                i=self.index(self.elapsed())
                if self.stats['virtual_activity_onsets'][i] is None:
                    self.stats['virtual_activity_onsets'][i]=self.elapsed()

    def discard_zero_output(self,node):
        amount=discard_leading_zeros(node.buffers)/48000
        node.record['discarded_exact_zero_seconds']=node.record.get('discarded_exact_zero_seconds',0)+amount

    async def retire(self,node):
        task=asyncio.create_task(node.close());self.closing.append(task)

    async def planned(self):
        async with self.transition_lock:
            old=self.input_node; replacement=self.prepared
            if self.elapsed()-old.record['opened_elapsed']<RENEW_AGE:return
            if replacement is None:replacement=await self.new_node()
            async with asyncio.timeout(BOUNDARY_WAIT):
                while self.elapsed()-self.last_source_activity<SOURCE_QUIET:await asyncio.sleep(0.05)
            async with asyncio.timeout(RECOVERY_LIMIT):
                async with self.send_lock:
                    input_pause=self.elapsed()
                    await old.end_input()
                    async with asyncio.timeout(DRAIN_LIMIT):
                        while not drain_ready(old,self.elapsed(),input_pause):await asyncio.sleep(0.05)
                    self.discard_zero_output(old)
                    self.input_node=self.output_node=replacement;self.prepared=None;self.catchup=True
                    ready=self.elapsed()
            record={'input_pause_elapsed':round(input_pause,3),'ready_elapsed':round(ready,3),
                    'pause_seconds':round(ready-input_pause,3),'generation':replacement.generation,
                    'old_received_accounted':old.buffers.output_bytes==old.buffers.played_bytes+
                         round(old.record.get('discarded_exact_zero_seconds',0)*48000)}
            self.stats['planned_transitions'].append(record)
            self.emit('planned_transition_completed',**record)
            await self.retire(old)

    async def recover(self,failed,error):
        if not recoverable(error):raise error
        began=self.elapsed()
        async with asyncio.timeout(RECOVERY_LIMIT):
            async with self.transition_lock:
                if failed is not self.input_node:return
                if len(self.stats['recoveries'])>=MAX_RECOVERIES:
                    raise RuntimeError('bounded_recovery_count_exhausted') from None
                record={'began_elapsed':round(began,3),**diagnostic_error(error),
                        'forced':self.forced and not any(r['forced'] for r in self.stats['recoveries'])}
                if began-self.last_source_activity<8:
                    slot=SCHEDULE[self.index(began)][0]
                    self.stats['uncertain_fixture_slots'].append(slot)
                self.emit('provider_recovery_started',**record,last_phrase_may_need_repeat=True)
                async with self.send_lock:
                    replacement=self.prepared
                    if replacement is None:replacement=await self.new_node()
                    # Keep playing already received old speech while reconnecting.
                    while not only_zero_tail(failed.buffers):await asyncio.sleep(0.05)
                    self.discard_zero_output(failed)
                    self.input_node=self.output_node=replacement;self.prepared=None;self.catchup=True
                record.update(ready_elapsed=round(self.elapsed(),3),
                              recovery_seconds=round(self.elapsed()-began,3),generation=replacement.generation)
                self.stats['recoveries'].append(record)
                self.emit('provider_recovery_completed',**record,last_phrase_may_need_repeat=True)
                await self.retire(failed)

    async def monitor(self):
        while True:
            await asyncio.sleep(0.05)
            if self.ending:return
            node=self.input_node
            if node.receiver.done():
                try:node.receiver.result()
                except Exception as exc:await self.recover(node,exc)
                else:await self.recover(node,RuntimeError('unexpected_server_close'))
                continue
            if self.name=='incoming' and not self.forced and self.elapsed()>=FORCE_AT:
                self.forced=True;self.stats['forced_disconnects']+=1
                self.emit('forced_public_sdk_close',elapsed=round(self.elapsed(),3))
                with suppress(TimeoutError):
                    async with asyncio.timeout(SEND_TIMEOUT):await node.session.close()
                await self.recover(node,RuntimeError('forced_disconnect'))
                continue
            age=self.elapsed()-node.record['opened_elapsed']
            if self.prepared is None and age>=RENEW_AGE-PREOPEN_LEAD:
                async with self.transition_lock:
                    self.prepared=await self.new_node()
            if age>=RENEW_AGE:
                await self.planned()

    def summarize_text(self):
        from handover_checks import RULES
        events=sorted(event for n in self.nodes for event in n.text_events)
        results=[]
        for i,(slot,fixture) in enumerate(SCHEDULE):
            end=SCHEDULE[i+1][0] if i+1<len(SCHEDULE) else self.elapsed()+1
            pieces=[text for t,text in events if slot<=t<end]
            target=''.join(pieces)
            matched=check_facts(self.name,target)['fact_matches']
            a,b=FACT_GROUPS[fixture];names=list(RULES[self.name])[a:b]
            results.append({'slot':slot,'fixture':fixture,
                'source_started':self.stats['source_activity_onsets'][i] is not None,
                'interrupted_or_uncertain':slot in self.stats['uncertain_fixture_slots'],
                'facts':{key:matched[key] for key in names},
                'semantic_reference_checks':check_semantics(self.name,fixture,pieces)})
        for n in self.nodes:n.text.clear();n.text_events.clear()
        self.stats['fixture_checks']=results

    async def run(self,barrier):
        try:
            self.input_node=self.output_node=await self.new_node()
            await barrier.wait();self.started=time.monotonic();self.stats['status']='running'
            self.tasks=[asyncio.create_task(self.capture()),asyncio.create_task(self.send()),
                        asyncio.create_task(self.sink()),asyncio.create_task(self.monitor())]
            async with asyncio.timeout(HARD_SECONDS-10):
                while True:
                    for task in self.tasks:
                        if task.done():task.result()
                    for task in self.closing:
                        if task.done():task.result()
                    if self.ending and self.input_node.receiver.done():
                        self.input_node.receiver.result()
                    if (self.tasks[1].done() and self.ending and
                        drain_ready(self.output_node,self.elapsed(),self.stats['input_end_elapsed'])):
                        self.stats['status']='sequence_completed';break
                    await asyncio.sleep(0.05)
        except asyncio.CancelledError:self.stats['status']='cancelled';raise
        except Exception as exc:
            self.stats.update(status='error',**diagnostic_error(exc));raise
        finally:
            for task in self.tasks:task.cancel()
            await asyncio.gather(*self.tasks,return_exceptions=True)
            retired_results=await asyncio.gather(*self.closing,return_exceptions=True)
            results=await asyncio.gather(*(node.close() for node in self.nodes),return_exceptions=True)
            cleanup_errors=[type(value).__name__ for value in retired_results+results
                            if isinstance(value,BaseException)]
            self.stats['cleanup_error_types']=cleanup_errors
            self.summarize_text()
            self.stats['elapsed_seconds']=round(self.elapsed(),3)
            self.stats['fixture_schedule']=SCHEDULE
            self.stats['source_to_virtual_activity_seconds']=[
                round(b-a,3) if a is not None and b is not None else None for a,b in zip(
                    self.stats['source_activity_onsets'],self.stats['virtual_activity_onsets'])]
            for key in ('source_activity_onsets','virtual_activity_onsets'):
                self.stats[key]=[round(v,3) if v is not None else None for v in self.stats[key]]
            self.stats['unplayed_received_seconds']=round(sum(n.record['output_seconds']-
                n.buffers.played_bytes/48000-n.record.get('discarded_exact_zero_seconds',0)
                for n in self.nodes),4)
            for n in self.stats['nodes']:
                for key,value in n.items():
                    if isinstance(value,float):n[key]=round(value,4)
            self.stats.pop('model',None)
            self.emit('route_final',**{k:v for k,v in self.stats.items() if k!='direction'})
        return self.stats


async def run(config,sources,credentials):
    barrier=asyncio.Barrier(2)
    routes=[Route(config,name,*source,credentials) for name,source in sources.items()]
    tasks=[asyncio.create_task(route.run(barrier)) for route in routes]
    started=time.monotonic();status='stopped';next_progress=0
    try:
        async with asyncio.timeout(HARD_SECONDS):
            while not all(task.done() for task in tasks):
                for task in tasks:
                    if task.done():task.result()
                now=time.monotonic()-started
                if now>=next_progress:
                    print(json.dumps({'event':'progress','elapsed':round(now,1),'routes':{
                        r.name:{'status':r.stats['status'],'sent':r.stats['sent_chunks'],
                                'recoveries':len(r.stats['recoveries']),
                                'planned':len(r.stats['planned_transitions'])} for r in routes}}),flush=True)
                    next_progress+=30
                await asyncio.wait(tasks,timeout=0.25,return_when=asyncio.FIRST_EXCEPTION)
            for task in tasks:task.result()
            status='sequence_completed'
    except Exception as exc:print(json.dumps({'event':'probe_fault',**diagnostic_error(exc)}),flush=True)
    finally:
        for task in tasks:
            if not task.done():task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        total_in=sum(n.record['input_seconds'] for r in routes for n in r.nodes)
        total_out=sum(n.record['output_seconds'] for r in routes for n in r.nodes)
        print(json.dumps({'event':'probe_final','status':status,'elapsed_seconds':round(time.monotonic()-started,3),
            'input_seconds':round(total_in,4),'output_seconds':round(total_out,4),
            'transport_estimate_usd':round(total_in*RATE_INPUT+total_out*RATE_OUTPUT,6),
            'allocation_usd':ALLOCATION,'physical_audio_verified':False}),flush=True)
    return 0 if status=='sequence_completed' else 2


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True);parser.add_argument('--synthesizer',required=True)
    parser.add_argument('--remaining-usd',type=float,required=True)
    parser.add_argument('--verified-on',required=True);parser.add_argument('--expires-on',required=True)
    parser.add_argument('--owner-approved-30-minute-recovery-test',action='store_true',required=True)
    args=parser.parse_args();estimate=preflight(args.remaining_usd,args.verified_on,args.expires_on)
    config=replace(Config(**tomllib.loads(Path(args.config).read_text())),call=None,bridge=None,
        credit={'eligible':True,'verified_on':args.verified_on,'expires_on':args.expires_on,
                'remaining_usd':args.remaining_usd})
    config.validate();sources={name:make_source(args.synthesizer,name) for name in FIXTURES}
    async def credentials():
        result=await asyncio.to_thread(subprocess.run,
            ['/opt/homebrew/bin/gcloud','auth','print-access-token'],capture_output=True,
            text=True,check=True,timeout=3)
        token=result.stdout.strip()
        if not token:raise RuntimeError('Existing authorized token unavailable')
        return Credentials(token)
    print(json.dumps({'event':'approved_start','source_seconds':SOURCE_SECONDS,'supervisor_seconds':HARD_SECONDS,
        'renew_age_seconds':RENEW_AGE,'recovery_limit_seconds':RECOVERY_LIMIT,'send_timeout_seconds':SEND_TIMEOUT,
        'max_recoveries_per_route':MAX_RECOVERIES,
        'forced_incoming_disconnect_at':FORCE_AT,'allocation_usd':ALLOCATION,
        'full_rate_estimate_usd':round(estimate,6),'with_reserve_usd':round(estimate*1.2,6),
        'physical_devices_opened':False,'raw_target_text_saved':False}),flush=True)
    return asyncio.run(run(config,sources,credentials))


if __name__=='__main__':
    try:raise SystemExit(main())
    except KeyboardInterrupt:print('STOPPED',flush=True);raise SystemExit(130)
    except Exception as exc:
        print(json.dumps({'event':'preflight_fault',**diagnostic_error(exc)}),flush=True)
        raise SystemExit(2)
