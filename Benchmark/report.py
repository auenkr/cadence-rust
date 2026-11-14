#!/usr/bin/env python3
"""Fail closed on correctness and load mismatch. Never infer missing metrics."""
import json
import math
import heapq
from pathlib import Path
import sys
from datetime import datetime


def percentiles(values):
    values=sorted(values)
    return {p:values[max(0,math.ceil(len(values)*q)-1)] if values else None
            for p,q in [('p50',.5),('p95',.95),('p99',.99),('p999',.999)]}


def backlog_series(rows,begin,duration):
    # Client-observed outstanding executions; deliberately not called task backlog.
    events=[]
    for r in rows:
        start=datetime.fromisoformat(r['scheduled'].replace('Z','+00:00')).timestamp()
        end=datetime.fromisoformat(r['completed'].replace('Z','+00:00')).timestamp()
        events.extend([(start,1,r['workflow_id']),(end,-1,r['workflow_id'])])
    events.sort(key=lambda event:(event[0],-event[1]));pending={};oldest=[];cursor=0;series=[]
    for tick in range(math.ceil(duration)+1):
        now=begin+tick
        while cursor<len(events) and events[cursor][0]<=now:
            when,kind,wid=events[cursor]
            if kind==1:pending[wid]=when;heapq.heappush(oldest,(when,wid))
            else:pending.pop(wid,None)
            cursor+=1
        while oldest and oldest[0][1] not in pending:heapq.heappop(oldest)
        series.append(dict(elapsed_seconds=tick,outstanding_workflows=len(pending),
                           oldest_outstanding_seconds=now-oldest[0][0] if oldest else 0))
    return series


def persistence_summary(resource):
    samples=resource['mysql_samples']
    if len(samples)<2:raise ValueError('missing persistence counters')
    first,last=samples[0]['counters'],samples[-1]['counters']
    elapsed=samples[-1]['elapsed_seconds']-samples[0]['elapsed_seconds']
    deltas={key:last[key]-first[key] for key in ('Questions','Connections','statement_count','statement_wait_ps')}
    if elapsed<=0 or any(v<0 for v in deltas.values()):raise ValueError('persistence counter reset or invalid duration')
    return dict(queries_per_second=deltas['Questions']/elapsed,
                new_connections_per_second=deltas['Connections']/elapsed,
                peak_connections=max(s['counters']['Threads_connected'] for s in samples),
                mean_statement_wait_ms=deltas['statement_wait_ps']/deltas['statement_count']/1e9 if deltas['statement_count'] else None,
                includes_observer_queries=True)


def summarize(rows, manifest):
    measured=[r for r in rows if r['phase']=='measure']
    expected=math.ceil(manifest['rate']*manifest['duration'])
    if len([r for r in rows if r['phase']=='warmup'])!=math.ceil(manifest.get('warmup',0)*manifest['rate']):
        raise ValueError('missing warmup ledger entries')
    if len(measured)!=expected or len({r['workflow_id'] for r in rows})!=len(rows):
        raise ValueError('missing or duplicate workflow ledger entries')
    if any(not r['correct'] or not r['start_ok'] or r['generator_dropped'] or r.get('error') for r in rows):
        raise ValueError('workflow correctness or generator failure (including warmup)')
    if any(len(r['activity_ms'])!=1 or len(r['activity_schedule_to_start_ms'])!=1 or len(r['decision_schedule_to_start_ms'])<2 for r in measured):
        raise ValueError('missing task processing evidence')
    if any(not r.get('run_id') or not r.get('expected_result_sha256') or r['expected_result_sha256']!=r.get('observed_result_sha256') for r in rows):
        raise ValueError('missing run identity or result digest mismatch')
    fields=['start_rpc_ms','completion_ms','generator_lag_ms','activity_ms',
            'activity_schedule_to_start_ms','decision_schedule_to_start_ms']
    latency={key:percentiles([v for r in measured for v in (r[key] if isinstance(r[key],list) else [r[key]])]) for key in fields}
    if any(not math.isfinite(v) or v<0 for r in measured for key in fields for v in (r[key] if isinstance(r[key],list) else [r[key]])):
        raise ValueError('invalid latency')
    # Refuse a generator whose scheduling delay could hide overload.
    if latency['generator_lag_ms']['p99']>max(10,1000/manifest['rate']):
        raise ValueError('generator cannot maintain requested arrival schedule')
    begin=min(datetime.fromisoformat(r['scheduled'].replace('Z','+00:00')) for r in measured)
    end=begin.timestamp()+manifest['duration']
    completed_in_window=sum(datetime.fromisoformat(r['completed'].replace('Z','+00:00')).timestamp()<=end for r in measured)
    started_in_window=sum(datetime.fromisoformat(r['started'].replace('Z','+00:00')).timestamp()+r['start_rpc_ms']/1000<=end for r in measured)
    return dict(scheduled=expected,correct=len(measured),errors=0,arrival_rate=manifest['rate'],
                successful_starts_per_second=started_in_window/manifest['duration'],
                completions_per_second=completed_in_window/manifest['duration'],
                activity_tasks_per_second=sum(begin.timestamp()<=t/1e9<end for r in measured for t in r['activity_completion_unix_nano'])/manifest['duration'],
                outstanding_at_arrival_end=expected-completed_in_window,latency_ms=latency,
                client_outstanding_timeline=backlog_series(measured,begin.timestamp(),manifest['duration']))


def main(path):
    root=Path(path);manifest=json.loads((root/'manifest.json').read_text());runs={}
    for file in sorted(root.glob('*-samples.jsonl')):
        runs[file.stem.removesuffix('-samples')]=summarize([json.loads(line) for line in file.read_text().splitlines()],manifest)
    if len(runs)!=manifest['repetitions']*2:raise ValueError('incomplete arm matrix')
    for rep in range(manifest['repetition_offset'],manifest['repetition_offset']+manifest['repetitions']):
        for arm in ('go','rust'):
            key=f'{rep:02}-{arm}'; resource=json.loads((root/(key+'-resources.json')).read_text())
            if resource.get('schema_version')!=2:raise ValueError('unsupported resource evidence')
            containers=resource['containers']
            if len(containers)!=5 or resource['duration_seconds']<=0 or resource['samples']<2:
                raise ValueError('incomplete service/database resource collection')
            for data in containers.values():
                if data['cpu_seconds']<0 or data['peak_cgroup_memory_bytes']<=0:
                    raise ValueError('invalid resource counters')
            runs[key]['resources']=resource
            runs[key]['persistence']=persistence_summary(resource)
    pairs={}
    for rep in range(manifest['repetition_offset'],manifest['repetition_offset']+manifest['repetitions']):
        go,rust=runs[f'{rep:02}-go'],runs[f'{rep:02}-rust']
        equal=abs(go['completions_per_second']-rust['completions_per_second'])<=.01*max(go['completions_per_second'],rust['completions_per_second'])
        pairs[str(rep)]={'equal_completion_throughput_within_one_percent':equal,
                         'resource_comparison_allowed':equal,'resource_deltas':{}}
        if equal:
            if go['resources']['containers'].keys()!=rust['resources']['containers'].keys():
                raise ValueError('incomparable service sets')
            for name,g in go['resources']['containers'].items():
                r=rust['resources']['containers'][name]
                pairs[str(rep)]['resource_deltas'][name]={
                    metric+'_rust_minus_go_percent':100*(r[metric]-g[metric])/g[metric] if g[metric] else None
                    for metric in ('average_cpu_cores','peak_cgroup_memory_bytes')}
    report=dict(manifest=manifest,runs=runs,pairs=pairs,
                performance_claims_allowed=False,
                limitations=['Initial pipeline validation only. No capacity or recovery conclusion.',
                             'A hosted runner pair controls CPU affinity and limits, not external host contention.',
                             'Backlog count is client-observed outstanding workflows, not server queue length.',
                             'Resources include bounded workflow drain and history audit after arrivals stop.',
                             'Full protocol requires >=3 pairs, 600s warmup, 1800s measure and missing metric collectors.'])
    (root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    lines=[f"# {manifest.get('service', 'matching').title()} one-activity fixed-arrival benchmark",'', 'Pipeline evidence only; no migration performance claim.','',
           '| Run | starts/s | completions/s | outstanding at arrival end | completion p99 ms | activity STS p99 ms |',
           '|---|---:|---:|---:|---:|---:|']
    for key,r in runs.items():lines.append(f"| {key} | {r['successful_starts_per_second']:.3f} | {r['completions_per_second']:.3f} | {r['outstanding_at_arrival_end']} | {r['latency_ms']['completion_ms']['p99']:.3f} | {r['latency_ms']['activity_schedule_to_start_ms']['p99']:.3f} |")
    (root/'report.md').write_text('\n'.join(lines)+'\n')

if __name__=='__main__':main(sys.argv[1])
