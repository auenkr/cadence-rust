#!/usr/bin/env python3
"""Run History, Frontend and Matching CI comparisons in parallel with identical protocol."""
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parent
REPO='auenkr/cadence-rust'
BRANCH='t3/matching-benchmark-harness'
SOURCE='c1bbd3ae71c84a71d5a226a70b0d8b71d021cb5b'
STATE=ROOT/'evidence/service-comparison-state.json'
state=json.loads(STATE.read_text()) if STATE.exists() else {
    'source_head':SOURCE,'runs':{'history':43479770520},'protocol':{
        'rate':5,'warmup_seconds':10,'measurement_seconds':30,'repetitions':3,
        'payload_bytes':1024,'activity_ms':10},'source_ci':{
        'url':'https://github.com/auenkr/cadence-rust/actions/runs/43477045500',
        'headSha':SOURCE,'conclusion':'success'}}

def save():STATE.write_text(json.dumps(state,indent=2)+'\n')
def gh(*args):return subprocess.check_output(['gh',*map(str,args),'--repo',REPO],text=True)
def checkpoint(message):
    save()
    subprocess.run(['git','add','Benchmark/evidence'],check=True)
    if subprocess.run(['git','diff','--cached','--quiet']).returncode:
        subprocess.run(['git','commit','-qm',message],check=True)
        subprocess.run(['git','push','origin','HEAD'],check=True)

def dispatch(service):
    if service in state['runs']:return state['runs'][service]
    if service in state.get('dispatched',{}):raise RuntimeError('Dispatch already sent; recover run ID before restarting')
    known={r['databaseId'] for r in json.loads(gh('run','list','--workflow','frontend-matching-benchmark.yml','--branch',BRANCH,'--limit','20','--json','databaseId'))}
    gh('workflow','run','frontend-matching-benchmark.yml','--ref',BRANCH,
       '-f','sdk_openloop=true','-f',f'service={service}','-f','sdk_repetitions=3','-f',f'source_ref={SOURCE}')
    state.setdefault('dispatched',{})[service]=datetime.now(timezone.utc).isoformat();save()
    time.sleep(60)  # one propagation delay; no discovery polling loop
    runs=json.loads(gh('run','list','--workflow','frontend-matching-benchmark.yml','--branch',BRANCH,'--limit','20','--json','databaseId,url,headSha'))
    new=[r for r in runs if r['databaseId'] not in known]
    if len(new)!=1:raise RuntimeError('Ambiguous dispatch; saved state prevents duplicate runs')
    state['runs'][service]=new[0]['databaseId'];checkpoint(f'Record isolated {service} benchmark dispatch')
    return new[0]['databaseId']

state['execution']='parallel CI runners; serialized evidence collection'
save()
for service in ('history','frontend','matching'):
    dispatch(service)
for service in ('history','frontend','matching'):
    run_id=state['runs'][service]
    result=subprocess.run([sys.executable,str(ROOT/'watch-benchmark.py'),str(run_id),'--service',service])
    state.setdefault('watch_exit_codes',{})[service]=result.returncode;save()

summary={'source_head':SOURCE,'protocol':state['protocol'],'services':{},
         'limitations':['Short low-load comparisons; no capacity, recovery or production performance claim.',
                        'Each service has its own paired all-Go baseline on its CI runner.',
                        'Reported values are medians of per-run values over three repetitions.',
                        'CPU/memory collection includes completion drain and History audits.']}
lines=['# Isolated service smoke comparisons','',
       'Same pinned source and workload. Three fresh-database pairs per service; 10s warmup / 30s measured at 5 starts/s.','',
       '| Service | Go p99 ms | Rust p99 ms | Go CPU cores | Rust CPU cores | Go peak MiB | Rust peak MiB | Correct workflows Go / Rust |',
       '|---|---:|---:|---:|---:|---:|---:|---:|']
for service,run_id in state['runs'].items():
    folder=ROOT/'evidence'/f'{service}-benchmark-{run_id}'
    status=json.loads((folder/'status.json').read_text())
    paths=list(folder.rglob('report.json'))
    item={'run':status,'run_id':run_id};summary['services'][service]=item
    if status['conclusion']!='success' or len(paths)!=1:
        item['comparison']='unavailable';lines.append(f'| {service.title()} | Failed/incomplete | — | — | — | — | — | — |');continue
    report=json.loads(paths[0].read_text());manifest=report['manifest']
    if manifest.get('source_head')!=SOURCE or manifest.get('service')!=service or manifest['repetitions']!=3:
        raise RuntimeError('Source, service or repetition mismatch; refusing combined report')
    item['rust_binary_sha256']=manifest['rust_binary_sha256']
    for arm in ('go','rust'):
        runs=[v for k,v in report['runs'].items() if k.endswith('-'+arm)]
        if len(runs)!=3:raise RuntimeError('Incomplete repetitions')
        containers=[next(v for k,v in r['resources']['containers'].items() if k.endswith('-'+service+'-1')) for r in runs]
        item[arm]={'completion_p99_ms':statistics.median(r['latency_ms']['completion_ms']['p99'] for r in runs),
                   'average_cpu_cores':statistics.median(c['average_cpu_cores'] for c in containers),
                   'peak_memory_mib':statistics.median(c['peak_cgroup_memory_bytes']/1048576 for c in containers),
                   'completions_per_second':statistics.median(r['completions_per_second'] for r in runs),
                   'correct_workflows':sum(r['correct'] for r in runs),'errors':sum(r['errors'] for r in runs)}
    g,r=item['go'],item['rust']
    item['equal_completion_throughput_within_one_percent']=abs(g['completions_per_second']-r['completions_per_second'])<=.01*max(g['completions_per_second'],r['completions_per_second'])
    if not item['equal_completion_throughput_within_one_percent']:
        summary['limitations'].append(f"{service.title()}: observed completions/s Go {g['completions_per_second']:.2f}, Rust {r['completions_per_second']:.2f}; resource figures do not establish equal-throughput gains.")
    lines.append(f"| {service.title()} | {g['completion_p99_ms']:.2f} | {r['completion_p99_ms']:.2f} | {g['average_cpu_cores']:.4f} | {r['average_cpu_cores']:.4f} | {g['peak_memory_mib']:.2f} | {r['peak_memory_mib']:.2f} | {g['correct_workflows']} / {r['correct_workflows']} |")
(ROOT/'evidence/service-comparison.json').write_text(json.dumps(summary,indent=2)+'\n')
(ROOT/'evidence/service-comparison.md').write_text('\n'.join(lines)+'\n\n'+'\n'.join('- '+note for note in summary['limitations'])+'\n')
state['finished_utc']=datetime.now(timezone.utc).isoformat();checkpoint('Retain compact isolated History Frontend Matching comparison')
print((ROOT/'evidence/service-comparison.md').read_text(),flush=True)
raise SystemExit(0 if all(code==0 for code in state['watch_exit_codes'].values()) else 1)
