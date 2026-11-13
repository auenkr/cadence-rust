#!/usr/bin/env python3
"""Watch CI and dispatch service tests after the benchmark, without local builds."""
import json
from pathlib import Path
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from compact_evidence import export_compact

ROOT = Path(__file__).resolve().parent
STATE = ROOT / 'evidence/after-benchmark-tests.json'
REPO = 'auenkr/cadence-rust'
BRANCH = 't3/matching-benchmark-harness'
state = json.loads(STATE.read_text())
state.update(watch_mode='gh run watch --interval 60', schedule_enabled=False)


def save():
    STATE.write_text(json.dumps(state, indent=2) + '\n')


def gh(*args, capture=False):
    cmd = ['gh', *map(str,args)]
    print('+', ' '.join(cmd), flush=True)
    if capture:
        return subprocess.check_output(cmd, text=True)
    subprocess.run(cmd, check=True)


def info(run_id):
    return json.loads(gh('run','view',run_id,'--repo',REPO,'--json',
                         'databaseId,status,conclusion,headSha,url',capture=True))


def wait(run_id):
    gh('run','watch',run_id,'--repo',REPO,'--interval','60')
    result=info(run_id)
    if result['status']!='completed':raise RuntimeError('watch ended before completion')
    return result


def retain_raw(run_id, name):
    destination=ROOT/'evidence'/name
    destination.mkdir(parents=True,exist_ok=True)
    artifacts=json.loads(gh('api',f'repos/{REPO}/actions/runs/{run_id}/artifacts',capture=True))
    (destination/'artifacts.json').write_text(json.dumps(artifacts,indent=2)+'\n')
    if sum(a['size_in_bytes'] for a in artifacts['artifacts'])>150_000_000:
        (destination/'download-skipped.txt').write_text('Artifact total exceeds compact evidence download limit.\n')
        return
    with tempfile.TemporaryDirectory(prefix='cadence-ci-evidence-') as tmp:
        if not artifacts['artifacts']:return
        gh('run','download',run_id,'--repo',REPO,'--dir',tmp)
        export_compact(tmp,destination)


def retain(run_id,name):
    try:retain_raw(run_id,name)
    except Exception as error:
        state.setdefault('evidence_errors',[]).append({'run_id':run_id,'error':str(error)})
        save()
        print(f'Evidence download failed: {error}; continuing requested tests',flush=True)


def checkpoint(message):
    save()
    subprocess.run(['git','add','Benchmark/evidence'],check=True)
    changed=subprocess.run(['git','diff','--cached','--quiet']).returncode
    if changed:
        subprocess.run(['git','commit','-m',message],check=True)
        subprocess.run(['git','push','origin','HEAD'],check=True)


def dispatch(key, workflow, inputs=()):
    if key in state['test_runs']:return state['test_runs'][key]['databaseId']
    if key in state.get('dispatches',{}):raise RuntimeError(f'{key} already dispatched but run ID not saved; identify it before restarting')
    baseline=json.loads(gh('run','list','--repo',REPO,'--workflow',workflow,'--branch',BRANCH,
                           '--event','workflow_dispatch','--limit','20','--json','databaseId',capture=True))
    known={r['databaseId'] for r in baseline}
    args=['workflow','run',workflow,'--repo',REPO,'--ref',BRANCH]
    for value in inputs:args+=['-f',value]
    gh(*args)
    state.setdefault('dispatches',{})[key]={'workflow':workflow,'inputs':list(inputs),
                                          'at':datetime.now(timezone.utc).isoformat()}
    save()
    # One short propagation delay, not a status polling loop.
    time.sleep(5)
    runs=json.loads(gh('run','list','--repo',REPO,'--workflow',workflow,'--branch',BRANCH,
                       '--event','workflow_dispatch','--limit','20','--json',
                       'databaseId,status,conclusion,headSha,url',capture=True))
    new=[r for r in runs if r['databaseId'] not in known]
    if len(new)!=1:raise RuntimeError(f'Cannot unambiguously identify {key}; dispatch is saved; do not redispatch blindly')
    state['test_runs'][key]=new[0]
    save()
    return new[0]['databaseId']


save()
state['benchmark']=wait(43460144386)
state['benchmark_status']=state['benchmark']['status']
retain(43460144386,'benchmark-43460144386')
checkpoint('Retain completed benchmark evidence before service validation')
frontend=dispatch('frontend_crate','rust-ci.yml',('package=cadence-frontend','clean_target=true'))
matching=dispatch('matching_crate','rust-ci.yml',('package=cadence-matching','clean_target=true'))
mixed=dispatch('matching_mixed','matching-mixed-gate.yml')
checkpoint('Record Frontend and Matching CI test runs')
state['test_runs']['matching_mixed']=wait(mixed)
retain(mixed,f'matching-mixed-{mixed}')
checkpoint('Record Matching mixed gate result')
frontend_mixed=dispatch('frontend_mixed','frontend-mixed-gate.yml',('database=mysql','kafka=true'))
checkpoint('Record Frontend mixed gate dispatch')
for key,run_id in [('frontend_mixed',frontend_mixed),('frontend_crate',frontend),('matching_crate',matching)]:
    state['test_runs'][key]=wait(run_id)
    retain(run_id,f'{key}-{run_id}')
    checkpoint(f'Retain {key} CI result')
state['finished_utc']=datetime.now(timezone.utc).isoformat()
state['all_passed']=all(r['conclusion']=='success' for r in state['test_runs'].values())
checkpoint('Complete post-benchmark service test evidence')
print(json.dumps(state,indent=2),flush=True)
