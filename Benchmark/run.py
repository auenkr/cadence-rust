#!/usr/bin/env python3
"""Fresh-database alternating Matching comparison. Heavy work: Actions only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'evidence' / 'run'
EVENTS = []
PROJECT = 'cadence-sdk-benchmark'


def run(command, name, env=None, timeout=600, check=True):
    command = list(map(str, command))
    path = OUT / (name + '.log')
    start = time.time()
    with path.open('w') as log:
        try:
            status = subprocess.run(command, cwd=ROOT, env=env, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            status = 124
    EVENTS.append(dict(command=shlex.join(command), exit_code=status,
                       elapsed_seconds=time.time()-start, log=path.name))
    (OUT / 'commands.json').write_text(json.dumps(EVENTS, indent=2))
    (OUT / (name + '.exit')).write_text(str(status) + '\n')
    if status and check:
        raise RuntimeError(f'{name} failed ({status}); see {path}')
    return status


def capture(command, env=None):
    return subprocess.check_output(command, cwd=ROOT, env=env, text=True).strip()


def deployments(arm):
    return {s: ('cadence-rust:ci' if s == 'matching' and arm == 'rust' else 'cadence-go:ci')
            for s in ('frontend', 'matching', 'history', 'worker')}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--rate', type=float, default=5)
    p.add_argument('--duration', type=int, default=30)
    p.add_argument('--warmup', type=int, default=10)
    p.add_argument('--repetitions', type=int, default=1)
    p.add_argument('--repetition-offset', type=int, default=0)
    p.add_argument('--payload-bytes', type=int, default=1024)
    p.add_argument('--activity-ms', type=int, default=10)
    p.add_argument('--worker-capacity', type=int, default=128)
    a = p.parse_args()
    if not 0 < a.rate <= 100000 or a.duration < 1 or a.warmup < 0 or a.repetitions < 1:
        p.error('invalid load parameters')
    OUT.mkdir(parents=True, exist_ok=True)
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 4:
        raise RuntimeError('need >=4 CPUs: generator, SDK worker, and two service CPUs')
    override = OUT / 'limits.yml'
    override.write_text('services:\n' + ''.join(
        f'  {s}:\n    cpuset: "{cpus[2]},{cpus[3]}"\n    cpus: 1.0\n    mem_limit: 1400m\n    memswap_limit: 1400m\n    pids_limit: 2048\n'
        for s in ('frontend', 'matching', 'history', 'worker')) +
        f'  mysql:\n    cpuset: "{cpus[2]},{cpus[3]}"\n')
    env = dict(os.environ, COMPOSE_FILE=os.pathsep.join((str(ROOT/'compose.yml'), str(override))))
    compose = ['docker', 'compose', '-p', PROJECT]
    manifest = dict(vars(a), head=capture(['git','rev-parse','HEAD']),
                    go_tree=capture(['git','rev-parse','HEAD:go']),
                    sdk_lock_sha256=hashlib.sha256((ROOT/'go.sum').read_bytes()).hexdigest(),
                    driver_sha256=hashlib.sha256((ROOT/'bin/driver').read_bytes()).hexdigest(),
                    cpu_affinity={'generator':cpus[0], 'sdk_worker':cpus[1], 'services_and_db':cpus[2:4]},
                    hardware=capture(['lscpu']), protocol='open-loop-one-activity-v1',
                    metrics_unavailable=['database query latency/ops/connections', 'server retry counters',
                                         'server queue backlog age'],
                    scope='Matching only; Go Frontend, History and system Worker in both arms')
    for arm, image in [('go','cadence-go:ci'),('rust','cadence-rust:ci')]:
        manifest[arm+'_image_id'] = capture(['docker','image','inspect',image,'--format','{{.Id}}'])
        manifest[arm+'_binary_sha256'] = capture(['docker','run','--rm','--entrypoint','sha256sum',image,'/usr/local/bin/cadence-server'])
    (OUT/'manifest.json').write_text(json.dumps(manifest, indent=2))
    try:
        for rep in range(a.repetition_offset, a.repetition_offset+a.repetitions):
            for arm in (['go','rust'] if rep%2==0 else ['rust','go']):
                key=f'{rep:02}-{arm}'
                expected=deployments(arm)
                arm_env=dict(env, **{'CADENCE_IMAGE_'+s.upper():im for s,im in expected.items()})
                run(compose+['down','-v','--remove-orphans'], key+'-reset', env=arm_env)
                (OUT/(key+'-compose.yml')).write_text(capture(compose+['config'],arm_env))
                sdk_worker = None
                try:
                    run(compose+['up','-d'],key+'-up',env=arm_env)
                    cli=[str(ROOT/'bin/cli'),'--address','127.0.0.1:7833','--transport','grpc','--domain','benchmark','domain']
                    for attempt in range(60):
                        if run(cli+['register','--rd','1'],key+f'-domain-{attempt}',timeout=15,check=False)==0:
                            break
                        if run(cli+['describe'],key+f'-describe-{attempt}',timeout=15,check=False)==0:
                            break
                        time.sleep(3)
                    else:
                        raise RuntimeError('domain not ready')
                    containers=[]; identities={}
                    for service,image in expected.items():
                        cid=capture(compose+['ps','-q',service],arm_env)
                        identity=json.loads(capture(['docker','inspect',cid]))[0]
                        if identity['Config']['Image']!=image: raise RuntimeError('unexpected service image')
                        run(['docker','exec',cid,'cat','/etc/cadence/config/docker.yaml'],key+'-config-'+service)
                        run(['docker','exec',cid,'cat','/etc/cadence/dynamicconfig/development.yaml'],key+'-dynamic-'+service)
                        identities[service]=identity
                        containers.append(identity['Name'].lstrip('/'))
                        run(['docker','top',cid,'-eo','pid,comm,args'],key+'-process-'+service)
                    cid=capture(compose+['ps','-q','mysql'],arm_env)
                    identities['mysql']=json.loads(capture(['docker','inspect',cid]))[0]
                    containers.append(identities['mysql']['Name'].lstrip('/'))
                    (OUT/(key+'-identities.json')).write_text(json.dumps(identities,indent=2))
                    common=[str(ROOT/'bin/driver'),'-domain','benchmark','-task-list',key,
                            '-worker-capacity',str(a.worker_capacity)]
                    worker_cmd=['taskset','-c',str(cpus[1]),*common,'-mode','worker']
                    with (OUT/(key+'-sdk-worker.log')).open('w') as log:
                        sdk_worker=subprocess.Popen(worker_cmd,stdout=log,stderr=subprocess.STDOUT)
                    time.sleep(3)
                    if sdk_worker.poll() is not None: raise RuntimeError('SDK worker died')
                    start,end,ready=[OUT/(key+'.'+suffix) for suffix in ('start','end','ready')]
                    for path in (start,end,ready):path.unlink(missing_ok=True)
                    collector_cmd=['taskset','-c',str(cpus[0]),'python3',str(ROOT/'collect_resources.py'),'--start-event',str(start),
                        '--end-event',str(end),'--ready-event',str(ready),'--event-timeout',str(a.warmup+a.duration+300),
                        '--interval','0.25','--output',str(OUT/(key+'-resources.json')),'--containers',*containers]
                    with (OUT/(key+'-collector.log')).open('w') as log:
                        collector=subprocess.Popen(collector_cmd,stdout=log,stderr=subprocess.STDOUT)
                    try:
                        run(['taskset','-c',str(cpus[0]),*common,'-mode','generate','-rate',str(a.rate),
                            '-warmup',f'{a.warmup}s','-duration',f'{a.duration}s',
                            '-payload-bytes',str(a.payload_bytes),'-activity-ms',str(a.activity_ms),
                            '-prefix',key,'-measurement-start-file',start,'-measurement-end-file',end,
                            '-collector-ready-file',ready,'-output',OUT/(key+'-samples.jsonl')],
                            key+'-generate',timeout=a.warmup+a.duration+360)
                        status=collector.wait(timeout=30)
                        if status: raise RuntimeError('resource collector failed')
                    finally:
                        if collector.poll() is None:collector.terminate();collector.wait(timeout=10)
                        EVENTS.append(dict(command=shlex.join(collector_cmd),exit_code=collector.returncode))
                finally:
                    if sdk_worker is not None:
                        sdk_worker.terminate();sdk_worker.wait(timeout=15)
                        EVENTS.append(dict(command=shlex.join(worker_cmd),exit_code=sdk_worker.returncode,termination='harness teardown'))
                    run(compose+['logs','--no-color'],key+'-services',env=arm_env,check=False)
                    run(compose+['down','-v','--remove-orphans'],key+'-cleanup',env=arm_env)
        run(['python3',str(ROOT/'report.py'),str(OUT)],'report')
    finally:
        run(compose+['down','-v','--remove-orphans'],'final-cleanup',env=env)

if __name__=='__main__':main()
