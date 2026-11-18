#!/usr/bin/env python3
"""Fresh-database alternating single-service comparison. Heavy work: Actions only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

ROOT = Path(__file__).resolve().parent
OUT = Path(os.environ.get('BENCHMARK_OUTPUT_DIR', ROOT / 'evidence' / 'run'))
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


def deployments(arm, service='matching'):
    return {s: (('cadence-rust-history:ci' if service == 'all' and s == 'history' else 'cadence-rust:ci') if (s == service or service == 'all' and s != 'worker') and arm == 'rust' else 'cadence-go:ci')
            for s in ('frontend', 'matching', 'history', 'worker')}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--service', choices=('matching', 'frontend', 'history', 'all'), default='matching')
    p.add_argument('--rate', type=float, default=5)
    p.add_argument('--duration', type=int, default=30)
    p.add_argument('--warmup', type=int, default=10)
    p.add_argument('--repetitions', type=int, default=1)
    p.add_argument('--repetition-offset', type=int, default=0)
    p.add_argument('--payload-bytes', type=int, default=1024)
    p.add_argument('--activity-ms', type=int, default=10)
    p.add_argument('--worker-capacity', type=int, default=128)
    p.add_argument('--worker-pollers', type=int, default=int(os.environ.get('WORKER_POLLERS', '0')))
    p.add_argument('--recovery', action='store_true', help='drop database TCP traffic for 3s, 10s into measurement')
    a = p.parse_args()
    if not 0 < a.rate <= 100000 or a.duration < 1 or a.warmup < 0 or a.repetitions < 1:
        p.error('invalid load parameters')
    OUT.mkdir(parents=True, exist_ok=True)
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 4:
        raise RuntimeError('need >=4 CPUs: generator, SDK worker, and two service CPUs')
    override = OUT / 'limits.yml'
    svc={s:{'cpuset':f'{cpus[2]},{cpus[3]}','cpus':1.0,'mem_limit':'1400m','memswap_limit':'1400m','pids_limit':2048}
         for s in ('frontend','matching','history','worker')}
    svc['mysql']={'cpuset':f'{cpus[2]},{cpus[3]}'}
    # Variant knobs; always identical for the Go and Rust arms.
    if os.environ.get('MYSQL_ARGS') not in (None,'','none'):svc['mysql']['command']=os.environ['MYSQL_ARGS'].split()
    if os.environ.get('MYSQL_TMPFS')=='true':svc['mysql']['tmpfs']=['/var/lib/mysql:size=1200m']
    if os.environ.get('DYNAMIC_CONFIG_EXTRA'):
        base=subprocess.check_output(['docker','run','--rm','--entrypoint','cat','cadence-go:ci','/etc/cadence/dynamicconfig/development.yaml'],text=True)
        dyn=OUT/'development.yaml'
        dyn.write_text(base.rstrip('\n')+'\n'+os.environ['DYNAMIC_CONFIG_EXTRA'].replace('\\n','\n')+'\n')
        for s in ('frontend','matching','history','worker'):svc[s]['volumes']=[f'{dyn}:/etc/cadence/dynamicconfig/development.yaml:ro']
    override = OUT / 'limits.yml'
    override.write_text(json.dumps({'services':svc},indent=1))
    env = dict(os.environ, COMPOSE_FILE=os.pathsep.join((str(ROOT/'compose.yml'), str(override))))
    compose = ['docker', 'compose', '-p', PROJECT]
    source = Path(os.environ.get('BENCHMARK_SOURCE_DIR', ROOT.parent))
    manifest = dict(vars(a), source_head=capture(['git','-C',str(source),'rev-parse','HEAD']),
                    rust_tree=capture(['git','-C',str(source),'rev-parse','HEAD:rust']), head=capture(['git','rev-parse','HEAD']),
                    go_tree=capture(['git','-C',str(source),'rev-parse','HEAD:go']),
                    sdk_lock_sha256=hashlib.sha256((ROOT/'go.sum').read_bytes()).hexdigest(),
                    driver_sha256=hashlib.sha256((ROOT/'bin/driver').read_bytes()).hexdigest(),
                    cpu_affinity={'generator':cpus[0], 'sdk_worker':cpus[1], 'services_and_db':cpus[2:4]},
                    hardware=capture(['lscpu']), protocol='open-loop-one-activity-v1',
                    variant={k:os.environ.get(k) for k in ('MYSQL_ARGS','MYSQL_TMPFS','NUM_HISTORY_SHARDS','DYNAMIC_CONFIG_EXTRA')},
                    metrics_unavailable=['database query latency percentiles', 'server retry counters',
                                         'server queue backlog age'],
                    scope='All Rust core services; Go system Worker' if a.service == 'all' else f'{a.service.title()} only; other core services and system Worker stay Go in both arms')
    for arm, image in [('go','cadence-go:ci'),('rust','cadence-rust:ci')]:
        manifest[arm+'_image_id'] = capture(['docker','image','inspect',image,'--format','{{.Id}}'])
        manifest[arm+'_binary_sha256'] = capture(['docker','run','--rm','--entrypoint','sha256sum',image,'/usr/local/bin/cadence-server'])
    if a.service == 'all':
        manifest['rust_history_binary_sha256'] = capture(['docker','run','--rm','--entrypoint','sha256sum','cadence-rust-history:ci','/usr/local/bin/cadence-server'])
    manifest['rust_toolchain'] = capture(['docker','run','--rm','--entrypoint','cat','cadence-rust:ci','/etc/cadence/cadence-toolchain.txt'])
    manifest['rust_allocator'] = 'jemalloc-default enabled for cadence-server and cadence-history; musl target from rust:1-alpine'
    (OUT/'manifest.json').write_text(json.dumps(manifest, indent=2))
    try:
        for rep in range(a.repetition_offset, a.repetition_offset+a.repetitions):
            for arm in (['go','rust'] if rep%2==0 else ['rust','go']):
                key=f'{rep:02}-{arm}'
                expected=deployments(arm,a.service)
                arm_env=dict(env, **{'CADENCE_IMAGE_'+s.upper():im for s,im in expected.items()})
                run(compose+['down','-v','--remove-orphans'], key+'-reset', env=arm_env)
                (OUT/(key+'-compose.yml')).write_text(capture(compose+['config'],arm_env))
                sdk_worker = None
                try:
                    # Register the benchmark domain before the system Worker creates its domains.
                    # Concurrent Go domain registrations can race on metadata versions.
                    run(compose+['up','-d','mysql','history','matching','frontend'],key+'-up',env=arm_env)
                    cli=[str(ROOT/'bin/cli'),'--address','127.0.0.1:7833','--transport','grpc','--domain','benchmark','domain']
                    for attempt in range(60):
                        if run(cli+['register','--rd','1'],key+f'-domain-{attempt}',timeout=15,check=False)==0:
                            break
                        if run(cli+['describe'],key+f'-describe-{attempt}',timeout=15,check=False)==0:
                            break
                        time.sleep(3)
                    else:
                        raise RuntimeError('domain not ready')
                    run(compose+['up','-d','worker'],key+'-worker-up',env=arm_env)
                    containers=[]; identities={}
                    for service,image in expected.items():
                        cid=capture(compose+['ps','-a','-q',service],arm_env)
                        if not cid: raise RuntimeError(f'{service} container missing; see {key}-services.log')
                        identity=json.loads(capture(['docker','inspect',cid]))[0]
                        if not identity['State']['Running']:
                            raise RuntimeError(f'{service} exited ({identity["State"]["ExitCode"]}); see {key}-services.log')
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
                            '-worker-capacity',str(a.worker_capacity),'-worker-pollers',str(a.worker_pollers)]
                    worker_cmd=['taskset','-c',str(cpus[1]),*common,'-mode','worker']
                    with (OUT/(key+'-sdk-worker.log')).open('w') as log:
                        sdk_worker=subprocess.Popen(worker_cmd,stdout=log,stderr=subprocess.STDOUT)
                    time.sleep(3)
                    if sdk_worker.poll() is not None: raise RuntimeError('SDK worker died')
                    start,end,ready=[OUT/(key+'.'+suffix) for suffix in ('start','end','ready')]
                    for path in (start,end,ready):path.unlink(missing_ok=True)
                    collector_cmd=['taskset','-c',str(cpus[0]),'python3',str(ROOT/'collect_resources.py'),'--start-event',str(start),
                        '--end-event',str(end),'--ready-event',str(ready),'--event-timeout',str(a.warmup+a.duration+300),
                        '--mysql-container',identities['mysql']['Name'].lstrip('/'),'--interval','0.25','--output',str(OUT/(key+'-resources.json')),'--containers',*containers]
                    with (OUT/(key+'-collector.log')).open('w') as log:
                        collector=subprocess.Popen(collector_cmd,stdout=log,stderr=subprocess.STDOUT)
                    fault = None
                    if a.recovery:
                        fault_cmd=['python3',str(ROOT/'fault.py'),'--ready',str(ready),'--output',str(OUT/(key+'-fault.json')),'--mysql-pid',str(identities['mysql']['State']['Pid'])]
                        fault_log=(OUT/(key+'-fault.log')).open('w')
                        fault=subprocess.Popen(fault_cmd,stdout=fault_log,stderr=subprocess.STDOUT)
                    def proc_cpu(pid):
                        f=open(f'/proc/{pid}/stat').read().rsplit(')',1)[1].split()
                        return (int(f[11])+int(f[12]))/os.sysconf('SC_CLK_TCK')
                    w0,t0=proc_cpu(sdk_worker.pid),time.monotonic()
                    try:
                        run(['taskset','-c',str(cpus[0]),*common,'-mode','generate','-rate',str(a.rate),
                            '-warmup',f'{a.warmup}s','-duration',f'{a.duration}s',
                            '-payload-bytes',str(a.payload_bytes),'-activity-ms',str(a.activity_ms),
                            '-prefix',key,'-measurement-start-file',start,'-measurement-end-file',end,
                            '-collector-ready-file',ready,'-output',OUT/(key+'-samples.jsonl')],
                            key+'-generate',timeout=a.warmup+a.duration+360)
                        w1,t1=proc_cpu(sdk_worker.pid),time.monotonic()
                        (OUT/(key+'-sdkworker.json')).write_text(json.dumps({'cpu_seconds':w1-w0,'elapsed_seconds':t1-t0,'average_cpu_cores':(w1-w0)/(t1-t0),'pollers':a.worker_pollers}))
                        if fault is not None and fault.wait(timeout=30): raise RuntimeError('fault injection failed')
                        status=collector.wait(timeout=30)
                        if status: raise RuntimeError('resource collector failed: '+(OUT/(key+'-collector.log')).read_text(errors='replace')[-1500:])
                    finally:
                        if fault is not None:
                            if fault.poll() is None: fault.terminate();fault.wait(timeout=15)
                            fault_log.close()
                        if collector.poll() is None:collector.terminate();collector.wait(timeout=10)
                        EVENTS.append(dict(command=shlex.join(collector_cmd),exit_code=collector.returncode))
                except Exception as error:
                    (OUT/(key+'-failure.json')).write_text(json.dumps({'error':str(error)},indent=2)+'\n')
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
