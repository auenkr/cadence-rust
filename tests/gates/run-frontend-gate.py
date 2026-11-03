#!/usr/bin/env python3
"""Frontend mixed gate (MySQL): Go History+Worker, Rust Matching and Rust Frontend (cadence-server).

No Go Frontend or Go Matching runs. Steps are recorded in OUT/steps.json; every failing step
keeps its error text, so Rust Frontend gaps are reported precisely instead of aborting the run.
Usage: run-frontend-gate.py --out DIR --rust-binary P --go-server P --go-cli P --driver P --rust-root DIR
MySQL comes from CADENCE_MYSQL_{ADDR,USER,PASSWORD} (databases cadence and cadence_visibility exist).
"""
import argparse, json, os, re, secrets, signal, socket, subprocess, sys, time
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument('--database', choices=['mysql', 'postgres', 'cassandra'], default='mysql')
ap.add_argument('--out', type=Path, required=True)
ap.add_argument('--rust-binary', type=Path, required=True)
ap.add_argument('--go-server', type=Path, required=True)
ap.add_argument('--go-cli', type=Path, required=True)
ap.add_argument('--driver', type=Path, required=True)
ap.add_argument('--rust-root', type=Path, required=True, help='checkout of rust_ref (for rust/config/development.yaml)')
ap.add_argument('--rust-outbound', choices=['grpc', 'tchannel'], default='grpc')
ap.add_argument('--sdk-timeout-seconds', type=int, default=900,
                help='SDK driver wall-clock limit, including restart and shutdown (default: 900)')
ap.add_argument('--kafka-broker', help='host:port of a plaintext Kafka broker; enables the async workflow acceptance steps')
ap.add_argument('--kafka-container', help='docker container name of that broker (stopped/started for the broker-down step)')
ap.add_argument('--two-history', action='store_true', help='run a second Go History host and kill/restart it while SDK workflows run')
ap.add_argument('--kafka-topic', default='cadence-async-gate')
ap.add_argument('--es-url', help='http://host:port of Elasticsearch; enables advanced visibility (needs --kafka-broker for the Go indexer)')
ap.add_argument('--es-index', default='cadence-visibility-dev')
ap.add_argument('--es-version', default='v7')
ap.add_argument('--s3-endpoint', help='http://host:port of MinIO; enables the S3 archival (s3store) steps')
ap.add_argument('--s3-bucket', default='cadence-archival-gate')
args = ap.parse_args()
S3_KEY, S3_SECRET = 'cadencegate', 'cadencegate-secret'
if args.s3_endpoint:
    # Go History/Worker and the Rust Frontend read AWS credentials from the environment.
    os.environ.update({'AWS_ACCESS_KEY_ID': S3_KEY, 'AWS_SECRET_ACCESS_KEY': S3_SECRET, 'AWS_REGION': 'us-east-1'})
if args.es_url and not args.kafka_broker:
    ap.error('--es-url requires --kafka-broker')
if args.es_url and args.database == 'cassandra':
    ap.error('--es-url is supported on the SQL lanes only')
if args.kafka_broker and not args.kafka_container:
    ap.error('--kafka-broker requires --kafka-container')
ASYNC_QUEUE = 'gate-async-queue'
async_yaml = ''
if args.kafka_broker:
    async_yaml = (f'\nasyncWorkflowQueues:\n  {ASYNC_QUEUE}:\n    type: "kafka"\n    config:\n'
                  f'      connection:\n        brokers:\n          - "{args.kafka_broker}"\n      topic: "{args.kafka_topic}"\n')
if args.sdk_timeout_seconds <= 0:
    ap.error('--sdk-timeout-seconds must be positive')
root = Path(__file__).resolve().parents[2]
work = args.out.resolve(); work.mkdir(parents=True, exist_ok=True)
(work / 'config').mkdir(exist_ok=True)
plugin = args.database
prefix = 'CADENCE_' + plugin.upper()
my_addr = os.environ.get(prefix + '_ADDR', '127.0.0.1:5432' if plugin == 'postgres' else '127.0.0.1:3306')
my_user = os.environ.get(prefix + '_USER', 'postgres' if plugin == 'postgres' else 'root')
my_pass = os.environ.get(prefix + '_PASSWORD', 'cadence')

keys = ['FRONTEND_PORT', 'MATCHING_PORT', 'HISTORY_PORT', 'WORKER_PORT', 'FRONTEND_PORT_GRPC', 'MATCHING_PORT_GRPC',
        'HISTORY_PORT_GRPC', 'FRONTEND_PORT_PPROF', 'MATCHING_PORT_PPROF', 'HISTORY_PORT_PPROF', 'WORKER_PORT_PPROF',
        'RUST_MATCHING_PORT', 'RUST_MATCHING_PORT_GRPC', 'HISTORY2_PORT', 'HISTORY2_PORT_GRPC', 'HISTORY2_PORT_PPROF']
socks = [socket.socket() for _ in keys]
for s in socks:
    s.bind(('127.0.0.1', 10000 + secrets.randbelow(18000)))
ports = {k: str(s.getsockname()[1]) for k, s in zip(keys, socks)}
for s in socks:
    s.close()
(work / 'ports.json').write_text(json.dumps(ports, indent=2))

# ---- configs
dyn = (root / 'go/config/dynamicconfig/development.yaml').read_text()
if args.es_url:
    assert 'value: "db"' in dyn
    dyn = dyn.replace('value: "db"', 'value: "es"')
(work / 'dynamic-config.yaml').write_text(dyn)
(work / 'rust-dynamic-config.yaml').write_text(dyn + f'\nsystem.enableGRPCOutbound:\n  - value: {str(args.rust_outbound == "grpc").lower()}\n')
(work / 'config/base.yaml').write_text((root / 'go/config/base.yaml').read_text())
seeds = ', '.join(f'"127.0.0.1:{ports[k]}"' for k in ['FRONTEND_PORT', 'HISTORY_PORT', 'WORKER_PORT', 'RUST_MATCHING_PORT'] + (['HISTORY2_PORT'] if args.two_history else []))
text = (root / 'go/config/development.yaml').read_text()
for k, v in ports.items():
    text = re.sub(r'\$\{' + k + r':\d+\}', v, text)
text = text.replace('"127.0.0.1:7933", "127.0.0.1:7934", "127.0.0.1:7935"', seeds)
text = text.replace('localhost:7833', f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}')
text = text.replace('"config/dynamicconfig/development.yaml"', json.dumps(str(work / 'dynamic-config.yaml')))
text = text.replace('/tmp/cadence_archival/development', str(work / 'history-archive'))
text = text.replace('/tmp/cadence_vis_archival/development', str(work / 'visibility-archive'))
text = text.replace('/tmp/blobstore', str(work / 'blobstore'))
s3_provider = (f'      s3store:\n        region: us-east-1\n        endpoint: {json.dumps(args.s3_endpoint)}\n        s3ForcePathStyle: true\n'
               if args.s3_endpoint else '')
def add_s3(cfg):
    return cfg.replace('        dirMode: "0766"\n', '        dirMode: "0766"\n' + s3_provider) if s3_provider else cfg
text = add_s3(text)
text = text.rstrip('\n') + '\n' + async_yaml
(work / 'config/development.yaml').write_text(text)
if args.two_history:
    # Second History host: same cluster/ringpop, distinct History ports (sees the same persistence config).
    (work / 'config2').mkdir(exist_ok=True)
    (work / 'config2/base.yaml').write_text((work / 'config/base.yaml').read_text())
    t2 = text
    for a, b in [('HISTORY_PORT_GRPC', 'HISTORY2_PORT_GRPC'), ('HISTORY_PORT_PPROF', 'HISTORY2_PORT_PPROF'), ('HISTORY_PORT', 'HISTORY2_PORT')]:
        t2 = t2.replace(ports[a], ports[b])
    (work / 'config2/development.yaml').write_text(t2)
def sql_store(name, db, conns):
    return (f'    {name}:\n      sql:\n        pluginName: {plugin}\n        databaseName: {json.dumps(db)}\n'
            f'        connectAddr: {json.dumps(my_addr)}\n        connectProtocol: tcp\n        user: {json.dumps(my_user)}\n'
            f'        password: {json.dumps(my_pass)}\n        nShards: 1\n        maxConns: {conns}\n        maxIdleConns: {conns}\n')
if plugin == 'cassandra':
    cassandra_hosts = os.environ.get('CADENCE_CASSANDRA_CONTACT_POINTS', '127.0.0.1')
    cassandra_port = int(os.environ.get('CADENCE_CASSANDRA_PORT', '9042'))
    cassandra_keyspace = os.environ.get('CADENCE_CASSANDRA_KEYSPACE', 'cadence')
    cassandra_visibility_keyspace = os.environ.get('CADENCE_CASSANDRA_VISIBILITY_KEYSPACE', 'cadence_visibility')
    go_cassandra = text.replace('hosts: "127.0.0.1"', f'hosts: {json.dumps(cassandra_hosts)}')
    go_cassandra = go_cassandra.replace('keyspace: "cadence_visibility"', f'keyspace: {json.dumps(cassandra_visibility_keyspace)}')
    go_cassandra = go_cassandra.replace('keyspace: "cadence"', f'keyspace: {json.dumps(cassandra_keyspace)}')
    # The Go development config's Cassandra port is the default 9042; keep
    # generated configs reproducible when the disposable container uses another port.
    go_cassandra = re.sub(r'(?m)^(\s+port:) 9042$', rf'\1 {cassandra_port}', go_cassandra)
    (work / 'config/development_cassandra.yaml').write_text(go_cassandra)
    def cassandra_store(name, keyspace):
        return (f'    {name}:\n      nosql:\n        pluginName: cassandra\n'
                f'        hosts: {json.dumps(cassandra_hosts)}\n        port: {cassandra_port}\n'
                f'        keyspace: {json.dumps(keyspace)}\n        connectTimeout: 2s\n        timeout: 5s\n'
                '        consistency: LOCAL_QUORUM\n        serialConsistency: LOCAL_SERIAL\n')
    persistence = ('persistence:\n  defaultStore: cass-default\n  visibilityStore: cass-visibility\n'
                   '  numHistoryShards: 4\n  datastores:\n'
                   + cassandra_store('cass-default', cassandra_keyspace)
                   + cassandra_store('cass-visibility', cassandra_visibility_keyspace))
else:
    persistence = (f'persistence:\n  defaultStore: {plugin}-default\n  visibilityStore: {plugin}-visibility\n  numHistoryShards: 4\n  datastores:\n'
                   + sql_store(plugin + '-default', 'cadence', 20) + sql_store(plugin + '-visibility', 'cadence_visibility', 2))
    (work / f'config/development_{plugin}.yaml').write_text(persistence)
    if args.two_history:
        (work / f'config2/development_{plugin}.yaml').write_text(persistence)
if args.es_url:
    from urllib.parse import urlparse
    u = urlparse(args.es_url)
    es_yaml = (f'    es-visibility:\n      elasticsearch:\n        disableSniff: true\n        version: "{args.es_version}"\n'
               f'        url:\n          scheme: "{u.scheme}"\n          host: "{u.netloc}"\n        indices:\n          visibility: {args.es_index}\n')
    vis_topic = args.es_index
    kafka_yaml = ('kafka:\n  tls:\n    enabled: false\n  clusters:\n    test:\n      brokers:\n'
                  f'        - {args.kafka_broker}\n  topics:\n    {vis_topic}:\n      cluster: test\n    {vis_topic}-dlq:\n      cluster: test\n'
                  f'  applications:\n    visibility:\n      topic: {vis_topic}\n      dlq-topic: {vis_topic}-dlq\n')
    persistence = persistence.replace('  numHistoryShards: 4\n', '  advancedVisibilityStore: es-visibility\n  numHistoryShards: 4\n', 1) + es_yaml + kafka_yaml
    (work / f'config/development_{plugin}.yaml').write_text(persistence)
    if args.two_history:
        (work / f'config2/development_{plugin}.yaml').write_text(persistence)
rt = (args.rust_root / 'rust/config/development.yaml').read_text()
for k, v in ports.items():
    rt = re.sub(r'\$\{' + k + r':\d+\}', v, rt)
rt, n = re.subn(r'(?ms)^persistence:\n.*?(?=^ringpop:\n)', persistence, rt, count=1); assert n == 1
rt, n = re.subn(r'(?m)^  bootstrapHosts: \[.*\]$', f'  bootstrapHosts: [{seeds}]', rt, count=1); assert n == 1
rt = rt.replace('filepath: "config/dynamicconfig/development.yaml"', 'filepath: ' + json.dumps(str(work / 'rust-dynamic-config.yaml')))
rt = rt.replace('localhost:7833', f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}')
rt = rt.replace('/tmp/cadence_archival/development', str(work / 'history-archive'))
rt = rt.replace('/tmp/cadence_vis_archival/development', str(work / 'visibility-archive'))
rt = rt.replace('/tmp/blobstore', str(work / 'blobstore'))
rt = add_s3(rt)
_ms = socket.socket(); _ms.bind(('127.0.0.1', 0)); prom_port = _ms.getsockname()[1]; _ms.close()
rt, n = re.subn(r'(?m)^(  frontend:\n(?:.*\n)*?    metrics:\n)      statsd:\n        hostPort: "127.0.0.1:8125"\n        prefix: "cadence"\n',
                lambda m: m.group(1) + f'      prometheus:\n        timerType: histogram\n        listenAddress: "127.0.0.1:{prom_port}"\n', rt, count=1)
assert n == 1, 'frontend metrics block not found'
assert not re.search(r'(?m)^histograms:', rt)
# Go emits cadence_latency_ns only when histogram migration allows it (default "timer").
rt = rt.rstrip('\n') + '\nhistograms:\n  default: both\n' + async_yaml
(work / 'rust.yaml').write_text(rt)

# ---- process helpers
procs = {}
def start(name, cmd, env=None):
    log = (work / f'{name}.log').open('ab')
    procs[name] = subprocess.Popen(cmd, cwd=work, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)
    (work / f'{name}.cmd').write_text(json.dumps([str(c) for c in cmd]))
def kill(name, sig=signal.SIGKILL):
    p = procs.get(name)
    if p and p.poll() is None:
        os.killpg(p.pid, sig)
        p.wait(timeout=60)
def wait_port(port, secs, name=None):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if name and procs[name].poll() is not None:
            return False
        try:
            socket.create_connection(('127.0.0.1', int(port)), timeout=0.5).close()
            return True
        except OSError:
            time.sleep(0.5)
    return False

steps = []
def record(name, ok, detail=''):
    steps.append({'step': name, 'ok': ok, 'detail': detail[-3000:]})
    print(f'GATE {"ok  " if ok else "FAIL"} {name} {detail[-300:] if not ok else ""}', flush=True)
    (work / 'steps.json').write_text(json.dumps(steps, indent=2))
def record_services_alive(phase):
    for name in ['go-history-worker', 'rust-matching', 'rust-frontend'] + (['go-history-2'] if args.two_history else []):
        exit_code = procs[name].poll()
        record(f'{name}-alive-{phase}', exit_code is None, f'exit={exit_code}')
def run(name, cmd, timeout=120, env=None):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    out = (r.stdout + r.stderr)
    (work / f'{name}.log').write_text(out)
    record(name, r.returncode == 0, out.strip())
    return r.returncode == 0

rust_matching = lambda: [args.rust_binary, 'start', '--config', work / 'rust.yaml', '--root', work, '--services', 'matching', '--port', '0',
    '--history-grpc-port', ports['HISTORY_PORT_GRPC'], '--matching-grpc-port', ports['RUST_MATCHING_PORT_GRPC'],
    '--matching-tchannel-port', ports['RUST_MATCHING_PORT'], '--data-dir', work / 'rust-matching-data']
rust_frontend = lambda: [args.rust_binary, 'start', '--config', work / 'rust.yaml', '--root', work, '--services', 'frontend',
    '--history-grpc-port', ports['HISTORY_PORT_GRPC'], '--data-dir', work / 'rust-frontend-data']
frontend_env = {
    **os.environ,
    'CADENCE_HISTORY_TCHANNEL_ADDR': f'127.0.0.1:{ports["HISTORY_PORT"]}',
    'CADENCE_MATCHING_TCHANNEL_ADDR': f'127.0.0.1:{ports["RUST_MATCHING_PORT"]}',
}
cli_t = [args.go_cli, '--address', f'127.0.0.1:{ports["FRONTEND_PORT"]}', '--transport', 'tchannel']
cli_g = [args.go_cli, '--address', f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}', '--transport', 'grpc']
status = 0
try:
    (work / 'rust-binary.sha256').write_text(subprocess.run(['sha256sum', str(args.rust_binary)], capture_output=True, text=True).stdout)
    start('go-history-worker', [args.go_server, '--config', work / 'config', '--zone', plugin, 'start', '--auto-setup', '--services', 'history,worker'])
    history_ready = wait_port(ports['HISTORY_PORT_GRPC'], 240, 'go-history-worker')
    record('go-history-listening', history_ready)
    if not history_ready:
        raise SystemExit(1)
    go_history2 = lambda: [args.go_server, '--config', work / 'config2', '--zone', plugin, 'start', '--services', 'history']
    if args.two_history:
        start('go-history-2', go_history2())
        h2 = wait_port(ports['HISTORY2_PORT_GRPC'], 240, 'go-history-2')
        record('go-history-2-listening', h2)
        if not h2:
            raise SystemExit(1)
    # Matching opens gRPC only after Ringpop bootstrap. Launch its Frontend
    # seed before waiting for Matching readiness so both can join the ring.
    start('rust-matching', rust_matching())
    start('rust-frontend', rust_frontend(), env=frontend_env)
    matching_ready = wait_port(ports['RUST_MATCHING_PORT_GRPC'], 120, 'rust-matching')
    record('rust-matching-listening', matching_ready)
    fe_t = wait_port(ports['FRONTEND_PORT'], 120, 'rust-frontend')
    fe_g = wait_port(ports['FRONTEND_PORT_GRPC'], 10, 'rust-frontend')
    record('rust-frontend-tchannel-listening', fe_t)
    record('rust-frontend-grpc-listening', fe_g)
    if not (matching_ready and fe_t and fe_g):
        raise SystemExit(1)
    time.sleep(20)  # ring convergence
    dom_t, dom_g = 'fe-gate-t-' + secrets.token_hex(3), 'fe-gate-g-' + secrets.token_hex(3)
    run('cli-register-domain-tchannel', [*cli_t, '--domain', dom_t, 'domain', 'register', '--retention', '1'])
    run('cli-register-domain-grpc', [*cli_g, '--domain', dom_g, 'domain', 'register', '--retention', '1'])
    dom_a = dom_n = None
    if args.kafka_broker:
        dom_a, dom_n = 'fe-gate-async-' + secrets.token_hex(3), 'fe-gate-noasync-' + secrets.token_hex(3)
        run('cli-register-domain-async', [*cli_t, '--domain', dom_a, 'domain', 'register', '--retention', '1'])
        run('cli-register-domain-noasync', [*cli_t, '--domain', dom_n, 'domain', 'register', '--retention', '1'])
        cfg = json.dumps({'Enabled': True, 'PredefinedQueueName': ASYNC_QUEUE})
        ok = run('cli-admin-async-config-update-grpc', [*cli_g, '--domain', dom_a, 'admin', 'async-wf-queue', 'update', '--json', cfg])
        if not ok:
            run('cli-admin-async-config-update-tchannel', [*cli_t, '--domain', dom_a, 'admin', 'async-wf-queue', 'update', '--json', cfg])
        run('cli-admin-async-config-get', [*cli_g, '--domain', dom_a, 'admin', 'async-wf-queue', 'get'])
    time.sleep(12)  # domain cache refresh in Go History/Worker
    run('cli-describe-domain-tchannel', [*cli_t, '--domain', dom_t, 'domain', 'describe'])
    run('cli-describe-domain-grpc', [*cli_g, '--domain', dom_g, 'domain', 'describe'])
    record_services_alive('before-sdk')
    marks = work / 'marks'; marks.mkdir(exist_ok=True)
    env = {**os.environ, **({'FE_CHURN': '1'} if args.two_history else {}), 'FE_TCHANNEL': f'127.0.0.1:{ports["FRONTEND_PORT"]}', 'FE_DOMAIN': dom_t, 'FE_MARK_DIR': str(marks), 'FE_CLI': str(args.go_cli), 'FE_GRPC': f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}'}
    drv = subprocess.Popen([str(args.driver)], cwd=work, stdout=(work / 'sdk.log').open('w'), stderr=subprocess.STDOUT, env=env, start_new_session=True)
    procs['sdk-driver'] = drv
    sdk_deadline = time.monotonic() + args.sdk_timeout_seconds
    restarted = False
    churned = False
    while drv.poll() is None:
        if (marks / 'history-churn-request').exists() and not churned:
            churned = True
            kill('go-history-2')
            record('go-history-2-killed', True)
            time.sleep(20)  # shard ownership moves to the surviving host while SDK traffic continues
            record('go-history-1-alive-during-churn', procs['go-history-worker'].poll() is None)
            record('rust-frontend-alive-during-churn', procs['rust-frontend'].poll() is None)
            start('go-history-2', go_history2())
            ok = wait_port(ports['HISTORY2_PORT_GRPC'], 240, 'go-history-2')
            record('go-history-2-restarted-listening', ok)
            time.sleep(30)  # ring reconvergence, shards move back
            (marks / 'history-churn-done').write_text('x')
        if time.monotonic() >= sdk_deadline:
            record('sdk-driver-timeout', False, f'exceeded {args.sdk_timeout_seconds}s wall-clock limit')
            kill('sdk-driver')
            break
        if (marks / 'restart-request').exists() and not restarted:
            restarted = True
            kill('rust-frontend')
            record('rust-frontend-killed', True)
            time.sleep(3)
            start('rust-frontend', rust_frontend(), env=frontend_env)
            ok = wait_port(ports['FRONTEND_PORT'], 120, 'rust-frontend') and wait_port(ports['FRONTEND_PORT_GRPC'], 10, 'rust-frontend')
            record('rust-frontend-restarted-listening', ok)
            time.sleep(15)
            (marks / 'restart-done').write_text('x')
        time.sleep(1)
    for line in (work / 'sdk.log').read_text().splitlines():
        m = re.match(r'STEP (\S+) (ok|FAIL)\s*(.*)', line)
        if m:
            record('sdk-' + m.group(1), m.group(2) == 'ok', m.group(3))
    print('SDK LOG TAIL\n' + '\n'.join((work / 'sdk.log').read_text().splitlines()[-40:]), flush=True)
    record('sdk-driver-exit', drv.returncode == 0, f'exit={drv.returncode}')
    record_services_alive('after-sdk')
    # ---- metrics parity: scrape Rust Frontend Prometheus, compare names/tags with Go's metered wrapper
    # (go/service/frontend/wrappers/metered: scope tagged operation=<Scope>+domain; cadence_requests,
    # cadence_latency timer, cadence_latency_ns histogram, cadence_errors* counters).
    run('metrics-probe-start', [*cli_g, '--domain', dom_t, 'workflow', 'start', '--tl', 'metrics-probe-tl', '--wt', 'MetricsProbe', '--wid', 'metrics-probe-' + secrets.token_hex(3), '--et', '60', '--dt', '10'])
    try:
        import urllib.request
        body =urllib.request.urlopen(f'http://127.0.0.1:{prom_port}/metrics', timeout=15).read().decode()
        (work / 'frontend-metrics.prom').write_text(body)
        def has(prefix, *must):
            return any(l.startswith(prefix) and all(m in l for m in must) for l in body.splitlines())
        dom = 'domain="' + dom_t.replace('-', '_') + '"'  # Prometheus label values keep tally's sanitised form
        for nm, pre, must in [
            ('requests-start', 'cadence_requests', ('operation="StartWorkflowExecution"', dom)),
            ('requests-poll-decision', 'cadence_requests', ('operation="PollForDecisionTask"', dom)),
            ('latency-start', 'cadence_latency', ('operation="StartWorkflowExecution"', dom)),
            ('latency-ns-start', 'cadence_latency_ns', ('operation="StartWorkflowExecution"', dom)),
        ]:
            record('metrics-' + nm, has(pre, *must), '' if has(pre, *must) else f'want domain {dom_t}; ' + ' | '.join(l[:200] for l in body.splitlines() if l.startswith(pre) and 'operation="' + must[0].split('"')[1] + '"' in l)[:1500] + f' || names={sorted({l.split("{")[0].split(" ")[0] for l in body.splitlines() if l and l[0] != "#"})[:40]}')
    except Exception as e:
        record('metrics-scrape', False, repr(e))
    if args.kafka_broker:
        amarks = work / 'async-marks'; amarks.mkdir(exist_ok=True)
        aenv = {**env, 'FE_MODE': 'async', 'FE_DOMAIN': dom_a, 'FE_DOMAIN_NOASYNC': dom_n, 'FE_MARK_DIR': str(amarks)}
        adrv = subprocess.Popen([str(args.driver)], cwd=work, stdout=(work / 'sdk-async.log').open('w'), stderr=subprocess.STDOUT, env=aenv, start_new_session=True)
        procs['sdk-async-driver'] = adrv
        adeadline = time.monotonic() + 900
        done_down = done_up = done_fe = False
        while adrv.poll() is None:
            if time.monotonic() >= adeadline:
                record('sdk-async-driver-timeout', False, 'exceeded 900s')
                kill('sdk-async-driver')
                break
            if (amarks / 'broker-down-request').exists() and not done_down:
                done_down = True
                r = subprocess.run(['docker', 'stop', '-t', '5', args.kafka_container], capture_output=True, text=True)
                record('kafka-broker-stopped', r.returncode == 0, r.stdout + r.stderr)
                (amarks / 'broker-down-done').write_text('x')
            if (amarks / 'frontend-restart-request').exists() and not done_fe:
                done_fe = True
                kill('rust-frontend')
                record('async-rust-frontend-killed', True)
                time.sleep(3)
                start('rust-frontend', rust_frontend(), env=frontend_env)
                ok = wait_port(ports['FRONTEND_PORT'], 120, 'rust-frontend') and wait_port(ports['FRONTEND_PORT_GRPC'], 10, 'rust-frontend')
                record('async-rust-frontend-restarted-listening', ok)
                time.sleep(15)
                (amarks / 'frontend-restart-done').write_text('x')
            if (amarks / 'broker-up-request').exists() and not done_up:
                done_up = True
                r = subprocess.run(['docker', 'start', args.kafka_container], capture_output=True, text=True)
                broker_port = args.kafka_broker.rsplit(':', 1)[1]
                ok = r.returncode == 0 and wait_port(broker_port, 120)
                record('kafka-broker-restarted', ok, r.stdout + r.stderr)
                time.sleep(20)  # KRaft leader election / group coordinator
                (amarks / 'broker-up-done').write_text('x')
            time.sleep(1)
        for line in (work / 'sdk-async.log').read_text().splitlines():
            m = re.match(r'STEP (\S+) (ok|FAIL)\s*(.*)', line)
            if m:
                record('sdk-' + m.group(1), m.group(2) == 'ok', m.group(3))
        print('ASYNC SDK LOG TAIL\n' + '\n'.join((work / 'sdk-async.log').read_text().splitlines()[-40:]), flush=True)
        record('sdk-async-driver-exit', adrv.returncode == 0, f'exit={adrv.returncode}')
        record_services_alive('after-async-sdk')
    if args.es_url:
        # ---- advanced visibility phase: real Elasticsearch, Go indexer (Kafka) in the Go Worker, Rust Frontend reads
        import urllib.request
        def es_get(path):
            return urllib.request.urlopen(args.es_url + path, timeout=30).read().decode()
        dom_es = 'fe-gate-es-' + secrets.token_hex(3)
        run('cli-register-domain-es', [*cli_t, '--domain', dom_es, 'domain', 'register', '--retention', '1'])
        time.sleep(3)
        es_key = 'GateAdded' + secrets.token_hex(2)
        for tname, base in (('grpc', cli_g),):
            r = subprocess.run([*base, 'admin', 'cluster', 'add-search-attr', '--search_attr_key', es_key, '--search_attr_type', '1'],
                               input='y\n', capture_output=True, text=True, timeout=60)
            (work / f'cli-add-search-attr-{tname}.log').write_text(r.stdout + r.stderr)
            record(f'cli-add-search-attr-{tname}', r.returncode == 0, r.stdout + r.stderr)
        try:
            mapping = es_get(f'/{args.es_index}/_mapping')
            (work / 'es-mapping.json').write_text(mapping)
            record('es-mapping-has-added-attribute', ('Attr' in mapping and es_key in mapping), mapping[:1500])
        except Exception as e:
            record('es-mapping-has-added-attribute', False, repr(e))
        emarks = work / 'es-marks'; emarks.mkdir(exist_ok=True)
        eenv = {**env, 'FE_MODE': 'es', 'FE_DOMAIN': dom_es, 'FE_ES_KEY': es_key, 'FE_MARK_DIR': str(emarks)}
        edrv = subprocess.Popen([str(args.driver)], cwd=work, stdout=(work / 'sdk-es.log').open('w'), stderr=subprocess.STDOUT, env=eenv, start_new_session=True)
        procs['sdk-es-driver'] = edrv
        try:
            edrv.wait(timeout=900)
        except subprocess.TimeoutExpired:
            record('sdk-es-driver-timeout', False, 'exceeded 900s')
            kill('sdk-es-driver')
        for line in (work / 'sdk-es.log').read_text().splitlines():
            m = re.match(r'STEP (\S+) (ok|FAIL)\s*(.*)', line)
            if m:
                record('sdk-' + m.group(1), m.group(2) == 'ok', m.group(3))
        print('ES SDK LOG TAIL\n' + '\n'.join((work / 'sdk-es.log').read_text().splitlines()[-40:]), flush=True)
        record('sdk-es-driver-exit', edrv.returncode == 0, f'exit={edrv.returncode}')
        try:
            docs = es_get(f'/{args.es_index}/_count')
            (work / 'es-doc-count.json').write_text(docs)
            record('es-index-has-documents', json.loads(docs).get('count', 0) >= 2, docs)
        except Exception as e:
            record('es-index-has-documents', False, repr(e))
        record_services_alive('after-es-sdk')
    run('cli-list-workflows-grpc', [*cli_g, '--domain', dom_t, 'workflow', 'list', '--open'])
    run('cli-list-workflows-tchannel', [*cli_t, '--domain', dom_t, 'workflow', 'list', '--open'])
    # ---- archival phase: Go History/Worker archive (filestore), Rust Frontend registers/reads
    hist_uri, vis_uri = f'file://{work / "history-archive"}', f'file://{work / "visibility-archive"}'
    dom_ar = 'fe-gate-arch-' + secrets.token_hex(3)
    arch_flags = ['--history_archival_status', 'enabled', '--history_uri', hist_uri,
                  '--visibility_archival_status', 'enabled', '--visibility_uri', vis_uri]
    cli_ar = lambda t, *a: [*(cli_t if t else cli_g), '--domain', dom_ar, *a]
    ok_reg = run('archival-register-domain', cli_ar(True, 'domain', 'register', '--retention', '1', *arch_flags))
    if ok_reg:
        time.sleep(12)  # domain cache refresh in Go History/Worker
        d = subprocess.run(cli_ar(True, 'domain', 'describe'), capture_output=True, text=True, timeout=60)
        (work / 'archival-describe-domain.log').write_text(d.stdout + d.stderr)
        record('archival-describe-domain', d.returncode == 0 and hist_uri in d.stdout and vis_uri in d.stdout and d.stdout.count('ENABLED') >= 2, d.stdout + d.stderr)
        d = subprocess.run(cli_ar(False, 'domain', 'describe'), capture_output=True, text=True, timeout=60)
        record('archival-describe-domain-grpc', d.returncode == 0 and hist_uri in d.stdout and vis_uri in d.stdout, d.stdout + d.stderr)
        wtype = 'ArchGateType'
        arch_ids = []
        for i in range(2):
            wid = f'arch-wf-{secrets.token_hex(3)}'
            arch_ids.append(wid)
            run(f'archival-start-{i}', cli_ar(True, 'workflow', 'start', '--tl', 'arch-gate-tl', '--wt', wtype, '--wid', wid, '--et', '600', '--dt', '10'))
            run(f'archival-terminate-{i}', cli_ar(True, 'workflow', 'terminate', '--wid', wid, '--reason', 'archive-gate'))
        # Archival is asynchronous (Go worker archiver): poll the visibility archive through Rust Frontend.
        found = False
        out = ''
        end = time.monotonic() + 240
        while time.monotonic() < end and not found:
            for t in (True, False):
                r = subprocess.run(cli_ar(t, 'workflow', 'listarchived', '--query', f"WorkflowType = '{wtype}'", '--all'), capture_output=True, text=True, timeout=60)
                out = r.stdout + r.stderr
                if r.returncode == 0 and all(w in out for w in arch_ids):
                    found = True
                else:
                    found = False
                    break
            if not found:
                time.sleep(5)
        (work / 'archival-listarchived.log').write_text(out)
        record('archival-list-archived-visibility', found, out)
        r = subprocess.run(cli_ar(True, 'workflow', 'listarchived', '--query', f"WorkflowType = '{wtype}'", '--ps', '1'), input='q\n', capture_output=True, text=True, timeout=60)
        record('archival-list-archived-paged', r.returncode == 0 and any(w in r.stdout for w in arch_ids), r.stdout + r.stderr)
        # Go archives visibility at close; history is archived at retention expiry (not reachable in a gate run).
        files = sorted(str(f.relative_to(work)) for f in (work / 'visibility-archive').rglob('*') if f.is_file()) if (work / 'visibility-archive').exists() else []
        record('archival-go-visibility-wrote-archive', len(files) >= 2, str(files[:10]))
        # Domain update validation errors must match Go (state machine in Rust Frontend).
        r = subprocess.run(cli_ar(True, 'domain', 'update', '--history_uri', f'file://{work / "other-archive"}'), capture_output=True, text=True, timeout=60)
        (work / 'archival-update-uri.log').write_text(r.stdout + r.stderr)
        record('archival-update-uri-rejected', r.returncode != 0 and 'Cannot update existing archival URI' in r.stdout + r.stderr, r.stdout + r.stderr)
        r = subprocess.run(cli_ar(True, 'domain', 'update', '--history_archival_status', 'disabled'), capture_output=True, text=True, timeout=60)
        record('archival-update-disable-history', r.returncode == 0, r.stdout + r.stderr)
        r = subprocess.run(cli_ar(True, 'domain', 'describe'), capture_output=True, text=True, timeout=60)
        record('archival-describe-after-disable', r.returncode == 0 and 'HistoryArchivalStatus: DISABLED' in r.stdout and 'VisibilityArchivalStatus: ENABLED' in r.stdout and vis_uri in r.stdout, r.stdout + r.stderr)
        # Domain without archival: ListArchived must fail with Go's domain gating error.
        dom_off = 'fe-gate-noarch-' + secrets.token_hex(3)
        run('archival-register-disabled-domain', [*cli_t, '--domain', dom_off, 'domain', 'register', '--retention', '1',
            '--history_archival_status', 'disabled', '--visibility_archival_status', 'disabled'])
        time.sleep(3)
        r = subprocess.run([*cli_t, '--domain', dom_off, 'workflow', 'listarchived', '--query', "WorkflowType = 'x'"], capture_output=True, text=True, timeout=60)
        record('archival-list-archived-domain-not-configured', r.returncode != 0 and 'Domain is not configured for visibility archival' in r.stdout + r.stderr, r.stdout + r.stderr)
        # Invalid status flag handled client-side by Go CLI; invalid cluster-state errors come from the frontend above.
    # ---- S3 archival phase (MinIO): Go History/Worker archive to s3store, Rust Frontend reads/validates
    if args.s3_endpoint:
        import hashlib, hmac, urllib.request, urllib.error, urllib.parse, datetime, uuid
        def s3_call(method, path, query='', body=b''):
            u = urllib.parse.urlparse(args.s3_endpoint)
            now = datetime.datetime.now(datetime.timezone.utc)
            amz, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
            ph = hashlib.sha256(body).hexdigest()
            canon_q = '&'.join(sorted(query.split('&'))) if query else ''
            hdrs = f'host:{u.netloc}\nx-amz-content-sha256:{ph}\nx-amz-date:{amz}\n'
            canon = f'{method}\n{urllib.parse.quote(path)}\n{canon_q}\n{hdrs}\nhost;x-amz-content-sha256;x-amz-date\n{ph}'
            scope = f'{day}/us-east-1/s3/aws4_request'
            sts = f'AWS4-HMAC-SHA256\n{amz}\n{scope}\n' + hashlib.sha256(canon.encode()).hexdigest()
            k = ('AWS4' + S3_SECRET).encode()
            for part in (day, 'us-east-1', 's3', 'aws4_request'):
                k = hmac.new(k, part.encode(), hashlib.sha256).digest()
            sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
            req = urllib.request.Request(args.s3_endpoint + path + ('?' + query if query else ''), data=body or None, method=method, headers={
                'x-amz-date': amz, 'x-amz-content-sha256': ph,
                'Authorization': f'AWS4-HMAC-SHA256 Credential={S3_KEY}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}'})
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    return r.status, r.read().decode()
            except urllib.error.HTTPError as e:
                return e.code, e.read().decode()
        bucket = args.s3_bucket
        st, body = s3_call('PUT', f'/{bucket}')
        record('s3-create-bucket', st == 200, f'{st} {body}')
        s3_hist, s3_vis = f's3://{bucket}/gate-history', f's3://{bucket}/gate-visibility'
        dom_s3 = 'fe-gate-s3-' + secrets.token_hex(3)
        cli_s3 = lambda *a: [*cli_t, '--domain', dom_s3, *a]
        r = subprocess.run(cli_s3('domain', 'register', '--retention', '1', '--history_archival_status', 'enabled',
                                  '--history_uri', f's3://no-such-bucket-{secrets.token_hex(3)}/x', '--visibility_archival_status', 'disabled'),
                           capture_output=True, text=True, timeout=60)
        (work / 's3-register-missing-bucket.log').write_text(r.stdout + r.stderr)
        record('s3-register-missing-bucket-rejected', r.returncode != 0 and 'bucket does not exist' in r.stdout + r.stderr, r.stdout + r.stderr)
        r = subprocess.run(cli_s3('domain', 'register', '--retention', '1', '--history_archival_status', 'enabled', '--history_uri', 'gs://x/y',
                                  '--visibility_archival_status', 'disabled'), capture_output=True, text=True, timeout=60)
        (work / 's3-register-wrong-scheme.log').write_text(r.stdout + r.stderr)
        record('s3-register-gcs-uri-rejected', r.returncode != 0, r.stdout + r.stderr)
        ok_reg = run('s3-register-domain', cli_s3('domain', 'register', '--retention', '1', '--history_archival_status', 'enabled',
                                                  '--history_uri', s3_hist, '--visibility_archival_status', 'enabled', '--visibility_uri', s3_vis))
        if ok_reg:
            time.sleep(12)  # domain cache refresh in Go History/Worker
            d = subprocess.run(cli_s3('domain', 'describe'), capture_output=True, text=True, timeout=60)
            (work / 's3-describe-domain.log').write_text(d.stdout + d.stderr)
            record('s3-describe-domain', d.returncode == 0 and s3_hist in d.stdout and s3_vis in d.stdout, d.stdout + d.stderr)
            m = re.search(r'UUID:\s*(\S+)', d.stdout)
            wtype3, ids3 = 'S3GateType', []
            for i in range(2):
                wid = f's3-wf-{secrets.token_hex(3)}'
                ids3.append(wid)
                run(f's3-start-{i}', cli_s3('workflow', 'start', '--tl', 's3-gate-tl', '--wt', wtype3, '--wid', wid, '--et', '600', '--dt', '10'))
                run(f's3-terminate-{i}', cli_s3('workflow', 'terminate', '--wid', wid, '--reason', 's3-gate'))
            found3, out3 = False, ''
            for _ in range(24):
                r = subprocess.run(cli_s3('workflow', 'listarchived', '--query', f"WorkflowTypeName = '{wtype3}'", '--all'), capture_output=True, text=True, timeout=60)
                out3 = r.stdout + r.stderr
                if r.returncode == 0 and all(w in out3 for w in ids3):
                    found3 = True
                    break
                time.sleep(5)
            (work / 's3-listarchived.log').write_text(out3)
            record('s3-list-archived-visibility', found3, out3)
            st, listing = s3_call('GET', f'/{bucket}', 'list-type=2&prefix=gate-visibility%2F')
            keys = re.findall(r'<Key>([^<]+)</Key>', listing)
            (work / 's3-visibility-objects.log').write_text('\n'.join(keys))
            record('s3-go-visibility-wrote-archive', st == 200 and len(keys) >= 2, '\n'.join(keys[:10]))
            # Archived-only history: Go archives history at retention expiry (not reachable in a gate run), so a
            # Go-format blob (header/body JSON at <path>/<domainID>/history/<wid>/<runID>/<version>/<batch>) is uploaded.
            if m:
                hwid, hrun = 's3-archived-' + secrets.token_hex(3), str(uuid.uuid4())
                ev = lambda i, t, a: {'eventId': i, 'timestamp': 1700000000000000000 + i, 'eventType': t, 'version': 0, 'taskId': i, a[0]: a[1]}
                started = ev(1, 'WorkflowExecutionStarted', ('workflowExecutionStartedEventAttributes',
                    {'workflowType': {'name': 'S3Archived'}, 'taskList': {'name': 's3-gate-tl'}, 'executionStartToCloseTimeoutSeconds': 60,
                     'taskStartToCloseTimeoutSeconds': 10, 'originalExecutionRunId': hrun, 'firstExecutionRunId': hrun,
                     'initiator': 'CronSchedule', 'attempt': 0}))
                closed = ev(2, 'WorkflowExecutionTerminated', ('workflowExecutionTerminatedEventAttributes', {'reason': 'archived-only'}))
                for idx, (events, last) in enumerate([([started], False), ([closed], True)]):
                    blob = json.dumps({'header': {'domain_name': dom_s3, 'domain_id': m.group(1), 'workflow_id': hwid, 'run_id': hrun,
                                                  'current_event_batch_version': 0, 'close_failover_version': 0, 'is_last': last},
                                       'body': [{'events': events}]}).encode()
                    st, resp = s3_call('PUT', f'/{bucket}/gate-history/{m.group(1)}/history/{hwid}/{hrun}/0/{idx}', body=blob)
                    record(f's3-upload-history-batch-{idx}', st == 200, f'{st} {resp}')
                pfx = urllib.parse.quote(f'gate-history/{m.group(1)}/history/{hwid}/{hrun}/', safe='')
                for tag, extra in (('delim', '&delimiter=%2F'), ('plain', '')):
                    st, listing = s3_call('GET', f'/{bucket}', f'list-type=2{extra}&prefix={pfx}')
                    (work / f's3-history-list-{tag}.log').write_text(f'{st}\n{listing}')
                hist_out = work / 's3-archived-history.json'
                r = subprocess.run(cli_s3('workflow', 'show', '--wid', hwid, '--rid', hrun, '--output_filename', str(hist_out)), capture_output=True, text=True, timeout=60)
                (work / 's3-show-archived.log').write_text(r.stdout + r.stderr)
                # The CLI writes the fetched history, then calls DescribeWorkflowExecution (which Go also fails for an
                # archived-only run), so success is judged on the exported events, not the exit code.
                exported = hist_out.read_text() if hist_out.exists() else ''
                record('s3-archived-only-history', 'WorkflowExecutionStarted' in exported and 'WorkflowExecutionTerminated' in exported, exported + r.stdout + r.stderr)
                r = subprocess.run(cli_s3('workflow', 'show', '--wid', hwid, '--rid', str(uuid.uuid4())), capture_output=True, text=True, timeout=60)
                record('s3-archived-history-missing-run', r.returncode != 0, r.stdout + r.stderr)
            else:
                record('s3-describe-has-uuid', False, d.stdout)
    # ---- durable dynamic config / search attribute phase (config-store insert_config on this lane)
    def dbq(stmt):
        c = os.environ.get(prefix + '_DOCKER_CONTAINER')
        if plugin == 'mysql':
            cmd = ['docker', 'exec', c, 'mysql', '-uroot', f'-p{my_pass}', '-h127.0.0.1', '-N', '-B', '-e', stmt]
        elif plugin == 'postgres':
            cmd = ['docker', 'exec', c, 'psql', '-U', my_user, '-d', 'cadence', '-At', '-c', stmt]
        else:
            cmd = ['docker', 'exec', c, 'cqlsh', '-k', cassandra_keyspace, '-e', stmt]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return r.stdout + r.stderr
    def versions():
        q = 'SELECT row_type, version FROM ' + ('cadence.' if plugin == 'mysql' else '') + 'cluster_config'
        out = dbq(q)
        if plugin == 'cassandra':
            return [tuple(int(x) for x in re.findall(r'-?\d+', l)[:2]) for l in out.splitlines() if re.match(r'^\s*-?\d+\s*\|\s*-?\d+\s*$', l)]
        return [tuple(int(x) for x in re.split(r'[\t|]', l)) for l in out.splitlines() if re.match(r'^-?\d+[\t|]-?\d+$', l)]
    def cli_out(name, cmd, expect=None, absent=None, timeout=60):
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        out = r.stdout + r.stderr
        (work / f'{name}.log').write_text(out)
        ok = r.returncode == 0 and (expect is None or expect in out) and (absent is None or absent not in out)
        record(name, ok, out.strip())
        return ok
    def restart_frontend(cfg_path):
        kill('rust-frontend')
        start('rust-frontend', [str(c) if c != work / 'rust.yaml' else str(cfg_path) for c in rust_frontend()], env=frontend_env)
        return wait_port(ports['FRONTEND_PORT'], 120, 'rust-frontend') and wait_port(ports['FRONTEND_PORT_GRPC'], 10, 'rust-frontend')
    cs_cfg = rt.replace('client: filebased', 'client: configstore', 1)
    (work / 'rust-configstore.yaml').write_text(cs_cfg)
    record('dynconfig-restart-with-configstore', restart_frontend(work / 'rust-configstore.yaml'))
    time.sleep(10)
    key = 'frontend.visibilityMaxPageSize'
    admin_t = [*cli_t, 'admin', 'config']
    val = '{"Value":777,"Filters":[]}'
    cli_out('dyncfg-update', [*admin_t, 'update', '--name', key, '--value', val])
    cli_out('dyncfg-get', [*admin_t, 'get', '--name', key], expect='777')
    cli_out('dyncfg-list', [*admin_t, 'list', '--name', key], expect='777')
    vs1 = versions()
    record('dyncfg-db-row-written', len(vs1) >= 1, str(vs1))
    cli_out('dyncfg-restore', [*admin_t, 'restore', '--name', key])
    # Go GetDynamicConfig returns EntityNotExists ("unable to find key") once restored: the CLI must fail without the value.
    r = subprocess.run([*admin_t, 'get', '--name', key], capture_output=True, text=True, timeout=60)
    (work / 'dyncfg-get-after-restore.log').write_text(r.stdout + r.stderr)
    record('dyncfg-get-after-restore', r.returncode != 0 and '777' not in r.stdout + r.stderr, r.stdout + r.stderr)
    cli_out('dyncfg-list-after-restore', [*admin_t, 'list', '--name', key], absent='777')
    vs2 = versions()
    record('dyncfg-db-version-advanced', len(vs2) > len(vs1), f'{vs1} -> {vs2}')
    cli_out('dyncfg-update-again', [*admin_t, 'update', '--name', key, '--value', '{"Value":888,"Filters":[]}'])
    # Seed a second "Go-written" row at the next version (same Go thriftrw blob format) and require it to be readable.
    latest = max(versions(), key=lambda t: abs(t[1]))
    if plugin == 'mysql':
        dbq(f"INSERT INTO cadence.cluster_config (row_type, version, timestamp, data, data_encoding) SELECT row_type, version-1, NOW(6), data, data_encoding FROM cadence.cluster_config WHERE row_type={latest[0]} ORDER BY version LIMIT 1")
    elif plugin == 'postgres':
        dbq(f"INSERT INTO cluster_config (row_type, version, timestamp, data, data_encoding) SELECT row_type, version-1, now(), data, data_encoding FROM cluster_config WHERE row_type={latest[0]} ORDER BY version LIMIT 1")
    else:
        o = dbq(f'SELECT values, encoding FROM cluster_config WHERE row_type={latest[0]} AND version={latest[1]}')
        m = re.search(r'(0x[0-9a-fA-F]+)\s*\|\s*(\S+)', o)
        if m:
            dbq(f"INSERT INTO cluster_config (row_type, version, timestamp, values, encoding) VALUES ({latest[0]}, {latest[1] + 1}, toTimestamp(now()), {m.group(1)}, '{m.group(2)}')")
    vs3 = versions()
    record('dyncfg-seeded-go-row', len(vs3) == len(versions()) and len(vs3) > len(vs2), f'{vs2} -> {vs3}')
    record('dynconfig-restart-persistence', restart_frontend(work / 'rust-configstore.yaml'))
    time.sleep(10)
    cli_out('dyncfg-get-after-restart', [*admin_t, 'get', '--name', key], expect='888')
    cli_out('dyncfg-update-after-seed', [*admin_t, 'update', '--name', key, '--value', '{"Value":999,"Filters":[]}'])
    cli_out('dyncfg-get-after-seed-update', [*admin_t, 'get', '--name', key], expect='999')
    vs4 = versions()
    record('dyncfg-db-version-after-seed', len(vs4) > len(vs3), f'{vs3} -> {vs4}')
    sa = 'GateAttr' + secrets.token_hex(2)
    r = subprocess.run([*cli_t, 'admin', 'cluster', 'add-search-attr', '--search_attr_key', sa, '--search_attr_type', '2'],
                       input='y\n', capture_output=True, text=True, timeout=60)
    (work / 'add-search-attr.log').write_text(r.stdout + r.stderr)
    record('dyncfg-add-search-attr', r.returncode == 0, r.stdout + r.stderr)
    vs5 = versions()
    record('dyncfg-add-search-attr-persisted', len(vs5) > len(vs4), f'{vs4} -> {vs5}')
    record('dynconfig-restart-search-attr', restart_frontend(work / 'rust-configstore.yaml'))
    time.sleep(10)
    cli_out('dyncfg-list-search-attr-after-restart', [*admin_t, 'list'], expect=sa)
    (work / 'rust.yaml').write_text(rt)
    record('dynconfig-restart-filebased', restart_frontend(work / 'rust.yaml'))
    # Exercise the configured IPv6 listener with an actual gRPC request, not a TCP-only probe.
    ipv6_available = True
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
            probe.bind(('::1', 0))
    except OSError as e:
        import errno
        if e.errno in (errno.EAFNOSUPPORT, errno.EADDRNOTAVAIL, errno.EPROTONOSUPPORT):
            ipv6_available = False
            print('SKIPPED ipv6 unavailable', flush=True)
            record('cli-describe-domain-grpc-ipv6', True, 'SKIPPED ipv6 unavailable')
        else:
            raise
    if ipv6_available:
        kill('rust-frontend')
        ipv6_config = rt
        ipv6_config, n = re.subn(r'(?s)(  frontend:\n    rpc:\n.*?)(      bindOnLocalHost: true)',
                                r'\1      bindOnLocalHost: false\n      bindOnIP: "::1"', ipv6_config, count=1)
        assert n == 1, 'Frontend bind config not found'
        (work / 'rust.yaml').write_text(ipv6_config)
        start('rust-frontend', rust_frontend(), env=frontend_env)
        # CLI retries allow startup/ring convergence; any IPv6 RPC failure is fatal.
        ipv6_cli = [args.go_cli, '--address', f'[::1]:{ports["FRONTEND_PORT_GRPC"]}', '--transport', 'grpc', '--domain', dom_t, 'domain', 'describe']
        last = ''
        ok = False
        for attempt in range(30):
            r = subprocess.run(ipv6_cli, capture_output=True, text=True, timeout=20)
            last = r.stdout + r.stderr
            if r.returncode == 0:
                ok = True
                break
            if procs['rust-frontend'].poll() is not None:
                break
            time.sleep(2)
        (work / 'cli-describe-domain-grpc-ipv6.log').write_text(last)
        record('cli-describe-domain-grpc-ipv6', ok, last)

    # ---- OAuth phase: Rust Frontend with authorization.oauthAuthorizer enabled.
    # Runs after the normal phase: the Go Worker/History cannot send a JWT to the
    # Frontend, so all client calls here come from the Go CLI (--jwt) through
    # TChannel and gRPC (`cadence-authorization` header).
    import base64, hashlib
    keydir = work / 'oauth'; keydir.mkdir(exist_ok=True)
    priv, pub = keydir / 'private.pem', keydir / 'public.pem'
    subprocess.run(['openssl', 'genrsa', '-out', str(priv), '2048'], check=True, capture_output=True)
    subprocess.run(['openssl', 'rsa', '-in', str(priv), '-pubout', '-out', str(pub)], check=True, capture_output=True)
    def b64(b):
        return base64.urlsafe_b64encode(b).rstrip(b'=').decode()
    def mint(claims, key=priv):
        signing = b64(json.dumps({'alg': 'RS256', 'typ': 'JWT'}).encode()) + '.' + b64(json.dumps(claims).encode())
        sig = subprocess.run(['openssl', 'dgst', '-sha256', '-sign', str(key)], input=signing.encode(), capture_output=True, check=True).stdout
        return signing + '.' + b64(sig)
    now = int(time.time())
    t_admin = mint({'admin': True, 'iat': now, 'ttl': 600})
    t_reader = mint({'groups': 'gate-readers', 'iat': now, 'ttl': 600})
    t_expired = mint({'admin': True, 'iat': now - 7200, 'exp': now - 3600})
    other = keydir / 'other.pem'
    subprocess.run(['openssl', 'genrsa', '-out', str(other), '2048'], check=True, capture_output=True)
    t_badsig = mint({'admin': True, 'iat': now, 'ttl': 600}, other)
    oauth_cfg = (rt.rstrip('\n') + '\nauthorization:\n  oauthAuthorizer:\n    enable: true\n    maxJwtTTL: 3600\n'
                 f'    jwtCredentials:\n      algorithm: "RS256"\n      publicKey: {json.dumps(str(pub))}\n')
    (work / 'rust.yaml').write_text(oauth_cfg)
    kill('rust-frontend')
    start('rust-frontend', rust_frontend(), env=frontend_env)
    record('oauth-frontend-listening', wait_port(ports['FRONTEND_PORT'], 120, 'rust-frontend') and wait_port(ports['FRONTEND_PORT_GRPC'], 10, 'rust-frontend'))
    def cli_expect(name, cmd, ok, expect=None):
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        out = r.stdout + r.stderr
        (work / f'{name}.log').write_text(out)
        good = (r.returncode == 0) == ok and (expect is None or expect in out)
        record(name, good, out.strip()[:500])
        return good
    desc = ['--domain', dom_t, 'domain', 'describe']
    for tname, base in (('tchannel', cli_t), ('grpc', cli_g)):
        cli_expect(f'oauth-{tname}-no-token-denied', [*base, *desc], False, 'Request unauthorized')
        cli_expect(f'oauth-{tname}-garbage-token-denied', [*base, '--jwt', 'x.y.z', *desc], False, 'Request unauthorized')
        cli_expect(f'oauth-{tname}-expired-token-denied', [*base, '--jwt', t_expired, *desc], False, 'Request unauthorized')
        cli_expect(f'oauth-{tname}-wrong-key-denied', [*base, '--jwt', t_badsig, *desc], False, 'Request unauthorized')
        cli_expect(f'oauth-{tname}-admin-token-allowed', [*base, '--jwt', t_admin, *desc], True)
    # Domain-group permissions: reader group on the domain.
    cli_expect('oauth-domain-update-groups', [*cli_t, '--jwt', t_admin, '--domain', dom_t, 'domain', 'update', '--domain_data', 'READ_GROUPS=gate-readers'], True)
    time.sleep(3)
    for tname, base in (('tchannel', cli_t), ('grpc', cli_g)):
        cli_expect(f'oauth-{tname}-reader-describe-allowed', [*base, '--jwt', t_reader, *desc], True)
        cli_expect(f'oauth-{tname}-reader-write-denied', [*base, '--jwt', t_reader, '--domain', dom_t, 'workflow', 'start', '--tl', 'oauth-tl', '--wt', 'OauthProbe', '--et', '60', '--dt', '10'], False, 'Request unauthorized')
    # Admin API (Go accesscontrolled.NewAdminHandler): admin claim required.
    for tname, base in (('tchannel', cli_t), ('grpc', cli_g)):
        cli_expect(f'oauth-{tname}-admin-api-no-token-denied', [*base, 'admin', 'config', 'list'], False, 'Request unauthorized')
        cli_expect(f'oauth-{tname}-admin-api-expired-denied', [*base, '--jwt', t_expired, 'admin', 'config', 'list'], False, 'Request unauthorized')
        cli_expect(f'oauth-{tname}-admin-api-non-admin-denied', [*base, '--jwt', t_reader, 'admin', 'config', 'list'], False)
        cli_expect(f'oauth-{tname}-admin-api-admin-allowed', [*base, '--jwt', t_admin, 'admin', 'config', 'list'], True)

except SystemExit:
    pass
except Exception as e:
    record('harness-exception', False, repr(e))
finally:
    for n in list(procs):
        kill(n)
    for n in ['rust-frontend', 'rust-matching', 'go-history-worker'] + (['go-history-2'] if args.two_history else []):
        lp = work / f'{n}.log'
        print(f'--- {n}.log tail', flush=True)
        print('\n'.join(lp.read_text(errors='replace').splitlines()[-25:]) if lp.exists() else '(none)', flush=True)
    failed = [s['step'] for s in steps if not s['ok']]
    (work / 'summary.json').write_text(json.dumps({'failedSteps': failed, 'stepCount': len(steps)}, indent=2))
    print('GATE SUMMARY failed:', failed, flush=True)
    status = 1 if failed else 0
sys.exit(status)
