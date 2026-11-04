#!/usr/bin/env python3
"""Frontend mixed gate (MySQL): Go History+Worker, Rust Matching and Rust Frontend (cadence-server).

No Go Frontend or Go Matching runs. Steps are recorded in OUT/steps.json; every failing step
keeps its error text, so Rust Frontend gaps are reported precisely instead of aborting the run.
Usage: run-frontend-soak.py --out DIR --rust-binary P --go-server P --go-cli P --driver P --rust-root DIR
MySQL comes from CADENCE_MYSQL_{ADDR,USER,PASSWORD} (databases cadence and cadence_visibility exist).
"""
import argparse, concurrent.futures, json, os, re, secrets, signal, socket, subprocess, sys, time
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
ap.add_argument('--es-container', help='docker container of the ES/OpenSearch node; enables the restart-resilience steps')
ap.add_argument('--s3-endpoint', help='http://host:port of MinIO; enables the S3 archival (s3store) steps')
ap.add_argument('--s3-bucket', default='cadence-archival-gate')
ap.add_argument('--gcs-endpoint', help='http://host:port of fake-gcs-server; enables the GCS archival (gstorage) steps')
ap.add_argument('--gcs-bucket', default='cadence-archival-gate-gcs')
ap.add_argument('--gcs-seed', help='path of the gcs-seed Go helper (prints Go gstorage history filenames)')
args = ap.parse_args()
if args.gcs_endpoint:
    # Go (storage client) and the Rust Frontend honour STORAGE_EMULATOR_HOST and skip authentication.
    os.environ['STORAGE_EMULATOR_HOST'] = args.gcs_endpoint.replace('http://', '')
S3_KEY, S3_SECRET = 'cadencegate', 'cadencegate-secret'
if args.s3_endpoint:
    # Go History/Worker and the Rust Frontend read AWS credentials from the environment.
    os.environ.update({'AWS_ACCESS_KEY_ID': S3_KEY, 'AWS_SECRET_ACCESS_KEY': S3_SECRET, 'AWS_REGION': 'us-east-1'})
if args.es_url and not args.kafka_broker:
    ap.error('--es-url requires --kafka-broker')
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
(work / 'rust-dynamic-config.yaml').write_text(dyn + f'\nsystem.enableGRPCOutbound:\n  - value: {str(args.rust_outbound == "grpc").lower()}\nfrontend.sendRawWorkflowHistory:\n  - value: true\n')
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
def add_gcs(cfg):
    # development.yaml already has gstorage for history only; add it where missing (visibility)
    return re.sub(r'(        dirMode: "0766"\n)(?!      gstorage)', r'\1      gstorage:\n        credentialsPath: ""\n', cfg) if args.gcs_endpoint else cfg
text = add_gcs(add_s3(text))
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
rt = add_gcs(add_s3(rt))
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
def dump_domain_rows(tag):
    """Diagnostics: persisted domain rows + notification version as Go History sees them."""
    try:
        c = os.environ.get(prefix + '_DOCKER_CONTAINER')
        if not c:
            return
        if plugin == 'mysql':
            stmts = ["SELECT HEX(id), name, shard_id FROM cadence.domains", "SELECT * FROM cadence.domain_metadata"]
            cmds = [['docker', 'exec', c, 'mysql', '-uroot', f'-p{my_pass}', '-h127.0.0.1', '-N', '-B', '-e', q] for q in stmts]
        elif plugin == 'postgres':
            stmts = ["SELECT encode(id,'hex'), name, shard_id FROM domains", "SELECT * FROM domain_metadata"]
            cmds = [['docker', 'exec', c, 'psql', '-U', my_user, '-d', 'cadence', '-At', '-c', q] for q in stmts]
        else:
            stmts = ["SELECT * FROM domains_by_name_v2 LIMIT 50", "SELECT * FROM domains LIMIT 50"]
            cmds = [['docker', 'exec', c, 'cqlsh', '-k', cassandra_keyspace, '-e', q] for q in stmts]
        out = ''
        for cmd in cmds:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            out += r.stdout + r.stderr + '\n'
        (work / f'db-domains-{tag}.log').write_text(out)
    except Exception as e:
        (work / f'db-domains-{tag}.log').write_text(repr(e))

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
    dump_domain_rows('before-sdk')
    record_services_alive('before-sdk')

    SOAK_SECONDS = int(os.environ.get('SOAK_SECONDS', '7500'))
    SOAK_RATE = os.environ.get('SOAK_RATE', '8')
    RESTART_AT = int(os.environ.get('SOAK_RESTART_AT', str(SOAK_SECONDS // 2)))
    SAMPLE = 60
    env = {**os.environ, 'FE_TCHANNEL': f'127.0.0.1:{ports["FRONTEND_PORT"]}', 'FE_DOMAIN': dom_t,
           'SOAK_DURATION': f'{SOAK_SECONDS}s', 'SOAK_RATE': SOAK_RATE, 'SOAK_DRAIN': '15m'}
    drv = subprocess.Popen([str(args.driver)], cwd=work, stdout=(work / 'soak-driver.log').open('w'), stderr=subprocess.STDOUT, env=env, start_new_session=True)
    procs['sdk-driver'] = drv
    def pid_of(name):
        return procs[name].pid
    def descend(pid):
        out = [pid]
        r = subprocess.run(['pgrep', '-P', str(pid)], capture_output=True, text=True)
        for c in r.stdout.split():
            out += descend(int(c))
        return out
    def proc_stats(name):
        p = procs[name]
        if p.poll() is not None:
            return None
        for pid in descend(p.pid):
            try:
                if os.readlink(f'/proc/{pid}/exe').endswith(os.path.basename(str(args.rust_binary))) or name.startswith('go'):
                    st = open(f'/proc/{pid}/status').read()
                    rss = int(re.search(r'VmRSS:\s+(\d+)', st).group(1))
                    thr = int(re.search(r'Threads:\s+(\d+)', st).group(1))
                    fds = len(os.listdir(f'/proc/{pid}/fd'))
                    f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
                    cpu = int(f[11]) + int(f[12])
                    return dict(pid=pid, rss_kb=rss, threads=thr, fds=fds, cpu_ticks=cpu)
            except Exception:
                continue
        return None
    samples = open(work / 'samples.jsonl', 'a')
    t0 = time.monotonic(); restarted = False; restart_ok = None
    driver_log = work / 'soak-driver.log'
    while drv.poll() is None:
        el = time.monotonic() - t0
        row = {'t': int(el), 'wall': int(time.time()), 'load1': os.getloadavg()[0]}
        for n in ['rust-frontend', 'rust-matching']:
            row[n] = proc_stats(n)
        lines = [l for l in driver_log.read_text(errors='replace').splitlines() if l.startswith('STATS ')]
        row['driver'] = json.loads(lines[-1][6:]) if lines else None
        row['fail_lines'] = sum(1 for l in driver_log.read_text(errors='replace').splitlines() if l.startswith('FAIL '))
        for n in ['rust-frontend', 'rust-matching', 'go-history-worker']:
            if procs[n].poll() is not None:
                record(f'{n}-died-during-soak', False, f'exit={procs[n].poll()} t={int(el)}')
        samples.write(json.dumps(row) + '\n'); samples.flush()
        if not restarted and el >= RESTART_AT:
            restarted = True
            kill('rust-frontend')
            record('rust-frontend-killed-mid-soak', True, f't={int(el)}')
            time.sleep(3)
            start('rust-frontend', rust_frontend(), env=frontend_env)
            restart_ok = wait_port(ports['FRONTEND_PORT'], 120, 'rust-frontend') and wait_port(ports['FRONTEND_PORT_GRPC'], 10, 'rust-frontend')
            record('rust-frontend-restarted-listening', bool(restart_ok), f't={int(time.monotonic()-t0)}')
        if el > SOAK_SECONDS + 1200:
            record('soak-driver-timeout', False, 'exceeded soak+20m')
            kill('sdk-driver')
            break
        time.sleep(SAMPLE)
    record('soak-driver-exit', drv.returncode == 0, f'exit={drv.returncode}')
    print('DRIVER TAIL\n' + '\n'.join(driver_log.read_text(errors='replace').splitlines()[-8:]), flush=True)
    record_services_alive('after-soak')
    for n in ['rust-frontend', 'rust-matching']:
        txt = (work / f'{n}.log').read_text(errors='replace')
        record(f'{n}-no-panic', 'panicked at' not in txt, '')


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
