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
ap.add_argument('--kafka-topic', default='cadence-async-gate')
args = ap.parse_args()
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
        'RUST_MATCHING_PORT', 'RUST_MATCHING_PORT_GRPC']
socks = [socket.socket() for _ in keys]
for s in socks:
    s.bind(('127.0.0.1', 10000 + secrets.randbelow(18000)))
ports = {k: str(s.getsockname()[1]) for k, s in zip(keys, socks)}
for s in socks:
    s.close()
(work / 'ports.json').write_text(json.dumps(ports, indent=2))

# ---- configs
dyn = (root / 'go/config/dynamicconfig/development.yaml').read_text()
(work / 'dynamic-config.yaml').write_text(dyn)
(work / 'rust-dynamic-config.yaml').write_text(dyn + f'\nsystem.enableGRPCOutbound:\n  - value: {str(args.rust_outbound == "grpc").lower()}\n')
(work / 'config/base.yaml').write_text((root / 'go/config/base.yaml').read_text())
seeds = ', '.join(f'"127.0.0.1:{ports[k]}"' for k in ['FRONTEND_PORT', 'HISTORY_PORT', 'WORKER_PORT', 'RUST_MATCHING_PORT'])
text = (root / 'go/config/development.yaml').read_text()
for k, v in ports.items():
    text = re.sub(r'\$\{' + k + r':\d+\}', v, text)
text = text.replace('"127.0.0.1:7933", "127.0.0.1:7934", "127.0.0.1:7935"', seeds)
text = text.replace('localhost:7833', f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}')
text = text.replace('"config/dynamicconfig/development.yaml"', json.dumps(str(work / 'dynamic-config.yaml')))
text = text.replace('/tmp/cadence_archival/development', str(work / 'history-archive'))
text = text.replace('/tmp/cadence_vis_archival/development', str(work / 'visibility-archive'))
text = text.replace('/tmp/blobstore', str(work / 'blobstore'))
text = text.rstrip('\n') + '\n' + async_yaml
(work / 'config/development.yaml').write_text(text)
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
rt = rt.rstrip('\n') + '\n' + async_yaml
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
    for name in ['go-history-worker', 'rust-matching', 'rust-frontend']:
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
    env = {**os.environ, 'FE_TCHANNEL': f'127.0.0.1:{ports["FRONTEND_PORT"]}', 'FE_DOMAIN': dom_t, 'FE_MARK_DIR': str(marks), 'FE_CLI': str(args.go_cli), 'FE_GRPC': f'127.0.0.1:{ports["FRONTEND_PORT_GRPC"]}'}
    drv = subprocess.Popen([str(args.driver)], cwd=work, stdout=(work / 'sdk.log').open('w'), stderr=subprocess.STDOUT, env=env, start_new_session=True)
    procs['sdk-driver'] = drv
    sdk_deadline = time.monotonic() + args.sdk_timeout_seconds
    restarted = False
    while drv.poll() is None:
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
    if args.kafka_broker:
        amarks = work / 'async-marks'; amarks.mkdir(exist_ok=True)
        aenv = {**env, 'FE_MODE': 'async', 'FE_DOMAIN': dom_a, 'FE_DOMAIN_NOASYNC': dom_n, 'FE_MARK_DIR': str(amarks)}
        adrv = subprocess.Popen([str(args.driver)], cwd=work, stdout=(work / 'sdk-async.log').open('w'), stderr=subprocess.STDOUT, env=aenv, start_new_session=True)
        procs['sdk-async-driver'] = adrv
        adeadline = time.monotonic() + 900
        done_down = done_up = False
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
    run('cli-list-workflows-grpc', [*cli_g, '--domain', dom_t, 'workflow', 'list', '--open'])
    run('cli-list-workflows-tchannel', [*cli_t, '--domain', dom_t, 'workflow', 'list', '--open'])
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

except SystemExit:
    pass
except Exception as e:
    record('harness-exception', False, repr(e))
finally:
    for n in list(procs):
        kill(n)
    for n in ['rust-frontend', 'rust-matching', 'go-history-worker']:
        lp = work / f'{n}.log'
        print(f'--- {n}.log tail', flush=True)
        print('\n'.join(lp.read_text(errors='replace').splitlines()[-25:]) if lp.exists() else '(none)', flush=True)
    failed = [s['step'] for s in steps if not s['ok']]
    (work / 'summary.json').write_text(json.dumps({'failedSteps': failed, 'stepCount': len(steps)}, indent=2))
    print('GATE SUMMARY failed:', failed, flush=True)
    status = 1 if failed else 0
sys.exit(status)
