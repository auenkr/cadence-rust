#!/usr/bin/env python3
"""Two-cluster Frontend redirection gate (MySQL).

Cluster A (cluster0): Rust Frontend + Rust Matching + Go History + Go Worker.
Cluster B (cluster1): all-Go server (frontend, matching, history, worker).
Both clusters share one MySQL container but use distinct schemas
(cadence_a*, cadence_b*), distinct ringpop seeds and ports. A global domain is
registered on B (active cluster1) and replicated to A by A's Go worker.

Phases (A's Rust Frontend is restarted with a different config for each):
  forward-selected : selected-apis-forwarding -> request on A is forwarded to B
                     (workflow start/signal and schedule create/pause)
  forward-all      : all-domain-apis-forwarding (target cluster1)
  noop             : DomainNotActiveError with Go's message
  failover         : domain made active in A (updated on B, replicated to A) -> runs locally
  resend           : remote cluster1 over gRPC; admin kafka rereplicate against A
Steps land in OUT/steps.json. Usage:
  run-frontend-two-cluster-gate.py --out DIR --rust-binary P --go-server P --go-cli P --rust-root DIR
MySQL: CADENCE_MYSQL_{ADDR,USER,PASSWORD}; databases cadence_a, cadence_a_visibility,
cadence_b, cadence_b_visibility must exist.
"""
import argparse, json, os, re, secrets, signal, socket, subprocess, sys, time
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument('--out', type=Path, required=True)
ap.add_argument('--rust-binary', type=Path, required=True)
ap.add_argument('--go-server', type=Path, required=True)
ap.add_argument('--go-cli', type=Path, required=True)
ap.add_argument('--rust-root', type=Path, required=True)
args = ap.parse_args()
root = Path(__file__).resolve().parents[2]
work = args.out.resolve(); work.mkdir(parents=True, exist_ok=True)
my_addr = os.environ.get('CADENCE_MYSQL_ADDR', '127.0.0.1:3306')
my_user = os.environ.get('CADENCE_MYSQL_USER', 'root')
my_pass = os.environ.get('CADENCE_MYSQL_PASSWORD', 'cadence')

keys = ['A_FE', 'A_FE_GRPC', 'A_HIST', 'A_HIST_GRPC', 'A_WORKER', 'A_RMATCH', 'A_RMATCH_GRPC',
        'A_PPROF1', 'A_PPROF2', 'A_PPROF3', 'A_PPROF4',
        'B_FE', 'B_FE_GRPC', 'B_MATCH', 'B_MATCH_GRPC', 'B_HIST', 'B_HIST_GRPC', 'B_WORKER',
        'B_PPROF1', 'B_PPROF2', 'B_PPROF3', 'B_PPROF4']
socks = [socket.socket() for _ in keys]
for s in socks:
    s.bind(('127.0.0.1', 0))
P = {k: str(s.getsockname()[1]) for k, s in zip(keys, socks)}
for s in socks:
    s.close()
(work / 'ports.json').write_text(json.dumps(P, indent=2))
_ms = socket.socket(); _ms.bind(('127.0.0.1', 0)); prom_port = _ms.getsockname()[1]; _ms.close()

dyn = (root / 'go/config/dynamicconfig/development.yaml').read_text()
if 'worker.enableScheduler' not in dyn:
    dyn += '\nworker.enableScheduler:\n  - value: true\n'
(work / 'dynamic-config.yaml').write_text(dyn)
(work / 'rust-dynamic-config.yaml').write_text(dyn + '\nsystem.enableGRPCOutbound:\n  - value: true\n')

def persistence(db, vis):
    def store(name, d, conns):
        return (f'    {name}:\n      sql:\n        pluginName: mysql\n        databaseName: {json.dumps(d)}\n'
                f'        connectAddr: {json.dumps(my_addr)}\n        connectProtocol: tcp\n        user: {json.dumps(my_user)}\n'
                f'        password: {json.dumps(my_pass)}\n        nShards: 1\n        maxConns: {conns}\n        maxIdleConns: {conns}\n')
    return ('persistence:\n  defaultStore: mysql-default\n  visibilityStore: mysql-visibility\n  numHistoryShards: 4\n  datastores:\n'
            + store('mysql-default', db, 20) + store('mysql-visibility', vis, 2))

def group_block(current, a_transport, b_transport, policy, target=''):
    """clusterGroupMetadata for a process; Go always sees gRPC, Rust Frontend gets the variant."""
    return ('clusterGroupMetadata:\n  failoverVersionIncrement: 10\n  primaryClusterName: "cluster1"\n'
            f'  currentClusterName: "{current}"\n  clusterGroup:\n'
            '    cluster0:\n      enabled: true\n      initialFailoverVersion: 0\n      rpcName: "cadence-frontend"\n'
            f'      rpcAddress: "127.0.0.1:{P["A_FE_GRPC"] if a_transport == "grpc" else P["A_FE"]}"\n      rpcTransport: "{a_transport}"\n'
            '    cluster1:\n      enabled: true\n      initialFailoverVersion: 1\n      rpcName: "cadence-frontend"\n'
            f'      rpcAddress: "127.0.0.1:{P["B_FE_GRPC"] if b_transport == "grpc" else P["B_FE"]}"\n      rpcTransport: "{b_transport}"\n'
            f'  clusterRedirectionPolicy:\n    policy: "{policy}"\n    allDomainApisForwardingTargetCluster: "{target}"\n\n')

def go_config(dirname, prefix, db, vis, seeds, current):
    d = work / dirname; d.mkdir(exist_ok=True)
    (d / 'base.yaml').write_text((root / 'go/config/base.yaml').read_text())
    text = (root / 'go/config/development.yaml').read_text()
    ren = {'FRONTEND_PORT': prefix + 'FE', 'FRONTEND_PORT_GRPC': prefix + 'FE_GRPC', 'MATCHING_PORT': prefix + 'MATCH',
           'MATCHING_PORT_GRPC': prefix + 'MATCH_GRPC', 'HISTORY_PORT': prefix + 'HIST', 'HISTORY_PORT_GRPC': prefix + 'HIST_GRPC',
           'WORKER_PORT': prefix + 'WORKER', 'FRONTEND_PORT_PPROF': prefix + 'PPROF1', 'MATCHING_PORT_PPROF': prefix + 'PPROF2',
           'HISTORY_PORT_PPROF': prefix + 'PPROF3', 'WORKER_PORT_PPROF': prefix + 'PPROF4'}
    for k, v in ren.items():
        text = re.sub(r'\$\{' + k + r':\d+\}', P.get(v, '1'), text)
    text = text.replace('"127.0.0.1:7933", "127.0.0.1:7934", "127.0.0.1:7935"', seeds)
    if prefix == 'B_':
        text = text.replace('name: cadence\n', 'name: cadence-b\n', 1)
    text = text.replace('"config/dynamicconfig/development.yaml"', json.dumps(str(work / 'dynamic-config.yaml')))
    text = re.sub(r'(?ms)^clusterGroupMetadata:.*?(?=^archival:)', group_block(current, 'grpc', 'grpc', 'noop'), text, count=1)
    text = text.replace('/tmp/cadence_archival/development', str(work / 'arch-h')).replace('/tmp/cadence_vis_archival/development', str(work / 'arch-v')).replace('/tmp/blobstore', str(work / 'blob'))
    (d / 'development.yaml').write_text(text)
    (d / 'development_mysql.yaml').write_text(persistence(db, vis))
    return d, text

a_seeds = ', '.join(f'"127.0.0.1:{P[k]}"' for k in ['A_FE', 'A_HIST', 'A_WORKER', 'A_RMATCH'])
b_seeds = ', '.join(f'"127.0.0.1:{P[k]}"' for k in ['B_FE', 'B_HIST', 'B_MATCH', 'B_WORKER'])
a_dir, _ = go_config('config-a', 'A_', 'cadence_a', 'cadence_a_visibility', a_seeds, 'cluster0')
b_dir, _ = go_config('config-b', 'B_', 'cadence_b', 'cadence_b_visibility', b_seeds, 'cluster1')

def rust_yaml(policy, target, b_transport):
    rt = (args.rust_root / 'rust/config/development.yaml').read_text()
    ren = {'FRONTEND_PORT': 'A_FE', 'FRONTEND_PORT_GRPC': 'A_FE_GRPC', 'MATCHING_PORT': 'A_RMATCH', 'MATCHING_PORT_GRPC': 'A_RMATCH_GRPC',
           'HISTORY_PORT': 'A_HIST', 'HISTORY_PORT_GRPC': 'A_HIST_GRPC', 'WORKER_PORT': 'A_WORKER', 'FRONTEND_PORT_PPROF': 'A_PPROF1',
           'MATCHING_PORT_PPROF': 'A_PPROF2', 'HISTORY_PORT_PPROF': 'A_PPROF3', 'WORKER_PORT_PPROF': 'A_PPROF4'}
    for k, v in ren.items():
        rt = re.sub(r'\$\{' + k + r':\d+\}', P[v], rt)
    rt, n = re.subn(r'(?ms)^persistence:\n.*?(?=^ringpop:\n)', persistence('cadence_a', 'cadence_a_visibility'), rt, count=1); assert n == 1
    rt, n = re.subn(r'(?m)^  bootstrapHosts: \[.*\]$', f'  bootstrapHosts: [{a_seeds}]', rt, count=1); assert n == 1
    rt = rt.replace('filepath: "config/dynamicconfig/development.yaml"', 'filepath: ' + json.dumps(str(work / 'rust-dynamic-config.yaml')))
    rt = rt.replace('/tmp/cadence_archival/development', str(work / 'arch-h')).replace('/tmp/cadence_vis_archival/development', str(work / 'arch-v')).replace('/tmp/blobstore', str(work / 'blob'))
    rt, n = re.subn(r'(?ms)^clusterGroupMetadata:.*?(?=^archival:)', group_block('cluster0', 'grpc', b_transport, policy, target), rt, count=1); assert n == 1
    rt, n = re.subn(r'(?m)^(  frontend:\n(?:.*\n)*?    metrics:\n)      statsd:\n        hostPort: "127.0.0.1:8125"\n        prefix: "cadence"\n',
                    lambda m: m.group(1) + f'      prometheus:\n        timerType: histogram\n        listenAddress: "127.0.0.1:{prom_port}"\n', rt, count=1)
    assert n == 1
    return rt.rstrip('\n') + '\nhistograms:\n  default: both\n'

procs = {}
def start(name, cmd, env=None):
    log = (work / f'{name}.log').open('ab')
    procs[name] = subprocess.Popen([str(c) for c in cmd], cwd=work, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)
def kill(name):
    p = procs.get(name)
    if p and p.poll() is None:
        os.killpg(p.pid, signal.SIGKILL); p.wait(timeout=60)
def wait_port(port, secs, name=None):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if name and procs[name].poll() is not None:
            return False
        try:
            socket.create_connection(('127.0.0.1', int(port)), timeout=0.5).close(); return True
        except OSError:
            time.sleep(0.5)
    return False
steps = []
def record(name, ok, detail=''):
    steps.append({'step': name, 'ok': bool(ok), 'detail': detail[-3000:]})
    print(f'GATE {"ok  " if ok else "FAIL"} {name} {detail[-400:] if not ok else ""}', flush=True)
    (work / 'steps.json').write_text(json.dumps(steps, indent=2))
def cli(addr, transport, *a, timeout=90, inp=None):
    r = subprocess.run([str(args.go_cli), '--address', addr, '--transport', transport, *a], capture_output=True, text=True, timeout=timeout, input=inp)
    return r.returncode, r.stdout + r.stderr
A_T, A_G = f'127.0.0.1:{P["A_FE"]}', f'127.0.0.1:{P["A_FE_GRPC"]}'
B_T, B_G = f'127.0.0.1:{P["B_FE"]}', f'127.0.0.1:{P["B_FE_GRPC"]}'
def poll(name, fn, secs=180, every=4):
    end = time.monotonic() + secs; last = ''
    while time.monotonic() < end:
        ok, last = fn()
        if ok:
            record(name, True, last); return True
        time.sleep(every)
    record(name, False, last); return False

rust_env = {**os.environ, 'CADENCE_HISTORY_TCHANNEL_ADDR': f'127.0.0.1:{P["A_HIST"]}', 'CADENCE_MATCHING_TCHANNEL_ADDR': f'127.0.0.1:{P["A_RMATCH"]}'}
def start_rust_frontend(tag, policy, target, b_transport):
    kill('rust-frontend')
    cfg = work / f'rust-{tag}.yaml'; cfg.write_text(rust_yaml(policy, target, b_transport))
    start('rust-frontend', [args.rust_binary, 'start', '--config', cfg, '--root', work, '--services', 'frontend',
                            '--history-grpc-port', P['A_HIST_GRPC'], '--data-dir', work / 'rust-frontend-data'], env=rust_env)
    ok = wait_port(P['A_FE'], 120, 'rust-frontend') and wait_port(P['A_FE_GRPC'], 10, 'rust-frontend')
    record(f'rust-frontend-start-{tag}', ok)
    time.sleep(15)
    return ok

def prom():
    import urllib.request
    try:
        return urllib.request.urlopen(f'http://127.0.0.1:{prom_port}/metrics', timeout=15).read().decode()
    except Exception as e:
        return repr(e)

status = 0
try:
    (work / 'rust-binary.sha256').write_text(subprocess.run(['sha256sum', str(args.rust_binary)], capture_output=True, text=True).stdout)
    start('go-b', [args.go_server, '--config', b_dir, '--zone', 'mysql', 'start', '--auto-setup'])
    start('go-a-history-worker', [args.go_server, '--config', a_dir, '--zone', 'mysql', 'start', '--auto-setup', '--services', 'history,worker'])
    ok = wait_port(P['B_FE_GRPC'], 300, 'go-b') and wait_port(P['A_HIST_GRPC'], 300, 'go-a-history-worker')
    record('go-clusters-listening', ok)
    if not ok:
        raise SystemExit(1)
    (work / 'rust-matching.yaml').write_text(rust_yaml('noop', '', 'tchannel'))
    start('rust-matching', [args.rust_binary, 'start', '--config', work / 'rust-matching.yaml', '--root', work, '--services', 'matching', '--port', '0',
                            '--history-grpc-port', P['A_HIST_GRPC'], '--matching-grpc-port', P['A_RMATCH_GRPC'],
                            '--matching-tchannel-port', P['A_RMATCH'], '--data-dir', work / 'rust-matching-data'])
    if not start_rust_frontend('forward-selected', 'selected-apis-forwarding', '', 'tchannel'):
        raise SystemExit(1)
    record('rust-matching-listening', wait_port(P['A_RMATCH_GRPC'], 120, 'rust-matching'))
    time.sleep(20)
    dom = 'tc-gate-' + secrets.token_hex(3)
    rc, out = cli(B_T, 'tchannel', '--domain', dom, 'domain', 'register', '--retention', '1', '--global_domain', 'true',
                  '--active_cluster', 'cluster1', '--cl', 'cluster0', '--cl', 'cluster1')
    record('register-global-domain-on-B', rc == 0, out)
    def a_has_domain():
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'domain', 'describe')
        return rc == 0 and 'cluster1' in out, out
    if not poll('domain-replicated-to-A', a_has_domain, 240):
        raise SystemExit(1)
    time.sleep(12)
    domain_id = ''
    rc, out = cli(A_T, 'tchannel', '--domain', dom, 'domain', 'describe')
    m = re.search(r'UUID:\s*(\S+)', out); domain_id = m.group(1) if m else ''
    record('domain-uuid', bool(domain_id), out)
    wids = {}
    def start_wf(tag, addr='A'):
        wid = f'tc-{tag}-{secrets.token_hex(3)}'
        rc, out = cli(A_T if addr == 'A' else B_T, 'tchannel', '--domain', dom, 'workflow', 'start', '--tl', 'tc-tl', '--wt', 'TcType', '--wid', wid, '--et', '600', '--dt', '10')
        m = re.search(r'run Id:\s*(\S+)', out, re.I)
        return rc, out, wid, (m.group(1) if m else '')
    # (a1) selected-apis-forwarding
    rc, out, wid, rid = start_wf('sel')
    record('selected-forward-start-on-A', rc == 0, out)
    wids['sel'] = (wid, rid)
    rc, out = cli(B_T, 'tchannel', '--domain', dom, 'workflow', 'describe', '--wid', wid)
    record('selected-forward-workflow-lives-on-B', rc == 0 and wid in out, out)
    body = prom()
    (work / 'frontend-metrics-selected.prom').write_text(body)
    record('selected-forward-metric', 'cluster_forwarding_policy_requests' in body and 'cadence_client_requests_redirection' in body,
           ' | '.join(l for l in body.splitlines() if 'redirection' in l or 'forwarding_policy' in l)[:800])
    rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'signal', '--wid', wid, '--name', 'sig', '--input', '"x"')
    record('selected-forward-signal', rc == 0, out)
    # Schedule writes go through Redirect too (Go api_generated.go); reads
    # (DescribeSchedule) stay local under selected-apis-forwarding.
    # The Go CLI schedule commands dial gRPC only.
    sid = 'tc-sched-' + secrets.token_hex(3)
    rc, out = cli(A_G, 'grpc', '--domain', dom, 'schedule', 'create', '--sid', sid, '--ce', '0 0 1 1 *',
                  '--wt', 'TcType', '--tl', 'tc-tl', '--et', '600')
    record('selected-forward-schedule-create-on-A', rc == 0, out)
    def b_has_schedule():
        rc, out = cli(B_G, 'grpc', '--domain', dom, 'schedule', 'describe', '--sid', sid)
        return rc == 0 and '0 0 1 1 *' in out, out
    poll('selected-forward-schedule-lives-on-B', b_has_schedule, 60)
    rc, out = cli(A_G, 'grpc', '--domain', dom, 'schedule', 'pause', '--sid', sid, '--reason', 'gate')
    record('selected-forward-schedule-pause-on-A', rc == 0, out)
    def b_paused():
        rc, out = cli(B_G, 'grpc', '--domain', dom, 'schedule', 'describe', '--sid', sid)
        return rc == 0 and re.search(r'paused', out, re.I) is not None, out
    poll('selected-forward-schedule-paused-on-B', b_paused, 60)
    # (a2) all-domain-apis-forwarding
    if start_rust_frontend('forward-all', 'all-domain-apis-forwarding', 'cluster1', 'tchannel'):
        rc, out, wid2, rid2 = start_wf('all')
        record('all-domain-forward-start-on-A', rc == 0, out)
        wids['all'] = (wid2, rid2)
        rc, out = cli(B_T, 'tchannel', '--domain', dom, 'workflow', 'describe', '--wid', wid2)
        record('all-domain-forward-workflow-lives-on-B', rc == 0 and wid2 in out, out)
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'list', '--open')
        record('all-domain-forward-list-sees-B-workflow', rc == 0 and wid2 in out, out)
    # (a3) forwarding over gRPC: cluster B is configured with rpcTransport grpc
    if start_rust_frontend('forward-grpc', 'selected-apis-forwarding', '', 'grpc'):
        rc, out, wid5, rid5 = start_wf('grpc')
        record('grpc-forward-start-on-A', rc == 0, out)
        wids['grpc'] = (wid5, rid5)
        def on_b():
            rc, out = cli(B_T, 'tchannel', '--domain', dom, 'workflow', 'describe', '--wid', wid5)
            return rc == 0 and wid5 in out, out
        poll('grpc-forward-workflow-lives-on-B', on_b, 60)
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'signal', '--wid', wid5, '--name', 'sig', '--input', '"x"')
        record('grpc-forward-signal', rc == 0, out)
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'terminate', '--wid', wid5, '--reason', 'gate')
        record('grpc-forward-terminate', rc == 0, out)
        def terminated():
            rc, out = cli(B_T, 'tchannel', '--domain', dom, 'workflow', 'describe', '--wid', wid5)
            return rc == 0 and 'terminated' in out.lower(), out
        poll('grpc-forward-terminated-on-B', terminated, 60)
    # (a3b) all-domain-apis-forwarding over gRPC: read APIs go to B and the converters decode its replies
    if start_rust_frontend('forward-all-grpc', 'all-domain-apis-forwarding', 'cluster1', 'grpc'):
        rc, out, wid6, rid6 = start_wf('allgrpc')
        record('grpc-all-start-on-A', rc == 0, out)
        wids['allgrpc'] = (wid6, rid6)
        def sig6():
            rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'signal', '--wid', wid6, '--name', 'sig', '--input', '"x"')
            return rc == 0, out
        poll('grpc-all-signal', sig6, 60)
        # Further APIs the Rust Frontend on A routes over gRPC when B is the target
        # (describe / history / list / query); success means the converters decode B's reply.
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'describe', '--wid', wid6)
        record('grpc-forward-describe', rc == 0 and wid6 in out, out)
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'show', '--wid', wid6)
        record('grpc-forward-history', rc == 0 and 'WorkflowExecutionSignaled' in out, out)
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'list', '--open')
        record('grpc-forward-list', rc == 0 and wid6 in out, out)
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'query', '--wid', wid6, '--qt', '__stack_trace')
        record('grpc-forward-query-reaches-B', 'not supported' not in out.lower(), out)

    # (b) noop
    if start_rust_frontend('noop', 'noop', '', 'tchannel'):
        rc, out, wid3, _ = start_wf('noop')
        record('noop-start-domain-not-active', rc != 0 and 'cluster1' in out and 'standby cluster' in out and 'cluster0' in out, f'rc={rc} {out}')
        rc, out = cli(B_T, 'tchannel', '--domain', dom, 'workflow', 'describe', '--wid', wid3)
        record('noop-nothing-created-on-B', rc != 0, out)
    # (c) failover to A (done on B, replicated to A)
    rc, out = cli(B_T, 'tchannel', '--domain', dom, 'domain', 'update', '--active_cluster', 'cluster0')
    record('failover-update-on-B', rc == 0, out)
    def a_active():
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'domain', 'describe')
        return rc == 0 and re.search(r'ActiveClusterName:\s*cluster0', out) is not None, out
    time.sleep(30)
    # restart Rust Frontend so its domain cache is fresh, then wait for replication
    if start_rust_frontend('after-failover', 'selected-apis-forwarding', '', 'tchannel'):
        poll('failover-replicated-to-A', a_active, 240)
        time.sleep(12)
        before = prom()
        rc, out, wid4, rid4 = start_wf('local')
        record('after-failover-start-runs-locally-on-A', rc == 0, out)
        rc, out = cli(A_T, 'tchannel', '--domain', dom, 'workflow', 'describe', '--wid', wid4)
        record('after-failover-describe-on-A', rc == 0 and wid4 in out, out)
        after = prom()
        def fwd_count(b):
            return sum(float(l.rsplit(' ', 1)[1]) for l in b.splitlines() if l.startswith('cluster_forwarding_policy_requests') and 'target_cluster="cluster1"' in l)
        record('after-failover-no-new-forward', fwd_count(after) == fwd_count(before), f'{fwd_count(before)} -> {fwd_count(after)}')
        # request to B (now standby) with its own noop policy must say cluster0 is active
        rc, out, _, _ = start_wf('onB', 'B')
        record('standby-B-rejects-with-domain-not-active', rc != 0 and 'cluster0' in out, f'rc={rc} {out}')
    # (d) ResendReplicationTasks from A against B over gRPC
    if start_rust_frontend('resend', 'noop', '', 'grpc'):
        wid, rid = wids.get('sel', ('', ''))
        rc, out = cli(A_G, 'grpc', 'admin', 'kafka', 'rereplicate', '--source_cluster', 'cluster1', '--domain_id', domain_id, '--wid', wid, '--rid', rid)
        record('resend-replication-tasks-grpc-remote', rc == 0, out)
        rc, out = cli(A_G, 'grpc', '--domain', dom, 'workflow', 'show', '--wid', wid, '--rid', rid)
        record('resend-history-visible-on-A', rc == 0 and 'WorkflowExecutionStarted' in out, out)
        rc, out = cli(A_G, 'grpc', 'admin', 'kafka', 'rereplicate', '--source_cluster', 'no-such-cluster', '--domain_id', domain_id, '--wid', wid, '--rid', rid)
        record('resend-unknown-cluster-fails', rc != 0, out)
        # Thrift/TChannel admin ResendReplicationTasks (was "unknown method" before the yarpc arm)
        rc, out = cli(A_T, 'tchannel', 'admin', 'kafka', 'rereplicate', '--source_cluster', 'cluster1', '--domain_id', domain_id, '--wid', wid, '--rid', rid)
        record('resend-replication-tasks-thrift-remote', rc == 0 and 'unknown method' not in out.lower(), out)
        rc, out = cli(A_T, 'tchannel', 'admin', 'kafka', 'rereplicate', '--source_cluster', 'no-such-cluster', '--domain_id', domain_id, '--wid', wid, '--rid', rid)
        record('resend-unknown-cluster-fails-thrift', rc != 0 and 'unknown method' not in out.lower(), out)
    for n in ['go-b', 'go-a-history-worker', 'rust-matching', 'rust-frontend']:
        record(f'{n}-alive-at-end', procs[n].poll() is None, f'exit={procs[n].poll()}')
except SystemExit:
    pass
except Exception as e:
    record('harness-exception', False, repr(e))
finally:
    for n in list(procs):
        kill(n)
    for n in procs:
        lp = work / f'{n}.log'
        print(f'--- {n}.log tail', flush=True)
        print('\n'.join(lp.read_text(errors='replace').splitlines()[-25:]) if lp.exists() else '(none)', flush=True)
    failed = [s['step'] for s in steps if not s['ok']]
    (work / 'summary.json').write_text(json.dumps({'failedSteps': failed, 'stepCount': len(steps)}, indent=2))
    print('GATE SUMMARY failed:', failed, flush=True)
    status = 1 if failed else 0
sys.exit(status)
