#!/usr/bin/env python3
"""History replication gate: Go History <-> Rust History across two clusters (both directions).

Cluster A (cluster0) is the initial STANDBY, cluster B (cluster1) the initial ACTIVE. Frontend, Matching and
Worker are always Go. --rust-cluster picks which cluster runs Rust History (the other cluster is all Go):
  A : Go active (B) -> Rust standby (A)
  B : Rust active (B) -> Go standby (A)
Steps: register global domain on B; run workflows (signal, activity, timer, child, query) on B with the Go SDK;
assert standby history equals active history (event ids/types/versions + version histories, via
`cadence workflow show` / `admin workflow describe`); fail over to A (plain domain update on B); finish the
parked workflow and run a new one on A; assert histories equal again (now A is active, B standby).
Databases: mysql | postgres | cassandra (disposable container, schemas created by Go --auto-setup).
"""
import argparse, json, os, re, secrets, signal, socket, subprocess, sys, time
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument('--database', choices=['mysql', 'postgres', 'cassandra'], default='mysql')
ap.add_argument('--out', type=Path, required=True)
ap.add_argument('--rust-history-binary', type=Path, required=True)
ap.add_argument('--rust-cluster', choices=['A', 'B'], required=True)
ap.add_argument('--go-server', type=Path, required=True)
ap.add_argument('--go-cli', type=Path, required=True)
ap.add_argument('--driver', type=Path, required=True)
ap.add_argument('--rust-root', type=Path, required=True)
args = ap.parse_args()
root = Path(__file__).resolve().parents[2]
work = args.out.resolve(); work.mkdir(parents=True, exist_ok=True)
plugin = args.database
envp = 'CADENCE_' + plugin.upper()
db_addr = os.environ.get(envp + '_ADDR', '127.0.0.1:5432' if plugin == 'postgres' else '127.0.0.1:3306')
db_user = os.environ.get(envp + '_USER', 'postgres' if plugin == 'postgres' else 'root')
db_pass = os.environ.get(envp + '_PASSWORD', 'cadence')
cass_host = os.environ.get('CADENCE_CASSANDRA_CONTACT_POINTS', '127.0.0.1')
cass_port = int(os.environ.get('CADENCE_CASSANDRA_PORT', '9042'))

keys = ['A_FE', 'A_FE_GRPC', 'A_HIST', 'A_HIST_GRPC', 'A_MATCH', 'A_MATCH_GRPC', 'A_WORKER', 'A_PPROF1', 'A_PPROF2', 'A_PPROF3', 'A_PPROF4',
        'B_FE', 'B_FE_GRPC', 'B_HIST', 'B_HIST_GRPC', 'B_MATCH', 'B_MATCH_GRPC', 'B_WORKER', 'B_PPROF1', 'B_PPROF2', 'B_PPROF3', 'B_PPROF4']
socks = [socket.socket() for _ in keys]
for s in socks:
    s.bind(('127.0.0.1', 0))
P = {k: str(s.getsockname()[1]) for k, s in zip(keys, socks)}
for s in socks:
    s.close()
(work / 'ports.json').write_text(json.dumps(P, indent=2))

dyn = (root / 'go/config/dynamicconfig/development.yaml').read_text()
(work / 'dynamic-config.yaml').write_text(dyn)
(work / 'rust-dynamic-config.yaml').write_text(dyn + '\nsystem.enableGRPCOutbound:\n  - value: true\n')

def persistence(db, vis):
    if plugin == 'cassandra':
        def store(name, ks):
            return (f'    {name}:\n      nosql:\n        pluginName: cassandra\n        hosts: {json.dumps(cass_host)}\n        port: {cass_port}\n'
                    f'        keyspace: {json.dumps(ks)}\n        connectTimeout: 2s\n        timeout: 5s\n'
                    '        consistency: LOCAL_QUORUM\n        serialConsistency: LOCAL_SERIAL\n')
        return ('persistence:\n  defaultStore: cass-default\n  visibilityStore: cass-visibility\n  numHistoryShards: 4\n  datastores:\n'
                + store('cass-default', db) + store('cass-visibility', vis))
    def store(name, d, conns):
        return (f'    {name}:\n      sql:\n        pluginName: {plugin}\n        databaseName: {json.dumps(d)}\n'
                f'        connectAddr: {json.dumps(db_addr)}\n        connectProtocol: tcp\n        user: {json.dumps(db_user)}\n'
                f'        password: {json.dumps(db_pass)}\n        nShards: 1\n        maxConns: {conns}\n        maxIdleConns: {conns}\n')
    return (f'persistence:\n  defaultStore: {plugin}-default\n  visibilityStore: {plugin}-visibility\n  numHistoryShards: 4\n  datastores:\n'
            + store(plugin + '-default', db, 20) + store(plugin + '-visibility', vis, 2))

def group_block(current):
    return ('clusterGroupMetadata:\n  failoverVersionIncrement: 10\n  primaryClusterName: "cluster1"\n'
            f'  currentClusterName: "{current}"\n  clusterGroup:\n'
            '    cluster0:\n      enabled: true\n      initialFailoverVersion: 0\n      rpcName: "cadence-frontend"\n'
            f'      rpcAddress: "127.0.0.1:{P["A_FE_GRPC"]}"\n      rpcTransport: "grpc"\n'
            '    cluster1:\n      enabled: true\n      initialFailoverVersion: 1\n      rpcName: "cadence-frontend"\n'
            f'      rpcAddress: "127.0.0.1:{P["B_FE_GRPC"]}"\n      rpcTransport: "grpc"\n'
            '  clusterRedirectionPolicy:\n    policy: "noop"\n    allDomainApisForwardingTargetCluster: ""\n\n')

DB = {'A': ('cadence_a', 'cadence_a_visibility'), 'B': ('cadence_b', 'cadence_b_visibility')}
seeds = {c: ', '.join(f'"127.0.0.1:{P[c + "_" + k]}"' for k in ['FE', 'HIST', 'MATCH', 'WORKER']) for c in 'AB'}
cur = {'A': 'cluster0', 'B': 'cluster1'}

def go_config(c):
    d = work / f'config-{c.lower()}'; d.mkdir(exist_ok=True)
    (d / 'base.yaml').write_text((root / 'go/config/base.yaml').read_text())
    text = (root / 'go/config/development.yaml').read_text()
    ren = {'FRONTEND_PORT': 'FE', 'FRONTEND_PORT_GRPC': 'FE_GRPC', 'MATCHING_PORT': 'MATCH', 'MATCHING_PORT_GRPC': 'MATCH_GRPC',
           'HISTORY_PORT': 'HIST', 'HISTORY_PORT_GRPC': 'HIST_GRPC', 'WORKER_PORT': 'WORKER', 'FRONTEND_PORT_PPROF': 'PPROF1',
           'MATCHING_PORT_PPROF': 'PPROF2', 'HISTORY_PORT_PPROF': 'PPROF3', 'WORKER_PORT_PPROF': 'PPROF4'}
    for k, v in ren.items():
        text = re.sub(r'\$\{' + k + r':\d+\}', P[c + '_' + v], text)
    text = text.replace('"127.0.0.1:7933", "127.0.0.1:7934", "127.0.0.1:7935"', seeds[c])
    if c == 'B':
        text = text.replace('name: cadence\n', 'name: cadence-b\n', 1)
    text = text.replace('"config/dynamicconfig/development.yaml"', json.dumps(str(work / 'dynamic-config.yaml')))
    text, n = re.subn(r'(?ms)^clusterGroupMetadata:.*?(?=^archival:)', group_block(cur[c]), text, count=1); assert n == 1
    text, n = re.subn(r'(?ms)^persistence:\n.*?(?=^ringpop:\n)', persistence(*DB[c]), text, count=1); assert n == 1
    text = text.replace('/tmp/cadence_archival/development', str(work / 'arch-h')).replace('/tmp/cadence_vis_archival/development', str(work / 'arch-v')).replace('/tmp/blobstore', str(work / 'blob'))
    (d / 'development.yaml').write_text(text)
    (d / f'development_{plugin}.yaml').write_text(persistence(*DB[c]))
    return d
cfg = {c: go_config(c) for c in 'AB'}

def rust_yaml(c):
    rt = (args.rust_root / 'rust/config/development.yaml').read_text()
    ren = {'FRONTEND_PORT': 'FE', 'FRONTEND_PORT_GRPC': 'FE_GRPC', 'MATCHING_PORT': 'MATCH', 'MATCHING_PORT_GRPC': 'MATCH_GRPC',
           'HISTORY_PORT': 'HIST', 'HISTORY_PORT_GRPC': 'HIST_GRPC', 'WORKER_PORT': 'WORKER', 'FRONTEND_PORT_PPROF': 'PPROF1',
           'MATCHING_PORT_PPROF': 'PPROF2', 'HISTORY_PORT_PPROF': 'PPROF3', 'WORKER_PORT_PPROF': 'PPROF4'}
    for k, v in ren.items():
        rt = re.sub(r'\$\{' + k + r':\d+\}', P[c + '_' + v], rt)
    rt, n = re.subn(r'(?ms)^persistence:\n.*?(?=^ringpop:\n)', persistence(*DB[c]), rt, count=1); assert n == 1
    rt, n = re.subn(r'(?m)^  bootstrapHosts: \[.*\]$', f'  bootstrapHosts: [{seeds[c]}]', rt, count=1); assert n == 1
    if c == 'B':
        rt = rt.replace('name: cadence\n', 'name: cadence-b\n', 1)
    rt = rt.replace('filepath: "config/dynamicconfig/development.yaml"', 'filepath: ' + json.dumps(str(work / 'rust-dynamic-config.yaml')))
    rt = rt.replace('/tmp/cadence_archival/development', str(work / 'arch-h')).replace('/tmp/cadence_vis_archival/development', str(work / 'arch-v')).replace('/tmp/blobstore', str(work / 'blob'))
    rt, n = re.subn(r'(?ms)^clusterGroupMetadata:.*?(?=^archival:)', group_block(cur[c]), rt, count=1); assert n == 1
    return rt

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
    print(f'GATE {"ok  " if ok else "FAIL"} {name} {detail[-600:] if not ok else ""}', flush=True)
    (work / 'steps.json').write_text(json.dumps(steps, indent=2))
def cli(addr, *a, timeout=120):
    r = subprocess.run([str(args.go_cli), '--address', addr, '--transport', 'tchannel', *a], capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout + r.stderr
def poll(name, fn, secs=240, every=5):
    end = time.monotonic() + secs; last = ''
    while time.monotonic() < end:
        ok, last = fn()
        if ok:
            record(name, True, last); return True
        time.sleep(every)
    record(name, False, last); return False

A_T, B_T = f'127.0.0.1:{P["A_FE"]}', f'127.0.0.1:{P["B_FE"]}'
ADDR = {'A': A_T, 'B': B_T}
dom = 'tc-repl-' + secrets.token_hex(3)
ANSI = re.compile(r'\x1b\[[0-9;]*m')

def show(side, wid, rid):
    """(rc, [(eventId, eventType, version)], raw) via `workflow show --output_filename` JSON."""
    f = work / f'show-{side}-{wid}.json'
    if f.exists():
        f.unlink()
    rc, out = cli(ADDR[side], '--domain', dom, 'workflow', 'show', '--wid', wid, '--rid', rid, '--output_filename', str(f))
    evs, raw = [], None
    if rc == 0 and f.exists():
        try:
            raw = json.loads(f.read_text())
            lst = raw['events'] if isinstance(raw, dict) else raw
            if isinstance(lst, dict):
                lst = lst.get('events') or lst.get('Events')
            for e in lst:
                low = {k.lower(): v for k, v in e.items()}
                evs.append((low.get('eventid', low.get('id')), low.get('eventtype'), low.get('version', 0)))
        except Exception as ex:
            return rc, [], out + ' parse: ' + repr(ex)
    return rc, evs, raw if raw is not None else out

def norm(o):
    if isinstance(o, dict):
        return {k.lower(): norm(v) for k, v in o.items() if 'branchtoken' not in k.lower() and not (k.lower() == 'currentversionhistoryindex' and v == 0)}
    if isinstance(o, list):
        return [norm(v) for v in o]
    return o

def version_histories(side, wid, rid):
    rc, out = cli(ADDR[side], '--domain', dom, 'admin', 'workflow', 'describe', '--wid', wid, '--rid', rid)
    if rc != 0:
        return None, out
    try:
        i = out.index('{')
        obj, _ = json.JSONDecoder().raw_decode(out[i:])
        low = {k.lower(): v for k, v in obj.items()}
        ms = json.loads(low['mutablestateindatabase'])
        for k, v in ms.items():
            if k.lower() == 'versionhistories':
                return norm(v), ''
        return None, 'no VersionHistories in mutable state: ' + str(list(ms)[:20])
    except Exception as ex:
        return None, repr(ex) + ' ' + out[:500]

def compare(tag, name, wid, rid, active, standby):
    """Poll until standby events equal active events, then compare version histories."""
    state = {}
    def f():
        rc1, ea, _ = show(active, wid, rid)
        rc2, es, raw2 = show(standby, wid, rid)
        state['ea'], state['es'] = ea, es
        ok = rc1 == 0 and rc2 == 0 and len(ea) > 0 and ea == es
        return ok, f'{active}: n={len(ea)} last={ea[-1] if ea else None} | {standby}: n={len(es)} last={es[-1] if es else None}'
    ok = poll(f'{tag}-{name}-history-equal({active}->{standby})', f, 300)
    if ok:
        ea = state['ea']
        record(f'{tag}-{name}-event-count-and-last-id', len(ea) >= 5 and ea[-1][0] == len(ea), f'count={len(ea)} last={ea[-1]}')
    def vf():
        va, ma = version_histories(active, wid, rid)
        vs, ms_ = version_histories(standby, wid, rid)
        return va is not None and va == vs, f'{active}={json.dumps(va)[:600]} {standby}={json.dumps(vs)[:600]} {ma} {ms_}'
    poll(f'{tag}-{name}-version-histories-equal', vf, 120)
    return state.get('ea', [])

def child_ids(side, wid, rid):
    rc, ea, raw = show(side, wid, rid)
    out = []
    lst = raw['events'] if isinstance(raw, dict) and 'events' in raw else (raw if isinstance(raw, list) else [])
    for e in lst if isinstance(lst, list) else []:
        low = {k.lower(): v for k, v in e.items()}
        if low.get('eventtype') == 'ChildWorkflowExecutionStarted':
            attrs = next((v for k, v in e.items() if k.lower().endswith('attributes') and isinstance(v, dict)), {})
            we = next((v for k, v in attrs.items() if k.lower() == 'workflowexecution'), {})
            w = next((v for k, v in we.items() if k.lower() == 'workflowid'), None)
            r = next((v for k, v in we.items() if k.lower() == 'runid'), None)
            if w and r:
                out.append((w, r))
    return out

def run_driver(name, cmd, extra, side):
    env = {**os.environ, 'FE_TCHANNEL': ADDR[side], 'FE_DOMAIN': dom, 'REPL_TL': 'repl-tl', 'REPL_CMD': cmd, **extra}
    r = subprocess.run([str(args.driver)], capture_output=True, text=True, timeout=720, env=env)
    out = r.stdout + r.stderr
    (work / f'driver-{name}.log').write_text(out)
    wfs = {m.group(1): (m.group(2), m.group(3)) for m in re.finditer(r'^WF (\S+) (\S+) (\S+)$', out, re.M)}
    for m in re.finditer(r'^STEP (\S+) (ok|FAIL)(.*)$', out, re.M):
        record(f'{name}-{m.group(1)}', m.group(2) == 'ok', m.group(3))
    record(f'{name}-driver-exit', r.returncode == 0, out[-800:])
    return wfs

rc_name = 'rust-history'
status = 0
try:
    (work / 'rust-history-binary.sha256').write_text(subprocess.run(['sha256sum', str(args.rust_history_binary)], capture_output=True, text=True).stdout)
    rust_c, go_c = args.rust_cluster, ('B' if args.rust_cluster == 'A' else 'A')
    record('mode', True, f'rust History on cluster {rust_c} ({"standby" if rust_c == "A" else "active"} first); db={plugin}')
    # create the per-cluster databases for sql lanes handled by the lane script; cassandra keyspaces too.
    start('go-all-' + go_c.lower(), [args.go_server, '--config', cfg[go_c], '--zone', plugin, 'start', '--auto-setup'])
    start('go-fmw-' + rust_c.lower(), [args.go_server, '--config', cfg[rust_c], '--zone', plugin, 'start', '--auto-setup', '--services', 'frontend,matching,worker'])
    ok = all(wait_port(P[f'{c}_FE_GRPC'], 300, n) for c, n in [(go_c, 'go-all-' + go_c.lower()), (rust_c, 'go-fmw-' + rust_c.lower())])
    ok = ok and wait_port(P[go_c + '_WORKER'], 300) and wait_port(P[rust_c + '_WORKER'], 300)
    (work / 'rust-history.yaml').write_text(rust_yaml(rust_c))
    start(rc_name, [args.rust_history_binary, '--config', work / 'rust-history.yaml', '--root', work, 'start', '--services', 'history'])
    ok = ok and wait_port(P[rust_c + '_HIST_GRPC'], 240, rc_name)
    record('clusters-listening', ok)
    if not ok:
        raise SystemExit(1)
    time.sleep(25)
    rc, out = cli(B_T, '--domain', dom, 'domain', 'register', '--retention', '1', '--global_domain', 'true',
                  '--active_cluster', 'cluster1', '--cl', 'cluster0', '--cl', 'cluster1')
    record('register-global-domain-on-B', rc == 0, out)
    def a_dom():
        rc, out = cli(A_T, '--domain', dom, 'domain', 'describe')
        return rc == 0 and 'cluster1' in out, out
    if not poll('domain-replicated-to-A', a_dom, 240):
        raise SystemExit(1)
    time.sleep(15)

    # ---- phase 1: workload on active B, compare with standby A
    w1 = run_driver('phase1', 'phase1', {}, 'B')
    wA, wB = w1.get('A'), w1.get('B')
    if not (wA and wB):
        record('phase1-workflows-started', False, str(w1)); raise SystemExit(1)
    evA = compare('p1', 'A-completed', wA[0], wA[1], 'B', 'A')
    compare('p1', 'B-parked', wB[0], wB[1], 'B', 'A')
    record('p1-A-has-all-event-kinds', all(any(t == k for _, t, _ in evA) for k in
           ['WorkflowExecutionSignaled', 'ActivityTaskCompleted', 'TimerFired', 'ChildWorkflowExecutionCompleted', 'WorkflowExecutionCompleted']),
           str(sorted({t for _, t, _ in evA})))
    kids = child_ids('B', *wA)
    record('p1-child-found', len(kids) == 1, str(kids))
    for i, (cw, cr) in enumerate(kids):
        compare('p1', f'A-child{i}', cw, cr, 'B', 'A')

    # ---- failover: plain domain update on the active cluster B -> cluster0
    rc, out = cli(B_T, '--domain', dom, 'domain', 'update', '--active_cluster', 'cluster0')
    record('failover-update-on-B', rc == 0, out)
    def active_is_a(side):
        def f():
            rc, out = cli(ADDR[side], '--domain', dom, 'domain', 'describe')
            return rc == 0 and re.search(r'ActiveClusterName:\s*cluster0', out) is not None, out
        return f
    poll('failover-visible-on-B', active_is_a('B'), 120)
    poll('failover-replicated-to-A', active_is_a('A'), 240)
    time.sleep(35)  # domain caches refresh, standby timer/transfer queues flip to active

    # ---- phase 2: workflow B continues on new active A; new workflow C; histories equal with A active, B standby
    w2 = run_driver('phase2', 'phase2', {'REPL_WID_B': wB[0], 'REPL_RID_B': wB[1]}, 'A')
    evB = compare('p2', 'B-continued', wB[0], wB[1], 'A', 'B')
    record('p2-B-completed', bool(evB) and evB[-1][1] == 'WorkflowExecutionCompleted', str(evB[-1:]))
    record('p2-B-has-failover-version', len({v for _, _, v in evB}) >= 2, f'versions={sorted({v for _, _, v in evB})}')
    wC = w2.get('C')
    if wC:
        compare('p2', 'C-new-on-A', wC[0], wC[1], 'A', 'B')
    else:
        record('p2-C-started', False, str(w2))
    compare('p2', 'A-old-completed-still-equal', wA[0], wA[1], 'A', 'B')
    for n in procs:
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
