#!/usr/bin/env python3
"""Soak analysis. Two modes:
  soak-analyze.py run --dir RUN --label go|rust --restart-at SEC --out OUT   -> OUT/samples.csv, OUT/summary.json
  soak-analyze.py compare --go OUT_GO --rust OUT_RUST --out FILE.md          -> verdict table, exit 1 if rust fails
Slope windows: the last 50 min of samples. The Frontend restarts at the halfway point, which resets its
RSS/fds, so Frontend slopes use only the part of that window after restart+5min (reported with its length);
Matching never restarts and uses the full 50 min.
"""
import argparse, csv, json, sys
from pathlib import Path


def slope_per_hour(pts):
    n = len(pts)
    if n < 3:
        return None
    mx, my = sum(x for x, _ in pts) / n, sum(y for _, y in pts) / n
    den = sum((x - mx) ** 2 for x, _ in pts)
    return None if den == 0 else sum((x - mx) * (y - my) for x, y in pts) / den * 3600


def run(a):
    d, out = Path(a.dir), Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rows = [json.loads(l) for l in (d / 'samples.jsonl').read_text().splitlines() if l.strip()]
    steps = json.loads((d / 'steps.json').read_text()) if (d / 'steps.json').exists() else []
    cols = ['t', 'load1', 'fe_rss_mb', 'fe_cpu_pct', 'fe_fds', 'fe_threads', 'mt_rss_mb', 'mt_cpu_pct', 'mt_fds', 'mt_threads', 'started', 'completed',
            'failed', 'inflight', 'retried_calls', 'query_init_attempts', 'query_init_workflows',
            'e2e_p50_ms', 'e2e_p95_ms', 'e2e_p99_ms', 'start_p95_ms', 'query_p50_ms', 'query_p95_ms', 'query_p99_ms']
    prev = {}
    table = []
    for r in rows:
        o = {'t': r['t'], 'load1': r.get('load1')}
        for key, pre in (('frontend', 'fe'), ('matching', 'mt')):
            s = r.get(key)
            if s:
                o[pre + '_rss_mb'] = round(s['rss_kb'] / 1024, 1)
                o[pre + '_fds'] = s['fds']
                o[pre + '_threads'] = s['threads']
                p = prev.get(key)
                if p and p['pid'] == s['pid'] and r['t'] > p['t']:
                    o[pre + '_cpu_pct'] = round((s['cpu_ticks'] - p['cpu_ticks']) / 100.0 / (r['t'] - p['t']) * 100, 1)
                prev[key] = {'pid': s['pid'], 'cpu_ticks': s['cpu_ticks'], 't': r['t']}
        o.update({k: v for k, v in (r.get('driver') or {}).items() if k in cols})
        table.append(o)
    with (out / 'samples.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(table)
    end = table[-1]['t'] if table else 0
    win0 = max(0, end - 3000)

    def sl(col, start):
        return slope_per_hour([(r['t'], r[col]) for r in table if r['t'] >= start and r.get(col) is not None])

    fe_start = max(win0, a.restart_at + 300)
    final = {}
    fl = [l for l in (d / 'soak-driver.log').read_text(errors='replace').splitlines() if l.startswith('FINAL ')]
    if fl:
        final = json.loads(fl[-1][6:])
    elif table and rows and rows[-1].get('driver'):
        final = rows[-1]['driver']
    log = (d / 'soak-driver.log').read_text(errors='replace')
    fails = [l for l in log.splitlines() if l.startswith('FAIL ')]
    stage_counts = {}
    for l in fails:
        parts = l.split(' ', 3)
        stage_counts[parts[2] if len(parts) > 2 else '?'] = stage_counts.get(parts[2] if len(parts) > 2 else '?', 0) + 1
    started, completed, failed = final.get('started', 0), final.get('completed', 0), final.get('failed', 0)
    panic = any('panicked at' in (d / f'{n}.log').read_text(errors='replace') or '\npanic:' in (d / f'{n}.log').read_text(errors='replace')
                for n in ('rust-frontend', 'rust-matching', 'go-frontend', 'go-matching', 'go-history-worker') if (d / f'{n}.log').exists())
    died = [s['step'] for s in steps if 'died' in s['step'] and not s['ok']]
    summary = {
        'label': a.label, 'samples': len(table), 'duration_s': end, 'restart_at_s': a.restart_at,
        'started': started, 'completed': completed, 'failed': failed,
        'in_flight_at_end': max(0, started - completed - failed),
        'fail_stage_counts': stage_counts, 'fail_examples': fails[:5],
        'query_init_attempts': final.get('query_init_attempts'), 'query_init_workflows': final.get('query_init_workflows'),
        'retried_calls': final.get('retried_calls'),
        'e2e_ms': {k: final.get('e2e_' + k + '_ms') for k in ('p50', 'p95', 'p99')} | {'max': final.get('e2e_max_ms')},
        'query_ms': {k: final.get('query_' + k + '_ms') for k in ('p50', 'p95', 'p99')},
        'start_ms': {k: final.get('start_' + k + '_ms') for k in ('p50', 'p95', 'p99')},
        'slopes': {
            'window_s': [win0, end], 'frontend_window_s': [fe_start, end],
            'fe_rss_mb_per_h': sl('fe_rss_mb', fe_start), 'fe_fds_per_h': sl('fe_fds', fe_start),
            'mt_rss_mb_per_h': sl('mt_rss_mb', win0), 'mt_fds_per_h': sl('mt_fds', win0),
        },
        'peak': {c: max([r[c] for r in table if r.get(c) is not None] or [None]) for c in ('fe_rss_mb', 'fe_fds', 'mt_rss_mb', 'mt_fds', 'load1')},
        'panic_in_logs': panic, 'service_died_steps': died,
        'failed_steps': [s['step'] for s in steps if not s['ok']],
    }
    (out / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


def compare(a):
    g = json.loads((Path(a.go) / 'summary.json').read_text())
    r = json.loads((Path(a.rust) / 'summary.json').read_text())
    res = []

    def chk(name, ok, detail):
        res.append((name, ok, detail))

    chk('no panics/crashes', not r['panic_in_logs'] and not r['service_died_steps'], f"panic={r['panic_in_logs']} died={r['service_died_steps']}")
    s = r['slopes']
    gs = g['slopes']
    fmt = lambda v: 'n/a' if v is None else f'{v:.2f}'
    # "near flat": RSS slope below 5 MB/h (or not worse than Go + 5), fd slope below 10/h.
    for k, lim, unit in (('fe_rss_mb_per_h', 5, 'MB/h'), ('mt_rss_mb_per_h', 5, 'MB/h'), ('fe_fds_per_h', 10, 'fds/h'), ('mt_fds_per_h', 10, 'fds/h')):
        v = s[k]
        ok = v is not None and (abs(v) <= lim or abs(v) <= abs(gs[k] or 0) + lim)
        chk(f'{k} near flat (<= {lim} {unit} or within +{lim} of Go)', ok, f'rust={fmt(v)} go={fmt(gs[k])}')
    chk('every started workflow completes (excluding in-flight at end)',
        r['completed'] + r['failed'] + r['in_flight_at_end'] == r['started'] and r['completed'] >= r['started'] - r['in_flight_at_end'] and r['failed'] == 0,
        f"started={r['started']} completed={r['completed']} failed={r['failed']} in_flight_at_end={r['in_flight_at_end']} (go: {g['started']}/{g['completed']}/{g['failed']}/{g['in_flight_at_end']})")
    rate = r['failed'] / max(1, r['started'])
    chk('error rate ~0 (< 0.1%) and not higher than Go', rate < 0.001 and r['failed'] <= g['failed'] + max(1, 0.001 * r['started']),
        f"rust={rate:.4%} ({r['failed']}) go={g['failed']} stages={r['fail_stage_counts']}")
    chk('query "init" after successful signal: none (as Go)', (r['query_init_workflows'] or 0) <= (g['query_init_workflows'] or 0),
        f"rust workflows={r['query_init_workflows']} attempts={r['query_init_attempts']}; go workflows={g['query_init_workflows']} attempts={g['query_init_attempts']}")
    for grp, label in (('e2e_ms', 'e2e'), ('query_ms', 'query call'), ('start_ms', 'start call')):
        for p in ('p50', 'p95', 'p99'):
            rv, gv = r[grp].get(p), g[grp].get(p)
            ok = rv is not None and gv is not None and rv <= max(1.5 * gv, gv + 50)
            chk(f'{label} {p} within 1.5x of Go (+50ms floor)', ok, f'rust={rv}ms go={gv}ms')
    ok_all = all(ok for _, ok, _ in res)
    lines = ['| criterion | verdict | detail |', '|---|---|---|'] + [f'| {n} | {"PASS" if ok else "FAIL"} | {d} |' for n, ok, d in res]
    lines.append(f'\nGo baseline sanity: panic={g["panic_in_logs"]} failed={g["failed"]} completed={g["completed"]}/{g["started"]} died={g["service_died_steps"]}')
    lines.append(f'\n**RUST OVERALL: {"PASS" if ok_all else "FAIL"}**')
    Path(a.out).write_text('\n'.join(lines) + '\n')
    print('\n'.join(lines))
    sys.exit(0 if ok_all else 1)


ap = argparse.ArgumentParser()
sub = ap.add_subparsers(dest='cmd', required=True)
p = sub.add_parser('run'); p.add_argument('--dir', required=True); p.add_argument('--label', required=True); p.add_argument('--restart-at', type=int, required=True); p.add_argument('--out', required=True)
p = sub.add_parser('compare'); p.add_argument('--go', required=True); p.add_argument('--rust', required=True); p.add_argument('--out', required=True)
a = ap.parse_args()
run(a) if a.cmd == 'run' else compare(a)
