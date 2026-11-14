"""Produce a compact Markdown comparison from downloaded suite-summary artifacts."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess

ROOT=Path(__file__).resolve().parent

def median(values):
    values=[x for x in values if x is not None]
    return statistics.median(values) if values else None

def fmt(value):return 'unavailable' if value is None else f'{value:.2f}'

def aggregate(stage,arm):
    runs=[r for k,r in stage['runs'].items() if k.endswith('-'+arm)]
    complete=len(runs)==3 and all(r.get('measured')==int(stage['rate']*30) and not r['errors'] for r in runs)
    resource=[]
    for r in runs:
        containers=r.get('resources',{}).get('containers',{})
        core={s:next((v for k,v in containers.items() if k.endswith('-'+s+'-1')),None) for s in ('frontend','matching','history')}
        if all(core.values()):resource.append(core)
    return {'complete':complete,'runs':len(runs),'correct':sum(r['correct'] for r in runs),
            'errors':sum(sum(r['errors'].values()) for r in runs),
            'throughput':median([r.get('completions_per_second') for r in runs]),
            'latency':{p:median([r.get('latency_ms',{}).get('completion_ms',{}).get(p) for r in runs]) for p in ('p50','p95','p99')},
            'cpu':median([sum(v['average_cpu_cores'] for v in c.values()) for c in resource]),
            'memory':median([sum(v['peak_cgroup_memory_bytes'] for v in c.values())/1048576 for c in resource]),
            'rss':median([sum(v['peak_summed_process_rss_bytes'] for v in c.values())/1048576 for c in resource if all(v.get('peak_summed_process_rss_bytes') is not None for v in c.values())]),
            'per_service':{s:{m:median([c[s].get(m) for c in resource]) for m in ('average_cpu_cores','peak_cgroup_memory_bytes','peak_summed_process_rss_bytes')} for s in ('frontend','matching','history')}}

def build(paths,statuses):
    suites={}
    for path in paths:
        suite=json.loads(path.read_text());suites[suite['service']]=suite
    lines=['# Updated Cadence SDK benchmark — 2026-11-14','',
           'Source: `3c2bf3aaabf160518afde04f75097d2dc2c374a6` (origin/master merged into the benchmark branch).',
           'One activity, existing pinned Go SDK over TChannel, 1 KiB payload, controlled 10ms activity, 128 worker capacity, non-sticky. 10s warmup / 30s measurement, three alternating Go/Rust pairs, fresh MySQL per arm. All-Rust retains the Go system Worker.',
           'Standard ubuntu-latest runners; services 1 CPU/1400MiB each, MySQL 1.5 CPU/1800MiB. SDK worker CPU1, generator CPU0, services/database CPUs2–3. Schema, DB version, shard count and generated configuration derive from the same pinned Go tree and Compose files in every arm.',
           '','## CI runs','']
    for run in statuses:lines.append(f"- {run['url']} — {run['conclusion']}; harness `{run['headSha']}`.")
    lines+=['','## Fixed 5 starts/sec','',
            'Medians of per-run values, not pooled percentiles. CPU is the sum for Frontend+Matching+History, excluding the Go Worker and MySQL. Memory/RSS are sums of per-container sampled peaks, not simultaneous cluster peaks. Collection includes drain and History audits. All-Go below uses the All-Rust lane baseline; other paired baselines follow.',
            '', '| Configuration | completions/s | p50 ms | p95 ms | p99 ms | core CPU | core peak MiB | core RSS MiB | correct | errors | valid repetitions |',
            '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|']
    labels={'all':'All Rust (Go Worker)','matching':'Rust Matching only','frontend':'Rust Frontend only','history':'Rust History only'}
    aggregates={}
    order=[('all','go','All Go')]+[(s,'rust',labels[s]) for s in ('matching','frontend','history','all')]
    for service,arm,label in order:
        suite=suites.get(service)
        fixed=next((s for s in suite['stages'] if s['mode']=='fixed'),None) if suite else None
        if fixed is None:
            lines.append(f'| {label} | unavailable | — | — | — | — | — | — | — | — | missing artifact |');continue
        v=aggregate(fixed,arm);aggregates[(service,arm)]=v
        lines.append(f"| {label} | {fmt(v['throughput'])} | {fmt(v['latency']['p50'])} | {fmt(v['latency']['p95'])} | {fmt(v['latency']['p99'])} | {fmt(v['cpu'])} | {fmt(v['memory'])} | {fmt(v['rss'])} | {v['correct']} | {v['errors']} | {v['runs']}/3 {'correct' if v['complete'] else 'INCOMPLETE/FAILED'} |")
    lines+=['','## Paired baseline and per-service resources','', '| Lane / arm | completions/s | FE CPU / MiB / RSS MiB | Matching CPU / MiB / RSS MiB | History CPU / MiB / RSS MiB |','|---|---:|---|---|---|']
    for service,suite in suites.items():
        fixed=next(s for s in suite['stages'] if s['mode']=='fixed')
        for arm in ('go','rust'):
            v=aggregate(fixed,arm)
            cells=[]
            for s in ('frontend','matching','history'):
                c=v['per_service'][s];cells.append(' / '.join(fmt(c[m]/1048576 if c[m] is not None and m!='average_cpu_cores' else c[m]) for m in ('average_cpu_cores','peak_cgroup_memory_bytes','peak_summed_process_rss_bytes')))
            lines.append(f"| {service} / {arm} | {fmt(v['throughput'])} | "+' | '.join(cells)+' |')
    lines+=['','## Capacity and recovery','',
            'New bounded smoke tests; no equivalent capacity/recovery measurements existed in the previous SDK smoke or run 41767896292. Capacity steps 5/25/100 starts/s require all three repetitions, zero workflow errors, p99 completion <=1000ms, valid open-loop generation and <=max(2,1% arrivals) outstanding at arrival end. Highest passing tested step is a short-run bound, not proven sustainable capacity.',
            'Recovery blocks MySQL TCP input for 3s, 10s after collector readiness while arrivals continue. Recovery is the end of the first full healthy five-second arrival window after restore (all correct, completion <=1s). Missing recovery windows are censored, not success.',
            '', '| Lane / arm | highest passing tested starts/s | recovery successes | median recovery seconds | recovery workflow errors |','|---|---:|---:|---:|---:|']
    for service,suite in suites.items():
        for arm in ('go','rust'):
            passed=[]
            for stage in suite['stages']:
                if stage['mode'] not in ('fixed','capacity'):continue
                runs=[r for k,r in stage['runs'].items() if k.endswith('-'+arm)]
                if len(runs)==3 and all(r.get('target_met',False) for r in runs):passed.append(stage['rate'])
            recovery=next((s for s in suite['stages'] if s['mode']=='recovery'),{'runs':{}})
            runs=[r for k,r in recovery['runs'].items() if k.endswith('-'+arm)]
            good=[r['recovery']['seconds_after_restore'] for r in runs if r.get('recovery',{}).get('recovered')]
            lines.append(f"| {service} / {arm} | {max(passed) if passed else 'none'} | {len(good)}/3 | {fmt(median(good))} | {sum(sum(r['errors'].values()) for r in runs)} |")
    lines+=['','## Comparison to previous results','',
            'Previous SDK smoke source `c1bbd3ae71c84a71d5a226a70b0d8b71d021cb5b`: Matching run 43480728156, History run 43479770520; Frontend rerun 43487649192 failed all 50 warmup workflows before measurement. These used the same 5/s, 10s/30s, three-pair one-activity protocol. Hosted runners and compiler/image resolution vary, so deltas are observations rather than isolated causal attribution.',
            '', '| Isolated service | previous Go/Rust p99 ms | updated Go/Rust p99 ms |','|---|---:|---:|']
    for service,old in [('matching','76.77 / 86.11'),('history','80.79 / 20073.00'),('frontend','unavailable (warmup failed)')]:
        suite=suites.get(service)
        if suite:
            stage=next(s for s in suite['stages'] if s['mode']=='fixed');g=aggregate(stage,'go');r=aggregate(stage,'rust')
            lines.append(f"| {service} | {old} | {fmt(g['latency']['p99'])} / {fmt(r['latency']['p99'])} |")
    lines+=['','Previous Matching isolated-service CPU Go/Rust 0.0274/0.0338 cores, peak cgroup memory 31.48/19.30 MiB, both 5 completions/s and 450 correct. Previous History CPU 0.1555/0.0659 cores and memory 40.97/26.70 MiB were NOT equal-throughput gains: observed completion rate was 5/1.67 per second and Rust drained later. Previous RSS and recovery: unavailable.',
            '', '### Older run 41767896292','',
            'https://github.com/auenkr/cadence-rust/actions/runs/41767896292 — source `316c28e4a2638e55cd52603a307fb6d7ffd09bd7`, combined Rust Frontend+Matching with Go History/Worker. Closed-loop concurrency 1/16/64, five repeats, 100 warmups and 1000 measured workflows, all 30000 correct. This differs from fixed-arrival isolated-service and All-Rust tests: do not compute cross-protocol speedups.',
            '', '| concurrency | Go/Rust completions/s | Go/Rust p50 ms | Go/Rust p95 ms | Go/Rust p99 ms |','|---:|---:|---:|---:|---:|']
    for c in (1,16,64):
        path=ROOT.parent/f'memory/evidence/frontend-matching-benchmark-full-41767896292/paired-benchmark/comparison-c{c}.json'
        if path.exists():
            arms=json.loads(path.read_text())['arms'];g,r=arms['go'],arms['rust']
            lines.append(f"| {c} | {g['median_throughput_workflows_per_second']:.2f} / {r['median_throughput_workflows_per_second']:.2f} | "+' | '.join(f"{g['median_latency_ms'][p]:.2f} / {r['median_latency_ms'][p]:.2f}" for p in ('p50','p95','p99'))+' |')
    lines+=['','At old concurrency16, per-1000-workflow FE CPU seconds Go/Rust were 4.069/5.008; Matching 2.687/5.045. FE peak cgroup MiB 39.25/29.27; Matching 35.66/26.80. These CPU seconds per fixed work count differ from new average CPU cores. RSS/capacity/recovery comparisons are unavailable.',
            '', '## Provenance and limits','',
            'Exact resource/Compose definitions are versioned in `Benchmark/compose.yml` and `Benchmark/run.py` at the harness SHA above; SDK locks in `Benchmark/go.mod` / `go.sum`. Runtime configs use the pinned source Go image template/dynamic defaults. Resolved Compose hashes follow. Rust History uses a standalone launcher; FE/Matching share the server launcher. All binaries are release builds; hashes are read from the actual runtime images.', '']
    for service,suite in suites.items():
        stage=suite['stages'][0];m=stage['manifest']
        lines.append(f"- **{service}**: source `{m.get('source_head')}`, harness `{m.get('head')}`, Rust tree `{m.get('rust_tree')}`, Go tree `{m.get('go_tree')}`, SDK lock SHA256 `{m.get('sdk_lock_sha256')}`; driver SHA256 `{m.get('driver_sha256')}`.")
        for key in ('go_binary_sha256','rust_binary_sha256','rust_history_binary_sha256'):
            if key in m:lines.append(f"  - {key}: `{m[key]}`")
        for arm,text in stage.get('resolved_compose',{}).items():lines.append(f"  - {arm} resolved Compose SHA256: `{hashlib.sha256(text.encode()).hexdigest()}`")
    lines+=['','Matching RSS trend is provisional: the memory-growth/soak-slope fix was still in progress at this source pin. No short smoke test establishes a stable RSS slope. CPU and memory improvements require equal verified completion throughput within each paired runner. No multi-workload, long soak, server retry, database query-tail or production-capacity claim. Failed/incomplete arms remain failures. Raw logs/samples are excluded from Git and have three-day CI retention.']
    return '\n'.join(lines)+'\n'

def main():
    p=argparse.ArgumentParser();p.add_argument('downloads',type=Path);p.add_argument('--statuses',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(build(list(a.downloads.rglob('suite-summary.json')),json.loads(a.statuses.read_text())))

if __name__=='__main__':main()
