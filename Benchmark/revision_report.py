"""Compare two pinned SDK suites; keep only compact aggregates and self-contained charts."""
import argparse
import json
from pathlib import Path
import statistics
from compare_suite import aggregate, fmt

LABELS={'matching':'Rust Matching only','frontend':'Rust Frontend only','history':'Rust History only','all':'All Rust (Go Worker)'}
ORDER=[('all','go','All Go')]+[(s,'rust',LABELS[s]) for s in ('matching','frontend','history','all')]

def load_suites(root):
    suites={}
    for f in Path(root).glob('*/suite-summary.json'):
        s=json.loads(f.read_text());suites[s['service']]=s
    return suites

def metrics(suite,arm):
    if not suite:return None
    fixed=next((s for s in suite['stages'] if s['mode']=='fixed'),None)
    if fixed is None:return None
    result=aggregate(fixed,arm)
    passing=[];capacity=[]
    for s in suite['stages']:
        if s['mode'] not in ('fixed','capacity'):continue
        runs=[r for k,r in s['runs'].items() if k.endswith('-'+arm)]
        passed=len(runs)==3 and all(r.get('target_met',False) for r in runs)
        if passed:passing.append(s['rate'])
        capacity.append({'rate':s['rate'],'passed':passed,'stage_exit':s['exit_code'],
                         'correct':sum(r['correct'] for r in runs),'errors':sum(sum(r['errors'].values()) for r in runs)})
    recovery=next((s for s in suite['stages'] if s['mode']=='recovery'),{'runs':{}})
    runs=[r for k,r in recovery['runs'].items() if k.endswith('-'+arm)]
    recovered=[r['recovery']['seconds_after_restore'] for r in runs if r.get('recovery',{}).get('recovered')]
    result.update(capacity=max(passing) if passing else None,capacity_steps=capacity,
                  recovery_passes=len(recovered),recovery_seconds=statistics.median(recovered) if recovered else None,
                  recovery_errors=sum(sum(r['errors'].values()) for r in runs))
    return result

def snapshot(root):
    suites=load_suites(root)
    return {'configurations':{label:metrics(suites.get(service),arm) for service,arm,label in ORDER},
            'paired_go':{s:metrics(suite,'go') for s,suite in suites.items()},
            'manifests':{s:next((v['manifest'] for v in suite['stages'] if v.get('manifest')), {}) for s,suite in suites.items()}}

def report(previous,current,attempts):
    lines=['# Final-code benchmark: 77dbaa379 versus 3c2bf3aaa','',
           'New source: `77dbaa379eaa88bc7b62b48fc3d711059e2dd93e`. Previous source: `3c2bf3aaabf160518afde04f75097d2dc2c374a6`.',
           'Same pinned SDK/one-activity workflow, 1 KiB payload, 10ms activity, non-sticky, worker capacity 128. Fixed load 5 starts/s; capacity steps 5/25/100; three alternating fresh-MySQL pairs, 10s warmup and 30s measured per arm. Recovery drops MySQL TCP input for 3s, 10s into measurement. One Go system Worker stays in every configuration.',
           'Standard ubuntu-latest, jobs <=90min. Services each 1 CPU/1400MiB; MySQL 1.5 CPU/1800MiB. SDK generator/worker occupy separate CPUs; services+DB share CPUs2–3. No allocator feature is enabled: this historical Alpine benchmark uses **System backed by musl**, not the glibc System used by the normal GNU build. It preserves the previous build for comparability and does not establish glibc allocator behavior.',
           '', '## Attempts and outcomes','']
    for service,history in attempts.items():
        for index,a in enumerate(history):
            lines.append(f"- {service}, attempt {index+1}: {a['url']} — {a.get('conclusion','pending')}; harness `{a.get('headSha','unknown')}`.")
            if a.get('diagnosis'):lines.append('  '+a['diagnosis'])
    lines+=['','## Fixed-load results','',
            'Each cell is previous → new. Medians of per-run values; CPU sums Frontend+Matching+History cores. Memory/RSS sum per-container sampled peaks, not simultaneous cluster peaks; summed process RSS can repeat shared pages. Worker/DB excluded. Resources include drain and History audit. All-Go main row uses the All-Rust lane baseline; every paired baseline is shown separately below.',
            '', '| Configuration | completions/s | p50 ms | p95 ms | p99 ms | CPU cores | memory MiB | RSS MiB | correct | errors |','|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for _,_,label in ORDER:
        old,new=previous['configurations'].get(label),current['configurations'].get(label)
        if old is None or new is None:
            lines.append(f'| {label} | unavailable | — | — | — | — | — | — | — | — |');continue
        values=[f"{fmt(old[k])} → {fmt(new[k])}" for k in ('throughput',)]
        values += [f"{fmt(old['latency'][p])} → {fmt(new['latency'][p])}" for p in ('p50','p95','p99')]
        values += [f"{fmt(old[k])} → {fmt(new[k])}" for k in ('cpu','memory','rss')]
        values += [f"{old[k]} → {new[k]}" for k in ('correct','errors')]
        lines.append('| '+label+' | '+' | '.join(values)+' |')
        if not new['complete']:lines.append(f"\n**{label}: new fixed-load evidence incomplete/failed ({new['runs']}/3 repetitions); no performance improvement claim.**\n")
    lines+=['','## Capacity and recovery','',
            'Capacity is highest tested rate passing all three repetitions: zero errors, valid generator, completion p99 <=1000ms and at most max(2,1% arrivals) outstanding at the end. It is a short tested bound, not long-term sustainable capacity. An empty capacity means no tested step passed all targets. Recovery measures the END of the first full healthy five-second arrival window after restoration; it cannot be less than 5s. Recovery errors include warmup failures; missing windows are not successes.',
            '', '| Configuration | passing tested starts/s old → new | recovery repetitions old → new | recovery seconds old → new | recovery errors old → new |','|---|---:|---|---:|---:|']
    for _,_,label in ORDER:
        o,n=previous['configurations'].get(label),current['configurations'].get(label)
        if o is None or n is None:continue
        lines.append(f"| {label} | {o['capacity'] or 'none'} → {n['capacity'] or 'none'} | {o['recovery_passes']}/3 → {n['recovery_passes']}/3 | {fmt(o['recovery_seconds'])} → {fmt(n['recovery_seconds'])} | {o['recovery_errors']} → {n['recovery_errors']} |")
    lines+=['','## Paired baselines and stage outcomes','',
            'Runner-to-runner variation is visible in the All-Go baselines. Compare Rust with its own Go pair before attributing changes to the migration. Retry results are identified above; first failures are retained rather than silently replaced.',
            '', '| Lane / source | Go fixed completion p99 ms | Go CPU cores | Go memory / RSS MiB | Go passing starts/s |','|---|---:|---:|---:|---:|']
    for service in LABELS:
        for revision,s in [('previous',previous),('new',current)]:
            m=s['paired_go'].get(service)
            if m:lines.append(f"| {service} / {revision} | {fmt(m['latency']['p99'])} | {fmt(m['cpu'])} | {fmt(m['memory'])} / {fmt(m['rss'])} | {m['capacity'] or 'none'} |")
    lines+=['','| New lane / arm | 25/s passed all targets | 100/s stage exit | 100/s recorded errors |','|---|---|---:|---:|']
    for service,label in LABELS.items():
        for arm in ('go','rust'):
            m=current['paired_go'].get(service) if arm=='go' else current['configurations'].get(label)
            if not m:continue
            lo=next((v for v in m['capacity_steps'] if v['rate']==25),{})
            hi=next((v for v in m['capacity_steps'] if v['rate']==100),{})
            lines.append(f"| {service} / {arm} | {lo.get('passed','missing')} | {hi.get('stage_exit','missing')} | {hi.get('errors','missing')} |")
    lines+=['','## Rust binary provenance','',
            'Source/harness/tree hashes and actual runtime-image executable SHA256s follow. Exact config is versioned at the harness SHA in Benchmark/compose.yml and run.py; Go config/dynamic defaults/schema come from the pinned source. New manifests record the Rust compiler/target. Historical rust:1-alpine and mysql:8.0 tags can move; these comparisons do not isolate compiler or hosted-runner effects.','']
    for revision,s in [('previous',previous),('new',current)]:
        for service,m in s['manifests'].items():
            lines.append(f"- {revision} {service}: source `{m.get('source_head')}`, harness `{m.get('head')}`, Rust tree `{m.get('rust_tree')}`, Go tree `{m.get('go_tree')}`.")
            for k in ('rust_binary_sha256','rust_history_binary_sha256','go_binary_sha256','driver_sha256','sdk_lock_sha256'):
                if k in m:lines.append(f"  - {k}: `{m[k]}`")
            if m.get('rust_toolchain'):lines.append('  - Compiler/target: '+m['rust_toolchain'].replace('\n','; '))
    lines+=['','## Separate older reference: CI 41767896292','',
            'https://github.com/auenkr/cadence-rust/actions/runs/41767896292 — source `316c28e4a2638e55cd52603a307fb6d7ffd09bd7`. Combined Rust Frontend+Matching with Go History/Worker; five closed-loop repetitions, concurrency 1/16/64, 100 warmups and 1000 measured workflows. All 30000 measured workflows succeeded. This is a different protocol and configuration, so its throughput is not an open-loop capacity comparison.',
            '', '| concurrency | Go/Rust workflows/s | Go/Rust p50 ms | Go/Rust p95 ms | Go/Rust p99 ms |','|---:|---:|---:|---:|---:|',
            '| 1 | 12.77 / 10.78 | 77.63 / 92.24 | 84.94 / 99.84 | 90.34 / 104.10 |',
            '| 16 | 33.57 / 29.82 | 484.45 / 541.93 | 618.96 / 709.16 | 671.86 / 769.26 |',
            '| 64 | 31.13 / 27.55 | 2294.92 / 2623.26 | 2600.80 / 3035.88 | 2884.15 / 3488.81 |',
            '', 'At old concurrency16, FE CPU seconds per 1000 workflows Go/Rust were 4.069/5.008, Matching 2.687/5.045; FE memory MiB 39.25/29.27, Matching 35.66/26.80. Older RSS and recovery are unavailable. CPU seconds per fixed work count differ from average CPU cores.',
            '', '## Limits','',
            'No speed/cost claim from incomplete arms or unequal completion throughput. Short RSS samples do not establish a soak slope or that QPS-bucket retention fixes have removed all growth. Matching QPS retention, cancellation-safe reads, pooled Frontend-to-History connections and History batch validation changed together; this test does not isolate their individual effects. Other scenario specifications remain unimplemented. Raw logs/ledgers stay ignored; only compact reports/charts are committed.']
    return '\n'.join(lines)+'\n'

def visual(previous,current):
    data=[{'label':label,'old':previous['configurations'].get(label),'new':current['configurations'].get(label)} for _,_,label in ORDER]
    return '''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Final Cadence benchmark comparison</title><style>
html,body{margin:0;background:transparent;color:var(--foreground);font-family:var(--font-sans,system-ui)}*{box-sizing:border-box}h2{font-size:20px;margin:0 0 8px}p{line-height:1.5}.box{padding:16px;background:var(--card);border:1px solid var(--border);border-radius:var(--radius);margin:14px 0}.muted,small{color:var(--muted-foreground)}select{font:inherit;padding:6px;background:var(--secondary);color:var(--foreground);border:1px solid var(--border)}.legend{font-size:12px;margin:10px 0}.sw{display:inline-block;width:12px;height:12px;margin:0 5px 0 12px}.old{background:var(--chart-2)}.new{background:var(--chart-1)}#chart{height:430px}.group{margin:17px 0}.label{font-size:13px;font-weight:600}.row{height:23px;display:flex;gap:8px;align-items:center}.bar{height:13px;min-width:2px;border-radius:3px}.number{font-size:12px}.scroll{overflow:auto}table{width:100%;border-collapse:collapse;font-size:12px}td,th{padding:8px 4px;text-align:left;border-bottom:1px solid var(--border)}.warn{color:var(--warning)}</style></head><body><h2>Previous versus final updated code</h2><p><code>3c2bf3aaa</code> → <code>77dbaa379</code>. Same one-activity smoke workload, 5 starts/sec, three fresh-database pairs. Select a metric below.</p><div class="box"><select id="metric"><option value="p99">Completion p99 · ms</option><option value="p95">Completion p95 · ms</option><option value="p50">Completion p50 · ms</option><option value="throughput">Completions per second</option><option value="cpu">Core service CPU · cores</option><option value="memory">Core service memory · MiB</option><option value="rss">Core service summed RSS · MiB</option></select><div class="legend"><i class="sw old"></i>Previous 3c2bf3aaa <i class="sw new"></i>Updated 77dbaa379</div><div id="chart"></div><small>Lower is better except throughput. CPU/memory cover FE+Matching+History, excluding DB and Go Worker. Memory/RSS sum sampled per-service peaks; shared pages can repeat in RSS. Runner/compiler variation remains: use the report's paired Go baselines before attributing improvements.</small></div><div class="box"><strong>Capacity and recovery</strong><div class="scroll"><table><thead><tr><th>Configuration</th><th>Passing tested starts/s<br>old → new</th><th>Recovery passes<br>old → new</th><th>Recovery seconds<br>old → new</th></tr></thead><tbody id="outcomes"></tbody></table></div><small>Capacity requires all three target passes at 5/25/100 starts/sec. None means no tested step passed every target. This is not long-term capacity. Recovery includes a mandatory healthy five-second window after DB restoration.</small></div><p class="warn">Historical benchmark builds use musl System, not glibc. These 30-second measurements cannot establish a stable RSS slope. CI 41767896292 remains a separate closed-loop reference in the report.</p><script>const data=__DATA__;const f=v=>v==null?'unavailable':v.toFixed(2);function render(){const metric=document.getElementById('metric').value;const value=x=>!x?null:(['p50','p95','p99'].includes(metric)?x.latency[metric]:x[metric]);const max=Math.max(1,...data.flatMap(d=>[value(d.old)||0,value(d.new)||0]))*1.15;const chart=document.getElementById('chart');chart.innerHTML='';for(const d of data){const group=document.createElement('div');group.className='group';const label=document.createElement('div');label.className='label';label.textContent=d.label;group.append(label);for(const [name,x] of [['old',d.old],['new',d.new]]){const v=value(x);const row=document.createElement('div');row.className='row';const bar=document.createElement('div');bar.className='bar '+name;bar.style.width=((v||0)/max*80)+'%';const n=document.createElement('span');n.className='number';n.textContent=f(v)+(x&&!x.complete?' · FAILED/INCOMPLETE':'');row.append(bar,n);group.append(row)}chart.append(group)}}document.getElementById('metric').addEventListener('change',render);render();for(const d of data){const o=d.old,n=d.new;const tr=document.createElement('tr');for(const text of [d.label,(o?.capacity||'none')+' → '+(n?.capacity||'none'),(o?.recovery_passes??'?')+'/3 → '+(n?.recovery_passes??'?')+'/3',f(o?.recovery_seconds)+' → '+f(n?.recovery_seconds)]){const td=document.createElement('td');td.textContent=text;tr.append(td)}document.getElementById('outcomes').append(tr)}</script></body></html>'''.replace('__DATA__',json.dumps(data))

def main():
    p=argparse.ArgumentParser();p.add_argument('--previous',required=True,type=Path);p.add_argument('--current',required=True,type=Path);p.add_argument('--attempts',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args()
    previous=snapshot(a.previous);current=snapshot(a.current);attempts=json.loads(a.attempts.read_text())
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.with_suffix('.md').write_text(report(previous,current,attempts))
    a.output.with_suffix('.html').write_text(visual(previous,current))

if __name__=='__main__':main()
