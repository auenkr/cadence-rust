"""Aggregate benchmark report.json files: python3 Benchmark/agg.py <dir containing <run>/<artifact>/report.json>."""
import json, sys, statistics as st, collections, glob
M = lambda xs: st.median(xs) if xs else 0
for path in sorted(glob.glob(sys.argv[1] + '/*/*/report.json')):
    rep = json.load(open(path)); man = rep['manifest']; runs = rep['runs']
    svc = man['service']; rate = man['rate']
    rid = path.split('/')[-3]
    print(f'\n## {svc} rate={rate} run {rid}')
    agg = {}
    for arm in ('go', 'rust'):
        rs = [v for k, v in runs.items() if k.endswith(arm)]
        conts = rs[0]['resources']['containers'] if rs else {}
        repl = [c for c in conts if svc == 'all' and not c.endswith('mysql-1') and not c.endswith('worker-1') or c.endswith(f'-{svc}-1')]
        def cpu(r, cs): return sum(r['resources']['containers'][c]['cpu_seconds'] for c in cs if c in r['resources']['containers'])
        def rss(r, cs): return sum(r['resources']['containers'][c]['peak_summed_process_rss_bytes'] for c in cs if c in r['resources']['containers']) / 2**20
        cad = [c for c in conts if not c.endswith('mysql-1')]
        # Per-arm select_by_table (complete per-table counts; top-N `rows` can omit tables).
        # Mean over the arms that have it, so the Rust/Go ratio is a ratio of means over complete data.
        sels = []
        for r in rs:
            sbt = (r['resources'].get('mysql_general_log') or {}).get('select_by_table')
            if sbt:
                c = collections.Counter()
                for k, v in sbt.items(): c[k.split()[-1]] += v
                sels.append(c)
        sel = sum(sels, collections.Counter())
        agg[arm] = dict(n=len(rs), err=sum(r['errors'] for r in rs),
            thr=M([r['completions_per_second'] for r in rs]),
            p50=M([r['latency_ms']['completion_ms']['p50'] for r in rs]),
            p99=M([r['latency_ms']['completion_ms']['p99'] for r in rs]),
            cpu=M([cpu(r, repl) for r in rs]), rss=M([rss(r, repl) for r in rs]),
            allcpu=M([cpu(r, cad) for r in rs]), mysqlcpu=M([cpu(r, [c for c in conts if c.endswith('mysql-1')]) for r in rs]),
            qps=M([r['persistence']['queries_per_second'] for r in rs]),
            sel={k: v / max(len(sels), 1) for k, v in sel.items()}, seln=len(sels))
    print('| Arm | n | errors | compl/s | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | cadence CPU s | MySQL CPU s | MySQL q/s |')
    print('|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
    for arm in ('go', 'rust'):
        a = agg[arm]
        print(f"| {arm} | {a['n']} | {a['err']} | {a['thr']:.2f} | {a['p50']:.1f} | {a['p99']:.1f} | {a['cpu']:.2f} | {a['rss']:.0f} | {a['allcpu']:.2f} | {a['mysqlcpu']:.2f} | {a['qps']:.0f} |")
    g, r = agg['go'], agg['rust']
    rat = lambda x, y: f'{y / x:.2f}' if x else 'n/a'
    print(f"\nRust/Go: p50 {rat(g['p50'], r['p50'])}, p99 {rat(g['p99'], r['p99'])}, replaced CPU {rat(g['cpu'], r['cpu'])}, RSS {rat(g['rss'], r['rss'])}, cadence CPU {rat(g['allcpu'], r['allcpu'])}, MySQL q/s {rat(g['qps'], r['qps'])}")
    tabs = sorted(set(g['sel']) | set(r['sel']), key=lambda t: -(g['sel'].get(t, 0) + r['sel'].get(t, 0)))
    if tabs:
        print(f"\nSELECTs/arm = mean of per-arm select_by_table (go arms {g['seln']}, rust arms {r['seln']})")
        print('\n| Table | Go SELECTs/arm | Rust SELECTs/arm | Rust/Go |\n|---|---:|---:|---:|')
        for t in tabs:
            print(f"| {t} | {g['sel'].get(t, 0):.0f} | {r['sel'].get(t, 0):.0f} | {rat(g['sel'].get(t, 0), r['sel'].get(t, 0))} |")
