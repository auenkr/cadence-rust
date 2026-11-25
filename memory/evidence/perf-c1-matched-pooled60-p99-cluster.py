#!/usr/bin/env python3 -I
"""Run-cluster bootstrap for p95 AND p99 of pooled per-rep medians. usage: <dir of run subdirs>"""
import glob, json, os, random, statistics as st, sys
root = sys.argv[1]; random.seed(1); B = 20000
data = {}
for rd in sorted(glob.glob(root + "/*")):
    run = os.path.basename(rd)
    for arm in ["go", "control", "rust"]:
        for f in sorted(glob.glob(f"{rd}/paired-benchmark/result-*-{arm}-c1.json")):
            l = json.load(open(f))["latency_ms"]
            data.setdefault(run, {}).setdefault(arm, []).append((l["p95"], l["p99"]))
runs = sorted(data)
def diff(rs, a, b, i):
    return st.median([r[i] for x in rs for r in data[x][a]]) - st.median([r[i] for x in rs for r in data[x][b]])
for a, b in [("rust", "go"), ("rust", "control")]:
    for i, m in [(0, "p95"), (1, "p99")]:
        pt = diff(runs, a, b, i); ds = []
        for _ in range(B):
            rs = [random.choice(runs) for _ in runs]
            ds.append(st.median([random.choice(data[x][a])[i] for x in rs for _ in range(5)]) - st.median([random.choice(data[x][b])[i] for x in rs for _ in range(5)]))
        ds.sort(); print(f"cluster {a}-{b} {m}: point {pt:+.1f} CI [{ds[int(.025*B)]:+.1f},{ds[int(.975*B)]:+.1f}]")
