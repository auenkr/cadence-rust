#!/usr/bin/env python3 -I
"""Analyse perf-pg-soak-tails runs. usage: analysis.py <dir-with-run-id-subdirs> <ids-file 'kind id' per line>
Windows = thirds of samples ordered by completion time (start_unix_nano + latency). wf/s per window = n / completion span."""
import json, sys, glob, random, statistics as st

root, idsf = sys.argv[1], sys.argv[2]
random.seed(20261126)
ARMS = ["go", "control", "rust"]


def pct(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def stats(xs):
    return {"p50": pct(xs, 50), "p95": pct(xs, 95), "p99": pct(xs, 99)}


def load(rid, arm):
    pb = glob.glob(f"{root}/{rid}/*/paired-benchmark")[0]
    s = json.load(open(f"{pb}/result-00-{arm}-c16-samples.json"))
    r = json.load(open(f"{pb}/result-00-{arm}-c16.json"))
    res = json.load(open(f"{pb}/resources-00-{arm}-c16.json"))
    s = sorted(s, key=lambda x: x["start_unix_nano"] + x["latency_ms"] * 1e6)
    n = len(s) // 3
    wins = []
    for i in range(3):
        w = s[i * n:(i + 1) * n if i < 2 else len(s)]
        ce = [x["start_unix_nano"] + x["latency_ms"] * 1e6 for x in w]
        span = (ce[-1] - ce[0]) / 1e9
        wins.append({**stats([x["latency_ms"] for x in w]), "wfs": len(w) / span})
    cpu = {k.split("-")[-2]: v["cpu_seconds"] for k, v in res["containers"].items()}
    return {"all": {**stats([x["latency_ms"] for x in s]), "wfs": r["successful_workflows_per_second"]},
            "wins": wins, "cpu": cpu, "failed": r["failed"]}


def boot(deltas, B=20000):
    m = [statistics_mean(random.choices(deltas, k=len(deltas))) for _ in range(B)]
    m.sort()
    return statistics_mean(deltas), m[int(.025 * B)], m[int(.975 * B)]


def statistics_mean(x):
    return sum(x) / len(x)


ids = [l.split()[:2] for l in open(idsf) if l.strip()]
for kind in ["paced50"]:
    rids = [i for k, i in ids if k == kind]
    D = {rid: {a: load(rid, a) for a in ARMS} for rid in rids}
    print(f"\n## {kind}")
    for rid in rids:
        for a in ARMS:
            d = D[rid][a]
            w = " | ".join(f"{x['p50']:.0f}/{x['p95']:.0f}/{x['p99']:.0f} @{x['wfs']:.1f}" for x in d["wins"])
            c = "/".join(f"{d['cpu'][k]:.0f}" for k in ["frontend", "history", "matching"])
            print(f"| {rid} | {a} | {d['all']['wfs']:.1f} | {d['all']['p50']:.0f}/{d['all']['p95']:.0f}/{d['all']['p99']:.0f} | {w} | {c} | {d['failed']} |")
    for other in ["go", "control"]:
        for key, get in [("p99 all", lambda d: d["all"]["p99"]), ("p99 last third", lambda d: d["wins"][2]["p99"]),
                         ("p99 first third", lambda d: d["wins"][0]["p99"]), ("p99 mid third", lambda d: d["wins"][1]["p99"]),
                         ("p50 all", lambda d: d["all"]["p50"]), ("p95 all", lambda d: d["all"]["p95"]),
                         ("wf/s", lambda d: d["all"]["wfs"])]:
            rel = [(get(D[r]["rust"]) - get(D[r][other])) / get(D[r][other]) * 100 for r in rids]
            ab = [get(D[r]["rust"]) - get(D[r][other]) for r in rids]
            m, lo, hi = boot(rel)
            ma = statistics_mean(ab)
            print(f"rust-{other} {key}: mean {ma:+.1f} abs, rel {m:+.1f}% CI[{lo:+.1f},{hi:+.1f}] per-run {[round(x,1) for x in rel]}")
