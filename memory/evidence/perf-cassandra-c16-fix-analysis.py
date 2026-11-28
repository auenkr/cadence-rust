#!/usr/bin/env python3
"""Cluster-bootstrap analysis of frontend-matching-benchmark runs (perf-cassandra-c16-fix).

Usage: perf-cassandra-c16-fix-analysis.py <dir-with-one-subdir-per-run>
Each run subdir contains frontend-matching-paired-benchmark-echo/paired-benchmark/result-RR-ARM-cC.json.
Per run and cell: d = mean over reps of (A - B) paired by rep; CI = percentile bootstrap over runs
(10000 resamples, seed 1). Same method as perf-cassandra-c16b-20261126.md.
"""
import glob, json, os, random, re, sys


def pct(xs, q):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def cpu(res):
    try:
        d = json.load(open(res))
    except Exception:
        return float("nan")
    tot = 0.0
    found = False
    def walk(o, path=""):
        nonlocal tot, found
        if isinstance(o, dict):
            for k, v in o.items():
                p = path + "/" + k
                if k in ("cpu_seconds", "cpu_s") and isinstance(v, (int, float)) and re.search(r"frontend|matching|history", p):
                    tot += v; found = True
                else:
                    walk(v, p)
        elif isinstance(o, list):
            for i, v in enumerate(o):
                walk(v, path)
    walk(d)
    return tot if found else float("nan")


def load(run):
    base = glob.glob(os.path.join(run, "**", "paired-benchmark"), recursive=True)[0]
    cells = {}
    for f in glob.glob(os.path.join(base, "result-*-c*.json")):
        m = re.match(r"result-(\d+)-(\w+)-c(\d+)\.json$", os.path.basename(f))
        if not m:
            continue
        rep, arm, c = int(m[1]), m[2], int(m[3])
        d = json.load(open(f))
        s = d.get("successful_latency_samples_ms") or []
        cells[(rep, arm, c)] = {
            "wfs": d["successful_workflows_per_second"],
            "p95": pct(s, 0.95),
            "p99": pct(s, 0.99),
            "cpu": cpu(os.path.join(base, f"resources-{m[1]}-{arm}-c{c}.json")),
            "failed": d.get("failed", 0),
        }
    return cells


def boot(vals, n=10000):
    rnd = random.Random(1)
    means = sorted(sum(rnd.choice(vals) for _ in vals) / len(vals) for _ in range(n))
    return means[int(0.025 * n)], means[int(0.975 * n) - 1]


def main():
    runs = sorted(d for d in glob.glob(os.path.join(sys.argv[1], "*")) if os.path.isdir(d))
    data = [load(r) for r in runs]
    print("runs:", ", ".join(os.path.basename(r) for r in runs))
    for a, b, title in (("rust", "go", "Rust(fix) - Go"), ("rust", "control", "Rust(fix) - Control(origin/master, before)"), ("control", "go", "Control(before) - Go")):
        print(f"\n### {title}\n\n| c | metric | diff | 95% CI | excludes 0 | per-run |\n|--|--|--|--|--|--|")
        for c in (1, 16, 64):
            for met in ("wfs", "p95", "p99", "cpu"):
                per = []
                for cells in data:
                    reps = sorted({k[0] for k in cells})
                    ds = [cells[(r, a, c)][met] - cells[(r, b, c)][met] for r in reps if (r, a, c) in cells and (r, b, c) in cells]
                    if ds:
                        per.append(sum(ds) / len(ds))
                if not per or any(x != x for x in per):
                    continue
                lo, hi = boot(per)
                m = sum(per) / len(per)
                ex = "YES" if lo > 0 or hi < 0 else "no"
                print(f"| {c} | {met} | {m:+.2f} | [{lo:+.2f}, {hi:+.2f}] | {ex} | {', '.join(f'{x:+.1f}' for x in per)} |")
    print("\n### Absolute means\n\n| c | arm | wf/s | p95 | p99 | cpu |\n|--|--|--|--|--|--|")
    for c in (1, 16, 64):
        for arm in ("go", "rust", "control"):
            v = [cells[k] for cells in data for k in cells if k[1] == arm and k[2] == c]
            if v:
                f = lambda met: sum(x[met] for x in v) / len(v)
                print(f"| {c} | {arm} | {f('wfs'):.1f} | {f('p95'):.0f} | {f('p99'):.0f} | {f('cpu'):.1f} |")
    print("\nfailed workflows total:", sum(x["failed"] for cells in data for x in cells.values()), "cells:", sum(len(c) for c in data))


main()
