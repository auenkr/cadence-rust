#!/usr/bin/env python3 -I
"""Knee analysis. usage: perf-c1-knee-analysis.py <dir> ; dir/<rate>/<runid>/{cpu-model.txt,paired-benchmark/result-*-<arm>-c1.json,resources-*.json}
PASS per rate iff run-cluster bootstrap 95% CI upper bound of (rust-go)/go <= +5% for p90 and p95 of pooled request latencies."""
import glob, json, os, random, sys
root = sys.argv[1]; random.seed(1); B = 20000; ARMS = ["go", "control", "rust"]

def pct(xs, p):
    xs = sorted(xs); k = (len(xs)-1)*p/100; lo = int(k); hi = min(lo+1, len(xs)-1)
    return xs[lo] + (xs[hi]-xs[lo])*(k-lo)

def load(rd):
    d = {a: [] for a in ARMS}
    for a in ARMS:
        for f in sorted(glob.glob(os.path.join(rd, "paired-benchmark", f"result-*-{a}-c1.json"))):
            j = json.load(open(f)); d[a].append((j["successful_latency_samples_ms"], j["successful_workflows_per_second"],
                                                 f.replace("result-", "resources-")))
    return d

def cpu_of(path):
    try:
        r = json.load(open(path)); s = json.dumps(r)
        for k in ("cpu_seconds_total", "cpu_seconds", "total_cpu_seconds"):
            if k in r: return r[k]
        return None
    except Exception: return None

for rate in sorted(os.listdir(root), key=lambda x: (x == "unpaced", int(x) if x.isdigit() else 0)):
    runs = {os.path.basename(r): load(r) for r in sorted(glob.glob(os.path.join(root, rate, "*")))}
    names = sorted(runs)
    print(f"\n## target_rate={rate}  runs={len(names)}")
    for n in names:
        print(f"- {n}: {open(os.path.join(root, rate, n, 'cpu-model.txt')).read().strip()} reps go/ctl/rust="
              + "/".join(str(len(runs[n][a])) for a in ARMS))
    allv = {a: [s for n in names for rep in runs[n][a] for s in rep[0]] for a in ARMS}
    print("\n| arm | N | wf/s median | p50 | p90 | p95 | p99 | share>=75ms |\n|--|--|--|--|--|--|--|--|")
    for a in ARMS:
        xs = allv[a]; w = sorted(rep[1] for n in names for rep in runs[n][a])
        print(f"| {a} | {len(xs)} | {w[len(w)//2]:.2f} | " + " | ".join(f"{pct(xs,p):.1f}" for p in (50, 90, 95, 99)) + f" | {sum(s>=75 for s in xs)/max(1,len(xs)):.3f} |")
    def boot(a, b, p):
        out = []
        for _ in range(B):
            sa, sb = [], []
            for _ in names:
                n = random.choice(names)
                ra = random.choices(runs[n][a], k=len(runs[n][a])); rb = random.choices(runs[n][b], k=len(runs[n][b]))
                sa += [s for r in ra for s in r[0]]; sb += [s for r in rb for s in r[0]]
            out.append((pct(sa, p)-pct(sb, p))/pct(sb, p)*100)
        out.sort(); return out[int(.025*B)], out[int(.975*B)]
    verdict = True
    print("\n| contrast | metric | point % | 95% CI % | ")
    print("|--|--|--|--|")
    for a, b in (("rust", "go"), ("rust", "control"), ("control", "go")):
        for p in (90, 95):
            pt = (pct(allv[a], p)-pct(allv[b], p))/pct(allv[b], p)*100
            lo, hi = boot(a, b, p)
            print(f"| {a}-{b} | p{p} | {pt:+.1f} | [{lo:+.1f}, {hi:+.1f}] |")
            if (a, b) == ("rust", "go") and hi > 5: verdict = False
    print(f"\n**Verdict rate {rate}: {'PASS' if verdict else 'FAIL'}**")
    print("\nPer-run p90/p95 (go | control | rust):")
    for n in names:
        print(f"- {n}: " + " | ".join(f"{pct([s for r in runs[n][a] for s in r[0]],90):.0f}/{pct([s for r in runs[n][a] for s in r[0]],95):.0f}" for a in ARMS))
