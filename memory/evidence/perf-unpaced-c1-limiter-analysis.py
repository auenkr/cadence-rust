#!/usr/bin/env python3 -I
"""Knee2 analysis (same criteria as perf-c1-knee-analysis.py, draws raised, plus CPU-stratified sensitivity). Knee analysis. usage: perf-c1-knee-analysis.py <dir> ; dir/<rate>/<runid>/{cpu-model.txt,paired-benchmark/result-*-<arm>-c1.json,resources-*.json}
PASS per rate iff run-cluster bootstrap 95% CI upper bound of (rust-go)/go <= +5% for p90 and p95 of pooled request latencies."""
import glob, json, os, random, sys, statistics
root = sys.argv[1]; random.seed(1); B = int(os.environ.get('KNEE_B', '10000')); ARMS = os.environ.get("ARMS","go,control,rust").split(",")

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

for rate in ([sys.argv[2]] if len(sys.argv) > 2 else sorted(os.listdir(root), key=lambda x: (x == "unpaced", int(x) if x.isdigit() else 0))):
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
    # one resample per draw shared by all arms/contrasts; per-rep samples pre-sorted
    for n in names:
        for a in ARMS:
            runs[n][a] = [(sorted(r[0]), r[1], r[2]) for r in runs[n][a]]
    D = []
    for _ in range(B):
        pools = {a: [] for a in ARMS}
        for _ in names:
            n = random.choice(names)
            for a in ARMS:
                for r in random.choices(runs[n][a], k=len(runs[n][a])):
                    pools[a] += r[0]
        q = {a: sorted(pools[a]) for a in ARMS}
        D.append({(a, p): pct(q[a], p) for a in ARMS for p in (90, 95)})
    def boot(a, b, p):
        out = sorted((d[(a, p)]-d[(b, p)])/d[(b, p)]*100 for d in D)
        return out[int(.025*B)], out[int(.975*B)]
    verdict = True
    print("\n| contrast | metric | point % | 95% CI % | ")
    print("|--|--|--|--|")
    for a, b in [x for x in (("rust", "go"), ("rust", "control"), ("control", "go")) if x[0] in ARMS and x[1] in ARMS]:
        for p in (90, 95):
            pt = (pct(allv[a], p)-pct(allv[b], p))/pct(allv[b], p)*100
            lo, hi = boot(a, b, p)
            print(f"| {a}-{b} | p{p} | {pt:+.1f} | [{lo:+.1f}, {hi:+.1f}] |")
            if (a, b) == ("rust", "go") and hi > 5: verdict = False
    print(f"\n**Verdict rate {rate}: {'PASS' if verdict else 'FAIL'}**")

    # --- Sensitivity: stratify by runner CPU model (resample runs within each stratum, strata sizes fixed) ---
    def fam(n):
        m = open(os.path.join(root, rate, n, 'cpu-model.txt')).read().strip().upper()
        return next((k for k in ("EPYC 7763", "EPYC 9V74", "EPYC 9V45", "XEON") if k in m), m)
    strata = {}
    for n in names: strata.setdefault(fam(n), []).append(n)
    print("\nSensitivity A (CPU-stratified): strata = " + ", ".join(f"{k}:{len(v)}" for k, v in sorted(strata.items())))
    random.seed(2); DS = []
    for _ in range(B):
        pools = {a: [] for a in ARMS}
        for k, v in strata.items():
            for _ in v:
                n = random.choice(v)
                for a in ARMS:
                    for r in random.choices(runs[n][a], k=len(runs[n][a])): pools[a] += r[0]
        q = {a: sorted(pools[a]) for a in ARMS}
        DS.append({p: (pct(q["rust"], p)-pct(q["go"], p))/pct(q["go"], p)*100 for p in (90, 95)})
    ok = True
    for p in (90, 95):
        o = sorted(d[p] for d in DS); lo, hi = o[int(.025*B)], o[int(.975*B)]
        print(f"- stratified rust-go p{p}: [{lo:+.1f}, {hi:+.1f}]"); ok &= hi <= 5
    print(f"- stratified verdict (same +5% criterion): {'PASS' if ok else 'FAIL'}")
    print("\nSensitivity B (per CPU stratum point estimates, pooled within stratum; rust-go %):")
    for k, v in sorted(strata.items()):
        g = {a: [s for n in v for r in runs[n][a] for s in r[0]] for a in ARMS}
        print(f"- {k} (runs={len(v)}): p90 {(pct(g['rust'],90)-pct(g['go'],90))/pct(g['go'],90)*100:+.1f}%  p95 {(pct(g['rust'],95)-pct(g['go'],95))/pct(g['go'],95)*100:+.1f}%  (go p90/p95 {pct(g['go'],90):.0f}/{pct(g['go'],95):.0f}, rust {pct(g['rust'],90):.0f}/{pct(g['rust'],95):.0f})")
    d90 = [(pct([s for r in runs[n]['rust'] for s in r[0]],90)/pct([s for r in runs[n]['go'] for s in r[0]],90)-1)*100 for n in names]
    d95 = [(pct([s for r in runs[n]['rust'] for s in r[0]],95)/pct([s for r in runs[n]['go'] for s in r[0]],95)-1)*100 for n in names]
    print(f"\nSensitivity C (paired per run, same runner for both arms): median rust-go p90 {statistics.median(d90):+.1f}%, p95 {statistics.median(d95):+.1f}%; runs with rust<=go: p90 {sum(x<=0 for x in d90)}/{len(d90)}, p95 {sum(x<=0 for x in d95)}/{len(d95)}; worst run p95 {max(d95):+.1f}%")
    print("\nPer-run p90/p95 (go | control | rust):")
    for n in names:
        print(f"- {n}: " + " | ".join(f"{pct([s for r in runs[n][a] for s in r[0]],90):.0f}/{pct([s for r in runs[n][a] for s in r[0]],95):.0f}" for a in ARMS))
