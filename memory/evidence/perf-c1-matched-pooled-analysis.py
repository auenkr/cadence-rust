#!/usr/bin/env python3 -I
"""Pooled c1 analysis. usage: perf-c1-pooled-analysis.py <dir-with-one-subdir-per-run-id>"""
import glob, json, os, random, statistics as st, sys

root = sys.argv[1]
random.seed(1)
B = 20000
ARMS = ["go", "control", "rust"]
data = {}  # run -> arm -> list of (p50,p95,p99,wfs,samples)
cpu = {}
for rd in sorted(glob.glob(os.path.join(root, "*"))):
    run = os.path.basename(rd)
    cpu[run] = open(os.path.join(rd, "cpu-model.txt")).read().strip()
    for arm in ARMS:
        for f in sorted(glob.glob(os.path.join(rd, "paired-benchmark", f"result-*-{arm}-c1.json"))):
            d = json.load(open(f))
            l = d["latency_ms"]
            data.setdefault(run, {}).setdefault(arm, []).append(
                (l["p50"], l["p95"], l["p99"], d["successful_workflows_per_second"], d["successful_latency_samples_ms"]))
runs = sorted(data)
M = ["p50", "p95", "p99", "wf/s"]


def vals(arm, i, rs=None):
    return [r[i] for run in (rs or runs) for r in data[run][arm]]


def pct(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def ci(f):
    xs = sorted(f() for _ in range(B))
    return xs[int(.025 * B)], xs[int(.975 * B)]


def boot_diff(a, b, stat=st.median):
    return ci(lambda: stat(random.choices(a, k=len(a))) - stat(random.choices(b, k=len(b))))


def boot_ratio(a, b):
    return ci(lambda: st.median(random.choices(a, k=len(a))) / st.median(random.choices(b, k=len(b))))


def clustered(arm_a, arm_b, i):
    """Cluster bootstrap: resample runs, then reps within run."""
    def one():
        sa, sb = [], []
        for _ in runs:
            run = random.choice(runs)
            ra, rb = data[run][arm_a], data[run][arm_b]
            sa += random.choices([r[i] for r in ra], k=len(ra))
            sb += random.choices([r[i] for r in rb], k=len(rb))
        return st.median(sa) - st.median(sb)
    return ci(one)


def p_pos(a, b):
    xs = [st.median(random.choices(a, k=len(a))) - st.median(random.choices(b, k=len(b))) for _ in range(B)]
    return sum(x <= 0 for x in xs) / B, sum(x >= 0 for x in xs) / B


out = []
w = out.append
w("## Runs\n\n| run | CPU | reps (go/control/rust) |\n|--|--|--|")
for r in runs:
    w(f"| {r} | {cpu[r]} | {len(data[r]['go'])}/{len(data[r]['control'])}/{len(data[r]['rust'])} |")

w("\n## Pooled per-rep medians (all reps)\n\n| arm | n | " + " | ".join(M) + " |\n|--|--|--|--|--|--|")
for a in ARMS:
    w(f"| {a} | {len(vals(a,0))} | " + " | ".join(f"{st.median(vals(a,i)):.1f}" for i in range(4)) + " |")
w("\nIQR of per-rep p95 (ms): " + ", ".join(f"{a} {pct(vals(a,1),25):.1f}-{pct(vals(a,1),75):.1f} (min {min(vals(a,1)):.1f}, max {max(vals(a,1)):.1f})" for a in ARMS))

w("\n## Bootstrap 95% CI of difference in pooled median of per-rep values (reps resampled, 20000 draws)\n")
w("| contrast | metric | point | 95% CI | share of draws with diff<=0 / >=0 |\n|--|--|--|--|--|")
for a, b in [("rust", "go"), ("control", "go"), ("rust", "control")]:
    for i in range(4):
        x, y = vals(a, i), vals(b, i)
        d = st.median(x) - st.median(y)
        lo, hi = boot_diff(x, y)
        pl, ph = p_pos(x, y)
        w(f"| {a} - {b} | {M[i]} | {d:+.1f} | [{lo:+.1f}, {hi:+.1f}] | {pl:.3f} / {ph:.3f} |")

w("\n## Cluster bootstrap (resample runs, then reps within run) of the same difference\n")
w("| contrast | metric | point | 95% CI |\n|--|--|--|--|")
for a, b in [("rust", "go"), ("rust", "control")]:
    for i in (1, 3):
        d = st.median(vals(a, i)) - st.median(vals(b, i))
        lo, hi = clustered(a, b, i)
        w(f"| {a} - {b} | {M[i]} | {d:+.1f} | [{lo:+.1f}, {hi:+.1f}] |")

w("\n## Within-run paired comparison (per run: median of 5 reps per arm, then difference)\n")
w("| run | CPU | go p95 | control p95 | rust p95 | rust-go | rust-control | go wf/s | rust wf/s |\n|--|--|--|--|--|--|--|--|--|")
dg, dc = [], []
for r in runs:
    g, c, ru = (st.median([x[1] for x in data[r][a]]) for a in ("go", "control", "rust"))
    gw, rw = (st.median([x[3] for x in data[r][a]]) for a in ("go", "rust"))
    dg.append(ru - g)
    dc.append(ru - c)
    w(f"| {r} | {cpu[r]} | {g:.1f} | {c:.1f} | {ru:.1f} | {ru-g:+.1f} | {ru-c:+.1f} | {gw:.1f} | {rw:.1f} |")


def sign_p(d):
    n = len(d)
    k = sum(x > 0 for x in d)
    from math import comb
    kk = max(k, n - k)
    return min(1, 2 * sum(comb(n, j) for j in range(kk, n + 1)) / 2 ** n)


w(f"\nrust-go p95 over {len(runs)} runs: mean {st.mean(dg):+.1f} ms, median {st.median(dg):+.1f}, rust worse in {sum(x>0 for x in dg)}/{len(dg)} runs, 2-sided sign-test p={sign_p(dg):.3f}; bootstrap 95% CI of mean [{ci(lambda: st.mean(random.choices(dg,k=len(dg))))[0]:+.1f}, {ci(lambda: st.mean(random.choices(dg,k=len(dg))))[1]:+.1f}] (single draw each, indicative).")
w(f"rust-control p95: mean {st.mean(dc):+.1f} ms, rust worse in {sum(x>0 for x in dc)}/{len(dc)} runs, sign-test p={sign_p(dc):.3f}.")
# paired per rep index (same iteration, interleaved arms in same run)
pg = [data[r]["rust"][k][1] - data[r]["go"][k][1] for r in runs for k in range(min(len(data[r]["rust"]), len(data[r]["go"])))]
w(f"Per-rep paired (same run, same rep index) rust-go p95: n={len(pg)}, mean {st.mean(pg):+.1f}, median {st.median(pg):+.1f}, rust worse {sum(x>0 for x in pg)}/{len(pg)}, bootstrap CI of mean "
  f"[{ci(lambda: st.mean(random.choices(pg,k=len(pg))))[0]:+.1f}, {ci(lambda: st.mean(random.choices(pg,k=len(pg))))[1]:+.1f}].")

w("\n## All-request pooled latency percentiles (ms; successful requests of all reps)\n")
w("| arm | N | p50 | p90 | p95 | p99 | p99.9 | share >=75 ms | share >=150 ms |\n|--|--|--|--|--|--|--|--|--|")
allv = {}
for a in ARMS:
    xs = [s for r in runs for rep in data[r][a] for s in rep[4]]
    allv[a] = xs
    w(f"| {a} | {len(xs)} | " + " | ".join(f"{pct(xs,p):.1f}" for p in (50, 90, 95, 99, 99.9)) + f" | {sum(s>=75 for s in xs)/len(xs):.3f} | {sum(s>=150 for s in xs)/len(xs):.3f} |")
w("\nBootstrap of the all-request p95 difference, resampling whole reps (20000 -> 2000 draws for speed):\n")
w("| contrast | point | 95% CI |\n|--|--|--|")
reps = {a: [rep[4] for r in runs for rep in data[r][a]] for a in ARMS}


def rep_boot(a, b):
    def one():
        ra = random.choices(reps[a], k=len(reps[a]))
        rb = random.choices(reps[b], k=len(reps[b]))
        return pct([s for x in ra for s in x], 95) - pct([s for x in rb for s in x], 95)
    xs = sorted(one() for _ in range(2000))
    return xs[50], xs[1949]


for a, b in [("rust", "go"), ("rust", "control"), ("control", "go")]:
    d = pct(allv[a], 95) - pct(allv[b], 95)
    lo, hi = rep_boot(a, b)
    w(f"| {a} - {b} | {d:+.1f} | [{lo:+.1f}, {hi:+.1f}] |")

w("\n## Per-run all-request p95 (ms)\n\n| run | go | control | rust |\n|--|--|--|--|")
for r in runs:
    w(f"| {r} | " + " | ".join(f"{pct([s for rep in data[r][a] for s in rep[4]],95):.1f}" for a in ARMS) + " |")
print("\n".join(out))
