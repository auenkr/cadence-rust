# PostgreSQL paced 50 wf/s soak (2026-11-26)

Branch `perf-pg-paced50`. No code change (rust/ and go/ untouched). Follow-up to perf-pg-soak-tails-20261126.md, whose paced test (70 wf/s) saturated Go; this is the clean paced test at 50 wf/s.

## Predeclared margin (same as before)
Rust p99 <= Go p99 * 1.05, judged on the upper bound of the 95% run-cluster bootstrap CI of the paired relative delta (Rust-Go)/Go (upper <= +5% pass; point <= +5% but upper > +5% inconclusive; point > +5% fail).

## Method
4 parallel dispatches of `frontend-matching-benchmark.yml` on master, `source_ref=22b0ef0b52f259059a039d69df883252ddb90d01`, `database=postgres concurrency=16 requests=12000 repetitions=1 target_rate=50 control_ref=origin/master rust_services=frontend,matching,history sdk_openloop=false`, default config. All 4 concluded success, 0 failed requests in every arm. Arms go/control/rust per run on one runner (control = same tree as rust, A/A).
Runs (https://github.com/auenkr/cadence-rust/actions/runs/<id>): 45708632120 (Xeon 6975P), 45708639681 (Xeon 6975P), 45708648251 (EPYC 7763), 45708653087 (EPYC 9V74).
Analysis: [perf-pg-paced50-analysis.py](perf-pg-paced50-analysis.py) (copy of perf-pg-soak-tails-analysis.py, only the kind list changed; thirds by completion time, 20000 bootstrap resamples, seed 20261126), input `paired-benchmark/result-00-<arm>-c16-samples.json`. Numbers in ms.
Binaries: Rust cadence-server `f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74`, cadence-history `1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5` (identical in all 4 runs; control same). Go binary (first 12 hex, rebuilt per run): 009ecda43aa9, 8259926499b4, 63284257b86f, 94c588725b8e.

## Results (table: run | arm | wf/s | p50/p95/p99 | windows p50/p95/p99 @wf/s | CPU-s FE/HI/MT | failed)
```

| 45708632120 | go | 50.0 | 89/114/127 | 88/113/124 @50.0 | 89/115/128 @50.0 | 90/115/128 @50.0 | 46/114/26 | 0 |
| 45708632120 | control | 50.0 | 82/109/113 | 81/109/112 @50.0 | 82/109/112 @50.0 | 82/109/113 @50.0 | 31/79/15 | 0 |
| 45708632120 | rust | 50.0 | 80/107/110 | 79/107/110 @50.0 | 80/107/110 @50.0 | 80/107/110 @50.0 | 30/79/15 | 0 |
| 45708639681 | go | 50.0 | 90/117/136 | 89/115/133 @50.0 | 91/118/138 @50.0 | 91/118/139 @50.0 | 47/115/26 | 0 |
| 45708639681 | control | 50.0 | 83/109/114 | 82/109/113 @50.0 | 83/109/114 @50.0 | 85/110/114 @50.0 | 33/85/17 | 0 |
| 45708639681 | rust | 50.0 | 80/108/111 | 80/108/112 @50.0 | 80/108/111 @50.0 | 80/108/111 @50.0 | 32/83/16 | 0 |
| 45708648251 | go | 50.0 | 108/178/215 | 106/172/209 @50.0 | 110/184/219 @50.0 | 110/177/209 @50.1 | 68/190/40 | 0 |
| 45708648251 | control | 50.0 | 90/116/127 | 89/116/124 @50.0 | 90/116/124 @50.0 | 90/117/128 @50.0 | 43/149/23 | 0 |
| 45708648251 | rust | 50.0 | 88/115/124 | 87/114/122 @50.0 | 89/115/125 @50.0 | 88/115/124 @50.0 | 43/149/23 | 0 |
| 45708653087 | go | 50.0 | 243/732/1148 | 264/958/1386 @44.4 | 258/575/896 @54.6 | 208/547/863 @52.3 | 52/135/31 | 0 |
| 45708653087 | control | 50.0 | 105/439/751 | 103/446/703 @50.0 | 112/451/696 @50.0 | 102/394/855 @50.0 | 34/117/20 | 0 |
```

Paired deltas (Rust minus other; mean over 4 runs, 95% run-cluster bootstrap CI of mean per-run relative delta):
```
| 45708653087 | rust | 50.0 | 96/326/665 | 95/354/681 @50.0 | 103/380/777 @50.0 | 93/227/456 @50.0 | 35/116/20 | 0 |
rust-go p99 all: mean -153.7 abs, rel -28.9% CI[-42.2,-15.7] per-run [-12.9, -18.4, -42.3, -42.0]
rust-go p99 last third: mean -134.5 abs, rel -30.5% CI[-44.0,-17.0] per-run [-13.8, -20.2, -40.9, -47.1]
rust-go p99 first third: mean -206.8 abs, rel -29.9% CI[-46.2,-13.5] per-run [-11.3, -15.7, -41.6, -50.9]
rust-go p99 mid third: mean -64.5 abs, rel -22.4% CI[-35.7,-13.6] per-run [-13.8, -19.4, -43.0, -13.3]
rust-go p50 all: mean -46.7 abs, rel -25.2% CI[-48.0,-10.8] per-run [-10.7, -10.9, -19.0, -60.4]
rust-go p95 all: mean -121.1 abs, rel -26.2% CI[-45.4,-7.0] per-run [-6.3, -7.8, -35.3, -55.4]
rust-go wf/s: mean -0.0 abs, rel -0.0% CI[-0.0,+0.0] per-run [0.0, -0.0, 0.0, -0.0]
rust-control p99 all: mean -23.3 abs, rel -4.5% CI[-9.1,-2.1] per-run [-2.0, -2.2, -2.2, -11.4]
rust-control p99 last third: mean -102.3 abs, rel -13.9% CI[-35.7,-2.6] per-run [-2.4, -2.8, -3.6, -46.6]
rust-control p99 first third: mean -6.9 abs, rel -2.0% CI[-2.7,-1.3] per-run [-2.0, -1.3, -1.4, -3.1]
rust-control p99 mid third: mean +19.5 abs, rel +2.1% CI[-2.0,+8.3] per-run [-1.9, -2.2, 0.7, 11.7]
rust-control p50 all: mean -3.7 abs, rel -3.9% CI[-6.5,-2.2] per-run [-2.6, -3.4, -1.9, -7.8]
rust-control p95 all: mean -29.2 abs, rel -7.4% CI[-19.6,-1.2] per-run [-1.5, -1.3, -1.2, -25.6]
rust-control wf/s: mean -0.0 abs, rel -0.0% CI[-0.0,+0.0] per-run [-0.0, -0.0, 0.0, -0.0]
```

## Verdict
- **Go sustained 50 wf/s: yes.** Overall wf/s was 50.0 for go, control and rust in 4/4 runs. Caveat: in run 45708653087 (EPYC 9V74) Go was visibly strained: first-third rate 44.4 wf/s (queue built), then 54.6 / 52.3 catching up, p99 1148 ms; Rust and control held 50.0 in every window of every run. That run is a Go-overload-flavoured comparison; the other three are clean (Go windows all 50.0).
- **p99 margin: PASS.** Rust-Go p99 = -28.9% (CI -42.2..-15.7%), upper bound far below +5%. Rust p99 below Go in 4/4 runs (-12.9, -18.4, -42.3, -42.0%), also in every third (last third -30.5%, CI upper -17.0%). Even on the three clean runs (Go held 50.0 in all windows) the delta is -12.9, -18.4, -42.3%, all negative. p50 -25.2% and p95 -26.2%, same direction.
- **Rust vs same-tree control:** p99 -4.5% (CI -9.1..-2.1%), p50 -3.9%; wf/s identical. Control deltas are small and slightly favour Rust arm (second in the A/A order), no Rust penalty; last-third p99 (-13.9%) is driven by one run on EPYC 9V74 (-46.6%), mid-third +2.1% CI [-2.0,+8.3] is noise.
- Caveats: n=4 runs, heterogeneous runners (two Xeon 6975P, EPYC 7763, EPYC 9V74), different Go binary build per run; absolute numbers not comparable across runs. Combined with perf-pg-soak-tails-20261126.md (unpaced -21.2%), PostgreSQL shows no Rust tail penalty vs Go at c16 either unpaced or at 50 wf/s paced.
