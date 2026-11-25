# c1 matched-rate pooled, 60 reps per arm (2026-11-26)

Appendix to perf-c1-matched-pooled-20261126.md. **No code change.** Adds 6 more matched-rate runs to the original 6 (30 reps/arm) -> 12 runs, 60 reps and 60 000 requests per arm.

## Method
New runs: 6 parallel dispatches of `frontend-matching-benchmark.yml` on master, `source_ref=22b0ef0b52f259059a039d69df883252ddb90d01`, `control_ref=origin/master`, `database=mysql concurrency=1 target_rate=15 repetitions=5 rust_services=frontend,matching,history sdk_openloop=false`, default config. All succeeded, 0 failed requests.
New runs (https://github.com/auenkr/cadence-rust/actions/runs/<id>): 45708658955, 45708663814, 45708667479, 45708671486, 45708675647, 45708683160. Old runs (source_ref 2d5e475...): 45690742608, 45690762986, 45690783935, 45690804882, 45690827757, 45690846871.
Rust binary SHA256 same in all new runs (`cadence-server` f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74, `cadence-history` 1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5, identical to the old runs). Go hash per run (first 12): feba31a73a0a, 33a4bcabf13b, be4b88b4e8fc, 4f2007dea50d, aa3e3b79d62c, 09c3baf04cc0.
Analysis: [perf-c1-matched-pooled-analysis.py](perf-c1-matched-pooled-analysis.py) over all 12 run dirs (unchanged script), plus [perf-c1-matched-pooled60-p99-cluster.py](perf-c1-matched-pooled60-p99-cluster.py) for a run-cluster CI on p99 (the original script only clustered p95). Seed 1, 20000 draws. Numbers in ms.

## Pooled per-rep medians (60 reps/arm)
| arm | n | p50 | p95 | p99 | wf/s |
|--|--|--|--|--|--|
| go | 60 | 36.0 | 48.3 | 59.4 | 15.0 |
| control | 60 | 24.0 | 31.3 | 42.2 | 15.0 |
| rust | 60 | 24.7 | 31.7 | 44.0 | 15.0 |

## Rust - Go, pooled-median difference (point, 95% CI)
| metric | rep-level bootstrap | run-cluster bootstrap |
|--|--|--|
| p50 | -11.2 [-15.9, -7.4] | n/a (not computed) |
| p95 | -16.6 [-20.7, -12.2] | -16.6 [-21.3, -7.7] |
| p99 | -15.4 [-21.1, -9.5] | -15.4 [-23.9, -5.3] |
| wf/s | +0.0 | +0.0 |

Rust - control (A/A): p95 +0.4 rep [-2.6,+4.6] / cluster [-1.4,+6.7]; p99 +1.8 rep [-5.8,+8.2] / cluster [-5.4,+7.7]. No Rust-specific difference between the two Rust arms.

Within-run paired p95 (12 runs): mean -10.2, median -13.2, Rust worse in 1/12, sign-test p=0.006, mean CI [-16.8, -0.0]. Per-rep paired (n=60): mean -13.8, median -15.2, Rust worse in 8/60, CI [-27.1, -3.1].
All-request pooled (60 000/arm): p95 go 55.2 / control 39.3 / rust 41.8; p99 go 139.8 / control 115.7 / rust 123.5; p99.9 go 418.6 / rust 323.5. All-request p95 rust-go -13.4 CI [-23.0, -1.3].

## Verdict
With 60 reps per arm the p99 CI is now decisive: Rust p99 is lower than Go by 15.4 ms at c1 matched 15 wf/s, excluding 0 under both rep-level [-21.1, -9.5] and run-cluster [-23.9, -5.3] bootstraps (the 30-rep p99 CI was [-95.4, +34.5], a tie). p95 likewise excluded 0 under both (-16.6). Throughput equal (15.0 wf/s, all runs). Rust is not distinguishable from its same-tree control. Caveat: one old run (45690827757, EPYC 9V74) had Rust worse by +40.6 ms and one new (45708675647/45708683160 EPYC 9V45) had a Rust arm slower than control (+5.0, +3.3), i.e. hardware noise remains per-run, but the pooled sign is stable (Rust better in 11/12 runs).
Full per-run tables and all-request percentiles: rerun the scripts over the artifacts of the 12 runs above.
