# c1 pooled comparison: Go vs Rust p95 under the default config (2026-11-25)

Branch `perf-c1-pooled`. **No code change** (rust/ and go/ untouched). Question: is Rust's c1 p95 (99.8 ms) statistically worse than Go's (89.2 ms) in
benchmark-final-38594e592-20261125.md, given Go's c1 p95 swings 55-107 ms between runs?

## Method
6 independent dispatches of `frontend-matching-benchmark.yml` on master, `source_ref=9cff5673d9fc46311e6072db4c74edce1c5c2880` (master head), `control_ref=origin/master`,
`rust_services=frontend,matching,history`, `concurrency=1`, `repetitions=5`, `sdk_openloop=false`, `database=mysql`, default dynamic config, shape echo, 1000 requests/rep,
warmup 10. Three arms per run (go, rust, control = same tree as rust, A/A). Total 30 reps per arm, 30 000 requests per arm. Artifact `frontend-matching-paired-benchmark-echo`
(`paired-benchmark/result-NN-<arm>-c1.json`, includes `successful_latency_samples_ms`; `cpu-model.txt`). All runs concluded success.
Analysis: `memory/evidence/perf-c1-pooled-analysis.py <dir>` (stdlib python, seed 1, 20 000 bootstrap draws). Run URLs:
- https://github.com/auenkr/cadence-rust/actions/runs/45658429542
- https://github.com/auenkr/cadence-rust/actions/runs/45658448462
- https://github.com/auenkr/cadence-rust/actions/runs/45658465039
- https://github.com/auenkr/cadence-rust/actions/runs/45658479512
- https://github.com/auenkr/cadence-rust/actions/runs/45658499995
- https://github.com/auenkr/cadence-rust/actions/runs/45658520690

## Runs

| run | CPU | reps (go/control/rust) |
|--|--|--|
| 45658429542 | AMD EPYC 9V74 80-Core Processor | 5/5/5 |
| 45658448462 | AMD EPYC 9V45 96-Core Processor | 5/5/5 |
| 45658465039 | AMD EPYC 9V45 96-Core Processor | 5/5/5 |
| 45658479512 | Intel(R) Xeon(R) 6975P-C | 5/5/5 |
| 45658499995 | Intel(R) Xeon(R) 6975P-C | 5/5/5 |
| 45658520690 | AMD EPYC 9V74 80-Core Processor | 5/5/5 |

## Pooled per-rep medians (all reps)

| arm | n | p50 | p95 | p99 | wf/s |
|--|--|--|--|--|--|
| go | 30 | 30.8 | 99.8 | 108.5 | 24.4 |
| control | 30 | 19.6 | 100.5 | 109.3 | 25.8 |
| rust | 30 | 19.4 | 100.5 | 109.6 | 25.8 |

IQR of per-rep p95 (ms): go 99.1-100.4 (min 67.6, max 178.1), control 100.5-100.9 (min 100.1, max 186.1), rust 100.3-101.0 (min 99.6, max 146.6)

## Bootstrap 95% CI of difference in pooled median of per-rep values (reps resampled, 20000 draws)

| contrast | metric | point | 95% CI | share of draws with diff<=0 / >=0 |
|--|--|--|--|--|
| rust - go | p50 | -11.4 | [-13.4, -7.1] | 1.000 / 0.000 |
| rust - go | p95 | +0.7 | [+0.1, +1.2] | 0.007 / 0.993 |
| rust - go | p99 | +1.1 | [-5.8, +6.3] | 0.333 / 0.667 |
| rust - go | wf/s | +1.3 | [+0.4, +2.2] | 0.006 / 0.994 |
| control - go | p50 | -11.2 | [-13.1, -6.9] | 1.000 / 0.000 |
| control - go | p95 | +0.7 | [+0.2, +1.3] | 0.000 / 1.000 |
| control - go | p99 | +0.7 | [-5.1, +6.5] | 0.390 / 0.610 |
| control - go | wf/s | +1.3 | [+0.4, +2.2] | 0.003 / 0.997 |
| rust - control | p50 | -0.2 | [-2.2, +1.7] | 0.767 / 0.233 |
| rust - control | p95 | -0.1 | [-0.3, +0.1] | 0.773 / 0.227 |
| rust - control | p99 | +0.4 | [-6.1, +5.3] | 0.505 / 0.495 |
| rust - control | wf/s | -0.0 | [-0.8, +0.8] | 0.508 / 0.492 |

## Cluster bootstrap (resample runs, then reps within run) of the same difference

| contrast | metric | point | 95% CI |
|--|--|--|--|
| rust - go | p95 | +0.7 | [-0.1, +8.5] |
| rust - go | wf/s | +1.3 | [+0.8, +1.9] |
| rust - control | p95 | -0.1 | [-0.7, +0.4] |
| rust - control | wf/s | -0.0 | [-0.6, +0.5] |

## Within-run paired comparison (per run: median of 5 reps per arm, then difference)

| run | CPU | go p95 | control p95 | rust p95 | rust-go | rust-control | go wf/s | rust wf/s |
|--|--|--|--|--|--|--|--|--|
| 45658429542 | AMD EPYC 9V74 80-Core Processor | 111.1 | 106.0 | 106.7 | -4.5 | +0.7 | 21.8 | 24.4 |
| 45658448462 | AMD EPYC 9V45 96-Core Processor | 75.2 | 100.8 | 99.9 | +24.7 | -0.9 | 22.8 | 24.0 |
| 45658465039 | AMD EPYC 9V45 96-Core Processor | 99.7 | 100.6 | 100.3 | +0.6 | -0.3 | 24.6 | 25.5 |
| 45658479512 | Intel(R) Xeon(R) 6975P-C | 100.4 | 100.5 | 100.3 | -0.2 | -0.2 | 25.5 | 26.6 |
| 45658499995 | Intel(R) Xeon(R) 6975P-C | 100.2 | 100.5 | 100.4 | +0.3 | -0.1 | 25.2 | 26.3 |
| 45658520690 | AMD EPYC 9V74 80-Core Processor | 99.1 | 100.5 | 100.6 | +1.6 | +0.1 | 24.3 | 26.0 |

rust-go p95 over 6 runs: mean +3.7 ms, median +0.4, rust worse in 4/6 runs, 2-sided sign-test p=0.688; bootstrap 95% CI of mean [-1.8, +12.6] (single draw each, indicative).
rust-control p95: mean -0.1 ms, rust worse in 2/6 runs, sign-test p=0.688.
Per-rep paired (same run, same rep index) rust-go p95: n=30, mean +3.9, median +0.7, rust worse 23/30, bootstrap CI of mean [-3.3, +10.2].

## All-request pooled latency percentiles (ms; successful requests of all reps)

| arm | N | p50 | p90 | p95 | p99 | p99.9 | share >=75 ms | share >=150 ms |
|--|--|--|--|--|--|--|--|--|
| go | 30000 | 31.7 | 81.9 | 100.1 | 118.5 | 269.5 | 0.122 | 0.005 |
| control | 30000 | 21.1 | 98.8 | 100.8 | 118.5 | 310.1 | 0.197 | 0.006 |
| rust | 30000 | 21.4 | 98.5 | 100.6 | 116.9 | 296.0 | 0.195 | 0.006 |

Bootstrap of the all-request p95 difference, resampling whole reps (20000 -> 2000 draws for speed):

| contrast | point | 95% CI |
|--|--|--|
| rust - go | +0.5 | [-0.1, +1.2] |
| rust - control | -0.1 | [-0.6, +0.2] |
| control - go | +0.7 | [-0.0, +1.3] |

## Per-run all-request p95 (ms)

| run | go | control | rust |
|--|--|--|--|
| 45658429542 | 120.9 | 117.9 | 110.0 |
| 45658448462 | 73.3 | 100.8 | 100.5 |
| 45658465039 | 99.7 | 100.5 | 100.3 |
| 45658479512 | 100.4 | 100.5 | 100.3 |
| 45658499995 | 100.3 | 100.5 | 100.4 |
| 45658520690 | 99.1 | 100.5 | 100.7 |

## Verdict
**Rust's c1 p95 is not statistically worse than Go's in any practically meaningful way.**
- Pooled median of per-rep p95: Go 99.8, control 100.5, Rust 100.5 ms. Rust-Go = +0.7 ms (rep bootstrap CI [+0.1, +1.2]; cluster bootstrap that respects run-to-run
  hardware variance CI [-0.1, +8.5], includes 0). All-request p95: Rust-Go +0.5 ms, CI [-0.1, +1.2], includes 0. Paired within-run: Rust worse in 4/6 runs, mean +3.7 ms,
  sign-test p=0.69, CI of mean includes 0; the +24.7 ms mean is driven by a single run (45658448462) where Go happened to land at 75 ms.
- Rust vs its own A/A control: -0.1 ms, CI [-0.3, +0.1]; the control differs from Go by the same +0.7 ms, so any Go gap is not Rust-specific.
- The earlier 99.8 vs 89.2 ms gap was Go variance: Go p95 is bimodal (99-101 ms in 4 of 6 runs; 75 ms and 111-121 ms in the other two) while Rust/control sit at 100-101 ms
  in 5 of 6 runs (the 6th, EPYC 9V74 run 45658429542, is 106-107 for both Rust arms and 111 Go). 100 ms is the 20 rps task-load limiter plateau (perf-c1-permit-timing-20261125.md).
- Where Rust does differ: p50 -11 ms (CI [-13.4, -7.1]) and wf/s +1.3 (+5%, CI [+0.4, +2.2]) in Rust's favour. The share of requests >= 75 ms is 19.5% for Rust vs 12.2% for Go,
  i.e. Rust's faster path lands more requests on the limiter plateau (same mechanism as the permit-timing note); this moves the p90 (98 vs 82), not p95/p99 (p99 116.9 vs 118.5).
- Caveat: Go's lower p95 in 1 of 6 runs (75 ms) shows Go can finish a run below the plateau; Rust never did in 30 reps (min 99.6). Rust min-p95 vs Go min-p95 (67.6) is the
  only observable asymmetry, and it requires a tail-free limiter pattern that Go reaches by being slower on average.
