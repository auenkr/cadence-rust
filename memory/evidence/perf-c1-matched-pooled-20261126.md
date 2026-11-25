# c1 matched-rate pooled comparison: Go vs Rust at target_rate=15 (2026-11-26)

Branch `perf-c1-matched-pooled`. **No code change** (rust/ and go/ untouched). Question: at an *equal start rate* (target_rate=15 wf/s, below what either side saturates),
is Rust's c1 p95 at or below Go's? Follow-up to perf-c1-pooled-20261125.md (unthrottled) and the matched-rate section of perf-c1-burst-20261125.md.

## Method
6 parallel dispatches of `frontend-matching-benchmark.yml` on master, `source_ref=2d5e47538ed0a73e561af4dabc3c5911ba60052d` (master head), `control_ref=origin/master`,
`concurrency=1`, `target_rate=15`, `database=mysql`, `rust_services=frontend,matching,history`, `sdk_openloop=false`, `repetitions=5`, default dynamic config, shape echo,
1000 requests/rep, warmup 10. Three arms per run (go, rust, control = same tree as rust, A/A) -> 30 reps and 30 000 requests per arm. Artifact
`frontend-matching-paired-benchmark-echo`. All 6 runs concluded success. Analysis: `memory/evidence/perf-c1-matched-pooled-analysis.py <dir>` (copy of the pooled script; stdlib python, seed 1,
20 000 bootstrap draws). Raw output: all numbers below come from that script.
Run URLs:
- https://github.com/auenkr/cadence-rust/actions/runs/45690742608
- https://github.com/auenkr/cadence-rust/actions/runs/45690762986
- https://github.com/auenkr/cadence-rust/actions/runs/45690783935
- https://github.com/auenkr/cadence-rust/actions/runs/45690804882
- https://github.com/auenkr/cadence-rust/actions/runs/45690827757
- https://github.com/auenkr/cadence-rust/actions/runs/45690846871

Rate check: per-rep wf/s medians go 15.01 / control 15.01 / rust 15.01; min go 9.6 (one rep failed to hold 15), control 14.75, rust 14.96. So the rate is matched in 29/30 go reps and 30/30 rust reps.

## Binary SHA256 (same in all 6 runs)
- Rust `cadence-server-real` (rust + control arms): `f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74`
- Go `cadence-history` (go arm): `1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5`
- `/usr/local/bin/cadence-server` (Go arm entry binary / wrapper), differs per run: see table below (first 16 hex).

| run | /usr/local/bin/cadence-server sha |
|--|--|
| 45690742608 | fee88ba93d293590 |
| 45690762986 | ce9377f04af5b6da |
| 45690783935 | eb6b6c9d7760bac1 |
| 45690804882 | bad493c7d47bd20e |
| 45690827757 | 0f2218b587e01f10 |
| 45690846871 | b3980327cc1f2ede |

## Runs

| run | CPU | reps (go/control/rust) |
|--|--|--|
| 45690742608 | AMD EPYC 9V45 96-Core Processor | 5/5/5 |
| 45690762986 | AMD EPYC 7763 64-Core Processor | 5/5/5 |
| 45690783935 | INTEL(R) XEON(R) PLATINUM 8573C | 5/5/5 |
| 45690804882 | AMD EPYC 9V74 80-Core Processor | 5/5/5 |
| 45690827757 | AMD EPYC 9V74 80-Core Processor | 5/5/5 |
| 45690846871 | AMD EPYC 9V45 96-Core Processor | 5/5/5 |

## Pooled per-rep medians (all reps)

| arm | n | p50 | p95 | p99 | wf/s |
|--|--|--|--|--|--|
| go | 30 | 38.5 | 49.6 | 60.4 | 15.0 |
| control | 30 | 24.4 | 31.5 | 49.3 | 15.0 |
| rust | 30 | 25.1 | 32.0 | 45.7 | 15.0 |

IQR of per-rep p95 (ms): go 45.5-101.2 (min 33.7, max 380.2), control 28.2-106.8 (min 22.7, max 169.0), rust 28.5-80.1 (min 21.4, max 212.8)

## Bootstrap 95% CI of difference in pooled median of per-rep values (reps resampled, 20000 draws)

| contrast | metric | point | 95% CI | share of draws with diff<=0 / >=0 |
|--|--|--|--|--|
| rust - go | p50 | -13.4 | [-15.8, -8.3] | 1.000 / 0.000 |
| rust - go | p95 | -17.5 | [-34.1, +0.3] | 0.977 / 0.023 |
| rust - go | p99 | -14.7 | [-95.4, +34.5] | 0.958 / 0.042 |
| rust - go | wf/s | +0.0 | [+0.0, +0.0] | 0.000 / 1.000 |
| control - go | p50 | -14.1 | [-16.3, -9.0] | 1.000 / 0.000 |
| control - go | p95 | -18.0 | [-35.4, +12.1] | 0.957 / 0.043 |
| control - go | p99 | -11.1 | [-94.6, +52.6] | 0.953 / 0.047 |
| control - go | wf/s | +0.0 | [+0.0, +0.0] | 0.000 / 1.000 |
| rust - control | p50 | +0.7 | [-1.0, +2.1] | 0.138 / 0.862 |
| rust - control | p95 | +0.5 | [-28.4, +16.8] | 0.384 / 0.616 |
| rust - control | p99 | -3.6 | [-65.4, +44.7] | 0.504 / 0.496 |
| rust - control | wf/s | +0.0 | [-0.0, +0.0] | 0.284 / 0.716 |

## Cluster bootstrap (resample runs, then reps within run) of the same difference

| contrast | metric | point | 95% CI |
|--|--|--|--|
| rust - go | p95 | -17.5 | [-29.4, -3.8] |
| rust - go | wf/s | +0.0 | [+0.0, +0.0] |
| rust - control | p95 | +0.5 | [-30.7, +2.7] |
| rust - control | wf/s | +0.0 | [-0.0, +0.0] |

## Within-run paired comparison (per run: median of 5 reps per arm, then difference)

| run | CPU | go p95 | control p95 | rust p95 | rust-go | rust-control | go wf/s | rust wf/s |
|--|--|--|--|--|--|--|--|--|
| 45690742608 | AMD EPYC 9V45 96-Core Processor | 45.4 | 31.8 | 31.8 | -13.7 | -0.1 | 15.0 | 15.0 |
| 45690762986 | AMD EPYC 7763 64-Core Processor | 48.2 | 28.3 | 28.5 | -19.7 | +0.2 | 15.0 | 15.0 |
| 45690783935 | INTEL(R) XEON(R) PLATINUM 8573C | 50.0 | 31.5 | 32.1 | -17.9 | +0.5 | 15.0 | 15.0 |
| 45690804882 | AMD EPYC 9V74 80-Core Processor | 114.5 | 111.9 | 102.5 | -12.0 | -9.4 | 15.0 | 15.0 |
| 45690827757 | AMD EPYC 9V74 80-Core Processor | 126.3 | 152.1 | 166.9 | +40.6 | +14.8 | 15.0 | 15.0 |
| 45690846871 | AMD EPYC 9V45 96-Core Processor | 37.3 | 24.4 | 24.5 | -12.7 | +0.2 | 15.0 | 15.0 |

rust-go p95 over 6 runs: mean -5.9 ms, median -13.2, rust worse in 1/6 runs, 2-sided sign-test p=0.219; bootstrap 95% CI of mean [-17.1, +13.0] (single draw each, indicative).
rust-control p95: mean +1.0 ms, rust worse in 4/6 runs, sign-test p=0.688.
Per-rep paired (same run, same rep index) rust-go p95: n=30, mean -18.7, median -17.5, rust worse 5/30, bootstrap CI of mean [-43.0, -0.0].

## All-request pooled latency percentiles (ms; successful requests of all reps)

| arm | N | p50 | p90 | p95 | p99 | p99.9 | share >=75 ms | share >=150 ms |
|--|--|--|--|--|--|--|--|--|
| go | 30000 | 38.9 | 50.2 | 68.0 | 213.6 | 524.0 | 0.043 | 0.018 |
| control | 30000 | 24.5 | 33.5 | 53.3 | 177.5 | 413.2 | 0.034 | 0.013 |
| rust | 30000 | 24.9 | 34.6 | 56.6 | 176.3 | 396.8 | 0.035 | 0.013 |

Bootstrap of the all-request p95 difference, resampling whole reps (20000 -> 2000 draws for speed):

| contrast | point | 95% CI |
|--|--|--|
| rust - go | -11.4 | [-53.2, +20.8] |
| rust - control | +3.3 | [-28.8, +34.5] |
| control - go | -14.7 | [-51.8, +15.8] |

## Per-run all-request p95 (ms)

| run | go | control | rust |
|--|--|--|--|
| 45690742608 | 44.8 | 34.1 | 32.0 |
| 45690762986 | 49.0 | 28.5 | 28.7 |
| 45690783935 | 50.4 | 31.6 | 32.1 |
| 45690804882 | 119.7 | 104.5 | 95.7 |
| 45690827757 | 199.4 | 149.3 | 161.8 |
| 45690846871 | 39.7 | 25.1 | 24.9 |

## Verdict
**Yes: at an equal 15 wf/s start rate Rust's c1 p95 is at or below Go's; no evidence it is worse.**
- Pooled median of per-rep p95: Go 49.6, control 31.5, Rust 32.0 ms. Rust-Go = -17.5 ms; rep bootstrap CI [-34.1, +0.3] (touches 0, 97.7% of draws <= 0); run-cluster bootstrap CI
  [-29.4, -3.8] (excludes 0, in Rust's favour). Per-rep paired (same run, same rep index): mean -18.7, Rust worse in 5/30 reps, CI of mean [-43.0, -0.0].
- Within-run paired: Rust better in 5/6 runs (-12 to -20 ms on 4 of them); only run 45690827757 (EPYC 9V74, all arms slow, 126-167 ms) is the other way (+40.6 vs Go). Mean -5.9 ms, median -13.2,
  sign-test p=0.22 (n=6 too small for significance on its own).
- p50: Rust -13.4 ms (CI [-15.8, -8.3]). p99: -14.7 ms but CI [-95.4, +34.5] includes 0, so p99 is a tie. All-request p95: Rust 56.6 vs Go 68.0 (-11.4, CI [-53.2, +20.8]): consistent but not
  significant at request level. Share of requests >= 75 ms: Rust 3.5% vs Go 4.3% (unlike the unthrottled run, no plateau penalty for Rust).
- Rust vs its own A/A control: p95 +0.5 ms, CI [-28.4, +16.8]; no Rust-specific difference between the two Rust arms (the control-go gap, -18.0, equals the rust-go gap).
- Caveat: run-to-run hardware noise dominates both arms (two EPYC 9V74 runs have p95 100-167 ms for all three arms; Go's per-rep p95 ranges 34-380 ms). The sign is stable, the magnitude is not.
- Together with perf-c1-pooled-20261125.md: unthrottled, p95 is a statistical tie at the 100 ms limiter plateau (+0.7 ms); matched at 15 wf/s, Rust is lower (-17.5 ms) with the same wf/s.
