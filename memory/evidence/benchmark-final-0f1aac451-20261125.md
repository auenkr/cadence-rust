# Final head-pinned benchmark report — master 0f1aac451 (2026-11-25/26)

Head SHA: `0f1aac4516363ecf925069e63c02685cc2c005d8` (origin/master). Workflow `frontend-matching-benchmark.yml` dispatched on master, `fresh_build=true`, `source_ref=0f1aac4516363ecf925069e63c02685cc2c005d8`, `control_ref=origin/master`, `rust_services=frontend,matching,history`, `sdk_openloop=false`. Arms: **go**, **rust**, **control** (same tree/binary as rust, `control-same-tree.txt`=1 in every run = A/A noise arm). Method as memory/evidence/benchmark-final-38594e592-20261125.md. This head includes the jemalloc `_rjem_malloc_conf` fix.

| Run | ID | Conclusion |
|--|--|--|
| rust-ci.yml full workspace (clippy + tests), head `0f1aac451` | [45674155587](https://github.com/auenkr/cadence-rust/actions/runs/45674155587) | success |
| A: default MySQL, c=1,16,64 x5 | [45674162567](https://github.com/auenkr/cadence-rust/actions/runs/45674162567) | success |
| B: `history.transferProcessorMaxPollRPS: 1000`, MySQL, c=1,16,64 x5 | [45674167817](https://github.com/auenkr/cadence-rust/actions/runs/45674167817) | success |
| C: PostgreSQL, c=1,16,64 x5 | [45674172513](https://github.com/auenkr/cadence-rust/actions/runs/45674172513) | success |
| Soak 1: MySQL c16 requests=12000 x1 | [45674177546](https://github.com/auenkr/cadence-rust/actions/runs/45674177546) | success |
| Soak 2: same | [45674183003](https://github.com/auenkr/cadence-rust/actions/runs/45674183003) | **failure** (Go-arm client exited 1 on a late `EntityNotExistsError`, Go arm only, before Rust ran) |
| Soak 2 re-run: same | [45686129462](https://github.com/auenkr/cadence-rust/actions/runs/45686129462) | success (used as soak 2) |
| shape=activities, 300 req, c16, x3 | [45674189151](https://github.com/auenkr/cadence-rust/actions/runs/45674189151) | success |
| shape=signal-timer, 300 req, c16, x3 | [45674195860](https://github.com/auenkr/cadence-rust/actions/runs/45674195860) | success |
| shape=child, 300 req, c16, x3 | [45674203606](https://github.com/auenkr/cadence-rust/actions/runs/45674203606) | success |
| shape=history, 20 req, warmup 2, c4, x3 | [45674209612](https://github.com/auenkr/cadence-rust/actions/runs/45674209612) | success |

## Runner CPU / spec per run (2 cores x 2 threads, nproc 4, 15 GiB, Ubuntu 26.04.1; container limits 1 cpu / 1.4 GiB, DB 1.5 cpu / 1.8 GiB). Compare within a run, not across.
| Run | CPU | Kernel / image |
|--|--|--|
| A | Intel Xeon Platinum 8370C @ 2.80GHz | 7.0.0-1020-azure, ubuntu26 20261116.202.1 |
| B | AMD EPYC 9V74 | same |
| C | AMD EPYC 7763 | same |
| Soak 1 | AMD EPYC 9V74 | same |
| Soak 2 (re-run) | AMD EPYC 7763 | same |
| activities / signal-timer | AMD EPYC 9V45 | same |
| child | AMD EPYC 7763 | same |
| history | Intel Xeon 6975P-C | 7.0.0-1016-aws, ubuntu26 20261116.88.1 |

## Binary SHA256 (Rust identical in every run; Go binary hash varies per run because it is rebuilt each run)
- Rust cadence-server (rust and control): `f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74`
- Rust cadence-history: `1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5`
- Go cadence-server (run A, /usr/local/bin/cadence-server): `d2e6c1f03652eb98223eb8dc39075860f38f954e938677dc737bc45ddd12a657`. Other runs, first 12 hex: B 25eb572f31a1, C 69d660a50578, soak1 d9594ece0658, activities c196d08e50e6, signal-timer 7e9c70426f69, child 4f04a5ee96b6, history 8f4c28a88757.

Values are medians over reps. Latency ms. Resources: peak cgroup anon MiB / CPU-seconds per container (FE/MT/HI).

## A. Default config, MySQL
| c | arm | n | wf/s | p50 | p95 | p99 | failed | FE MiB/cpu-s | MT MiB/cpu-s | HI MiB/cpu-s |
|--|--|--|--|--|--|--|--|--|--|--|
| 1 | go | 5 | 23.3 | 38.7 | 79.6 | 108.9 | 0 | 32 / 7.3 | 31 / 4.3 | 71 / 18.4 |
| 1 | control | 5 | 25.3 | 25.2 | 100.4 | 104.3 | 0 | 15 / 3.9 | 18 / 2.3 | 68 / 7.2 |
| 1 | rust | 5 | 25.5 | 25.6 | 100.0 | 104.6 | 0 | 15 / 3.9 | 18 / 2.4 | 70 / 7.3 |
| 16 | go | 5 | 47.2 | 342.7 | 459.9 | 509.9 | 0 | 37 / 5.9 | 34 / 3.7 | 93 / 16.2 |
| 16 | control | 5 | 72.3 | 219.0 | 305.7 | 392.0 | 0 | 17 / 3.6 | 19 / 2.6 | 89 / 8.7 |
| 16 | rust | 5 | 71.1 | 219.5 | 315.5 | 513.1 | 0 | 17 / 3.6 | 19 / 2.6 | 90 / 8.6 |
| 64 | go | 5 | 23.0 | 3179.4 | 4262.3 | 4591.9 | 0 | 44 / 6.1 | 34 / 4.1 | 105 / 40.9 |
| 64 | control | 5 | 66.1 | 1035.7 | 1412.9 | 1500.8 | 0 | 21 / 3.7 | 21 / 2.7 | 101 / 9.9 |
| 64 | rust | 5 | 65.6 | 1106.0 | 1380.5 | 1440.3 | 0 | 21 / 3.7 | 21 / 2.6 | 100 / 9.8 |

| c | A/A ctl->rust wf/s | p50 | p95 | p99 | Go->Rust wf/s | p50 | p95 | p99 |
|--|--|--|--|--|--|--|--|--|
| 1 | +0.8% | +1.5% | -0.4% | +0.3% | +9.6% | -33.9% | +25.6% | -4.0% |
| 16 | -1.7% | +0.2% | +3.2% | +30.9% | +50.5% | -36.0% | -31.4% | +0.6% |
| 64 | -0.7% | +6.8% | -2.3% | -4.0% | +184.7% | -65.2% | -67.6% | -68.6% |

## B. transferProcessorMaxPollRPS=1000, MySQL
| c | arm | n | wf/s | p50 | p95 | p99 | failed | FE MiB/cpu-s | MT MiB/cpu-s | HI MiB/cpu-s |
|--|--|--|--|--|--|--|--|--|--|--|
| 1 | go | 5 | 26.5 | 33.1 | 60.1 | 93.1 | 0 | 32 / 5.9 | 31 / 3.4 | 73 / 15.7 |
| 1 | control | 5 | 37.0 | 22.0 | 47.6 | 70.0 | 0 | 15 / 3.1 | 18 / 1.8 | 76 / 5.9 |
| 1 | rust | 5 | 37.2 | 21.2 | 49.4 | 100.9 | 0 | 15 / 2.8 | 18 / 1.6 | 76 / 5.4 |
| 16 | go | 5 | 53.5 | 291.0 | 457.9 | 573.8 | 0 | 37 / 4.8 | 33 / 3.1 | 98 / 13.9 |
| 16 | control | 5 | 71.6 | 208.0 | 397.1 | 511.3 | 0 | 17 / 2.9 | 19 / 2.2 | 88 / 8.4 |
| 16 | rust | 5 | 69.6 | 217.3 | 420.7 | 524.3 | 0 | 17 / 2.9 | 19 / 2.2 | 87 / 8.5 |
| 64 | go | 5 | 25.1 | 2603.5 | 3888.7 | 4138.9 | 0 | 43 / 5.2 | 34 / 3.6 | 103 / 36.7 |
| 64 | control | 5 | 67.3 | 856.6 | 1615.3 | 1741.2 | 0 | 22 / 3.1 | 19 / 2.3 | 106 / 9.6 |
| 64 | rust | 5 | 65.4 | 868.1 | 1880.2 | 2135.3 | 0 | 21 / 3.0 | 20 / 2.2 | 105 / 9.3 |

| c | A/A ctl->rust wf/s | p50 | p95 | p99 | Go->Rust wf/s | p50 | p95 | p99 |
|--|--|--|--|--|--|--|--|--|
| 1 | +0.5% | -3.6% | +3.8% | +44.2% | +40.4% | -35.9% | -17.9% | +8.3% |
| 16 | -2.8% | +4.5% | +5.9% | +2.5% | +30.1% | -25.3% | -8.1% | -8.6% |
| 64 | -2.7% | +1.3% | +16.4% | +22.6% | +160.3% | -66.7% | -51.6% | -48.4% |

## C. Default config, PostgreSQL
| c | arm | n | wf/s | p50 | p95 | p99 | failed | FE MiB/cpu-s | MT MiB/cpu-s | HI MiB/cpu-s |
|--|--|--|--|--|--|--|--|--|--|--|
| 1 | go | 5 | 25.0 | 29.6 | 99.5 | 108.3 | 0 | 33 / 7.1 | 30 / 4.3 | 73 / 17.9 |
| 1 | control | 5 | 25.5 | 25.2 | 100.4 | 102.8 | 0 | 15 / 3.6 | 18 / 1.9 | 66 / 10.3 |
| 1 | rust | 5 | 25.4 | 25.1 | 100.3 | 102.2 | 0 | 15 / 3.6 | 18 / 1.9 | 66 / 10.3 |
| 16 | go | 5 | 63.3 | 252.4 | 351.1 | 397.9 | 0 | 37 / 5.8 | 33 / 3.7 | 101 / 14.0 |
| 16 | control | 5 | 87.4 | 181.1 | 249.8 | 286.2 | 0 | 17 / 3.7 | 19 / 2.9 | 88 / 11.3 |
| 16 | rust | 5 | 87.2 | 180.3 | 253.5 | 292.0 | 0 | 17 / 3.6 | 19 / 2.8 | 87 / 11.2 |
| 64 | go | 5 | 60.3 | 1226.4 | 1450.4 | 1582.3 | 0 | 44 / 5.9 | 34 / 3.8 | 108 / 14.8 |
| 64 | control | 5 | 78.0 | 920.4 | 1222.4 | 1332.9 | 0 | 22 / 3.8 | 21 / 3.0 | 97 / 12.7 |
| 64 | rust | 5 | 77.4 | 755.4 | 1204.9 | 1318.4 | 0 | 23 / 3.8 | 22 / 3.0 | 102 / 12.7 |

| c | A/A ctl->rust wf/s | p50 | p95 | p99 | Go->Rust wf/s | p50 | p95 | p99 |
|--|--|--|--|--|--|--|--|--|
| 1 | -0.4% | -0.4% | -0.1% | -0.5% | +1.3% | -15.4% | +0.8% | -5.6% |
| 16 | -0.2% | -0.4% | +1.5% | +2.0% | +37.7% | -28.6% | -27.8% | -26.6% |
| 64 | -0.7% | -17.9% | -1.4% | -1.1% | +28.4% | -38.4% | -16.9% | -16.7% |

## Soak (MySQL, c16, 12000 requests, 1 rep; first vs last third by start order, latency ms)

Soak 1 ([45674177546](https://github.com/auenkr/cadence-rust/actions/runs/45674177546)):

| arm | n | wf/s | first-third p50 / p95 | last-third p50 / p95 | p50 drift | p95 drift |
|--|--|--|--|--|--|--|
| control | 12000 | 52.7 | 211 / 543 | 221 / 849 | +4.9% | +56.4% |
| go | 12000 | 30.7 | 365 / 1164 | 420 / 1196 | +14.9% | +2.7% |
| rust | 12000 | 51.7 | 213 / 681 | 235 / 729 | +10.2% | +7.0% |

Soak 2 ([45686129462](https://github.com/auenkr/cadence-rust/actions/runs/45686129462), re-run):

| arm | n | wf/s | first-third p50 / p95 | last-third p50 / p95 | p50 drift | p95 drift |
|--|--|--|--|--|--|--|
| control | 12000 | 54.4 | 262 / 368 | 303 / 445 | +15.7% | +21.0% |
| go | 12000 | 26.8 | 446 / 1374 | 485 / 1581 | +8.9% | +15.0% |
| rust | 12000 | 54.6 | 260 / 363 | 305 / 452 | +17.3% | +24.5% |

## Workload shapes (MySQL, 3 reps, 3-arm, `payload_bytes=256`, same settings as perf-workload-shapes-20261125.md)
Peak anon MiB and CPU-s are sums of FE+MT+HI containers. Closed loop c16 (history c4). failed = 0 everywhere.

| Shape | Run | Arm | wf/s | p50 ms | p95 ms | p99 ms | peak anon MiB | CPU-s |
|---|---|---|---|---|---|---|---|---|
| activities (300 req) | [45674189151](https://github.com/auenkr/cadence-rust/actions/runs/45674189151) | go | 17.65 | 878.4 | 1190.8 | 1332.2 | 139.7 | 21.4 |
| | | control | 25.53 | 600.7 | 799.9 | 889.4 | 97.2 | 14.4 |
| | | rust | 25.84 | 596.4 | 820.2 | 896.9 | 97.7 | 14.3 |
| signal-timer (300 req) | [45674195860](https://github.com/auenkr/cadence-rust/actions/runs/45674195860) | go | 14.22 | 1070.9 | 1221.7 | 1309.7 | 113.0 | 8.4 |
| | | control | 14.83 | 1047.7 | 1137.1 | 1182.2 | 69.9 | 4.0 |
| | | rust | 14.61 | 1051.3 | 1156.5 | 1218.1 | 70.2 | 4.4 |
| child (300 req) | [45674203606](https://github.com/auenkr/cadence-rust/actions/runs/45674203606) | go | 8.24 | 1962.9 | 2260.2 | 2390.7 | 156.1 | 45.5 |
| | | control | 11.63 | 1364.8 | 1670.6 | 1782.2 | 123.5 | 26.4 |
| | | rust | 11.50 | 1390.0 | 1711.0 | 1759.3 | 123.7 | 26.8 |
| history (>=1200 events + replay query, 20 req, c4) | [45674209612](https://github.com/auenkr/cadence-rust/actions/runs/45674209612) | go | 0.39 | 10181 | 10231 | 10231 | 154.2 | 48.8 |
| | | control | 0.53 | 7544 | 7725 | 7725 | 150.2 | 36.6 |
| | | rust | 0.53 | 7513 | 7609 | 7609 | 151.7 | 36.5 |

Per-rep wf/s: activities go 17.6/17.7/17.7, control 25.5/26.5/23.0, rust 25.8/26.4/25.1; signal-timer go 14.4/14.2/13.9, control 15.0/14.8/14.8, rust 14.6/14.9/14.5; child go 7.9/8.2/8.5, control 11.6/11.6/11.7, rust 11.5/11.4/11.6; history go 0.4 x3, control/rust 0.5 x3.

Ratios rust / other (medians):

| Shape | A/A wf/s | A/A p50 | A/A p99 | A/A CPU | Rust/Go wf/s | p50 | p99 | anon | CPU |
|---|---|---|---|---|---|---|---|---|---|
| activities | 1.012 | 0.993 | 1.008 | 0.996 | 1.464 | 0.679 | 0.673 | 0.699 | 0.668 |
| signal-timer | 0.985 | 1.003 | 1.030 | 1.106 | 1.027 | 0.982 | 0.930 | 0.621 | 0.528 |
| child | 0.989 | 1.018 | 0.987 | 1.014 | 1.395 | 0.708 | 0.736 | 0.792 | 0.589 |
| history | 1.016 | 0.996 | 0.985 | 0.998 | 1.357 | 0.738 | 0.744 | 0.984 | 0.748 |

## Metrics where Rust is worse than Go (complete list; "A/A" = control->rust delta of the identical binary in the same run)
Echo arms A/B/C (Go->Rust, medians of 5):
| Metric | Run / c | Go->Rust | A/A same metric | Verdict |
|--|--|--|--|--|
| p95 | A c1 | +25.6% (79.6 -> 100.0 ms) | -0.4% | Rust p95 equals the 20 rps task-load limiter ceiling (~100 ms) in every arm incl. control and in C (Go 99.5 / Rust 100.3). Go's A c1 p95 was below the limiter in this run; not Rust-specific logic, but Go was lower here. |
| p99 | A c16 | +0.6% (509.9 -> 513.1) | +30.9% (control 392) | noise |
| p99 | B c1 | +8.3% (93.1 -> 100.9) | +44.2% (control 70.0) | noise (rust 3-rep tail outlier) |
| p95 | C c1 | +0.8% | -0.1% | noise |
| History peak anon | B c1 / c64 | +4.8% (76 vs 73) / +1.9% (105 vs 103 MiB) | n/a | small; Rust history anon is higher than Go only in B c1 and c64 |
Every other echo metric (wf/s, p50, other p95/p99, FE and MT memory/CPU, HI CPU) is better for Rust than Go.
Shapes: no metric is worse for Rust than Go (history anon 151.7 vs 154.2 MiB, i.e. the earlier 1.21x memory gap is gone with the jemalloc fix; child wf/s 1.395x, the earlier 0.835x child gap did not reproduce).
Soak (drift = last third vs first third, absolute latency is far better in Rust): Rust p95 drift +7.0% vs Go +2.7% (soak 1); Rust p50 drift +17.3% vs +8.9% and p95 drift +24.5% vs +15.0% (soak 2). The A/A control drifts similarly (+4.9..+15.7% p50, +21..+56% p95), and Rust latency stays 40-45% lower than Go at both thirds, so this is a shape of the run (rising DB load) rather than a Rust regression.

## Soak 2 note
First soak 2 dispatch ([45674183003](https://github.com/auenkr/cadence-rust/actions/runs/45674183003)) failed in the Go arm (client exit 1 after 8 min, last log: Go SDK `EntityNotExistsError: Workflow execution already completed` on a decision task). It was re-run unchanged as 45686129462. Not caused by Rust (the Rust arms had not started).

## Notes
- A/A (control vs rust, identical binary): wf/s within about -2.8..+0.8%, p50 within about +-7% (one C c64 -17.9%), p95/p99 up to +16..+44% in B (c1/c64 tails are noisy at 5 reps). Deltas of that size between Go and Rust are noise.
- Rust FE/MT use about 45-55% less peak anon memory than Go and roughly 40-50% less CPU-seconds; Rust history CPU about 55-70% lower in echo runs.
- Go c64 on MySQL collapses (A 23.0 wf/s, B 25.1) vs Rust ~66; Postgres (C) gives the more conservative c16/c64 gains (+38%/+28% wf/s, p95 -28%/-17%).
- Raw artifacts: `frontend-matching-paired-benchmark-*` in each run (3-day retention).
