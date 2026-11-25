# Final benchmark report — master de38ba36c (2026-11-25)

Head SHA: `de38ba36c53ef9b0b41c1f339bea6574d37988f2` (origin/master). Workflow `frontend-matching-benchmark.yml`, `source_ref=control_ref=master`, `rust_services=frontend,matching,history`, concurrency 1,16,64, 5 repetitions. Arms: **go** (all-Go), **rust** (Rust FE+MT+HI), **control** (same tree/binary as rust = A/A noise arm).

| Config | Run | Conclusion |
|--|--|--|
| A: default | https://github.com/auenkr/cadence-rust/actions/runs/45575046346 | success |
| B: `history.transferProcessorMaxPollRPS: 1000` | https://github.com/auenkr/cadence-rust/actions/runs/45575051781 | success |

Runner (both runs): Intel Xeon 6975P-C, 2 cores x 2 threads (nproc 4), 15 GiB RAM, Ubuntu 26.04.1, kernel 7.0.0-1016-aws, runner image ubuntu26 20261116.88.1, mysql:8.0. Container limits: 1 cpu / 1.4 GiB for most services (one 1.5 cpu / 1.8 GiB). The two runs used separate runners concurrently.
Binary SHA256 (run A): Rust cadence-server `4b8c1cd73a206d53957a974b4b78d0af259af8ddb4a6a47833b7fb39daacc07f` (control = same hash), Rust cadence-history `01deeb50c121bf59d3168dff0b6345f217d751a26439ef619180bca4786edb69`, Go cadence-server `c80a0ebb4ebf03f34016c071fe455e2252d33381c01c0cc2171523a33e7fc5f3`. Run B: cadence-server `2e715229083ab94463967b40f621d024a8adfc4630d5842457b39506618259dd`, cadence-history `b0a6615a88c7e278959ecb57e5943b524ec8444653782129c54b904abf62b428`.

Values are medians over 5 reps. Latency in ms. Resources: peak cgroup anon MiB / CPU-seconds per container (FE=frontend, MT=matching, HI=history).

## A. Default config

| c | arm | n | wf/s | p50 | p95 | p99 | FE MiB/cpu-s | MT MiB/cpu-s | HI MiB/cpu-s |
|--|--|--|--|--|--|--|--|--|--|
| 1 | go | 5 | 25.5 | 24.2 | 100.5 | 104.3 | 32 / 4.8 | 31 / 2.8 | 71 / 12.0 |
| 1 | control | 5 | 26.8 | 16.3 | 100.5 | 103.7 | 18 / 2.5 | 19 / 1.3 | 70 / 4.8 |
| 1 | rust | 5 | 26.5 | 16.1 | 100.5 | 103.5 | 18 / 2.5 | 19 / 1.3 | 69 / 4.7 |
| 16 | go | 5 | 47.4 | 326.9 | 559.2 | 610.5 | 37 / 4.1 | 33 / 2.3 | 92 / 10.7 |
| 16 | control | 5 | 57.3 | 288.0 | 409.5 | 475.4 | 20 / 2.6 | 21 / 1.7 | 92 / 6.6 |
| 16 | rust | 5 | 57.6 | 284.7 | 415.5 | 477.0 | 20 / 2.5 | 20 / 1.7 | 90 / 6.7 |
| 64 | go | 5 | 48.0 | 1426.0 | 1968.3 | 2159.3 | 43 / 4.2 | 34 / 2.5 | 106 / 19.9 |
| 64 | control | 5 | 52.5 | 1398.3 | 1702.0 | 1749.5 | 26 / 2.6 | 24 / 1.7 | 101 / 7.2 |
| 64 | rust | 5 | 52.7 | 1379.2 | 1702.0 | 1760.8 | 26 / 2.6 | 24 / 1.8 | 100 / 7.2 |

| c | A/A ctl->rust wf/s | p50 | p95 | p99 | Go->Rust wf/s | p50 | p95 | p99 |
|--|--|--|--|--|--|--|--|--|
| 1 | -1.2% | -1.6% | +0.0% | -0.2% | +3.7% | -33.6% | -0.0% | -0.7% |
| 16 | +0.6% | -1.1% | +1.5% | +0.3% | +21.5% | -12.9% | -25.7% | -21.9% |
| 64 | +0.4% | -1.4% | +0.0% | +0.6% | +9.8% | -3.3% | -13.5% | -18.5% |

## B. transferProcessorMaxPollRPS=1000

| c | arm | n | wf/s | p50 | p95 | p99 | FE MiB/cpu-s | MT MiB/cpu-s | HI MiB/cpu-s |
|--|--|--|--|--|--|--|--|--|--|
| 1 | go | 5 | 26.2 | 37.3 | 45.1 | 55.0 | 32 / 7.3 | 31 / 4.2 | 73 / 18.0 |
| 1 | control | 5 | 42.8 | 23.1 | 25.9 | 29.7 | 18 / 3.5 | 20 / 2.0 | 77 / 6.4 |
| 1 | rust | 5 | 41.7 | 23.8 | 26.5 | 29.6 | 18 / 3.7 | 20 / 2.1 | 76 / 6.7 |
| 16 | go | 5 | 49.6 | 311.5 | 463.6 | 508.8 | 37 / 5.8 | 33 / 3.6 | 97 / 15.5 |
| 16 | control | 5 | 70.6 | 212.8 | 372.2 | 514.4 | 20 / 3.5 | 20 / 2.5 | 90 / 8.6 |
| 16 | rust | 5 | 70.2 | 213.5 | 372.1 | 517.9 | 20 / 3.6 | 20 / 2.6 | 88 / 8.9 |
| 64 | go | 5 | 23.9 | 2853.0 | 4047.3 | 4476.6 | 43 / 6.2 | 34 / 4.0 | 107 / 39.3 |
| 64 | control | 5 | 66.9 | 848.7 | 1650.8 | 1740.9 | 25 / 3.7 | 21 / 2.7 | 106 / 9.5 |
| 64 | rust | 5 | 68.1 | 844.2 | 1646.1 | 1734.1 | 25 / 3.6 | 21 / 2.6 | 103 / 9.4 |

| c | A/A ctl->rust wf/s | p50 | p95 | p99 | Go->Rust wf/s | p50 | p95 | p99 |
|--|--|--|--|--|--|--|--|--|
| 1 | -2.7% | +3.1% | +2.6% | -0.3% | +59.3% | -36.2% | -41.2% | -46.2% |
| 16 | -0.6% | +0.4% | -0.0% | +0.7% | +41.4% | -31.5% | -19.7% | +1.8% |
| 64 | +1.9% | -0.5% | -0.3% | -0.4% | +184.5% | -70.4% | -59.3% | -61.3% |

## Notes
- A/A noise (control vs rust, identical binary): throughput within about +/-3%, latency within about +/-3%. Go-vs-Rust deltas of that size are noise.
- c1 p95 ~100 ms under the default config (all arms, including A/A) is the shared 20 rps queue task-load limiter, not a service difference; with the limit raised (config B) rust c1 p95 drops to ~26 ms. See memory/evidence/benchmark-limiter1000-20261125.md.
- Go arm in config B is much worse at c64 (23.9 wf/s, p99 4.5 s) than in config A, while rust/control improve; treat the large Go->Rust deltas in B (e.g. +184% at c64) as unstable. Config A deltas (c16 +21%, c64 +10% throughput; p95 -26% / -14%) are the more conservative comparison.
- Rust FE/MT use roughly 40% less peak anon memory and about half the CPU-seconds of Go; Rust history CPU is roughly 40-75% lower.
- Raw artifacts: `frontend-matching-paired-benchmark` in each run (3-day retention).
