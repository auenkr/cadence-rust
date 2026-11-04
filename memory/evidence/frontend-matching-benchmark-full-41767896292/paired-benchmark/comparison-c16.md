# Frontend + Matching benchmark

Scope: combined Frontend+Matching end-to-end SDK workflow; same Go History/system Worker and persistence required.

Workload: 1000 measured workflows/run, 100 warmup, concurrency 16, payload 1024 bytes, tchannel.

| Arm | Runs | Median throughput (workflows/s) | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |
|---|---:|---:|---:|---:|---:|
| go | 5 | 33.575 | 484.454 | 618.958 | 671.856 |
| rust | 5 | 29.821 | 541.928 | 709.161 | 769.264 |

Rust vs Go median throughput: -11.18% (-3.754 workflows/s). Latency deltas (Rust − Go): p50 +57.473 ms, p95 +90.203 ms, p99 +97.408 ms.

Resource comparison status: comparable.

## Median resources per run

| Container | Go CPU (s) | Rust CPU (s) | Rust − Go CPU (s) | Go peak cgroup memory (MiB) | Rust peak cgroup memory (MiB) | Rust − Go memory (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| cadence-fm-benchmark-frontend-1 | 4.069 | 5.008 | +0.939 | 39.25 | 29.27 | -9.98 |
| cadence-fm-benchmark-history-1 | 11.591 | 13.944 | +2.352 | 98.18 | 96.51 | -1.67 |
| cadence-fm-benchmark-matching-1 | 2.687 | 5.045 | +2.358 | 35.66 | 26.80 | -8.87 |
| cadence-fm-benchmark-worker-1 | 0.086 | 0.138 | +0.051 | 30.19 | 30.14 | -0.05 |

Raw run JSON, including successful latency samples, is preserved in the comparison JSON. These are closed-loop end-to-end measurements; SDK retries are included and client queue time is excluded. Memory reports cgroup usage, which includes charged cache and is not RSS.
