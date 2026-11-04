# Frontend + Matching benchmark

Scope: combined Frontend+Matching end-to-end SDK workflow; same Go History/system Worker and persistence required.

Workload: 1000 measured workflows/run, 100 warmup, concurrency 1, payload 1024 bytes, tchannel.

| Arm | Runs | Median throughput (workflows/s) | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |
|---|---:|---:|---:|---:|---:|
| go | 5 | 12.774 | 77.627 | 84.939 | 90.341 |
| rust | 5 | 10.778 | 92.244 | 99.838 | 104.098 |

Rust vs Go median throughput: -15.63% (-1.996 workflows/s). Latency deltas (Rust − Go): p50 +14.617 ms, p95 +14.899 ms, p99 +13.757 ms.

Resource comparison status: comparable.

## Median resources per run

| Container | Go CPU (s) | Rust CPU (s) | Rust − Go CPU (s) | Go peak cgroup memory (MiB) | Rust peak cgroup memory (MiB) | Rust − Go memory (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| cadence-fm-benchmark-frontend-1 | 4.942 | 5.183 | +0.241 | 36.41 | 22.39 | -14.02 |
| cadence-fm-benchmark-history-1 | 12.343 | 15.057 | +2.714 | 78.31 | 77.29 | -1.02 |
| cadence-fm-benchmark-matching-1 | 2.926 | 4.723 | +1.797 | 34.42 | 25.71 | -8.71 |
| cadence-fm-benchmark-worker-1 | 0.230 | 0.316 | +0.086 | 30.08 | 30.08 | +0.00 |

Raw run JSON, including successful latency samples, is preserved in the comparison JSON. These are closed-loop end-to-end measurements; SDK retries are included and client queue time is excluded. Memory reports cgroup usage, which includes charged cache and is not RSS.
