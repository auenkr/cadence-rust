# Frontend + Matching benchmark

Scope: combined Frontend+Matching end-to-end SDK workflow; same Go History/system Worker and persistence required.

Workload: 1000 measured workflows/run, 100 warmup, concurrency 64, payload 1024 bytes, tchannel.

| Arm | Runs | Median throughput (workflows/s) | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |
|---|---:|---:|---:|---:|---:|
| go | 5 | 31.129 | 2294.921 | 2600.804 | 2884.152 |
| rust | 5 | 27.549 | 2623.260 | 3035.882 | 3488.815 |

Rust vs Go median throughput: -11.50% (-3.581 workflows/s). Latency deltas (Rust − Go): p50 +328.338 ms, p95 +435.078 ms, p99 +604.663 ms.

Resource comparison status: comparable.

## Median resources per run

| Container | Go CPU (s) | Rust CPU (s) | Rust − Go CPU (s) | Go peak cgroup memory (MiB) | Rust peak cgroup memory (MiB) | Rust − Go memory (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| cadence-fm-benchmark-frontend-1 | 4.144 | 5.139 | +0.994 | 48.58 | 34.57 | -14.01 |
| cadence-fm-benchmark-history-1 | 23.075 | 26.238 | +3.163 | 105.79 | 112.55 | +6.77 |
| cadence-fm-benchmark-matching-1 | 2.780 | 5.148 | +2.367 | 36.08 | 27.29 | -8.79 |
| cadence-fm-benchmark-worker-1 | 0.102 | 0.131 | +0.030 | 30.59 | 30.62 | +0.04 |

Raw run JSON, including successful latency samples, is preserved in the comparison JSON. These are closed-loop end-to-end measurements; SDK retries are included and client queue time is excluded. Memory reports cgroup usage, which includes charged cache and is not RSS.
