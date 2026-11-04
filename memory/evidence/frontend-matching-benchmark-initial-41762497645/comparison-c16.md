# Frontend + Matching benchmark

Scope: combined Frontend+Matching end-to-end SDK workflow; same Go History/system Worker and persistence required.

Workload: 100 measured workflows/run, 10 warmup, concurrency 16, payload 1024 bytes, tchannel.

| Arm | Runs | Median throughput (workflows/s) | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |
|---|---:|---:|---:|---:|---:|
| go | 5 | 67.987 | 221.268 | 334.764 | 370.627 |
| rust | 5 | 50.719 | 290.774 | 420.481 | 460.639 |

Rust vs Go median throughput: -25.40% (-17.267 workflows/s). Latency deltas (Rust − Go): p50 +69.506 ms, p95 +85.717 ms, p99 +90.012 ms.

Resource comparison status: not_collected.

Raw run JSON, including successful latency samples, is preserved in the comparison JSON. These are closed-loop end-to-end measurements; SDK retries are included and client queue time is excluded. Memory reports cgroup usage, which includes charged cache and is not RSS.
