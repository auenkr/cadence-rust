# Frontend + Matching benchmark

Scope: combined Frontend+Matching end-to-end SDK workflow; same Go History/system Worker and persistence required.

Workload: 100 measured workflows/run, 10 warmup, concurrency 64, payload 1024 bytes, tchannel.

| Arm | Runs | Median throughput (workflows/s) | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |
|---|---:|---:|---:|---:|---:|
| go | 5 | 62.677 | 855.598 | 1329.977 | 1378.750 |
| rust | 5 | 51.713 | 1125.113 | 1594.718 | 1662.934 |

Rust vs Go median throughput: -17.49% (-10.964 workflows/s). Latency deltas (Rust − Go): p50 +269.515 ms, p95 +264.740 ms, p99 +284.184 ms.

Resource comparison status: not_collected.

Raw run JSON, including successful latency samples, is preserved in the comparison JSON. These are closed-loop end-to-end measurements; SDK retries are included and client queue time is excluded. Memory reports cgroup usage, which includes charged cache and is not RSS.
