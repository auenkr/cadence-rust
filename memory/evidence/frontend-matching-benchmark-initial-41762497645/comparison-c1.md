# Frontend + Matching benchmark

Scope: combined Frontend+Matching end-to-end SDK workflow; same Go History/system Worker and persistence required.

Workload: 100 measured workflows/run, 10 warmup, concurrency 1, payload 1024 bytes, tchannel.

| Arm | Runs | Median throughput (workflows/s) | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |
|---|---:|---:|---:|---:|---:|
| go | 5 | 25.191 | 38.676 | 44.752 | 77.699 |
| rust | 5 | 17.722 | 47.579 | 149.473 | 151.355 |

Rust vs Go median throughput: -29.65% (-7.468 workflows/s). Latency deltas (Rust − Go): p50 +8.903 ms, p95 +104.721 ms, p99 +73.656 ms.

Resource comparison status: not_collected.

Raw run JSON, including successful latency samples, is preserved in the comparison JSON. These are closed-loop end-to-end measurements; SDK retries are included and client queue time is excluded. Memory reports cgroup usage, which includes charged cache and is not RSS.
