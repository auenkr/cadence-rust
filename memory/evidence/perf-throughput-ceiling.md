# perf-throughput-ceiling (2026-11-19)

Branch perf-throughput-ceiling, source eb04ad094db1667e9eba7f9ec2d27746b3e6203b (origin/master).
Workflow sdk-matching-benchmark.yml, service=all (Rust FE+Matching+History, Go Worker vs all-Go),
duration=60 warmup=15 repetitions=2 mysql_tmpfs=true. profile=1 at 40/60/80 (perf on Rust arm only).

## Runs
| rate | run | profile |
|---|---|---|
| 25 | https://github.com/auenkr/cadence-rust/actions/runs/44475474989 | 0 |
| 40 | https://github.com/auenkr/cadence-rust/actions/runs/44475486454 | 1 |
| 60 | https://github.com/auenkr/cadence-rust/actions/runs/44475490221 | 1 |
| 80 | https://github.com/auenkr/cadence-rust/actions/runs/44475494391 | 1 |

## Results
(pending)
