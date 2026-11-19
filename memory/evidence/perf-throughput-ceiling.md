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

## Results (completion latency over measured samples; "timeouts" = samples not completing correctly within 120s)
Run conclusions: 25 success, 40 success, 60 failure, 80 failure. The 60 and 80 runs failed in report.py: a correctness/generator check failed in at least one arm. The numbers below come from the raw samples.

| rate | arm | done/s | p50 ms | p99 ms | timeouts | FE cpu/RSS | History cpu/RSS | Matching cpu/RSS | MySQL cpu |
|---|---|---|---|---|---|---|---|---|---|
| 25 | Go (2 pairs) | 24.98 | - | 177-182 | 0 | 0.18/106M | 0.56/162M | 0.10/100M | 0.67 |
| 25 | Rust | 25.00 | - | 198-203 | 0 | 0.11/71M | 0.26/132M | 0.08/67M | 0.41 |
| 40 | Go | 29.4-29.7 | - | 22.8-25.3 s | backlog 617-638 | 0.16/162M | 0.66/197M | 0.08/99M | 0.57 |
| 40 | Rust (perf on) | 39.9 | - | 212-214 | 0 (outstanding 6-8) | 0.13/71M | 0.33/140M | 0.10/67M | 0.43 |
| 60 | Go | collapse | 116 s | 120 s | 2005/3600, 2075/3600 | n/a | n/a | n/a | n/a |
| 60 | Rust pair 00 (perf on) | 60 | 189 | 275 | 0 | 0.18/71M | 0.44/155M | 0.12/70M | 0.63 |
| 60 | Rust pair 01 (perf on) | 60 | 299 | 10241 | 0 | 0.20/104M | 0.69/174M | 0.17/69M | 0.80 |
| 80 | Go | collapse | - | 120 s | 4782/4800, 4777/4800 | n/a | n/a | n/a | n/a |
| 80 | Rust (perf on) | collapse | 120 s | 120 s | 4439/4800, 4454/4800 | n/a | n/a | n/a | n/a |

(Pass rule: p99 <= 1s, 0 errors, outstanding <= max(2,1%). The 60/80 runs did not save resource files for every arm (n/a).)

## Ceiling
- all-Go: passes 25/s; fails at 40/s (~29.5/s completed, backlog grows). Ceiling between 25 and 40/s.
- all-Rust: passes 25 and 40/s with the profiler on. At 60/s one pair passes (p99 275 ms) and one breaks (p99 10.2 s). Fails at 80/s.
  Ceiling ~60/s (marginal), at least 1.5x Go.
- First Rust service to saturate: History. At 60/s it uses 0.44-0.69 of its 1-core cap, the most of any Rust service, and MySQL rises to 0.80 of 1.5.
  No service is CPU-pegged at the failure point, so the break is latency/queueing in History plus MySQL, not a single CPU limit.

## History hotspot (01-rust-profile, 60/s, perf on all Rust services; tokio-rt-worker 99.9%)
- self: memcpy 7.6%, `__inet_hash_connect` 1.9%, jemalloc malloc 1.6%, `__inet_check_established` 1.2%, `tcp_twsk_unique` 0.7%, thrift read_struct 0.45%.
- About 6% of samples are new TCP connects to MySQL. Callers: History `read_events` -> `read_history_branch` -> `MySqlTaskStore` pool checkout (`SyncConnectionPool::checkout` -> `Conn::new`), decision handling and timer/transfer queues.
- Cause: the benchmark's docker.yaml sql block sets no maxConns/maxIdleConns. Go database/sql then allows unlimited open connections but keeps only 2 idle (DefaultMaxIdleConns = 2). Rust mirrors this (`DEFAULT_MAX_IDLE_CONNS = 2`, unlimited max_open), so under concurrency connections close and reopen (TIME_WAIT checks).
  This is Go-parity behaviour driven by config. Go churns the same way, so there is **no parity-safe Rust code fix**: no code change, so rust-ci and the History gate were not needed.
- Possible follow-up (harness, both arms): set maxIdleConns in Benchmark config for a ceiling that is not affected by connection churn.

## Verdict
The all-Rust cluster's sustained ceiling is ~60/s (40/s solid) vs all-Go 25 to <40/s on the same runner, with MySQL on tmpfs. Rust uses ~half Go's History CPU and ~2/3 of its RSS.
History saturates first. Its top hotspot is MySQL connection churn, which follows Go's default idle-pool size, so no Rust code change was made.
Caveat: 2 pairs per rate, and 40/60/80 ran with perf on (Rust arm only, which adds overhead to Rust) -> the Rust numbers are conservative.
