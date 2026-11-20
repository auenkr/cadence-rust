# eff-history-waits (2026-11-21) — diagnosis only, no Rust code change

Branch eff-history-waits (base origin/master bcc88c2a3). Gated/benchmarked SHA 1c5004975438db2daa0299564512d58f96f9c7d3
(only change vs master: Benchmark/compose.yml + sdk-matching-benchmark.yml pass `history_sql_slow_ms` / `sql_stats_secs`
through to the History container as CADENCE_HISTORY_SQL_SLOW_MS / CADENCE_SQL_STATS_SECS; empty = off, no Rust change).

## Runs (service=all, duration=120 warmup=15 reps=2, mysql_tmpfs=true, slow_ms=200, stats_secs=30, profile=0)
Runners: Intel Xeon Platinum 8370C or 8573C (never EPYC 7763). Both runs: conclusion failure, because report.py rejects the Go arm
(correctness failure, "00-go-generate failed"), not the Rust arm.
- 40/s https://github.com/auenkr/cadence-rust/actions/runs/44803696266
- 50/s https://github.com/auenkr/cadence-rust/actions/runs/44803708666

Measure phase, completion latency (ms), from raw samples:
| rate | arm | pair | correct/total | p50 | p99 | decision sched->start p50 / p99 |
|---|---|---|---|---|---|---|
| 40 | Rust | 00 | 4800/4800 | 77 | 124 | 26.9 / 46.9 |
| 40 | Rust | 01 | 4800/4800 | 81 | 133 | 24.8 / 49.2 |
| 40 | Go | 00 | 1153/4800 | 2992 | 14586 | 1384 / 9048 |
| 40 | Go | 01 | 1151/4800 | 3108 | 15892 | 1549 / 10725 |
| 50 | Rust | 00 | 6000/6000 | 123 | 4816 | 41.6 / 2427 |
| 50 | Rust | 01 | 6000/6000 | 101 | 225 | 29.8 / 81.1 |
| 50 | Go | 00 | 614/6000 | 5701 | 13302 | 2744 / 7697 |
| 50 | Go | 01 | 645/6000 | 5269 | 13068 | 2476 / 7738 |

## Where History waits
- SQL query latency: CADENCE_HISTORY_SQL_SLOW_MS=200 produced ZERO slow-transaction lines in all Rust arms (40/s and 50/s).
  No History SQL transaction exceeded 200 ms.
- SQL volume (CADENCE_SQL_STATS_SECS=30, 50/s, final line): pair 00 transactions=38854 autocommit=109605 exec=172139 query=165053;
  pair 01 transactions=39093 autocommit=109763 exec=173231 query=165615. Identical load in both pairs.
- CPU (50/s avg cores): History 0.50-0.53, Matching 0.14-0.16, Frontend 0.21-0.22, MySQL 0.74-0.80 (container cap 1.5). History is not CPU bound.
- The only Rust tail (50/s pair 00): latency flat ~150-250 ms until t=65 s, then max grows linearly 1.1 s -> 5.1 s (backlog), while pair 01 with the
  same SQL counts stayed at p99 225 ms. In pair 00 MySQL CPU per 10 s rose 6.5 -> 10 core-s (0.65 -> 1.0 core) and host CPU pressure kept
  climbing, with History CPU growing only mildly: this is a MySQL/runner-side slowdown, not a History lock/queue/pool wait.
  Existing diagnostics do not cover pool-acquire/shard-lock/workflow-lock waits, but nothing observed points at them (no slow txns, flat History CPU),
  so no extra instrumentation was added.
- The arm that fails the benchmark at 40/s and above is Go (Rust completes ~100% correct at p99 <= 225 ms in 3 of 4 pairs; rate 40 and 50 both pass Rust).
  The earlier "60/s past ceiling" failure was likewise driven by the Go arm.

## Result
No clear parity-safe History wait found; no code change, so no rust-ci / history-mixed-gate / restart-gate runs and no before/after A/B
(nothing to compare). Benchmark pass-through (CI-only) is the sole change.
