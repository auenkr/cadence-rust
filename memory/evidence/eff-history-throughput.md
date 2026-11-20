# eff-history-throughput (2026-11-21) — INCOMPLETE, no code change made

Branch eff-history-throughput, base/source add27d89d6304468b570ef8d7b41b64a6b765911 (origin/master).

## Baseline re-profile (all-Rust, 60/s, profile=1)
Run https://github.com/auenkr/cadence-rust/actions/runs/44795145268 (sdk-matching-benchmark, service=all, duration=120 warmup=15 reps=2,
mysql_tmpfs=true, sql_log_secs=120). Runner CPU: AMD EPYC 9V74 (not 7763). Conclusion: failure in report.py (correctness check), as in perf-throughput-ceiling.
From raw samples (measure phase, 7200 samples/arm, correct+no error, completion latency):
| arm | pair | correct/7200 | p50 ms | p99 ms |
|---|---|---|---|---|
| Rust (perf on) | 00 | 1425 | 8906 | 32509 |
| Rust (perf on) | 01 | generator failed | - | - |
| Go | 00 | 278 | 5931 | 16024 |
| Go | 01 | 200 | 7041 | 19372 |
So 60/s is well past the ceiling on this runner (the earlier run on another CPU passed one pair): the ceiling is runner-dependent and marginal.

## Code review (no change made)
- History SQL pool (`SyncConnectionPool`, matching_mysql.rs) matches database/sql: max_open unlimited by default, max_idle default 2, LIFO idle reuse,
  slot reserved under the mutex and the mutex is released while connecting. No lock held across connect.
- No parity-safe pool change identified. Run did not retain History-side lock/queue diagnostics (perf data not kept in the artifact), so lock-wait
  attribution (shard lock / workflow lock / queue concurrency) remains unmeasured.

## Not done
No code change, no rust-ci / history-mixed-gate / restart-gate runs, no after-pairs. Next: rerun with CADENCE_HISTORY_SQL_SLOW_MS and
CADENCE_SQL_STATS_SECS enabled (dynamic_config_extra / env in Benchmark) at 40-50/s to attribute History waits before changing code.
