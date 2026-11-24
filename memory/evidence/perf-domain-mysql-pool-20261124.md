# Pool MySQL deadline queries (perf-domain-mysql-pool)

Change: `SyncConnectionPool::query_until` (rust/crates/cadence-persistence/src/matching_mysql.rs) no longer opens and disconnects a new `mysql_async::Conn` per call. Each datastore pool now lazily creates one shared `mysql_async::Pool` (OnceLock) from the same Opts. Pool min = maxIdleConns, max = maxConns, capped at 1024 because maxConns<=0 means unlimited (usize::MAX) and mysql_async pre-allocates for the max. The existing max-open reservation (`reserve_until`), deadline/timeout, error mapping and read-only semantics are unchanged. Other `Conn::new` site: only the sync pool checkout (already pooled). The single-worker query runtime is KEPT: the callers are sync, and the owner-thread design avoids self-join panics on drop.
Test added (ignored, needs MySQL): `timed_domain_query_reuses_pooled_connection` asserts CONNECTION_ID() is the same across 3 calls. Not run in CI (opt-in MySQL job).

Bug found and fixed: first head 4f74d6814 panicked "capacity overflow" in the benchmark Rust history (unlimited maxConns); benchmark run 45334627422 failed. Fixed in 49b263d22 (cap). The gates on 4f74d6814 passed because their configs set maxConns, so they were rerun on the fix.

## Runs on head 49b263d221118f3648984175e7a2bfe017977336
- rust-ci cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/45339410947 success
- Matching mixed gate (MySQL): .../45339414506 success (gate.exit 0; this workflow prints no "GATE SUMMARY" line)
- Frontend mixed gate (mysql): .../45339419246 success, `GATE SUMMARY failed: []`
- History mixed gate (mysql): .../45339424504 success, `GATE SUMMARY failed: []`
- Matching dependency oracles: .../45339429817 success
- Paired benchmark (rust_services=frontend,matching,history): .../45339434399 success, artifact verification exit 0

## Benchmark vs baseline run 45291978847 (head 500dec50a), median of 5 paired reps
| | baseline | pooled |
|---|---|---|
| Rust wf/s | 57.1 | 65.9 (+15%) |
| Rust p50/p95/p99 ms | 1281/1600/1719 | 998/1650/2062 |
| Go wf/s (same run) | 20.7 | 28.5 |
| Rust/Go throughput | 2.76x | 2.31x |
Go arm was ~38% faster in this run (runner variance), so the ratio fell although absolute Rust throughput rose. Rust p99 is worse; one run is not conclusive on tail latency. Gains are modest as expected: domain-cache reads are not on the per-workflow hot path.
