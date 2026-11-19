# Postgres per-call OS thread removal (2026-11-19, branch perf-pg-sql-thread)

## Finding: TRUE
`matching_postgres::outside_runtime` spawned a scoped OS thread (`std::thread::scope` + `spawn`) for every call made inside
any Tokio runtime. Callers: History Postgres SQL (`history_sql_backend.rs` transaction/autocommit, `history_shard_sql.rs`),
Matching task store + pool checkout/drop (`matching_postgres.rs`), domain store (`matching_domain_postgres.rs`),
`cassandra.rs`. Frontend has no own Postgres path (uses these stores).

## Fix
`outside_runtime`: on a MultiThread runtime use `tokio::task::block_in_place(operation)` (same as MySQL `blocking()`); the
sync postgres driver's nested `block_on` is permitted inside block_in_place. Current-thread runtimes keep the scoped-thread
fallback; no runtime -> inline. SQL, error mapping, timeouts unchanged. New test
`multi_thread_boundary_allows_nested_runtime_on_same_thread` (nested blocking()+outside_runtime, inner runtime block_on).

## Validation (head 262e66c98f54f810908d2de8c07f9f649f43f335)
- rust-ci cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/44475125262 — success
- History mixed gate database=postgres: https://github.com/auenkr/cadence-rust/actions/runs/44475131379 — success,
  log `GATE SUMMARY failed: []`
- Benchmark: no Postgres benchmark lane exists (benchmark workflows are MySQL only); no p50/p99/CPU numbers.

## Verdict
Finding confirmed and fixed; gates green. Perf impact unmeasured (no PG lane).
