# MySQL pool maxConns invariant (2026-11-24)

Verdict: BUG (real). File: rust/crates/cadence-persistence/src/matching_mysql.rs.
- Before: `query_until` reserved a sync-pool slot (reserve_until) but released it at query end, while the shared async pool (async_pool(), min=maxIdle, max=maxConns) kept its sockets idle. Sync open (<=maxConns) + async idle (<=maxConns) could reach 2x maxConns; Go database/sql caps total open at SetMaxOpenConns.
- Idle: mysql_async does not pre-open `min` connections (lazy, like Go); min only retains idle sockets. Lifetime: async pool had no equivalent of maxConnLifetime.
- Fix: `split_max_conns` divides maxConns (sync = n - n/2, async = n/2, async capped at 1024); async pool max = async share, and `abs_conn_ttl` = maxConnLifetime. Pooled path no longer takes a sync reservation. maxConns==1: no async budget, per-query dedicated connection disconnected before the reservation is released. maxConns<=0 unlimited unchanged.
- Trade-off: sync pool cap is now about half of maxConns.
- Unit test: `split_max_conns_never_exceeds_total` (1..=2048).

Runs (head code SHA 9fa50b9cb7bae482a8ebd45f79bedea9969e73ca, branch mysql-pool-maxconns):
- rust-ci package=cadence-persistence: success https://github.com/auenkr/cadence-rust/actions/runs/45400292647
- matching-mixed-gate: success https://github.com/auenkr/cadence-rust/actions/runs/45400297540
- frontend-mixed-gate (mysql): success, `GATE SUMMARY failed: []` https://github.com/auenkr/cadence-rust/actions/runs/45400303083
