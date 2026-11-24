# perf-mysql-hop (2026-11-24)
Change (rust/crates/cadence-persistence/src/matching_mysql.rs, `query_until`):
- result channel is now a bounded `sync_channel(1)` (no unbounded queue alloc); semantics, timeouts, errors, split_max_conns unchanged.
- stale "shared max-open reservation" comments fixed (async pool has its own budget).
- NOT done: removing the query_runtime thread hop (block_in_place / current-handle reuse) — judged a risk to deadline/pool-runtime semantics without local measurement; remains a follow-up.
CI (rust-ci.yml, head 77efcc3b86162a47da585feaff9bc392bc573cf1, both success):
- cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/45459884230
- cadence-matching: https://github.com/auenkr/cadence-rust/actions/runs/45459892830
