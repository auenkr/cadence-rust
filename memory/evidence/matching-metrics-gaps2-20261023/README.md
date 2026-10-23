# Matching metrics gaps 2 (2026-10-23)

Branch: `matching-metrics-gaps2-20261023` from `origin/master` (`6c886372d`).
Worktree: `/mnt/storage/service/github/wt/metrics2`. Cargo workspace is `rust/`.
Build wrapper: `/mnt/storage/service/bin/cargo-safe` (one host-wide lock, `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared`).

## Commands and real exit codes

| Step | Command (run from `rust/`) | Log | Exit file |
|---|---|---|---|
| 0 (invalid, wrong dir) | `cargo-safe check -p cadence-matching --all-targets` from worktree root | `check1-wrong-dir.log` | `check1-wrong-dir.exit` = 101 (no Cargo.toml at root; not a code result) |
| 1 batch 1 | `cargo-safe check -p cadence-matching --all-targets` | `check1.log` | `check1.exit` = 0 |
| 2 batch 2 | `cargo-safe check -p cadence-matching --all-targets` | `check2.log` | `check2.exit` (see table below) |

Clippy and test: see sections below once run.

## Code changes

- `cadence-service-core/src/matching_metrics.rs`
  - `poll_match_latency`: tags now the Go manager scope (`operation=TaskListMgr`, `tasklistType`, `domain`, `tasklist`). Previously only domain and tasklist were set.
  - `async_match_handler_latency`: new H-scope `asyncmatch_latency_per_tl` / `_ns` (Go `createPollFor*TaskResponse`, only when the task was not sync matched).
  - `record_persistence_empty_response`: `persistence_empty_response` with `operation`, `task_category=none`.
  - `record_persistence_host_latency`: `persistence_latency_histogram_per_host`, `operation=persistence_operations`, `host`.
  - `record_persistence_manual_latency`: `persistence_latency_histogram`, `operation` only (opt-in).
  - Tests: `poll_match_latency_carries_manager_scope_tags`, `async_match_handler_latency_uses_poll_rpc_scope`, `persistence_empty_response_and_host_latency_use_go_tags`.
- `cadence-service-core/src/config.rs`: `PersistenceConfig.enablePersistenceLatencyHistogramMetrics` (default false, Go key).
- `cadence-matching/src/matching_persistence_limiter.rs`: host name, manual histogram flag, `record_empty_response`; tests `empty_rows_and_host_latency_follow_go_metered_wrapper`, `manual_latency_histogram_is_opt_in_and_operation_tagged`.
- `cadence-matching/src/matching_persistence.rs`: `get_tasks` emits the empty-response count on a successful zero-row result.
- `cadence-matching/src/launcher.rs`: sets host name (`os.Hostname` equivalent) and manual-histogram flag on the limiter.
- `cadence-matching/src/matching_dispatch.rs`: async H-scope latency at poll delivery when `task.sync` is None.
- `cadence-matching/src/grpc_matching.rs`: `matching_host_name` now `pub(crate)`.

## Matrix

`memory/matching-metrics-matrix-20261023.md` is updated for all 142 rows. Rust column = emit function plus production callers. Status column per row:

- `audited:` emit site present, tag and scope reviewed against Go (source only).
- `fixed (this branch):` code changed in this branch.
- `implemented (this branch):` new emission.
- `N/A for Matching:` Go producer is not reachable from Matching (see reason in row).
- `MISSING:` still open, with reason.

Matrix correction: Go per-host operation tag is `persistence_operations` (`defs.go:691`), not `PersistencePerHost`.

Raw audit data: `audit-lookup.tsv` (the per-name lookup used to rewrite the matrix).
