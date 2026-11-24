# perf-sql-blob-copies (2026-11-24)

Branch `perf-sql-blob-copies`, head 0e29cf20aaf5c43a4d4a33d609d739c6c5eb15ad.

Copies removed (rust/crates/cadence-persistence/src):
- sql_history_store.rs read_history_branch: rows consumed by value; node data/encoding moved out of `Value` (`into_bytes`/`into_text`) instead of `Value::bytes()`/`text()` clones.
- history_sql.rs `build_history_branch_page`: takes `Vec<HistoryNode>` and moves `data`/`data_encoding` into `DataBlob` (was `.clone()` per row).
- sql_history_store.rs tree decode and history_sql_backend.rs `uuid_string`: borrow via new `Value::as_bytes`/`as_text`.
SQL statements, params and stored bytes unchanged.

CI:
- rust-ci (package=cadence-persistence): https://github.com/auenkr/cadence-rust/actions/runs/45404882064 success
- history-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/45404889230 success
