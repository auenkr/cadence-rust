# Matching domain cache acceptance evidence

All Go comparisons use source revision recorded in `go-source-revision.txt`.
The Go-only oracle runner exited 0; its JSON trace is in `go-oracle.log`.
`trace-compare.txt` records a structural JSON comparison against the trace from
`cache-focused.log` after the final Rust changes.

Rust validation used `CARGO_BUILD_JOBS=1` and
`CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-a33b4846/rust/target`:

- Persistence: `cargo test -p cadence-persistence matching_domain_metadata -- --nocapture` — 9 passed, 1 ignored. The ignored test requires a separately generated Go SQLite database. The run includes the new file-backed SQLite exclusive-lock timeout regression.
- Cache: `cargo test --locked -p cadence-server --lib matching_domain_cache -- --nocapture` — 14 passed, 0 failed (final rerun after retry jitter/classifier updates).
- Lint: `cargo clippy --locked --workspace --all-targets -- -D warnings` — passed.

Exact outputs and exit codes are saved alongside this file. The SQL error
classification audit and Go source references are in
`sqlite-classification-audit.txt`.
