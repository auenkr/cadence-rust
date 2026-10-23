# Merge notes 2026-10-23 (merge-all-20261023)

Merged in order with --no-ff onto origin/master: matching-mixed-gates, twonode, query-parity, http-tls, backlog-forward-v3, metrics-parity, rpc-parity, audit/matching-subsystem, audit/matching-subsystem-2, audit/matching-parity-v2-20261022. No audit memory filename collisions. AGENTS.md is master's.

## Conflicts
- merge1 conflicts: orchestration-2026-10-19-live.md (kept both), lib.rs, matching_registry.rs, matching_runtime.rs, matching_shard_processor.rs (took mixed-gates side of conflict hunks)
- merge5 (backlog-forward-v3) conflict: matching_reader.rs tests hunk, kept both tests
- twonode, query-parity, http-tls, metrics-parity, rpc-parity, audit branches: clean merges.

## Post-merge fix
- clippy items_after_test_module in matching_metrics_statsd.rs and matching_metrics_m3.rs: added #[allow(clippy::items_after_test_module)] on the test modules (functions after tests came from combined branches).

## Verification
- cargo-safe check -p cadence-server --all-targets: exit 0
- cargo-safe clippy -p cadence-server --all-targets -- -D warnings: first run exit 101 (the 2 lints above), after fix exit 0
- Nothing beyond compile+clippy was run. Tests were NOT run.
