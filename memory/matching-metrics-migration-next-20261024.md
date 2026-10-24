# Matching metrics migration configuration — confirmed gap

Primary-source review on integration candidate b67072ec2 (2026-10-24).

The old blanket claim that all rollups are missing is stale: matching_persistence_limiter.rs record_task_call emits root-tag-free persistence request/latency/error rollups, with test per_domain_task_calls_emit_go_rollups. Response row/payload sizes and bad-request/DB-unavailable metrics likewise have actual emit sites and tests (non_empty_list_emits_row_size_and_db_unavailable_and_bad_request_classes and related tests). Preserve these.

The real missing deployment behavior is configuration-dependent migration selection:

- Go common/config/config.go:82-89 top-level Config fields: `histograms`, `gauge-migration`, `counter-migration` (NOT per-service metrics fields).
- Go common/metrics/config.go: HistogramMigration, GaugeMigration, CounterMigration; allowed-name maps; explicit names override default. Invalid modes and unknown metric names fail parsing. MigrationConfig.EmitTimer intersects all three policies.
- Go common/metrics/scope.go gates each timer/histogram/gauge/counter and rollup by its emitted name. Read exact predicates before porting; defaults do not simply emit every histogram.
- Rust cadence-service-core/src/config.rs has no migration fields; matching_metrics_runtime.rs constructs the reporter sink directly. Matching emitters mostly emit both timer and histogram forms unconditionally.

Next task: port top-level config parsing/validation plus a Matching metric sink policy (or equivalent) that filters by the Go migration predicates. Ensure explicit rollup gating and existing root-scope tags survive. Scope is Matching dependencies only; don't start other services. Use primary Go allowlists and Go tests in common/config/config_test.go / common/metrics/config_test.go for representative default, override, invalid-name and invalid-mode assertions. Validate via GitHub Actions only, whole workspace before integration. No local Cargo.

A live Go-vs-Rust emission comparison should cover at least Matching/persistence names, kinds, tags, default suppression and configured migration toggles. Compiling a list of names alone is insufficient evidence.
