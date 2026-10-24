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

## Implementation and CI evidence (2026-10-24)

Implemented on branch `t3/matching-metrics-migration`; tested implementation head:
`545a2fbbe99d3828fbf83b65f61feb12e5159eaa`.

- Added top-level `Config.histograms`, `Config.gauge_migration` (`gauge-migration`), and `Config.counter_migration` (`counter-migration`) fields. Validation runs while parsing config. A missing `default` retains Go's zero-value behavior; an explicitly supplied empty or unknown mode is rejected. Unknown override names are rejected against the matching Go allowlists.
- Ported the complete Go allowlists: 297 histogram names, 7 gauge names, 3 counter names. Timer emission intersects histogram, gauge, and counter policies; kind-specific emits use their own Go semantics. Per-name `true`/`false` overrides take precedence over mode defaults.
- Matching's reporter runtime now wraps the selected sink with the migration policy, filtering by event kind and emitted name without changing event tags. The persistence latency ns root-rollup candidate is now produced with Go histogram metadata, so the policy suppresses it by default and permits it when configured.
- Tests cover all modes for migrated timer/histogram/gauge/counter kinds, mode and allowlist validation, omitted versus explicit-empty defaults, name override true/false, and recording real metric events including root rollup histogram tags.

Go source provenance (`git hash-object`):

- `go/common/metrics/config.go`: `00d9542516fe1f721a383e41a82f69a0f1de930c`
- `go/common/metrics/scope.go`: `9d2483edf564f40e1e0a8e309271e14899f983b6`
- `go/common/config/config.go`: `7be649fb3aeac32d940b4da7646c0c51e377a5ec`

Final CI gate: [run 40083695239](https://github.com/auenkr/cadence-rust/actions/runs/40083695239), head `545a2fbbe99d3828fbf83b65f61feb12e5159eaa`, conclusion `success`. It used a fresh uncached target directory. Workspace clippy passed, followed sequentially by workspace tests. The Matching crate had 683 passing tests (4 ignored); service-core had 145 passing tests. The newly added migration and persistence rollup tests passed. No Cargo command was run on the development host.

This is evidence for the Matching metrics migration slice only. It does not constitute the Matching service parity audit or mixed Go/Rust cluster gate.
