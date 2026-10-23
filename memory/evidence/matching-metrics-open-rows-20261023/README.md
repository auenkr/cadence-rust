# Matching metrics open rows (2026-10-23, branch matching-metrics-open-rows-20261023)

Scope: rows in memory/matching-metrics-matrix-20261023.md that were not closed by a verified claim.
Base: origin/master 92668eb2f. No Go service involved; unit tests through the recording sink only.
No mixed-cluster gate was run.

## Changes
- rust/crates/cadence-matching/src/matching_persistence.rs: new test module
  `list_domain_metric_tests::empty_list_domains_emits_go_empty_count_and_envelope_payload`
  (ListDomain zero-row emits persistence_empty_response with operation=ListDomain,
  task_category=none, and a 32-byte payload histogram; no row-size).
- rust/crates/cadence-service-core/src/matching_metrics.rs: doc comment on
  `record_persistence_empty_response` corrected (row and payload histograms are implemented).
- memory/matching-metrics-matrix-20261023.md: status updated per row; open-rows section appended.

No program logic changed.

## Verified against Go (source)
- Go `base.call` / `callWithoutDomainTag` / `updateErrorMetric(PerDomain)`: names, types, the
  `histogram_start/end/scale` tags, and the failure-counter pairing match `record_call` and
  `PersistenceErrorClass::metric_names`.
- Go defs.go metric types, using unset metricType = Counter (zero value of MetricType): matched for
  every Go-emitted name in the matrix that Rust emits.
- Go emptyCountedMethods / payloadSizeEmittingMethods include DomainManager.ListDomains and
  TaskManager.GetTasks: both emit via record_list_response.

## Commands and exit codes
See exit-codes.md. Commands were run from rust/ in the worktree with
`CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared` through
`/mnt/storage/service/bin/cargo-safe`, as instructed.

Caveat (from gaps3 evidence): worktrees sharing cargo-target-shared can reuse other worktrees'
workspace-crate artifacts. A pass here is therefore only as valid as the cargo-safe touch step
forced a rebuild of the workspace sources. The AGENTS.md final gate requires
CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-final with the binary sha256 recorded; this
pass is not that final gate.
