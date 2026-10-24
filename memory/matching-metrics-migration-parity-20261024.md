# Matching metric migration parity (2026-10-24)

Implementation recovered on `t3/matching-metrics-parity` from the focused
`t3/matching-metrics-migration` commits through `5fa032ef1`. The implementation
is limited to Matching metrics configuration, filtering, persistence emission,
and CI formatting validation. No OpenFeature reader/evaluator source was taken.

## Go behavior compared

- `go/common/metrics/config.go` allowlist maps match the Rust migration arrays
  exactly: histogram 297/297, gauge 7/7, counter 3/3 (set comparison run on
  2026-10-24; no missing or extra names).
- Empty, unspecified policy defaults retain Go semantics: migrated timers emit;
  migrated histograms/gauges/counters do not. Unmigrated names always pass.
  Per-name `names` entries override the corresponding kind's default. Timer
  events apply the intersection of histogram, gauge, and counter `EmitTimer`
  predicates, matching Go `MigrationConfig.EmitTimer`.
- Invalid modes and names fail configuration parsing. Explicit empty mode values
  are invalid, matching Go's YAML unmarshallers; the Rust zero/default struct
  value remains the Go zero-value behavior.
- `go/common/metrics/scope.go` emits rollups separately at the root scope and
  gates them by their rollup metric name. Matching persistence now submits the
  `persistence_latency_ns_rollup` candidate with Go `Default1ms100s` bucket
  tags; the migration sink suppresses it by default and allows it when enabled.

## Production Matching comparison

- Go `MetricDefs[Matching]` (`go/common/metrics/defs.go`, around lines
  4042-4086) pairs `syncmatch_latency_per_tl` (Timer) with
  `syncmatch_latency_per_tl_ns` (Histogram, `Low1ms100s`). Rust uses those exact
  names/kinds/spec in `matching_metrics.rs::handler_sync_match_latency` and the
  production matcher path. A focused Rust test now asserts default timer-only
  output and configured dual emission while preserving operation, domain,
  tasklist, tasklistType, and histogram bucket tags through the migration sink.
- Go's task-list manager scope adds `operation=TaskListMgr`; `NewPerTaskListScope`
  adds `domain` and `tasklist`; manager metrics add `tasklistType=decision|activity`
  (`go/service/matching/tasklist/task_list_manager.go`). Rust's manager tag
  builder follows those values. The condition-failed Rust test now checks all
  four tags and both the condition-failed and forward-throttle counter identity.
- Go `reportConditionFailed` increments `ConditionFailedErrorPerTaskListCounter`
  on reader/writer `ConditionFailed` (`task_reader.go:280`, `task_writer.go:142`).
  Rust wires the durable queue condition-failed observer in
  `cadence-matching/src/matching_registry.rs`; the `condition_failed` metric
  method emits one counter with the manager scope tags. This is a source-level
  mapping and unit coverage, not a live exporter comparison.
- Existing broader source inventory and limitations remain in
  `memory/matching-metrics-matrix-20261023.md`. This change does not claim full
  Matching metric parity or replace that inventory.

## Validation evidence

Requested reference run: https://github.com/auenkr/cadence-rust/actions/runs/40082857956

- Source SHA: `5fa032ef187f3cc13d6fa1f9a311bd0329461d61`
- At inspection time (2026-10-24): clippy succeeded; workspace tests were still
  running; run conclusion was pending.
- The current branch adds CI-hosted `cargo fmt --all`/`cargo fmt --all -- --check`
  before clippy and tests. The format patch is uploaded as a short-retention
  artifact so formatting can be applied from CI without running Cargo locally.
- Final run URL, source SHA, and per-job conclusion will be appended after the
  current implementation is pushed and the fresh-target GitHub Actions run
  completes.
