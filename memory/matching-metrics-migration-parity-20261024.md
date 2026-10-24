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
- Production source inventory from `MetricDefs[Matching]` has 93 definitions;
  84 have a non-test Go Matching callsite (41 counters, 11 gauges, 16 timers,
  16 histograms), and all 84 names are present in Rust. The remaining nine
  definitions are catalog-only in Go (three buffer-isolation redirect names
  and six forward call/error names), with no Go Matching production callsite.
  Reproduce the name/kind/callsite inventory with
  `python3 rust/tools/audit-matching-metrics.py`; the existing metric matrix
  records each definition's Go/Rust references, kind, tags, histogram buckets,
  and callsite status.

## Production Matching comparison

- Go `MetricDefs[Matching]` (`go/common/metrics/defs.go`, around lines
  4042-4086) pairs `syncmatch_latency_per_tl` (Timer) with
  `syncmatch_latency_per_tl_ns` (Histogram, `Low1ms100s`). Rust uses those exact
  names/kinds/spec in `matching_metrics.rs::handler_sync_match_latency` and the
  production handler path. A focused Rust test exercises that emitter through
  the migration filter: default policy emits only the timer; `both` emits both
  kinds, preserving Go request tags (`operation`, `domain`, sanitized
  `tasklist`, `caller`, `transport`). Histogram bucket tags are added by the
  exporter after migration filtering.
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
- Run `40082857956` completed failure: clippy passed; one rollup-order assertion
  failed. The fix matches upstream commit `f9e410aa1`.
- Run `40083501645` completed failure after clippy passed: the two test failures
  were the same rollup-order issue and a histogram-tag assertion. The latter is
  fixed with the upstream `545a2fbbe` approach: provide Go bucket tags on the
  fixture and assert `histogram_scale=2`. Its formatter-generated patch was
  applied only to Matching metric files; unrelated workspace formatting was
  excluded.
- Upstream focused run https://github.com/auenkr/cadence-rust/actions/runs/40083695239
  at `545a2fbbe99d3828fbf83b65f61feb12e5159eaa` passed clippy and tests.
- The current CI workflow follows master commit `49fa0bf81`: `cargo fmt --all
  -- --check` runs on GitHub Actions without mutating checked-out source. The
  final strict run will use fresh target storage and committed source only.
