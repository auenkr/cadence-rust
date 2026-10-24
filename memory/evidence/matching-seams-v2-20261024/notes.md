# matching-seams-v2-20261024: un-ignore two parity tests

Branch: `matching-seams-v2-20261024` (from `origin/master` a1288a8cf).
Scope: close two `#[ignore]` parity gaps in `cadence-matching`. No program logic change
beyond the seams below. Verification is GitHub Actions only (no local cargo).

## 1. Raw `GetTasksBatchSize` (matching_reader.rs)

**Go semantics.** `Config.GetTasksBatchSize` (go/service/matching/config/config.go:47,136)
returns the raw dynamic-property value, including 0 and negatives. Validation lives at the
usage sites: `newTaskReader` (go/service/matching/tasklist/task_reader.go:109-116) and
`getTaskBatchWithRange` (task_reader.go:300-307) both replace a value `<= 0` with the
`matching.getTasksBatchSize` default (1000). Go's `TestGetTasksBatchSizeValidation`
(go/service/matching/config/config_test.go:211) asserts the raw 0 and -5.

**Rust change.**
- `ReaderConfig::raw_batch_size() -> i64` (new, `pub`): the live value, or the fixed batch
  size when no live provider is installed. Unvalidated, like Go's `Config`.
- `ReaderLive::raw_batch_size()` replaces the old validating `ReaderLive::batch_size()`.
- `ReaderConfig::buffer_batch_size()` now holds the single validation rule (`<= 0` ->
  1000, otherwise clamp to `u32`). `read_batch_size()` goes through it, so both Go call
  sites share one validation point.
- Test `go_get_tasks_batch_size_returns_raw_non_positive_values`: `#[ignore]` removed; it
  asserts `raw_batch_size()` returns 0 / -5 and that `read_batch_size()` falls back to 1000.

## 2. Mocked owner QPS tracker (matching_registry.rs)

**Go semantics.** `TestQueriesPerSecond` (go/service/matching/tasklist/task_list_manager_test.go:447)
replaces the tracker with a gomock that returns 42.5.

**Rust change.**
- `QueueRegistry.qps_clock: cadence_common::qps::Clock`, defaulting to
  `cadence_common::qps::system_clock()`. Production stays on the wall clock.
- `QueueRegistry::with_qps_clock` is `#[cfg(test)]` only, so the seam does not exist in
  production builds.
- Owner construction passes `registry.qps_clock.clone()` to `EmaQpsTracker::new` where it
  previously passed `system_clock()`.
- Test `loaded_owner_reports_mocked_tracker_qps_after_arrivals`: `#[ignore]` removed. The
  mocked clock starts at 0. 425 arrivals are reported in the first 10s window (the default
  `DEFAULT_QPS_TRACKER_INTERVAL_NANOS`), then the clock moves to 10s. The first fold stores
  `425 / 10.0 = 42.5` exactly, which matches Go's mock value.

## Verification

- Local: no cargo (per task). Diff reviewed by reading only.
- CI: see the run table below (filled in after each run).

| Run | Ref / head | Scope | Result |
|-----|-----------|-------|--------|
| pending | matching-seams-v2-20261024 | `-f package=cadence-matching` | pending |
| pending | final code head | whole workspace (no package input) | pending |

## Not merged

Branch is pushed for review. Do not merge without an explicit instruction.
