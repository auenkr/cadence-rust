# Matching panic recovery, background goroutines (2026-10-24)

Branch `matching-panic-bg-20261024`, base `origin/matching-panic-recovery-20261024` (code head ee15c99b0).
Follow-up to `memory/evidence/matching-panic-recovery-20261024/notes.md`, which covered the handler, membership
and semaphore-stop recovery. This change covers the remaining Go `recover()` sites in matching's background goroutines.
No local cargo run; every verification is a GitHub Actions run.

## Go recovery sites in matching (grep of `recover()|CapturePanic|defer.*panic`)

| Go site | Behaviour on panic | Rust site | Status |
|---|---|---|---|
| `tasklist/task_list_manager.go:522` `notifyPartitionConfig` errgroup goroutine | `CapturePanic` logs `Panic is captured`, sets the goroutine's error. The other goroutines still run. The caller logs `failed to notify all partitions`. The root update RPC does not fail. | `matching_registry.rs` `notify_read_partitions` | Done here |
| `tasklist/adaptive_scaler.go:338` `fetchMetricsFromPartitions` errgroup goroutine | `CapturePanic` logs, sets the error. `g.Wait` returns it. `run()` logs `Failed to collect partition metrics`, resets underLoad, overLoad and isolation, and returns. The periodic loop continues on the next timer tick. | `matching_registry.rs` `scaler_update` isolation describe loop | Done here |
| `handler/handler.go` (13 RPCs), `handler/membership.go:53,104,166` | Done on `origin/matching-panic-recovery-20261024` | | Earlier branch |

Go sites with no Rust equivalent to fix:

- `tasklist/adaptive_scaler.go:292` `adjustReadPartitions` calls `describePartition` directly, with no `recover`. A panic
  there crashes the Go process. Rust does not add a recovery here. The Rust `adjust_read_partitions_async` describe closure
  is unchanged, and a panic in it unwinds the scaler task (no process crash). This is the one place Rust still differs.
- `tasklist` task reader, writer, GC and completer loops: no `recover()` anywhere in Go. Nothing to port.
- `common/task/parallel_task_processor.go`, `common/future`, `common/quotas/global/*`: not reached from matching
  (history, or shared resource wiring for other services).

## Changes

- `rust/crates/cadence-matching/src/matching_panic.rs`:
  - Test seam (`#[cfg(test)]` thread-local, `arm_panic_for_test`, `armed_left_for_test`). An armed `capture(operation)`
    panics at its first poll, before the wrapped future runs. Production builds compile `armed_for_test` to `false`.
  - Unit test `armed_operation_panics_once_then_runs_normally`.
- `matching_registry.rs` `notify_read_partitions`:
  - Each partition notification is a `capture("notify_partition_config", ...)`. Owner resolution moved inside the
    capture, because Go's goroutine covers it too.
  - A captured panic is reported as `warning: notifying Matching partitions failed: <message>`. The remaining partitions
    still run, and the caller is not failed, as Go.
- `matching_registry.rs` `scaler_update` isolation describe loop:
  - Each describe is a `capture("describe_partition", ...)`. Any failure (a captured panic or an RPC/status error) takes
    the Go `run()` error branch: `ERROR Failed to collect partition metrics error=...`, `scaler.reset()`, and
    `return Ok(true)` so the loop continues.
  - The non-panic error branch previously logged nothing. It now logs the Go line too (the same branch as Go's `run()`).

## Deviations (documented, not hidden)

1. Go's errgroup keeps running the remaining describes after a failed one, then discards the results. Rust returns at the
   first failure, as it already did for errors. The skipped describes are read-only, so the observable outcome (sustains
   reset, no config change, loop continues) is the same.
2. Rust notifies in parallel with a JoinSet and reports each failure. Go logs one summary line through `g.Wait`.
   The logging shape differs, the outcome does not.
3. `adjust_read_partitions_async` describes are not captured (see the table). Go would crash the process there.

## Tests

- `matching_panic::tests::armed_operation_panics_once_then_runs_normally`
- `matching_registry::tests::notify_read_partitions_panic_is_captured_and_does_not_fail_the_caller`: one of two
  partitions panics (armed once). The call returns normally, and the arm is consumed.
- `matching_registry::tests::scaler_describe_panic_is_a_collection_failure_and_the_tick_returns`: the tick returns
  `Ok(true)` (loop continues) and the arm is consumed. The sustain reset is not directly observable from the registry
  (the `Scaler` fields are private to `matching_scaler`), so it is not asserted.

## CI evidence

Pending. Code head and run IDs are recorded below after the runs finish.
