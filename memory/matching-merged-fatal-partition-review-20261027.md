# Matching merged fatal and partition retry review (2026-10-27)

Scope: source audit of merged commits `d9e190d3d` and `b07c80398` at `967d4a6e7`. No Cargo, Go, formatting, build, or test command was run. No implementation files were changed. CI validation remains owned by the parent; this note does not claim validation.

## Confirmed gap

`b07c80398` does not propagate cancellation of a caller that has no deadline into the blocking persistence retry. In `rust/crates/cadence-matching/src/matching_registry.rs`, `update_partition_config_inner` moves a `cancelled` closure into `spawn_blocking`; it checks `caller_deadline` and, for scaler operations only, `handle.queue.is_stopped()`. The RPC passes only `caller_deadline` to `update_partition_config_for_domain` (`grpc_matching.rs`, `UpdateTaskListPartitionConfig` handler). If the client cancels an RPC without a deadline while persistence returns a transient error, the detached blocking closure continues retrying until success or retry-policy expiration, and may persist and notify after the handler is gone.

Go counterpart: `go/service/matching/tasklist/task_list_manager.go`, `UpdateTaskListPartitionConfig` -> `updatePartitionConfig(ctx, ...)` -> `c.throttleRetry.Do(ctx, ...)`. That `ctx` supports cancellation independently of a deadline; `ThrottleRetry` is configured with context expiration in its backoff implementation. This is a behavioral mismatch for caller cancellation. A cancellation token/drop guard shared with the blocking closure (or an equivalent request cancellation signal) is needed; deadline-only checking does not cover it.

## Reviewed behavior / bounded clear scope

- SQL fatal rendering remains routed through `FatalExit::Sql` to the original `SqlMatchingWriterFatal` wrapper, and `main.rs` still prints it and exits 2. Reader invariant fatal is routed as `MatchingReaderFatal` and exits 1. The new `MatchingFatal::SqlRunId` display delegates to the original cause, retaining its underlying text.
- Reader sink is supplied at construction before the pump is spawned (`matching_reader.rs`). The callback sends synchronously to an unbounded channel; reader shutdown waits for pump completion. Runtime registry wiring sends reader fatal through the same host fatal channel. Launcher samples it after runtime shutdown has joined owners/writers and before final exit. No ordering defect was confirmed in these paths.
- `ScriptedReadStore` returns injected IDs once then delegates to SQLite. The reader tests assert callback delivery, queue stop, and wait completion; the blocking callback test uses a two-worker Tokio runtime and releases its Condvar before awaiting completion. No definite test-store or thread-blocking correctness defect was confirmed by source inspection.
- Go partition manager validates first, returns early for unchanged/default config, persists with the caller context, increments the failed counter and stops the manager on persistence failure. Rust `PartitionCache::update_until` follows that ordering, calls the failure callback only after persistence failure, and stops the queue. The scaler path uses queue-stopped state as its manager-context approximation. Failure-counter tags are populated from the RPC's resolved domain for the manual RPC path.
- `matching_retry::persistence` is used by queue metadata reads and queue task reads in addition to partition updates. The non-cancelable wrapper retains the old no-cancellation API; the new cancellation behavior is limited to `persistence_until` callers. This review did not identify a regression in the existing wrapper's call sites.

## Validation status

Source review only. The parent reported whole-workspace formatting/CI diagnostics running at immutable tag `matching-source-check-20261027-1`, run `40527340382`; result was not independently checked here. No service-completion or mixed-cluster parity conclusion is made.
