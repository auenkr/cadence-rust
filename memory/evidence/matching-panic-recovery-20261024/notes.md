# Matching panic recovery parity (2026-10-24)

Branch `matching-panic-recovery-20261024`, base `origin/master` (e7285da4f). No local cargo run; every
verification is a GitHub Actions run (see CI section).

## Go behaviour (read from source)

- `go/service/matching/handler/handler.go`: 13 handlers (AddActivityTask, AddDecisionTask,
  PollForActivityTask, PollForDecisionTask, QueryWorkflow, RespondQueryTaskCompleted, CancelOutstandingPoll,
  DescribeTaskList, ListTaskListPartitions, GetTaskListsByDomain, UpdateTaskListPartitionConfig,
  RefreshTaskListPartitionConfig, AcquireSemaphore) each run `defer func() { log.CapturePanic(recover(), h.logger, &retError) }()`.
- `go/common/log/panic.go` `CapturePanic`: a panic value that is an `error` is used as is; any other value becomes
  `panic object is not error: %#v`. It logs `Error("Panic is captured", stack, err)` and sets the named return.
  It emits no metric. `handleErr` is not on this path, so no per-RPC failure counter is bumped.
- Wire result, because the captured error is untyped:
  - gRPC: `go/service/matching/wrappers/grpc` → `proto.FromError` default → `CodeUnknown` with `err.Error()`.
  - thrift: `go/common/types/mapper/thrift/errors.go` `FromError` returns the error unchanged → generic thrift error.
- `go/service/matching/handler/membership.go` `runMembershipChangeLoop`: a deferred recover logs
  `matching membership watcher changes caused a panic, recovering` and the loop ends. `shutdownCompletion.Done()` runs
  first (LIFO), so shutdown sees no error.
- `shutDownNonOwnedSemaphoreManagers`: each bucket is an errgroup goroutine with `CapturePanic`. A panic becomes that
  goroutine's error, which the loop logs. The other buckets still unload. `unloadSemaphoreManager` unregisters first
  and calls `Stop` only if the unregister succeeded.

## Deviation from the brief

The brief said "return InternalServiceError". Go does not return that type on the panic path. The captured error is
untyped, so the gRPC wire is `Unknown`. This change returns `Status::unknown(message)`, which matches Go's gRPC result.

Thrift has a remaining, pre-existing divergence that this change does not touch. Rust's `application_error`
(`thrift_matching.rs`) maps any non-typed status to `InternalServiceError`, while Go sends a generic thrift error.
Changing that mapping would affect every error, not only panics.

## Brief premise that did not hold

`grep -rn "parity gap" rust/crates | grep -i panic` returns nothing. No ignored Rust test is a membership panic test.
The two Go panic tests (`TestMembershipSubscriptionRecoversAfterPanic`, `TestShutDownNonOwnedSemaphoreManagersReportsAPanic`)
were MISSING, not ignored (see `memory/matching-cannot-port-review-20261024.md` rows 5 and 6). They are added here as
active tests with the same intent:

- `matching_rpc_membership_loop_panic_ends_loop_without_error`
- `matching_rpc_membership_semaphore_stop_panic_is_contained`
- `matching_rpc_handler_panic_is_captured_as_go_unknown_error` (RPC-level, via `acquire_semaphore`)

## Change

- `rust/crates/cadence-matching/src/matching_panic.rs` (new):
  - `capture(op, fut) -> Result<Result<T,E>, String>`: polls the future under `catch_unwind` with `pin!` + `poll_fn`,
    so no new dependency and no `unsafe`.
  - `capture_sync(op, f)`: same for sync calls such as `Manager::stop`.
  - `panic_message`: Go's `panic object is not error: %#v` text, plus the `ERROR Panic is captured` stderr line.
- `grpc_matching.rs`:
  - all 13 `MatchingApi` methods wrap their bodies in `capture(...).unwrap_or_else(|m| Err(Status::unknown(m)))`.
    Thrift `invoke` calls the same trait methods, so both transports are covered by one seam.
  - `maintain_membership_until` wraps the loop; a panic ends it with `Ok(())`, as Go does.
  - the per-bucket `manager.stop()` goes through `capture_sync`. The bucket is still unregistered, and the panic is reported.
  - the shape of the diff is mostly re-indentation. `git diff -w` shows the logic change (about 65 lines).
- Test-only poison hooks (`#[cfg(test)]`). Each poisons a real lock, so the production `expect(...)` panics:
  - `Manager::poison_for_test` (`matching_semaphore.rs`, index lock)
  - `Registry::poison_for_test` (`matching_semaphore.rs`, registry lock)
  - `Membership::poison_ring_for_test` (`matching_membership.rs`, ring lock)

## Not covered

- Panics inside `tokio::spawn`-ed tasks (`matching_dispatch.rs`) surface as `JoinError` today. Go has its own recovery in
  `tasklist/task_list_manager.go:522` and `tasklist/adaptive_scaler.go:338`. Those are outside the handler/membership/semaphore
  scope of this brief.
- No thrift-wire assertion for a panic (see the deviation above).
- The gate (mixed Go/Rust) was not run for this change.

## CI evidence

(recorded below after the runs)
