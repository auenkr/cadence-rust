# Matching partition-config update: deadline, failure counter, BadRequest audit (2026-10-27)

Read-only source audit, no Cargo/Go run. Supersedes the "ctx deadline -> Internal context deadline exceeded" row in `matching-partition-error-parity-resolution-20261027.md` (that claim is wrong, see Q1).

## Go trace
- engine.go:1274 passes `hCtx.Context` to `tlMgr.UpdateTaskListPartitionConfig`; the scaler (adaptive_scaler.go:210) calls the same method with `a.ctx`.
- task_list_manager.go:467-495 `updatePartitionConfig`: `validatePartitionConfig` (BadRequest) returns BEFORE the counter. Then `throttleRetry.Do(ctx, db.UpdateTaskListPartitionConfig)`; the DB call receives no ctx. On any error: `scope.IncCounter(TaskListPartitionUpdateFailedCounter)`, `c.Stop()`, raw error.
- backoff/retry.go:204-262 `Do`: the first attempt always runs (ctx is not checked first). `ctx.Done()` is only observed in the `select` on the backoff wait after a retryable failure; it then returns `prevErr` (error before the current attempt) else `err`, i.e. the PERSISTENCE error, never ctx.Err(). Retry policy (common.CreatePersistenceRetryPolicy) is 50ms/10s/30s expiration. ServiceBusy additionally applies the throttle policy (1s initial, 10s max, no expiry): `next = max(next, throttleBackoff)`.
- handler/context.go:83 `handleErr`: BadRequestError passes through (InvalidArgument); InternalService, ServiceBusy pass through; everything else (Timeout, DBUnavailable, ConditionFailed, ...) becomes InternalServiceError.
- Counter scope: `NewPerTaskListScope(domain, tasklist, kind) + task-list-type tag`, operation `TaskListMgr`.

## Rust trace
- `grpc_matching.rs:3439-3500`: `caller_deadline = request_deadline(grpc-timeout)` is passed to the registry only for notification. After `registry.update_partition_config_with_deadline(...).await`, `update.is_err()` emits `PARTITION_UPDATE_FAILED`, then `update?`.
- `matching_registry.rs:1981-2035`: whole `PartitionCache::update` runs in `spawn_blocking`; no ctx/deadline reaches `matching_retry::persistence`.
- `matching_partition.rs:170-215`: `validate(&config)?` first, then persistence retry, then `queue.stop()` + `update_failure_status`.
- Counter tags (`matching_metrics.rs:1399`, operation `TaskListMgr`, type, domain, tasklist, sticky/ephemeral mapping) match Go's scope. `Count(1)` per call matches Go's one increment per failed update.
- Adaptive scaler path (`matching_registry.rs:1949-1962`) calls `update_partition_config` and records only the scaler's own update counter.

## Answers
1. **Deadline stage: MISMATCH.** tonic enforces `grpc-timeout` by dropping the handler future. The `spawn_blocking` closure is not cancelled and keeps retrying up to the 30s policy expiry ignoring the deadline (Go stops retrying at the first backoff wait after expiry and returns the last persistence error). Go keeps the in-flight attempt uncancelled too, which Rust already matches. Rust's gRPC-level result for a dropped call is a tonic DeadlineExceeded rather than Go's persistence-error Internal, but the client has already timed out, so only the retry cut-off, counter and manager stop timing are observable.
2. **Counter: MISMATCH in three ways.**
   a. Validation failures (`validate`) also emit the counter in the gRPC layer; Go emits none (returns before). Also emitted for the `JoinError` path.
   b. Deadline drop: the code after `.await` never runs, so a failure whose caller already timed out emits nothing; Go emits at the failure point.
   c. Scaler-initiated failures emit nothing; Go emits (same method).
   Tags and count per emission are correct.
3. **BackendErrorKind::BadRequest:** in Go a raw BadRequestError passes `handleErr` (InvalidArgument), is not transient (no retry), still counts, stops the manager. In Rust `update_failure_status` maps it to Internal. No production code creates `BackendErrorKind::BadRequest` (only `matching_persistence_limiter.rs` classifies it, plus tests), and Go's SQL/Cassandra task stores cannot return BadRequest for UpdateTaskList; so it is unreachable today. Mapping it is a one-line defensive parity fix, not a behavior bug.
4. Minor (same function): ServiceBusy throttle backoff (1s..10s) is absent from `matching_retry::persistence`; ServiceBusy only occurs with SQL throttling (`BackendErrorKind::Throttled`), so it is reachable on MySQL/Postgres.

## Minimal fix plan
1. `matching_retry.rs`: add `persistence_until(operation, cancelled: impl Fn() -> bool)` (keep `persistence` as `persistence_until(op, || false)`). In `run`, after `next_delay` is computed and before sleeping, sleep in slices (or `park_timeout` on a deadline) and if `cancelled()` becomes true return `previous_before_this_error.unwrap_or(error)`, where `previous_before_this_error` is the value from BEFORE `previous = Some(error)` (Go `prevErr`). First attempt always runs. Add the throttle policy: when error is `Backend{Throttled}`, `delay = max(delay, throttle_policy.next(attempt))` with 1s initial, 10s max, no expiry.
2. `matching_partition.rs`: `PartitionCache::update(..., cancelled: &dyn Fn() -> bool, on_persist_failure: impl FnOnce())`. Call `on_persist_failure()` only in the `Err` branch after the retry (before/with `queue.stop()`), never for `validate`. Add `BackendErrorKind::BadRequest => Status::invalid_argument(message)` to `update_failure_status`.
3. `matching_registry.rs` `update_partition_config_with_deadline`: build `cancelled = move || caller_deadline.is_some_and(|d| tokio::time::Instant::now() >= d)` and pass it plus a failure callback into the `spawn_blocking` closure (the closure outlives a dropped future, so the counter fires at the failure point). The callback records `PARTITION_UPDATE_FAILED` with a `PollMetricContext` (domain name, task list name/kind, type); the registry needs the domain name resolver (pass the context in, built by the gRPC handler and by the scaler caller). Scaler path: pass the scaler's stop flag as `cancelled` (Go `a.ctx`).
4. `grpc_matching.rs`: delete both `if update.is_err() { PARTITION_UPDATE_FAILED }` blocks around 3494-3515 for the queue path (the store fallback at ~3520 keeps its own, it has no validate step before the store call; check `validate` there too).

## Regression strategy (CI only)
- `matching_retry` unit tests with fake clock/pause closure: (a) cancelled during first backoff returns the first error; (b) cancelled during the second wait returns the FIRST error (prevErr semantics); (c) cancel flag already true still performs exactly one attempt; (d) throttled errors use >=1s delay.
- `matching_partition` tests: validation failure does not invoke the failure callback; fenced-store failure invokes it exactly once and stops the queue; BadRequest maps to InvalidArgument.
- registry test: transient-failing fake store + `caller_deadline` a few ms ahead: assert retries stop (attempt count bounded), counter emitted once with TaskListMgr tags, task is detached when the outer future is dropped (`tokio::time::timeout` around the call).
- scaler test: failing store during a scaler tick increments `tasklist_partition_update_failed_per_tl`.
- Mixed gate: Go-oracle comparison of the counter via the existing metrics-scrape harness with a fenced lease and with an invalid manual config.
