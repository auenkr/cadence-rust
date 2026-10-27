# Matching persistence deviation resolution (2026-10-27)

Review only. Method: source reading. No Cargo, Go, CI or source edit. Follow-up to
`matching-task-persistence-deep-audit-20261027.md`.

## 1. Rewrite retry on transient backend errors (audit B1)
- Go: `task_writer.go` 215-232, `throttleRetry` with `persistence.IsTransientError`. `handleErr` (138-150) turns a sticky ConditionFailed into InternalServiceError, which is transient and so retried.
- Rust: **already fixed in the uncommitted working tree** of `cadence-matching/src/matching_writer.rs` ~348:
  `retryable = QueueError::Storage(e) if e.is_transient()` OR (sticky kind 1 and ConditionFailed).
  `StorageError::is_transient` (`matching_task_store.rs:62`) = Sql | Backend{Timeout,Throttled,Internal,DbUnavailable}.
- Tests (added, uncommitted, not yet run on CI): `matching_writer_rewrite_retries_transient_backend_errors_like_go`, `matching_writer_rewrite_does_not_retry_nontransient_backend_errors`.
- Action: CI must run `-p cadence-matching` and the result be recorded. No further source change.

## 2. ackManager ReadItem error: Go `logger.Fatal` vs Rust unload
- Go: `task_reader.go:370-374` `addSingleTaskToBuffer` crashes the process. `ackManager.ReadItem` (`common/messaging/ackManager.go:68-92`) errors only if `itemID <= readLevel` or itemID already outstanding.
- Rust: `matching_queue.rs:493-510 register_read` returns `QueueError::ReaderState`, so that task list unloads.
- Reachability: rows come from `SELECT ... task_id > readLevel AND task_id <= maxReadLevel ORDER BY task_id` (mysql 459, postgres 290, sqlite 460) with a task_id primary key. Reads are strictly increasing, and expired rows only raise readLevel to an id already seen. A store honouring ordering and uniqueness cannot trigger it. It needs a store ordering/PK violation or reader-state corruption.
- Verdict: **intentional, unreachable on a correct store, but not equal.** Go kills the whole host, Rust drops one list. It is not SDK-observable and no test pins it. Not a fix candidate (a Rust panic/abort would make a worse product). Keep as a recorded divergence for the user's decision.

## 3. Lost-lease during partition-config update and final ack
- Go: `task_list_manager.go:455-490 updatePartitionConfig`: retry only transient errors (ConditionFailed is not); on ANY error increments `TaskListPartitionUpdateFailedCounter`, calls `c.Stop()`, returns the raw error. Final ack in `taskReader.Stop` only logs, but the manager is already stopping.
- Rust: `matching_partition.rs:145-190 PartitionCache::update`: `matching_retry::persistence` then on any error `queue.stop()` and `Status::unavailable`. `check_with(.., report=false)` stops without reporting. Final ack: queue is already stopped by `Owner::finish`.
- Verdict: **the stop behaviour is equal to Go** (the audit's "Go only logs" described only the final ack, where it is moot). The earlier worry is withdrawn.
- **Remaining unverified mismatch:** error code to the caller. Go returns the raw persistence error through `FromError` (`common/types/mapper/proto/errors.go`, default `CodeUnknown` unless the handler wraps it, not traced). Rust returns `Code::Unavailable` for every failure, so SDK/admin retry behaviour may differ. Affects only manual/adaptive partition update failures (admin path). Fix plan if confirmed: map ConditionFailed to the same code as Go in `matching_partition.rs update` (owner of that file needed). Needs the handler wrap trace in `go/service/matching/handler/handler.go` 470-500 first.
- Also in the Go queue `UpdateTaskListPartitionConfig` the metric counter `TaskListPartitionUpdateFailedCounter` is not confirmed in Rust (not searched).

## 4. Standby completer constants
- Go `common/util.go:86-88,231-235`: initial 1s, max 10s, expiration 5 minutes; `isRetryableError` false only for `errDomainIsActive`; `errTaskNotStarted` is retried until expiry.
- Rust `matching_standby.rs:106-108`: initial 1s, max 10s, expiration 300s. **Equal.** Test: `matching_standby_original_go_completion_decisions`. Go `task_completer.go` logic (domain active -> no retry, EntityNotExists > 24h -> Finish(nil), CloseStatus != nil -> started, decision schedule ID comparison, activity pending lookup) matched by audit table row; not re-diffed line-by-line here.

## 5. Engine decision "already started" classification
- Go `handler/engine.go:~800-850`: DomainNotActive -> refreshWorkflowTasks then Finish(nil|err); EntityNotExists / WorkflowExecutionAlreadyCompleted / EventAlreadyStarted -> Finish(nil); any other error -> `Finish(err)` (rewrite).
- Rust `matching_history.rs:30-52 start_error_kind` plus `matching_dispatch.rs:1424-1490`: same three-way split. Note: classification needs the first typed gRPC detail; missing/undecodable details fall to Retry, which equals Go's default arm. Test: `matching_dispatch_typed_terminal_error_drops_without_rewrite`. **Equal.**

## 6. Manager Stop ordering and task-count gauge
- Go `Stop` (`task_list_manager.go:318-340`): unregister, adaptive scaler, qps, liveness, writer.Stop, reader.Stop (cancel, final persistAckLevel logged on error, `taskGC.RunNow`), disconnect pollers.
- Rust `Owner::stop/finish` (`matching_registry.rs:115-139`): scaler, writer, reader, queue stop, then final ack and forced GC; failures other than ConditionFailed are collected into `failure` and returned (`flush` continues over other owners). Order of the effects is equal. Difference: Go only logs a final-ack failure, Rust propagates it as an `Unavailable` from `shutdown`/flush at process stop. Observable only in the log/exit path of the host; no RPC or SDK effect. Harmless; if strict parity wanted, log instead of returning (owner of `matching_registry.rs`).
- Gauge: Go `task_reader.go:269-274` ack timer reads `GetTaskListSize(ackLevel)` first, sets `TaskCountPerTaskListGauge` on success only, then persists. Rust `matching_registry.rs:2392-2397` does `task_list_size()` on success then `record_task_count`, then `record_task_lag` when ack >= 0, then `persist_ack`. **Equal order.** No direct test of the gauge found (`grep` hit only `record_task_lag` at `matching_reader.rs:1100`); a metric test would be an addition.

## Open items
1. Run CI for the B1 fix (writer.rs diff).
2. Trace Go handler wrapping of ConditionFailed from partition update, then decide the code mapping (item 3).
3. User decision on item 2 (accepted divergence) and optional item 6 shutdown log-only change.
