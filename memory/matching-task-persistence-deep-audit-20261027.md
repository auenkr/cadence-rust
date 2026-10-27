# Matching task persistence deep audit (2026-10-27)

Scope: Go `service/matching/tasklist/{task_reader,task_writer,task_gc,task_completer}.go`, `common/messaging/ackManager.go`
vs Rust `cadence-matching/src/{matching_reader,matching_writer,matching_registry,matching_dispatch,matching_standby,matching_history}.rs`,
`cadence-persistence/src/{matching_queue,matching_gc,matching_retry,matching_task_store}.rs`, `cadence-common/src/ack_manager.rs`.
Method: source reading only. No Cargo, Go or CI was run or dispatched. Source was not edited.

## Confirmed bug

### B1: rewrite retry ignores transient backend errors (non-SQLite stores)
- Rust: `matching_writer.rs` ~342-350, `TaskWriter::append_inner` sets `retryable` only for `StorageError::Sql(_)`
  or sticky-kind `ConditionFailed`. `StorageError::Backend{Timeout|Throttled|Internal|DbUnavailable}` (emitted by MySQL, Postgres
  and Cassandra stores) is never retryable.
- Go: `task_writer.go` rewrite path uses `throttleRetry` (30s) with `persistence.IsTransientError`
  (`data_manager_interfaces.go:3187`: InternalServiceError, ServiceBusyError, TimeoutError, DBUnavailableError).
- Repro: failed delivery -> `matching_dispatch.rs rewrite()` -> `writer.append_rewrite`; store returns `Backend{Timeout}` once
  then succeeds. Go retries and succeeds. Rust returns the first error, `rewrite()` calls `queue.stop()`, and the task list unloads.
- Fix: for `QueueError::Storage(e)`, `retryable = e.is_transient()`; keep sticky-kind ConditionFailed retryable.
  Add a test: a store failing once with `Backend{Timeout}`, then `append_rewrite` must succeed.

## Verified parity (Go behavior -> Rust function -> test evidence)
| Go behavior | Rust | Tests |
|---|---|---|
| AckManager ReadItem/AckItem/SetAckLevel/SetReadLevel (start -1, gap skip, dup ack no-op) | `ack_manager.rs` | `matching_ack_go_fixtures` |
| getTaskBatch: up to 10 ranges, upper=min(read+range,max), empty batch sets read level, ack advance if ack==initial read | `matching_queue.rs scan_batch/reader_turn/register_read`, reader pump | `matching_queue_get_task_batch_bounds_follow_read_range`, `matching_queue_empty_reads_advance_ack_over_renewed_ranges`, `matching_queue_original_go_reader_fixtures` |
| Expired rows only SetReadLevel; dispatch-time expiry acks without GC | reader pump, dispatch loop | `matching_reader_read_level_advances_past_expired_rows_without_ack`, `matching_reader_drops_row_that_expired_behind_dispatch_head`, `matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads`, `matching_queue_original_go_expiry_fixtures`, `matching_queue_expiry_boundaries_and_scan_budget` |
| Ack timer rereads backlog | pump | `matching_reader_ack_timer_rereads_backlog_without_append_signal` |
| Transient read retry | `matching_retry.rs` | `matching_queue_reader_retries_transient_query_without_losing_task`, `matching_retry_original_go_outcomes` |
| taskGC tryLock/precond/loop/cursor, strict time boundary | `matching_gc.rs`, `run_garbage_collection*` | `matching_gc_original_go_fixtures`, `matching_gc_time_boundary_is_strict_and_failure_defers_small_retry`, `zero_and_negative_batches_keep_go_error_and_repeat_boundaries_bounded` |
| Writer batching, live dynamic config, maxReadLevel update | `matching_writer.rs` | `matching_writer_original_go_batches`, `matching_writer_batches_follow_live_dynamic_config_without_recreation` |
| Range fencing, lease renew per block | `matching_queue.rs open/append_serialized` | `matching_queue_range_id_renews_per_block_and_fences_stale_writer`, `matching_queue_lease_retries_sql_failure_and_stops_on_terminal_renewal` |
| ConditionFailed -> report + stop (writer and ack) | `check_with`, registry | `matching_queue_reports_condition_failed_for_writer_and_ack_not_partition_update`, registry `TestGetTasksPumpConditionFailedUnloadsTaskList` port |
| Rewrite: new task id, ack only after success, failure -> stop (signalFatal) | `matching_dispatch.rs finish_delivery/rewrite` | `matching_queue_failure_rewrite_is_durable_before_ack`, `matching_writer_rewrite_retry_uses_original_go_outcomes` |
| Shutdown: stop, final ack, force GC, ConditionFailed tolerated | `Owner::finish`, `persist_acks/flush/maintain_until` | `shutdown_tolerates_lost_task_list_ownership`, `shutdown_waits_for_inflight_writer_fatal_notification` |
| Delivery finish classification (engine.go 800-870, 960-1010): Entity/AlreadyCompleted/EventAlreadyStarted -> Finish(nil); DomainNotActive -> refresh then Finish(nil) or Finish(err); other -> rewrite | `matching_dispatch.rs` ~1420-1480 (`start_error_kind`: Discard/InactiveDomain/Retry), `matching_history.rs` 42-47, 1475-1503 | `matching_dispatch_typed_terminal_error_drops_without_rewrite` |
| Standby task completer (decision/activity started, 24h missing-execution window) | `matching_standby.rs` | `matching_standby_original_go_completion_decisions` |
| Append timeout/status mapping | `queue_status` | `queue_status_maps_throttling_to_service_busy_and_other_errors_to_internal` |

## Observations (not bugs)
- Go `logger.Fatal` on an ackManager ReadItem error crashes the process. Rust returns `QueueError::ReaderState` and unloads the list. This is an intentional safer difference and is practically unreachable.
- `check_with` stops the queue on ConditionFailed even for partition-config update. Go only logs on the final ack. This has low impact because the lease is already lost.
- The non-writer `rewrite_failed` branch in `rewrite()` has no retry. It looks test-only (`self.writer` is None); not verified.
- Rust reads maxReadLevel and initialReadLevel in the opposite order to Go. This is benign.

## Untraced / needs verification
- Standby completer retry constants (Rust 1s initial, 10s max, 300s expiration) were not compared with the Go `common/util.go` taskCompleter* constants.
- Go "decision already started / mutable state" Finish(nil) path around engine.go ~790 was not traced in detail.
- Manager Stop ordering (writer.Stop vs reader.Stop) and the `GetTaskListSize` gauge were not audited.
- Non-SQLite persistence SQL/CQL strings were compared only for the CompleteTasksLessThan and CreateTasks TTL semantics, not exhaustively.

No CI or local Cargo was run for this audit.
