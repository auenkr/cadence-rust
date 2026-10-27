# Matching native durable queue (2026-10-28)

New file: `rust/crates/cadence-persistence/src/matching_native_queue.rs` (parent must add `pub mod matching_native_queue;` to lib.rs). Not compiled or tested by this worker (no local Cargo); CI is parent-owned.

## What it is
`DurableTaskQueueBytes` is a byte-keyed port of `matching_queue.rs::DurableTaskQueue` over `Arc<dyn MatchingTaskStoreBytes>`. It uses `TaskListKeyBytes`, `TaskDataBytes`, `TaskListDataBytes`, `PartitionConfigBytes` and `AllocatedTaskBytes` end to end, with no UTF-8 conversion. The legacy queue is untouched.
- Same API shape as the legacy queue: `open`, `append`, `append_in_domain`, `append_mixed`, `append_pending(PendingTaskBytes)`, `read_batch*`, `reader_turn` (`ReaderTurnBytes`), `complete`, `rewrite_failed`, `persist_ack`, `persist_final_ack`, `update_partition_config`, GC (`run_garbage_collection*`, `collect_garbage`), `levels`, `status`, `task_lag`, `task_list_size`, `stop`, `set_stop_waker`, lease and condition-failed observers.
- Behavior copied from `matching_queue.rs`:
  - block range leasing, with the lease renewed under the database gate and observed;
  - writer gate, then database gate, then `create_tasks_with_domains`;
  - `max_read_level` advanced after the write even on error;
  - reader gate and permit, a ten-range scan per turn, and per-query retry;
  - expiry skip advancing the read level;
  - `AckManager` reuse;
  - ack persisted with the existing lease fence;
  - `ConditionFailed` reports once and stops (not reported for partition updates or the final ack);
  - source-domain row keys;
  - `TaskGarbageCollector` reuse.
- Retry: `matching_retry` is typed on the legacy `MatchingTaskStoreError`. `retry` and `retry_lease` adapt native errors. Legacy and Backend errors pass through with their transient classification. Other native errors (`ConditionFailed`, `MalformedSqlRunId`) are non-transient, so they are stashed and restored unchanged.

## Source pointers
Go behavior mirrors `matching_queue.rs` (comments there cite the Go taskReader, taskWriter and taskListDB). Backends: `matching_{sqlite,mysql,postgres}_native.rs`.

## Tests (native SQLite, deterministic)
- Raw `\xff`, `\xfe` and U+FFFD queues stay isolated, and raw `workflow_id` bytes round-trip.
- A stale lease fences the writer, reports the failure once and stops the queue. The new owner is unaffected.
- Range rollover renews the lease and never reuses IDs.
- Ack persists and reopen resumes after the ack level, including the final ack after stop.
- GC deletes only acked rows of the raw-keyed queue, and `task_list_size` is checked.
- Expired tasks advance the cursor without entering flight.
- Cross-domain append keeps the source-domain row key.

## Limitations and compile risks
- Not yet adopted by writer actor, reader pump, registry or manager. Those still use the legacy queue, so this is not production integration.
- `ReaderGate` is duplicated because the legacy copy is private.
- A native `Backend` error that comes back through the retry wrapper is restored as `Legacy(Backend)` (same kind and message, different wrapper variant).
- Unverified against rustc: closure borrows in `retry_lease` inside `append_serialized`, `is_none_or` use in the GC (pre-existing), and unused-import warnings under `-D warnings` (none expected).
- Not implemented: a native version of `matching_writer_actor.rs` (batching and retry actor), and the registry and manager hooks.

Parent registered matching_native_queue in persistence lib.rs and included it and Cassandra native in the CI formatting list. Static review confirms the generic existing garbage collector is reused; legacy APIs remain separate. Compilation, tests and production adoption remain pending.
