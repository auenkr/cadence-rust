# Go task_list_manager_test.go 17 and 20 ports (2026-10-23)

## Test 17 (TestTaskExpiryAndCompletion, task_list_manager_test.go:1241)
- Go GC: go/service/matching/tasklist/task_gc.go:62-85. `checkPrecond`: delete when backlog >= MaxTaskDeleteBatchSize, or backlog > 0 and (ignoreTimeCond or time since last delete > MaxTimeBetweenTaskDeletes).
- Rust GC: rust/crates/cadence-persistence/src/matching_gc.rs (already equal to Go; covered by the Go-fixture test `matching_gc_original_go_fixtures`). No Rust change needed.
- New test: `matching_queue.rs::matching_queue_gc_thresholds_drive_stored_row_counts` asserts stored row counts over SQLite: size threshold (batch 2), below-threshold no-op, time threshold (1ns).
- Not ported: the Go expiry/poll half (expired heads dropped while polling) is covered by `matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads`.

## Test 20 (TestUpdateTaskListPartitionConfig, task_list_manager_test.go:1498-1700)
- Rust path: `PartitionCache::update` (matching_partition.rs) over SQLite `DurableTaskQueue::update_partition_config`.
- New test: `matching_partition.rs::update_partition_config_cases_match_go_task_list_manager_test`: nil-config 1/1 no-op, same-partitions no-op, update bumps version and persists, invalid config rejected, persistence failure (lease fenced) keeps old config and stops the queue.
- Not ported: mocked RefreshTaskListPartitionConfig push expectations (needs a membership client; exercised by `matching_rpc_native_partition_cache_updates_refreshes_and_recovers`).
- No Rust program logic changed.

## CI
