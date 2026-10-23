# Matching TLM batch-read / lease / dispatch tests (2026-10-23)

Test-only change; no Rust program logic changed. Harness: `SqliteTaskStore` (tempdir) + `DurableTaskQueue`
(`rust/crates/cadence-persistence/src/matching_queue.rs`), whose private `scan_batch` is the Rust analogue of Go `getTaskBatch`.

| Go test | Go file:line | Rust test |
|---|---|---|
| 14 TestTaskListManagerGetTaskBatch | go/service/matching/tasklist/task_list_manager_test.go:974 (getTaskBatch task_reader.go:328) | `matching_queue.rs::matching_queue_get_task_batch_bounds_follow_read_range` |
| 16 ..._ReadBatchDone | task_list_manager_test.go:1196 | `matching_queue_get_task_batch_done_only_at_max_read_level` |
| 15 ReaderPumpAdvancesAckLevelAfterEmptyReads | task_list_manager_test.go:1104 (renewLeaseWithRetry task_writer.go:222) | `matching_queue_empty_reads_advance_ack_over_renewed_ranges` |
| block allocation / lease renewal on write | task_writer.go:209,222 | `matching_queue_range_id_renews_per_block_and_fences_stale_writer` |
| 25 TestDispatchTask (success path only) | task_list_manager_test.go:1822 | `matching_reader.rs::matching_reader_dispatches_new_task_to_waiting_poller_then_acks` |

Not ported from Go 25: the mocked `MustOffer` error and standby `TaskCompleter` branches (no such hooks in Rust).
Go 14 partial remainder: the Go test's second manager and GetTask/Finish row-count (GC) assertions are approximated by read_batch + complete.
No Rust/Go mismatch found. Tests not run locally (CI only).

## CI
(filled below)
- sha 7b449805de6449ea5f7e0df5c907bd4115219263, package=cadence-matching: https://github.com/auenkr/cadence-rust/actions/runs/40015006344 success (553 passed; dispatch test ok).
- same sha, package=cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/40015344818 success (4 new matching_queue tests ok).
