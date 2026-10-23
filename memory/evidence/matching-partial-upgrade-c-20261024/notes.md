# Matching PARTIAL upgrade C (rows 17–24) — 2026-10-24

Source map: `memory/matching-go-test-map-20261023.md` (origin/master). Not edited.
Filter: PARTIAL rows in document order of the "Per-test map" table, excluding rows whose Go file is
engine_test.go, engine_integration_test.go, matcher_test.go, forwarder_test.go, task_completer_test.go,
or handler/membership_test.go. That leaves 39 rows; this run takes 17–24 (rows 1–16 belong to
h-partup-a / h-partup-b). Rows 17–24 are the map lines 202, 219, 228, 229, 230, 231, 232, 233.

Verification: no local cargo. Each test was checked by reading the Go test, the Go production code it
calls, and the Rust production code it exercises. Compile and pass evidence comes from GitHub Actions.

## Table

| # | Map line | Go test (file:line) | Rust test(s) | Old → new | Run |
|---|---|---|---|---|---|
| 17 | 202 | semaphore/semaphore_manager_test.go `TestStartSkipsRowsItCannotClassify` (:227) | `matching_semaphore_start_skips_malformed_known_rows` (existing, representable subset); `matching_semaphore_scan_skips_nil_and_unknown_rows` (existing, `#[ignore]`) | PARTIAL → PARTIAL | RUN_URL |
| 18 | 219 | semaphore/semaphore_manager_test.go `TestALateStopUnregistersOnlyItself` (:1028) | `matching_semaphore_late_stop_leaves_replacement_registered` (new); `matching_semaphore_late_stop_unregisters_only_itself` (existing, `#[ignore]`) | PARTIAL → PARTIAL | RUN_URL |
| 19 | 228 | tasklist/task_reader_test.go `TestDispatchSingleTaskFromBuffer` (:57) | `matching_reader_dispatch_success_uses_known_or_default_group` (new); retry, stop, and expiry rows stay with existing tests | PARTIAL → PARTIAL | RUN_URL |
| 20 | 229 | tasklist/task_reader_test.go `TestGetDispatchTimeout` (:244) | `tests/matching_partial_upgrade_c.rs::matching_dispatch_timeout_matches_go_get_dispatch_timeout_table` (new) | PARTIAL → PORTED | RUN_URL |
| 21 | 230 | tasklist/task_reader_test.go `TestTaskPump` (:303) | `matching_reader_pump_falls_back_to_default_for_unknown_isolation_group` (new) | PARTIAL → PORTED | RUN_URL |
| 22 | 231 | tasklist/task_reader_test.go `TestGetTasksPumpConditionFailedUnloadsTaskList` (:443) | `ack_condition_failure_unloads_owner_without_stop_signal` (new, `matching_registry.rs` tests); `fatal_owner_wakeup_does_not_wait_for_ack_interval` (existing, uses `queue.stop()`) | PARTIAL → PORTED | RUN_URL |
| 23 | 232 | tasklist/task_reader_test.go `TestTaskReaderBatchSizeValidation` (:464) | `matching_reader_batch_size_fallback_sizes_group_buffer_like_go` (new) | PARTIAL → PORTED | RUN_URL |
| 24 | 233 | tasklist/task_list_registry_test.go `TestTaskListRegistry_RegisterLookupAndUnregister` (:30) | `registry_register_lookup_and_unregister_matches_go` (new); gauge transitions in existing `task_list_manager_gauge_tracks_registry_lifecycle_not_get_calls` | PARTIAL → PORTED | RUN_URL |

## Notes per row

- **17.** The representable malformed rows (token ID 0, owner with HeldToken 0, owner with empty ID)
  are already asserted. Go's zero-value row type, unknown row type 7, and nil row cannot be built as a
  Rust `SemaphoreRow`. The Cassandra decoder rejects an unknown row type
  (`cadence-persistence/src/cassandra.rs`, "unknown semaphore row type"), while Go skips it. That gap
  stays an `#[ignore]` marker. Nothing else changed for this row.
- **18.** The Go assertion (after the old manager stops late, the replacement is still the
  registered one) is now a passing test. Go's `OnStopFn` self-unregister has no Rust counterpart
  (`Manager::stop` has no registry callback), so that half stays ignored.
- **19.** Go's 12-row table is split across tests. The success rows (known group, no group) are
  asserted here. The expired-behind-head row is `matching_reader_drops_row_that_expired_behind_dispatch_head`,
  the retry rows are `matching_reader_retries_throttled_dispatch_without_acking` and
  `matching_reader_counts_unknown_dispatch_errors_but_not_not_started`, and the stop row is
  `stop_joins_pump_blocked_on_buffered_task_without_poller`. Go's "already expired at dispatch" row
  is not reachable from the Rust pump, because expired rows are dropped at read time. Left PARTIAL.
- **20.** Go calls `getDispatchTimeout` on the private reader. Rust calls
  `cadence_common::task_isolation::dispatch_timeout` with `buffers = isolated.len() + 1`, the same
  production arithmetic. The test passes the Go table with `buffers = 5`.
- **21.** Go's pump reads persisted rows; the Rust test does the same through `TaskReader` and
  `poll_group` / `poll`.
- **22.** Go's test drops the lease with `SetRangeID(999)`. The Rust test steals the lease in SQLite
  (`store.lease(&key, 0, 0, 2)`), so the next ack persist returns `ConditionFailed`. `check_with`
  reports the condition and stops the queue, and the reaper detaches the owner.
- **23.** Go's test re-implements the fallback rule and never calls production code. The Rust test
  calls production `ReaderConfig::buffer_batch_size` (through `ReaderLive`) and checks `batch - 1`.
- **24.** Go's `Register` / `ManagerByTaskListIdentifier` / `Unregister` map onto `QueueRegistry`
  `get` / `state.owners` / `unload`. The stale-owner case and the empty-lookup case are both asserted.

## Not changed

- No production code changed. All edits are tests (`#[test]` / `#[tokio::test]`) or ignore reasons.
- The shared map (`memory/matching-go-test-map-20261023.md`) is not edited.
