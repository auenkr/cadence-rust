# Matching `task_list_manager_test.go` port inventory (P1-12)

Go source: `go/service/matching/tasklist/task_list_manager_test.go` (2086 lines, 28 `Test` functions).
Rust target: `rust/crates/cadence-matching/src/matching_registry.rs` test module (owner lifecycle,
liveness, idle sweep, QPS, describe), `rust/crates/cadence-matching/src/matching_reader.rs` test module
(buffer, dispatch, read/ack levels) and existing dispatch/writer tests where they cover the same
behavior. Rust has no `taskListManagerImpl`; the registry `QueueRegistry` owner is the analogue.

Status legend:
- PORTED: new Rust test asserts the same behavior as the Go test.
- PARTIAL: Rust test covers the Go test's core behavior; named sub-cases are listed as not ported.
- COVERED: an existing Rust test already asserts the Go behavior (no new test added).
- CANNOT-PORT: no Rust analogue; reason given.
- NOT-PORTED: behavior not yet asserted in Rust; reason given; follow-up.
- PENDING: not yet ported (group named).
- IGNORED: Rust differs from Go; `#[ignore]` with `parity gap:` marker.

## Per-test status

| # | Go test (line) | Rust test | Status |
|---|---|---|---|
| 1 | TestDeliverBufferTasks (127) | `matching_reader.rs::stop_joins_pump_blocked_on_buffered_task_without_poller` | PARTIAL: cancel case only. The rate-limiter branch (`ReportLimit(0.1)` + token consume) is not ported. |
| 2 | TestTaskListString (155) | none | CANNOT-PORT: Go `String()` is a debug dump of internal ack/read/range fields. Rust has no equivalent diagnostic formatter and no RPC or log consumer depends on it. |
| 3 | TestDeliverBufferTasks_NoPollers (164) | `matching_reader.rs::stop_joins_pump_blocked_on_buffered_task_without_poller` | PORTED |
| 4 | TestReadLevelForAllExpiredTasksInBatch (181) | `matching_reader.rs::matching_reader_read_level_advances_past_expired_rows_without_ack` | PORTED (Go calls `addTasksToBuffer` directly; Rust drives the same read through the queue and reader pump) |
| 5 | TestTaskListManagerRegistryNotification (272) | `matching_registry.rs::unload_notifies_registry_once_and_second_unload_is_noop` | PORTED |
| 6 | TestDescribeTaskList (304) | `matching_registry.rs::describe_by_domain_lists_loaded_root_without_config_or_pollers` | PARTIAL: only the "no status, pollers, or config" case. Not ported: "no status, with config", "with pollers", "with status" (TaskListStatus fields come from `describe_scaled_partition`, a different Rust path), "with status, tasks completed", "with status, pollers and metrics". |
| 7 | TestQueriesPerSecond (457) | `matching_registry.rs::loaded_owner_reports_zero_qps_before_arrivals` | PARTIAL: zero-QPS case only. "returns QPS from tracker" (42.5) needs a deterministic EMA clock; the Rust tracker is `cadence_common::qps::EmaQpsTracker` with a system clock. |
| 8 | TestCheckIdleTaskList (497) | `matching_registry.rs::idle_check_unloads_only_after_full_ttl_without_activity` | PORTED (idle, poller, add-task subtests) |
| 9 | TestAddTaskStandby (590) | `matching_registry.rs::append_after_writer_stop_fails_without_persisting` | PARTIAL: stopped-writer error and no persistence. The standby-domain forwarded branch is not ported. |
| 10 | TestRateLimitErrorsFromTasklistDispatch (646) | `matching_dispatch.rs::queue_status_maps_throttling_to_service_busy_and_other_errors_to_internal` (indirect) | NOT-PORTED: Go injects a `dispatchTask` hook returning `ErrTasklistThrottled` and asserts the retry loop. Rust has no dispatch hook; the throttle mapping is covered only by the status-mapping test. Follow-up. |
| 11 | TestMisconfiguredZoneDoesNotBlock (671) | `matching_sync.rs::sync_match_invalid_isolation_group_poll_falls_back_to_default_group` (poll side) | PARTIAL: poll-side fallback to the default group is covered. The dispatch-side fallback (`dispatchSingleTaskFromBuffer` redirect, `dispatched[""]` count) is not ported. |
| 12 | TestGetIsolationGroupForTask (709) | - | NOT-PORTED: the selection logic is an inline closure in `grpc_matching.rs` (about lines 800-830) over live domain and dynamic-config overlay state, not a pure function. The 13-case table needs that fixture. Follow-up. |
| 13 | TestTaskWriterShutdown (943) | `matching_registry.rs::append_after_writer_stop_fails_without_persisting` (queue stop, not writer stop) | PARTIAL: stopped-task-list append error is covered. The explicit `taskWriter.Stop()` path and the `stopped` race sub-case are not ported. |
| 14 | TestTaskListManagerGetTaskBatch (974) | `matching_reader.rs::matching_reader_prefetch_backpressure_and_stop_preserve_unacked_rows` (indirect) | PARTIAL: Go calls `taskReader.getTaskBatch(readLevel, maxReadLevel)` directly. Rust has no batch function with that signature; the batch read is fused into the queue pump. The 1200-task sub-assertions (range/batch sizing) are not ported. |
| 15 | TestTaskListReaderPumpAdvancesAckLevelAfterEmptyReads (1104) | - | NOT-PORTED: ack advancement over 15 empty leased ranges (`renewLeaseWithRetry`) and the post-advance write offset. Needs a lease-renewal-without-write harness; follow-up. |
| 16 | TestTaskListManagerGetTaskBatch_ReadBatchDone (1196) | - | NOT-PORTED: depends on `getTaskBatch` `isReadBatchDone` return value, which has no Rust analogue (see row 14). |
| 17 | TestTaskExpiryAndCompletion (1241) | `matching_reader.rs::matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads` (indirect) | NOT-PORTED: the Go test asserts task-GC row counts under two thresholds (`MaxTaskDeleteBatchSize=2`, `MaxTimeBetweenTaskDeletes=1ns`) after polls. The Rust GC batch and time knobs are not wired to this assertion. Follow-up. |
| 18 | TestTaskListManagerImpl_HasPollerAfter (1343) | `matching_registry.rs::has_poller_after_reports_outstanding_or_finished_pollers` | PORTED (outstanding and finished-poller sub-cases in one sequenced test) |
| 19 | TestRefreshTaskListPartitionConfig (1380) | `matching_registry.rs::refresh_partition_config_from_request_ignores_older_versions` | PARTIAL: "refresh from request" and "ignore older version" are ported. "refresh from database" and "failed to refresh from database" are NOT-PORTED: they need mocked `GetTaskList` results and an injected error. The Rust store is SQLite with no fault hook. Follow-up. |
| 20 | TestUpdateTaskListPartitionConfig (1498) | `matching_rpc_tests.rs::matching_rpc_native_partition_cache_updates_refreshes_and_recovers` (RPC path, indirect) | NOT-PORTED: the Go cases (no-op, nil pointer, update, version fencing, persistence errors) use mocked `UpdateTaskList` expectations. Needs a Rust update-path harness over SQLite. Follow-up. |
| 21 | TestRefreshTaskListPartitionConfigConcurrency (1703) | `matching_rpc_tests.rs::matching_rpc_native_partition_concurrent_versions_and_fencing` (indirect) | PARTIAL: concurrent refresh is covered at the RPC level. The Go version pins a per-manager lock order that is not asserted directly. |
| 22 | TestUpdateTaskListPartitionConfigConcurrency (1720) | `matching_rpc_tests.rs::matching_rpc_native_partition_concurrent_versions_and_fencing` (indirect) | PARTIAL: same as row 21, update side. |
| 23 | TestManagerStart_RootPartition (1739) | `matching_registry.rs::shard_policy_accepts_local_assignment_and_rechecks_before_publish` (indirect) | PARTIAL: root start is covered by ownership policy rechecks. The Go direct `Start` partition assertions are not ported. |
| 24 | TestManagerStart_NonRootPartition (1777) | `matching_rpc_tests.rs::matching_rpc_child_partition_add_falls_back_to_local_persist` (indirect) | PARTIAL: child partition add and local-persist fallback are covered at the RPC level. Direct `Start` assertions are not ported. |
| 25 | TestDispatchTask (1822) | `matching_dispatch.rs` tests (indirect, no direct analogue) | NOT-PORTED: Go mocks `TaskMatcher.MustOffer` and the standby `TaskCompleter` and asserts the returned error. Rust has no `DispatchTask` entry point with those hooks. Follow-up. |
| 26 | TestGetNumPartitions (1953) | `matching_rpc_tests.rs::matching_rpc_lists_configured_write_partitions` (indirect) | CANNOT-PORT: the only Go assertion is `NotPanics` on a Go-only `MatchingEnableGetNumberOfPartitionsFromCache` toggle around `limiter.ReportLimit`. Rust has no panicking path there. Partition-count provider behavior is covered by the indirect RPC test. |
| 27 | TestDisconnectBlockedPollers (1961) | `matching_registry.rs::release_blocked_pollers_wakes_waiting_decision_poll` | PORTED (disconnect case). The shutting-down no-op case is covered by `shutdown_wakes_blocked_dispatcher` (not a 1:1 port; the Rust API returns no error). |
| 28 | TestTaskListUsesOverrideRPS (2024) | `matching_rpc_tests.rs::matching_rpc_live_rps_override_matches_go_precedence` | COVERED (existing test, no new port) |

## CI runs

Each group is pushed to `matching-tlm-tests-20261023` and run with `rust-ci.yml`, `package=cadence-matching`.

- Group 1 (Go 2, 5, 6, 7, 8, 9): commit `be8d3127f`, run https://github.com/auenkr/cadence-rust/actions/runs/40010474303, conclusion success. The five new tests in `matching_registry.rs` ran and passed: `unload_notifies_registry_once_and_second_unload_is_noop`, `idle_check_unloads_only_after_full_ttl_without_activity`, `loaded_owner_reports_zero_qps_before_arrivals`, `describe_by_domain_lists_loaded_root_without_config_or_pollers`, `append_after_writer_stop_fails_without_persisting`. Crate result: 495 passed, 0 failed, 9 ignored.
- Group 2 (Go 1, 3, 4, 14, 15, 16): commit `cde29f739`, run https://github.com/auenkr/cadence-rust/actions/runs/40011223704, conclusion success. New reader tests `stop_joins_pump_blocked_on_buffered_task_without_poller` and `matching_reader_read_level_advances_past_expired_rows_without_ack` passed. Go 14, 15, 16 are not fully ported (see rows).
- Group 3 (Go 10, 11, 12, 13, 17, 18): only Go 18 is a new port (`has_poller_after_reports_outstanding_or_finished_pollers`). Go 10, 12 and 17 are NOT-PORTED with reasons; Go 11 and 13 are PARTIAL. CI: commit `2f6e726cc`, run https://github.com/auenkr/cadence-rust/actions/runs/40011880148, conclusion success (`has_poller_after_reports_outstanding_or_finished_pollers` ok).
- Group 4 (Go 19 to 24): one new port (`refresh_partition_config_from_request_ignores_older_versions`, covering Go 19's request and older-version cases). Go 20 is NOT-PORTED. Go 21 to 24 are PARTIAL via existing RPC or registry tests. CI: commit `0948373d3`, run https://github.com/auenkr/cadence-rust/actions/runs/40012767742 (workflow_dispatch), conclusion success. Run `40012768608` (push) was cancelled by the concurrency group when the manual dispatch raced it; not a test failure.
- Group 5 (Go 25 to 28): one new port (`release_blocked_pollers_wakes_waiting_decision_poll`, Go 27). Go 25 is NOT-PORTED, Go 26 is CANNOT-PORT, Go 28 is COVERED by existing test. CI: pending
