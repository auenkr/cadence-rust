# Matching `task_list_manager_test.go` port inventory (P1-12)

Go source: `go/service/matching/tasklist/task_list_manager_test.go` (2086 lines, 28 `Test` functions).
Rust target: `rust/crates/cadence-matching/src/matching_registry.rs` test module (owner lifecycle,
liveness, idle sweep, QPS, describe) plus existing reader/writer/dispatch tests where they cover the
same behavior. Rust has no `taskListManagerImpl`; the registry `QueueRegistry` owner is the analogue.

Status legend:
- PORTED: new Rust test asserts the same behavior as the Go test.
- PARTIAL: Rust test covers the Go test's core behavior; named sub-cases are listed as not ported.
- COVERED: an existing Rust test already asserts the Go behavior (no new test added).
- CANNOT-PORT: no Rust analogue; reason given.
- PENDING: not yet ported (group named).
- IGNORED: Rust differs from Go; `#[ignore]` with `parity gap:` marker.

Group 1 (branch `matching-tlm-tests-20261023`): Go tests 2, 5, 6, 7, 8, 9.

| # | Go test (line) | Rust test | Status |
|---|---|---|---|
| 1 | TestDeliverBufferTasks (127) | - | PENDING (group 2) |
| 2 | TestTaskListString (155) | none | CANNOT-PORT: Go `String()` is a debug dump of internal ack/read/range fields. Rust has no equivalent diagnostic formatter and no RPC or log consumer depends on it. |
| 3 | TestDeliverBufferTasks_NoPollers (164) | - | PENDING (group 2) |
| 4 | TestReadLevelForAllExpiredTasksInBatch (181) | - | PENDING (group 2) |
| 5 | TestTaskListManagerRegistryNotification (272) | `matching_registry.rs::unload_notifies_registry_once_and_second_unload_is_noop` | PORTED |
| 6 | TestDescribeTaskList (304) | `matching_registry.rs::describe_by_domain_lists_loaded_root_without_config_or_pollers` | PARTIAL: only the "no status, pollers, or config" case. Not ported: "no status, with config", "with pollers", "with status" (TaskListStatus fields come from `describe_scaled_partition`, a different Rust path), "with status, tasks completed", "with status, pollers and metrics". |
| 7 | TestQueriesPerSecond (457) | `matching_registry.rs::loaded_owner_reports_zero_qps_before_arrivals` | PARTIAL: zero-QPS case only. "returns QPS from tracker" (42.5) needs a deterministic EMA clock; the Rust tracker is `cadence_common::qps::EmaQpsTracker` with a system clock. |
| 8 | TestCheckIdleTaskList (497) | `matching_registry.rs::idle_check_unloads_only_after_full_ttl_without_activity` | PORTED (idle, poller, add-task subtests) |
| 9 | TestAddTaskStandby (590) | `matching_registry.rs::append_after_writer_stop_fails_without_persisting` | PARTIAL: stopped-writer error and no persistence. The standby-domain forwarded branch is not ported. |
| 10 | TestRateLimitErrorsFromTasklistDispatch (646) | - | PENDING (group 3) |
| 11 | TestMisconfiguredZoneDoesNotBlock (671) | - | PENDING (group 3) |
| 12 | TestGetIsolationGroupForTask (709) | - | PENDING (group 3) |
| 13 | TestTaskWriterShutdown (943) | - | PENDING (group 3) |
| 14 | TestTaskListManagerGetTaskBatch (974) | - | PENDING (group 2) |
| 15 | TestTaskListReaderPumpAdvancesAckLevelAfterEmptyReads (1104) | - | PENDING (group 2) |
| 16 | TestTaskListManagerGetTaskBatch_ReadBatchDone (1196) | - | PENDING (group 2) |
| 17 | TestTaskExpiryAndCompletion (1241) | - | PENDING (group 3) |
| 18 | TestTaskListManagerImpl_HasPollerAfter (1343) | - | PENDING (group 3) |
| 19 | TestRefreshTaskListPartitionConfig (1380) | - | PENDING (group 4) |
| 20 | TestUpdateTaskListPartitionConfig (1498) | - | PENDING (group 4) |
| 21 | TestRefreshTaskListPartitionConfigConcurrency (1703) | - | PENDING (group 4) |
| 22 | TestUpdateTaskListPartitionConfigConcurrency (1720) | - | PENDING (group 4) |
| 23 | TestManagerStart_RootPartition (1739) | - | PENDING (group 4) |
| 24 | TestManagerStart_NonRootPartition (1777) | - | PENDING (group 4) |
| 25 | TestDispatchTask (1822) | - | PENDING (group 5) |
| 26 | TestGetNumPartitions (1953) | - | PENDING (group 5) |
| 27 | TestDisconnectBlockedPollers (1961) | - | PENDING (group 5) |
| 28 | TestTaskListUsesOverrideRPS (2024) | `matching_rpc_tests.rs::matching_rpc_live_rps_override_matches_go_precedence` | COVERED (existing test, no new port) |

## CI runs

Each group is pushed to `matching-tlm-tests-20261023` and run with `rust-ci.yml`, `package=cadence-matching`.
Run URLs, sha and conclusions are recorded below as they complete.

- Group 1: pending
