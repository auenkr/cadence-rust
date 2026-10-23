# engine_test.go functions 7-12 → Rust (matching-engine-tests3-20261024)

Go source: `go/service/matching/handler/engine_test.go` lines 637-1249 (file order, functions 7-12).
Rust file: `rust/crates/cadence-matching/tests/matching_engine_go_tests_b.rs`.
Based on origin/master e9b8e9a0e. Functions 1-6 are on `matching-engine-tests2-20261024`; this file does not duplicate them.

## CI evidence

| Run | Head | Result | Notes |
| --- | --- | --- | --- |
| https://github.com/auenkr/cadence-rust/actions/runs/40031294950 | d77d163c8 | FAIL (Test step) | `update_task_list_partition_config_success`: `read partitions < 1` |
| https://github.com/auenkr/cadence-rust/actions/runs/40031973899 | 018745711 | PASS (clippy + test, `package=cadence-matching`) | `matching_engine_go_tests_b`: 14 passed, 0 failed, 6 ignored |

Final run: https://github.com/auenkr/cadence-rust/actions/runs/40031973899

Root cause of the failure: the Go test passes `{0: {}}` straight to a gomock, so Go never converts the proto. Rust reads the deprecated `num_read_partitions` / `num_write_partitions` counts (`partition_map`). Go's own proto mapper sends `NumReadPartitions = len(ReadPartitions)`. The fixture now sends the same counts. This is a fixture fix; production code is unchanged.

## Test table

Status: PASS = passing Rust test; ADAPTED = passing, but the input or assertion differs from Go as noted; IGNORED = `#[ignore = "parity gap: ..."]`.

| Go test (line) | Go case | Rust test | Status | Run |
| --- | --- | --- | --- | --- |
| TestQueryWorkflow (637) | invalid tasklist name | `query_workflow_invalid_tasklist_name_is_error` | PASS | 40031973899 |
| TestQueryWorkflow (637) | sticky worker unavailable | `query_workflow_sticky_worker_unavailable_is_error` | PASS (also asserts `Unavailable`) | 40031973899 |
| TestQueryWorkflow (637) | failed to dispatch query task | `query_workflow_failed_to_dispatch_query_task_is_error` | IGNORED: no seam to inject a dispatch failure | — |
| TestQueryWorkflow (637) | success | `query_workflow_success_returns_worker_answer` | ADAPTED: a parked poller completes the query through the real `QueryChannel` instead of a mock; asserts query result only | 40031973899 |
| TestQueryWorkflow (637) | success (partition config part) | `query_workflow_success_reports_partition_config` | IGNORED: Rust returns `partition_config: None` | — |
| TestWaitForQueryResult (780) | internal error | `wait_for_query_result_internal_error_is_passed_through` | PASS | 40031973899 |
| TestWaitForQueryResult (780) | strong consistency not supported | `wait_for_query_result_strong_consistency_version_unsupported` | ADAPTED: real version gate rejects uber-go 1.0.0; Go's mock error text `version error` is not reproduced, Rust asserts `FailedPrecondition` / "Client version not supported" | 40031973899 |
| TestWaitForQueryResult (780) | success - query task completed | `wait_for_query_result_success_query_task_completed` | ADAPTED: uses uber-go 1.5.0 so the real gate passes (Go mocks it to pass at 1.0.0) | 40031973899 |
| TestWaitForQueryResult (780) | query task failed | `wait_for_query_result_query_task_failed` | PASS (Rust maps to `InvalidArgument`, message "query failed") | 40031973899 |
| TestWaitForQueryResult (780) | unknown query result | `wait_for_query_result_unknown_query_result_is_internal_error` | IGNORED: Rust returns `InvalidArgument "Query failed."`, Go returns `Internal "unknown query completed type"` | — |
| TestIsShuttingDown (918) | Start/Stop flag | `is_shutting_down_is_false_until_stop` | IGNORED: no public shutting-down state on `MatchingRuntime` | — |
| TestGetTasklistsNotOwned (942) | non-owned list | `get_tasklists_not_owned_returns_only_non_owned_managers` | IGNORED: `unload_non_owned` detaches in place and returns `()`; no list is observable | — |
| TestShutDownTasklistsNotOwned (978) | tl2 moved to host456 is stopped | `shut_down_tasklists_not_owned_unloads_only_moved_owner` | ADAPTED: task list names are chosen from the real hash ring (two that stay with self, one that moves); the assertion is `contains_cached` after `unload_non_owned`, not a `Stop` call | 40031973899 |
| TestUpdateTaskListPartitionConfig (1025) | success | `update_task_list_partition_config_success` | PASS (deprecated counts set to match maps, as Go's mapper does) | 40031973899 |
| TestUpdateTaskListPartitionConfig (1025) | tasklist manager error | `update_task_list_partition_config_tasklist_manager_error` | IGNORED: no seam to make the durable update fail | — |
| TestUpdateTaskListPartitionConfig (1025) | non root partition error | `update_task_list_partition_config_non_root_partition_is_error` | PASS | 40031973899 |
| TestUpdateTaskListPartitionConfig (1025) | invalid tasklist name | `update_task_list_partition_config_invalid_tasklist_name_is_error` | PASS (asserts the Go message as a substring) | 40031973899 |
| TestUpdateTaskListPartitionConfig (1025) | nil partition config | `update_task_list_partition_config_nil_partition_config_is_error` | PASS | 40031973899 |
| TestUpdateTaskListPartitionConfig (1025) | invalid tasklist kind | `update_task_list_partition_config_sticky_kind_is_error` | PASS | 40031973899 |
| TestUpdateTaskListPartitionConfig (1025) | manual update not allowed | `update_task_list_partition_config_manual_update_rejected_when_scaler_enabled` | PASS (`matching.enableAdaptiveScaler` = true) | 40031973899 |

## Totals

- Go cases in functions 7-12: 19 (QueryWorkflow 4, WaitForQueryResult 5, IsShuttingDown 1, GetTasklistsNotOwned 1, ShutDownTasklistsNotOwned 1, UpdateTaskListPartitionConfig 7).
- Rust tests in the file: 20. 14 pass, 6 ignored.
- Go cases covered by a passing test: 14. Of these, 4 are ADAPTED (WaitForQueryResult version cases ×2, QueryWorkflow success, ShutDownNotOwned).
- Go cases covered only by an ignored test: 5. One extra ignored test (`query_workflow_success_reports_partition_config`) holds the partition-config assertion split out of the Go success case.

## Parity gaps found (not fixed; production code is not changed)

1. `QueryWorkflow` success: Go returns the root partition config (version 1). Rust `query_outcome` always sets `partition_config: None`, and the query path does not fill it (`grpc_matching.rs` around line 2748).
2. `waitForQueryResult` with an unknown `QueryTaskCompletedType`: Go returns `Internal "unknown query completed type"`. Rust treats any non-answered result as a failed query and returns `InvalidArgument "Query failed."`.
3. No public seam for: injected dispatch failure, injected partition update failure, `isShuttingDown`, and a list of non-owned managers.

## Reproduce

Push the branch and run `gh workflow run rust-ci.yml --ref matching-engine-tests3-20261024 -f package=cadence-matching`. Pass when the Test step reports `matching_engine_go_tests_b ... 14 passed; 6 ignored`.
