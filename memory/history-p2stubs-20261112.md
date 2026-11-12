# history-p2stubs (2026-11-12)

## Dead-stub audit (rust/crates/cadence-history, non-test code)
| Item | Decision |
|---|---|
| `UnwiredDecisionHandler`, `UnwiredWorkflowResetter`, `UnwiredTaskRefresher` (engine_factory.rs) | Deleted: zero references; real `EngineDecisionHandler`/`EngineWorkflowResetter` are wired. Header comment fixed. |
| `UnwiredReplicator` (replication_builders.rs) | Deleted: superseded by `ndc_adapters::NdcReplicator`, unreferenced. |
| `ShardTaskReinjector::reinject_history_tasks` "not implemented for SQL" (shard_factory.rs) | Kept: matches Go `sqlExecutionStore.CreateHistoryTasks` (`errHistoryTasksNotImplemented`). Gap: Go on Cassandra performs the write (task-id allocation + `CreateHistoryTasks`); Rust has no execution-manager handle in the shard here. Left as a follow-up, not dead. |
| `archiver_client.rs` `NOT_PORTED` (s3/gs) | Not touched (history-s3gs worker owns it). |
| ndc/mod.rs default-error trait methods (`NdcMutableState`, `NdcWorkflow::lend_state`...) | Kept: used by test fakes that do not override them; explicit errors, no Go-side equivalent callers broken. Removal needs every fake to implement them (test-only churn). |
| `unimplemented!()` in `*_tests.rs`, `test_support.rs`, `standby_test_support.rs`, `transaction_manager.rs:546` (inside `#[cfg(test)]` fake) | Test fakes for trait methods a test never reaches; not stubs of product logic. Kept. |

## Executor tests added
- transfer_active_task_executor_tests.rs: `reset_workflow_success_path_builds_request_from_reset_point`, `reset_workflow_bad_request_is_dropped`, `reset_workflow_other_error_is_retried`, `reset_workflow_without_version_histories_errors`, `decision_ephemeral_task_list_kind_is_forwarded`, `copy_search_attributes_nil_and_deep_copy`.
- timer_standby_task_executor_tests.rs: `execute_activity_retry_timer_task_is_dropped_on_standby` (Go TestProcessRetryTimeout, via the `Executor::execute` dispatch).

## CI evidence
Final head fd4b7d9af74f82755f92a889a1f7ef4ce8c9105c (includes merged origin/master with history-s3gs):
- rust-ci cadence-history: success https://github.com/auenkr/cadence-rust/actions/runs/43174667623
- history-mixed-gate mysql/grpc/true: success, `GATE SUMMARY failed: []` https://github.com/auenkr/cadence-rust/actions/runs/43174672604
- history-mixed-gate cassandra/grpc/true: success, `GATE SUMMARY failed: []` https://github.com/auenkr/cadence-rust/actions/runs/43174676967
- history-mixed-gate postgres/tchannel/false: success, `GATE SUMMARY failed: []` https://github.com/auenkr/cadence-rust/actions/runs/43174681013
(Earlier pre-merge head f5771ccb0: rust-ci success 43172459690; its gates were cancelled and re-run post-merge.)
