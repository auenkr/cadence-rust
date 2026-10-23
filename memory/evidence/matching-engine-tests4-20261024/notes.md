# Matching engine_test.go parity: functions 13-18 (matching-engine-tests4-20261024)

Go source: `go/service/matching/handler/engine_test.go` (the brief's
`go/service/matching/engine_test.go` path does not exist).
Rust port: `rust/crates/cadence-matching/tests/matching_engine_go_tests_c.rs`.
Functions 1-6 are covered by `matching_engine_go_tests.rs` on
`matching-engine-tests2-20261024`; this file covers 13-18 in file order.

## Evidence

- Commit: `5435afad9` on `matching-engine-tests4-20261024`
- CI run: https://github.com/auenkr/cadence-rust/actions/runs/40031273396
  (`rust-ci.yml`, `package=cadence-matching`, conclusion: success)
- Result for `matching_engine_go_tests_c`: **10 passed, 0 failed, 10 ignored**
  (20 Go subtests: 10 run, 10 ignored with named gaps)
- Local cargo was not run; the CI log is the only evidence.

## Per-case table

| Go test | Rust test | Status | Run URL |
| --- | --- | --- | --- |
| TestRefreshTaskListPartitionConfig / success | refresh_task_list_partition_config_success_accepts_partitioned_normal_list | pass (partial: Go's pushed-config mock expectation not observable) | 40031273396 |
| TestRefreshTaskListPartitionConfig / tasklist manager error | refresh_task_list_partition_config_manager_error_is_propagated | ignored: no seam to inject a durable-refresh failure | 40031273396 |
| TestRefreshTaskListPartitionConfig / invalid tasklist name | refresh_task_list_partition_config_invalid_tasklist_name_is_invalid_argument | pass | 40031273396 |
| TestRefreshTaskListPartitionConfig / invalid tasklist kind | refresh_task_list_partition_config_sticky_kind_is_invalid_argument | pass (kind 2 = STICKY in proto) | 40031273396 |
| TestRefreshTaskListPartitionConfig / invalid request for root partition | refresh_task_list_partition_config_root_with_partition_config_is_invalid_argument | pass | 40031273396 |
| Test_domainChangeCallback | domain_change_callback_updates_failover_version_and_releases_eligible_pollers | ignored: closure is private in matching_runtime.rs | 40031273396 |
| Test_registerDomainFailoverCallback / catchUpFn no updates (v0) | register_domain_failover_callback_catch_up_without_updates_keeps_version | ignored: registered callback private | 40031273396 |
| Test_registerDomainFailoverCallback / catchUpFn no updates (v3) | register_domain_failover_callback_catch_up_advances_version_to_highest | ignored: registered callback private | 40031273396 |
| TestRefreshWorkflowTasks / success | refresh_workflow_tasks_success_sends_domain_and_execution | pass (scripted gRPC history peer) | 40031273396 |
| TestRefreshWorkflowTasks / entity not exists returns nil | refresh_workflow_tasks_entity_not_exists_returns_ok | ignored: needs typed wire detail; `detailed_status` is pub(crate) | 40031273396 |
| TestRefreshWorkflowTasks / internal service error | refresh_workflow_tasks_internal_status_is_propagated | pass (partial: untyped INTERNAL, not typed InternalServiceError) | 40031273396 |
| TestRefreshWorkflowTasks / generic error propagated | refresh_workflow_tasks_generic_error_is_propagated | pass | 40031273396 |
| TestRefreshWorkflowTasks / workflow execution already completed | refresh_workflow_tasks_already_completed_is_propagated | ignored: needs typed wire detail | 40031273396 |
| TestRefreshWorkflowTasks / context canceled | refresh_workflow_tasks_canceled_context_is_propagated | ignored: Rust takes a deadline, not a context | 40031273396 |
| TestRefreshWorkflowTasks / empty domain id | refresh_workflow_tasks_empty_domain_id_is_forwarded | pass | 40031273396 |
| TestRefreshWorkflowTasks / nil workflow execution | refresh_workflow_tasks_nil_execution_is_forwarded | ignored: AllocatedTask cannot represent nil | 40031273396 |
| TestSetupExecutorWithEmptyConfig | setup_executor_with_empty_config_installs_noop_executor | ignored: no setupExecutor/executor in Rust | 40031273396 |
| TestShardDistributorOnboarded / offboarded at zero | shard_distributor_onboarded_zero_excludes_every_task_list | pass (via task_list_excluded_from_shard_distributor) | 40031273396 |
| TestShardDistributorOnboarded / onboarded above zero (1) | shard_distributor_onboarded_partial_percentage_is_engine_wide | ignored: Rust gating is per task list, no engine-wide predicate | 40031273396 |
| TestShardDistributorOnboarded / onboarded at full | shard_distributor_onboarded_full_excludes_no_task_list | pass | 40031273396 |

## Notes for the next worker

- Proto enum values used: `TaskListKind` STICKY=2, EPHEMERAL=3; `TaskListType`
  DECISION=1, ACTIVITY=2. Go's activity cases send 2, not 1.
- `refresh_task_list_partition_config` checks the domain before the kind and
  name checks, so every case needs the domain present, as the tests do.
- Domain IDs for partition config must be UUIDs (the handler parses them).
  Run IDs must be 16-byte UUIDs (`AllocatedTask` persists them as bytes).
- Ignored gaps that would close with a seam: a failure-injection hook on the
  durable partition refresh; a public history error-detail builder for tests
  (today `grpc::detailed_status` is `pub(crate)`); and a public engine-wide
  onboarded predicate.
