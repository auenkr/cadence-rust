# QueryWorkflow parity fixes (matching-query-fixes-20261024)

Base: origin/master bafd65a77. Source of the gaps: `memory/evidence/matching-engine-tests3-20261024/notes.md`
(gaps 1 and 2). Go reference: `go/service/matching/handler/engine.go` `QueryWorkflow` (line 1014) and
`waitForQueryResult` (line 1064). Only these two behaviours changed; no other behaviour was touched.

## Fix 1: QueryWorkflow success returns the partition config

Go sets `resp.PartitionConfig = tlMgr.TaskListPartitionConfig()` on both the locally answered path and
the forwarded path. The config is read from the task list the caller asked for, not the partition the
query was forwarded to.

Rust change (`rust/crates/cadence-matching/src/grpc_matching.rs`, `query_workflow`):

- `queue.partitions.clone()` is captured before the handle is moved into the forwarding loop.
- The local answered path fills `partition_config` from that cache.
- The forwarded success path overwrites the remote response's `partition_config` with the local one,
  matching Go's overwrite. No test covers the forwarded path (not changed by the port tests).
- Conversion is `partition_config_to_proto`: `QueryWorkflowResponse.partition_config` is the `api`
  proto type, not the `matching` one.

Important: Go's real `TaskListPartitionConfig()` returns `nil` when no config is stored
(`go/service/matching/tasklist/task_list_manager.go:362`). The `version 1, read/write {0}` value in the
Go test comes from the gomock. Rust returns `None` for an unseeded queue, which already matches Go's
production behaviour. The real gap was that Rust dropped a stored config.

Test change: `query_workflow_success_reports_partition_config` seeds the cache with
`queue.partitions.push(StoredPartitionConfig { version: 1, read/write {0} })`.
Seeding through `update_task_list_partition_config` does not work, because `PartitionCache::update`
returns early for a single-partition config when nothing is cached (`matching_partition.rs`).

## Fix 2: unknown completed type is an internal error

Go `waitForQueryResult` switches on the completed type: answered returns the payload, failed returns
`QueryFailedError`, and default returns `InternalServiceError "unknown query completed type"`.

Rust change (`rust/crates/cadence-matching/src/matching_query.rs`, `query_outcome`): the failed check
is now explicit (`QueryResultType::Failed`, typed `QueryFailedError` detail as before). Any other value,
including the proto default 0, returns `Status::internal("unknown query completed type")`. The
strong-consistency version gate still runs first, as in Go.

Un-ignored tests:

- `query_workflow_success_reports_partition_config`
- `wait_for_query_result_unknown_query_result_is_internal_error`

## CI

Branch `matching-query-fixes-20261024`, workflow `rust-ci.yml`, `package=cadence-matching`.

| Run | Head | Result | Cause |
| --- | --- | --- | --- |
| https://github.com/auenkr/cadence-rust/actions/runs/40032886320 | 6f7c9259b | FAIL (clippy, E0308) | `QueryWorkflowResponse.partition_config` is the `api` type. Fixed in f80492ff1. |
| https://github.com/auenkr/cadence-rust/actions/runs/40033053538 | f80492ff1 | FAIL (test) | `query_workflow_success_reports_partition_config` `unwrap` on `None`: no stored config. |
| https://github.com/auenkr/cadence-rust/actions/runs/40033460139 | 448c44863 | FAIL (test) | Same panic: the `update_task_list_partition_config` seed was a no-op for a single-partition config. |
| https://github.com/auenkr/cadence-rust/actions/runs/40033795108 | 35f096625 | PASS (clippy exit 0, test) | `matching_engine_go_tests_b`: 16 passed, 0 failed, 4 ignored. Both target tests `ok`. |

Final run: https://github.com/auenkr/cadence-rust/actions/runs/40033795108

Runs 40033459189 and 40033053670 are automatic `push` runs. They were not used as evidence.

## Remaining ignored tests in matching_engine_go_tests_b (unchanged)

- `query_workflow_failed_to_dispatch_query_task_is_error`: no seam to inject a dispatch failure.
- `update_task_list_partition_config_tasklist_manager_error`: no seam to make a durable update fail.
- `is_shutting_down_is_false_until_stop`: no public shutting-down state.
- `get_tasklists_not_owned_returns_only_non_owned_managers`: `unload_non_owned` returns `()`.

## Not verified

- No test covers the forwarded success path, where the local config overwrites the remote one.
- Go's thrift `QueryWorkflowResponse` partitionConfig is not checked; this branch changes only the gRPC
  handler.
