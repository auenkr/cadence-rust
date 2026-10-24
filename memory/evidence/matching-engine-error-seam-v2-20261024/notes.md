# matching-engine-error-seam-v2-20261024

Goal: un-ignore three Rust matching tests that need an injectable engine failure:

- `go_handler_describe_task_list_engine_error_is_internal_service_error`
- `go_handler_list_task_list_partitions_engine_error_is_internal_service_error`
- `go_handler_update_task_list_partition_config_engine_error_is_internal_service_error`

Base: `origin/master` (a1288a8cf). Branch: `matching-engine-error-seam-v2-20261024`.
No local cargo; verification is GitHub Actions only (`rust-ci.yml`).

## Go reference

- `go/service/matching/handler/handler_test.go`
  - TestDescribeTaskList, "Error case - DescribeTaskList failed" (~line 633): mock `DescribeTaskList` returns `errors.New("describe-tasklist-error")`, expects `&types.InternalServiceError{Message: "describe-tasklist-error"}`.
  - TestListTaskListPartitions, "Error case - ListTaskListPartitions failed" (~line 690): `list-tasklist-partitions-error`.
  - TestUpdateTaskListPartitionConfig, "Error case - UpdateTaskListPartitionConfig failed" (~line 889): `update-tasklist-error`.
- `go/service/matching/handler/context.go:83` `handleErr`: typed errors pass through; the default case (line ~136) returns `&types.InternalServiceError{Message: err.Error()}`.
- `go/service/matching/handler/handler.go` DescribeTaskList / ListTaskListPartitions / UpdateTaskListPartitionConfig: rate-limit check, then engine call, then `hCtx.handleErr(err)`.

Wire expectation: `tonic::Code::Internal` with message equal to the engine error text.

## Rust seam

Existing `matching_fault_injection.rs` is random production-config fault injection (persistence/client error rates). It cannot force a deterministic engine error for one RPC, so it was not reused.

Added in `rust/crates/cadence-matching/src/grpc_matching.rs`:

- `Matching.engine_fault: Arc<std::sync::Mutex<Option<(&'static str, String)>>>`, behind `#[cfg(test)]`. `Arc` is required because `Matching` derives `Clone`.
- Set in `Matching::new` under `#[cfg(test)]` (`Default::default()`). `new` is the only struct literal for `Matching`.
- `injected_engine_fault(&self, rpc)`, `#[cfg(test)]`: if the stored RPC name matches, returns `Status::internal(message)`; otherwise `Ok(())`.
- One `#[cfg(test)]` call per handler, placed where the engine work begins, after the handler's own request validation and domain resolution:
  - DescribeTaskList: after `payload.domain = self.domain_name(..)?`, before the queue lookup.
  - ListTaskListPartitions: after the task-list name parse, before partition metadata.
  - UpdateTaskListPartitionConfig: after the root-partition check, before the registry update.

Production builds do not compile the field, the helper, or the call sites. Behaviour is unchanged when `cfg(test)` is off.

## Tests

- The three target tests are un-ignored. Each sets `engine_fault` for its RPC, calls a valid request, and asserts `Code::Internal` plus the Go message.
- `go_handler_refresh_partition_config_engine_error_is_internal_service_error` stays `#[ignore]`. It was not in scope and has no seam call yet.

## Status

- [x] Go reference read
- [x] Seam implemented (cfg(test)-only)
- [x] Three tests un-ignored
- [ ] CI: `rust-ci.yml` package=cadence-matching
- [ ] CI: whole workspace on final head
- Not merged.

## CI runs

(filled in below as they complete)
