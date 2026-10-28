# Matching native History control calls (UNVERIFIED, no CI run yet)

Raw-byte (GoString) equivalents of legacy `mutable_state`, `describe_execution`,
`refresh_workflow_tasks` on `MatchingHistoryClient`, for gRPC and TChannel.
Task-start native calls untouched.

## Files
- `rust/crates/cadence-proto/build/native_model.rs`: roots added: history
  `GetMutableStateRequest`, `DescribeWorkflowExecutionRequest/Response`,
  `RefreshWorkflowTasksRequest/Response`; `native::admin` re-export added.
- `matching_history_native.rs` (gRPC + shared retry/redirect/metrics):
  - `mutable_state_native[_with_deadline](&GoString domain_id, &n::api::WorkflowExecution)`
  - `describe_execution_native[_with_deadline](&GoString id, &GoString name, &AllocatedTaskBytes)`
  - `refresh_workflow_tasks_native[_with_deadline|_until](&GoString, &AllocatedTaskBytes)`;
    missing-execution maps to Ok(()) like legacy.
  - Same metric names (GetMutableState, DescribeWorkflowExecution, RefreshWorkflowTasks),
    `self.retry` (resource retry), workflow id bytes for routing.
- `matching_history_tchannel_native.rs`: `*_request_native` encoders,
  `get_mutable_state_response_native`, `describe_workflow_execution_response_native`,
  `TChannelHistoryClient::{get_mutable_state,describe_workflow_execution,refresh_workflow_tasks}_native`.
  Field ids/enum offsets/units/optional presence copied from the legacy codec
  (`matching_history_tchannel.rs`); strings via `GoString`, no UTF-8 check.

## Integration
Callers needing raw bytes switch to the `*_native` methods; no lib/launcher edits made.

## Risks / not validated
- Nothing compiled. Likely compile risks: generated native field names/types
  (`n::admin` path, `n::shared::VersionHistories`, map key `GoString` Hash),
  `pub use admin::v1 as admin` ordering in generated file.
- No scripted-peer/gRPC test or real Go oracle run for these calls; only codec unit tests.
- Unsupported History event types are not involved (control responses carry none).

## Parent integration checkpoint

The parent extended the existing real-Go TChannel peer oracle to all five native History methods: mutable state, describe, refresh, decision start and activity start. The peer asserts invalid bytes in request IDs/domain names, and Rust asserts raw response workflow/task-list/type/client-version fields; the fixture now requires five calls. The describe assertion uses the actual proto `type` field (`r#type`). These changes remain unexecuted pending the next same-pin dependency-oracle workflow. Native gRPC control peer coverage remains outstanding.
