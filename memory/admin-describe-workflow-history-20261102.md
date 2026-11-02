# Admin DescribeWorkflowExecution History proxy fix — 2026-11-02

## Source audit

- `go/service/frontend/admin/handler.go:252-296`: validates nil request and execution, computes Go workflow shard, resolves History owner, resolves domain UUID, invokes History `DescribeMutableState` with `{domain_id, workflow_execution}`, then returns shard ID as decimal text, History host address and both mutable-state strings.
- `rust/crates/cadence-service-core/src/admin.rs:1826` previously called `AppState::execution_store(...).describe_admin(...)` unconditionally. That was local state and not compatible with production Go History.
- `rust/crates/cadence-service-core/src/admin.rs:1439-1477` already owns the shared History transport resolver: shard-owner routing, TLS endpoint construction, and static configured-channel fallback. `history_admin_request` applies `rpc-service=cadence-history`, `rpc-caller=cadence-frontend`, `rpc-encoding=proto`, and a 30-second deadline.
- The actual Go Admin RPC is `DescribeMutableState`, confirmed in `go/proto/internal/uber/cadence/history/v1/service.proto:579-590`; it is distinct from WorkflowService `DescribeWorkflowExecution`.
- `rust/crates/cadence-frontend/src/history_client.rs` has a typed History method for `DescribeMutableState`, but Frontend Admin routing is currently hosted in service-core `AppState` handlers; the reusable shared channel/TLS/membership/request metadata helpers above are the applicable installed proxy architecture. No changes were made to scheduler-owned files (`schedule_flow.rs`, `handlers.rs`, `yarpc.rs`).

## Change

`describe_workflow_execution_admin` now uses Go's shard hash, resolves domain ID and History owner, calls generated History `DescribeMutableState` over the existing shared transport helper, and maps the response into the Admin response. The existing local execution-store path is kept only when neither a production membership resolver nor configured History channel exists. History errors keep the established Admin internal-error response contract.

## Validation

No Cargo, rustfmt, Go, runtime or mixed-cluster validation was run locally, per repository instructions. CI evidence: pending dispatch.
