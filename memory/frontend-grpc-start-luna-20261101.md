# Frontend gRPC start routing slice (2026-11-01)

Branch: `t3/frontend-grpc-start-signal`
Base: `ace9aefd49431bdb709d66be77c036797e5200d9` (integration of `fe-workflow-service` + `fe-admin-domain`).

## Implementation

- `rust/crates/cadence-matching/src/grpc.rs::hook_operation` now maps the internal start and signal-with-start routes to `StartWorkflowExecution` and `SignalWithStartWorkflowExecution`.
- `Frontend::start_workflow_execution` and `Frontend::signal_with_start_workflow_execution` call `call_with_headers` only when the installed `AppState.workflow_api` reports that exact operation as served. This uses the existing `GRPC_CALLER` task-local to carry lower-cased caller headers and the parsed gRPC deadline into `CallerContext`; the hook's typed response supplies the gRPC `run_id`, and `call_with_headers` retains the established Cadence error-to-gRPC conversion.
- If there is no hook or it does not serve the operation, the existing local Matching domain lookup, validation, persistence behavior and typed StartError conversion remain in place. Async operations are untouched.
- Added focused tests in `grpc.rs::workflow_hook_tests` for both operation names, caller header/deadline propagation, returned run IDs, and hook error conversion.

## Source pointers examined

- Go ordering and validation: `go/service/frontend/api/handler.go::StartWorkflowExecution` (~1652) and `SignalWithStartWorkflowExecution` (~2270).
- Hook codec types: `rust/crates/cadence-service-core/src/workflow_api.rs::codec` (`StartWorkflowExecutionRequest` / `SignalWithStartWorkflowExecutionRequest`, response `StartWorkflowExecutionResponse`).
- Rust WorkflowHandler conversion and dispatch: `rust/crates/cadence-frontend/src/handler_workflow.rs` (`workflow_request`, validation/dispatch for the two operations).
- Existing gRPC hook caller context, transport error conversion and neighboring routing: `rust/crates/cadence-matching/src/grpc.rs::CallerScope`, `Frontend::call_with_headers`.

## CI evidence

Dispatched with the required command:
`gh workflow run rust-ci.yml --ref t3/frontend-grpc-start-signal -f package=cadence-matching -f format_all=true`

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41327541884
- Head: `e059623c4dae11b2f25926c01c51ce3142e8aea1`
- State at handoff: in progress; no conclusion yet. The CI run is on the dedicated feature branch and has not been canceled.
- Next action: inspect run 41327541884; if `workspace-formatting-patch` is non-empty, apply it, commit/push, and rerun the exact workflow. Once completed, append its conclusion and final head here.

No Cargo or Go validation has been run locally, per project instructions. Formatting artifacts must be inspected and applied through CI only.

## Scope/gaps

This is a bounded routing slice only. It does not complete Frontend parity. No async operation, persistence/Admin wrapper, Go service, or shared service behavior was changed.
