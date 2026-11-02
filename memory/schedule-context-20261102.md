# Frontend schedule request context — 2026-11-02

## Source audit

- Rust `schedule_flow.rs::api_call` serialized the workflow RPC request then called `WorkflowApi::call` with `CallerContext::default()`. This discarded all inbound headers and deadline for Create, Describe, List, Signal, Delete signal, and Delete fallback Terminate.
- Rust HTTP schedule handlers in `handlers.rs` used the production `workflow_api` branch when configured, but accepted no `HeaderMap`, so they could not propagate request metadata.
- Go `go/service/frontend/api/handler_schedule.go` passes the incoming `ctx` into StartWorkflowExecution, SignalWorkflowExecution, QueryWorkflow, list operations, and Delete's TerminateWorkflowExecution fallback. The signal helper and delete fallback retain that context; no internal background context replacement is intended.
- YARPC's general WorkflowService dispatcher already builds a CallerContext from incoming `Context-TTL-MS` and `Rpc-Header-*` headers. Schedule RPC handler names are separately dispatched through the YARPC schedule handlers; those currently use the local execution store. The change here is scoped to the HTTP production schedule-to-WorkflowApi path described by `handlers.rs`.

## Change

Commit `2353717e8062367cb39ce58350fb93d32f403b2b` changes schedule API functions to accept and clone the caller context for every backend operation. HTTP schedule handlers now pass caller headers (including unprefixed headers and decoded `Rpc-Header-*`) and a bounded `Context-TTL-MS` deadline. The local engine path remains as it was.

## Validation status

- `git diff --check` passed before commit.
- Required CI was dispatched: https://github.com/auenkr/cadence-rust/actions/runs/41429636203
- Run head: `2353717e8062367cb39ce58350fb93d32f403b2b`; status was `queued` at handoff, conclusion pending.
- A hook-level test that executes real schedule operations while asserting preserved metadata/deadline and error behavior is still required. CI completion alone does not cover that request; add this test, rerun this workflow, and update this note with final conclusion before treating the task complete.
