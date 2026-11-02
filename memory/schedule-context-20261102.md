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

## Follow-up implementation and CI handoff (2026-11-02)

- Integrated `origin/t3/schedule-context` (implementation `2353717e8062367cb39ce58350fb93d32f403b2b` and evidence commit `9db05037b`) into branch `t3/schedule-context-tests`.
- Corrected HTTP schedule TTL parsing: preserve the parsed remaining caller budget exactly (including sub-100ms and >300s values); malformed or missing values retain the 60s default. YARPC's existing transport parser still clamps its own TTL to 100–300000ms; the schedule HTTP path deliberately does not silently extend/cap the caller budget.
- Header collisions are deterministic: plain application headers are loaded first, then `Rpc-Header-*` values override the matching normalized key, matching the explicit application-header encoding.
- Added a schedule context parser test for short/long TTLs and prefixed collision precedence. Added a production `WorkflowApi` test driving scheduler Create, Describe, List, Signal, and Delete's signal-to-Terminate fallback; it checks operation sequence, unchanged absolute deadline and metadata on every backend call, plus normalized signal error propagation.
- Scope gap: YARPC schedule procedure handlers remain on their existing local execution-store path and are not covered by this HTTP WorkflowApi context slice. No mixed Go/Rust cluster gate is claimed.
- Source/tests commit: `3e93b82b1921a9be0ec7f9423d33a248d8b4e06f`.
- CI handoff: https://github.com/auenkr/cadence-rust/actions/runs/41430225291, dispatched for package `cadence-service-core` with `format_all=true` (format patch may be uploaded as an artifact). At dispatch the run was active; conclusion and formatting artifact application remain pending. Do not treat as validated until the artifact is inspected/applied if present and a final CI run passes on the resulting exact head.

## Parent review correction

Reviewed on `frontend-schedule-context-reviewed`. Fixed the short-TTL assertion to bracket the actual context capture between before/after instants. Changed the delete-signal mock from EntityNotExists (which intentionally returns idempotent success without termination) to BadRequest, so the expected TerminateWorkflowExecution fallback is actually exercised. Code head `66fd2b282`; replacement CI with formatting requested: https://github.com/auenkr/cadence-rust/actions/runs/41430427854. Conclusion pending. No local Cargo/Go/formatting commands executed.
