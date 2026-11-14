# History start/signal-with-start/reset parity re-read (2026-11-14, branch history-startreset)

## Compared line by line (Go -> Rust)
- engineimpl/start_workflow_execution.go -> engine/start_workflow_execution.rs (ID reuse policy, terminateAndStart, delayed start/cron, first decision, SignalWithStart, orphan branch cleanup): no divergence.
- engineimpl/signal_workflow_execution.go -> engine/signal_workflow_execution.rs: request-id dedup, signal limit, child-only parent check match.
- engineimpl/reset_workflow_execution.go -> engine/reset_workflow_execution.rs: match.
- reset/resetter.go -> wiring/assembly/engine_resetter.rs + reset/{mod,history}.rs: flow matches (terminate current, replay, fork, close pending decision, failInflightActivity, reapply, persistToDB).
- Strong-idempotency empty data (P2-5): Go WorkflowRequest has no data field; the Rust `data` field is unused. Request-id dedup matches Go. No change needed.

## Fixes (each with a unit test)
1. `HistoryError::DuplicateRequest` Display now `Request has already been applied to runID: {run_id}` (Go DuplicateRequestError.Error()). Test: `duplicate_request_message_matches_go` (engine/deps.rs).
2. Reset `failInflightActivity` passed empty heartbeat details; Go passes `ai.Details`. Now `ai.details`. Test: `pending_activity_infos_carry_heartbeat_details` (engine_resetter.rs).

## Remaining / not verified
- (Resolved in `history-resetverify`) Reset request parity: Go `insertWorkflowRequest` in `mutable_state_decision_task_manager.go` stores a reset row only when `EnableStrongIdempotency(domain)` is enabled and the request ID is nonempty. The row carries request ID, event version, and `WorkflowRequestTypeReset`; `execution/context.go` passes snapshot/mutation workflow requests to persistence under `CreateWorkflowRequestModeNew`. Rust now gates reset insertion using the same domain config and keeps the row through `prepare()` instead of unconditionally clearing it. Unit test: `reset_workflow_request_matches_go_strong_idempotency_gate`.
- (Resolved in `history-resetverify`) `types.EventType.String()` and Rust `EventType` display use the same CamelCase name table (including `DecisionTaskCompleted`). Added a reset validation test asserting the complete Go-compatible error text for `ActivityTaskStarted`: `invalid_reset_event_error_uses_go_event_type_string`.
- Required CI validation is complete; results are recorded below.

## Follow-up from mixed gate
- First mixed-gate attempt exposed Rust's generic encoded-data precondition on reset `WorkflowRequest` rows. Go persistence stores reset rows using request ID/version/type only, matching Go's `WorkflowRequest` shape. Rust now exempts reset rows from the extra `DataBlob` validation while retaining the check for other request types. Unit test: `reset_workflow_request_needs_no_encoded_data_like_go_persistence`.
- First mixed-gate run: failure; `GATE SUMMARY failed: ['sdk-api-reset-workflow-execution', 'sdk-driver-exit', 'sdk-scen-autoreset-new-run-started', 'sdk-scen-driver-exit']`. Reset API reported `workflow request <id> has no encoded deduplication data`; autoreset then timed out waiting for the new run. This identified the Rust-only precondition fixed above. https://github.com/auenkr/cadence-rust/actions/runs/43561699801

## `history-resetverify` validation
- Code head validated: `63e9559391a0859dd9329528bfb6ba04279f3b94`.
- `rust-ci.yml`, package `cadence-history`: success — https://github.com/auenkr/cadence-rust/actions/runs/43563447415
- `history-mixed-gate.yml`, MySQL/gRPC, `two_history=true`: success; log contains `GATE SUMMARY failed: []` — https://github.com/auenkr/cadence-rust/actions/runs/43563940541
- Mixed-gate Rust binary SHA256: cadence-server `37aff8a3b2ec676c0a1394b6f42070e29de7b41a150ac874485bbedd0350775a`; cadence-history `575948d5aff41a7da2e16533c9fb3bde2c3a9913f0a7d6677499ed9478af96a6`.
- Evidence artifact recorded source head `63e9559391a0859dd9329528bfb6ba04279f3b94`; the subsequent commit only updates this memory record.

## Runs
See bottom (filled after CI).

Code head SHA: cb78803f94d3b83f1786a116b6abf99c890c3993
- rust-ci package=cadence-history: success https://github.com/auenkr/cadence-rust/actions/runs/43559355790
- history-mixed-gate mysql/grpc two_history=true: success, GATE SUMMARY failed: [] https://github.com/auenkr/cadence-rust/actions/runs/43559368401
- history-mixed-gate cassandra/tchannel two_history=true: success, GATE SUMMARY failed: [] https://github.com/auenkr/cadence-rust/actions/runs/43559371751
