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
- Reset `prepare()` clears insert_workflow_requests unconditionally; not re-verified against Go's Reset request recording under EnableStrongIdempotency.
- EventType Display text in the reset error message not compared with Go String().

## Runs
See bottom (filled after CI).

Code head SHA: cb78803f94d3b83f1786a116b6abf99c890c3993
- rust-ci package=cadence-history: success https://github.com/auenkr/cadence-rust/actions/runs/43559355790
- history-mixed-gate mysql/grpc two_history=true: success, GATE SUMMARY failed: [] https://github.com/auenkr/cadence-rust/actions/runs/43559368401
- history-mixed-gate cassandra/tchannel two_history=true: success, GATE SUMMARY failed: [] https://github.com/auenkr/cadence-rust/actions/runs/43559371751
