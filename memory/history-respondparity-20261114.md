# history-respondparity (2026-11-14), branch `history-respondparity`

Static line-level comparison (no local cargo/go/docker). PARTIAL scope, see "Not covered".

| Go file | Rust file | Result |
|---|---|---|
| engineimpl/respond_activity_task_completed.go (L42-125) | engine/activity_responses.rs | MATCH (token err, stale L79, not-found L91, internal msg L107) |
| respond_activity_task_failed.go / canceled.go / heartbeat.go | engine/activity_responses.rs | MATCH (same helper `activity()`); failed/canceled/heartbeat bodies not re-read beyond shared helper |
| record_activity_task_started.go (L38-170) | engine/record_activity_task_started.rs | MATCH (resurrection L73-99, stale L104, dup L118, already started L148) |
| signal_workflow_execution.go (L36-130) | engine/signal_workflow_execution.rs | MATCH (deprecated domain, dedupe before running check, corruption, signal limit, parent mismatch) |
| request_cancel_workflow_execution.go, terminate_workflow_execution.go, reset_sticky_tasklist.go, remove_signal_mutable_state.go | engine/workflow_mutations.rs | MATCH (errors.go L35-51 messages verified) |
| decision/handler.go HandleDecisionTaskStarted (L157-251) | wiring/assembly/engine_decision.rs | FIXED: not-running returned "Workflow execution already completed." -> Go ErrNotExists "workflow execution already completed" (L186) |
| decision/handler.go HandleDecisionTaskCompleted (L293-345) | engine_decision.rs `completed_run` | FIXED: not-running returned EntityNotExists; Go returns WorkflowExecutionAlreadyCompletedError (L345, errors.go L39); token error message now "error deserializing task token" (errors.go L43) |
| decision/handler.go HandleDecisionTaskFailed (L253-291) | engine_decision.rs | FIXED: already-completed type/message (L278), token error message |
| decision/handler.go HandleDecisionTaskScheduled (L100-155) | engine_decision.rs | MATCH |

Unit test: `respond_parity_tests::go_error_messages_and_types` in engine_decision.rs.

## Not covered (still need line-level re-read)
reset_workflow_execution.go, start_workflow_execution.go / signal-with-start, decision/checker.go (blob/history size limits), decision/task_handler.go, remainder of HandleDecisionTaskCompleted (L345-948: sticky, query, size/count limits).

## CI
See bottom (filled after runs).

Head SHA tested: 8ee911975882f4412fcffcedb10c4adef78b4d7d (code unchanged since; memory-only commit follows)
- rust-ci package=cadence-history: success https://github.com/auenkr/cadence-rust/actions/runs/43506131715
- history-mixed-gate two_history=true, all success AND `GATE SUMMARY failed: []`, run order mysql/grpc, mysql/tchannel, postgres/grpc, postgres/tchannel, cassandra/grpc, cassandra/tchannel:
  43507148597, 43507154108, 43507159837, 43507164987, 43507170076, 43507175899 (https://github.com/auenkr/cadence-rust/actions/runs/<id>)
