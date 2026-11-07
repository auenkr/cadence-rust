# history-ms-childsignal (2026-11-07)

Ports Go `mutable_state_builder_methods_child_workflow.go` and `mutable_state_builder_methods_signal.go` as `impl TypedMutableState` in
`rust/crates/cadence-history/src/execution/mutable_state_methods_child_workflow.rs` / `mutable_state_methods_signal.rs` (registered in `execution/mod.rs`).

## Ported
Child workflow: GetChildExecutionInfo, GetPendingChildExecutionInfos, DeletePendingChildExecution, Add/Replicate StartChildWorkflowExecutionInitiated,
ChildWorkflowExecutionStarted, StartChildWorkflowExecutionFailed, ChildWorkflowExecution{Completed,Failed,Canceled,Terminated,TimedOut}.
Signal: IsSignalRequested, GetSignalInfo, GetPendingSignalExternalInfos, Add/Replicate WorkflowExecutionSignaled, ExternalWorkflowExecutionSignaled,
SignalExternalWorkflowExecutionFailed, SignalExternalWorkflowExecutionInitiated, DeletePendingSignal.
(AddSignalRequested/DeleteSignalRequested already exist in mutable_state_builder.rs.)

## Gaps (not wired, same as sibling method files)
- Events cache: GetChildExecutionInitiatedEvent, PutEvent.
- Domain cache: child domain ID/name passed in by caller (`child_domain_id`, `child_domain_name`, `domain_name`).
- Task generation: GenerateChildWorkflowTasks, GenerateSignalExternalTasks.
- insertWorkflowRequest in ReplicateWorkflowExecutionSignaled (only signal_count++).
- Logging / data-inconsistency reporting for missing entries in Delete*; the History builder fallback (empty domain -> parent domain) is done inline.
- Child-terminated attributes arg is unused (Go also ignores details for the event).

## Evidence
CI rust-ci (package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/42337484577 — success on code head (see branch history-ms-childsignal).
