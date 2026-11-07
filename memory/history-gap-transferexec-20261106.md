# History gap audit: transfer executors (2026-11-06)

Branch: history-gap-transferexec. Go: go/service/history/task. Rust: rust/crates/cadence-history/src/task.

## Function status

| Go file / function | Status | Note |
|---|---|---|
| transfer_active_task_executor.go: Execute dispatch | identical | reset workflow outside default timeout |
| processActivityTask / processDecisionTask | identical | |
| processCloseExecution, processRecordWorkflowClosed, processRecordChildExecutionCompleted | identical | |
| processCancelExecution / processSignalExecution | identical | |
| processStartChildExecution | identical | |
| processRecordWorkflowStarted / UpsertHelper | identical | |
| processResetWorkflow / resetWorkflow | identical | |
| recordChildExecutionStarted | differs (fixed) | Go uses task.TargetWorkflowID for the child execution; Rust used attributes workflow id. Header argument still not forwarded (trait signature, open gap) |
| recordStartChildExecutionFailed, createFirstDecisionTask | identical | |
| requestCancel/signal External completed/failed | identical | |
| updateWorkflowExecution | identical | |
| *WithRetry helpers, parent close policy, filterPendingChildExecutions, getWorkflowExecutionStatus | identical | |
| transfer_standby_task_executor.go: getCurrentTime | differs (fixed) | now sync, uses cluster name |
| processTransfer | differs (fixed) | clock evaluated before mutable state load; release(nil) on Redispatch |
| standby discard on resend path | differs (fixed) | uses standbyTaskPostActionWriteToDLQ |
| transfer_task_executor_base.go: push (not-should-push path) | differs (fixed) | Go fatal checks added; NoOp before discard delay, else write-to-DLQ by HistoryTaskDLQMode; TransferDependencies exposes dlq_writer and history_task_dlq_mode |
| standby_task_util.go | identical | NoOp message uses literal "Some(info)" instead of Go %T (cosmetic) |
| task_util.go (verifyTaskVersion, loadMutableState, timeoutWorkflow, retryWorkflow, getWorkflowExecution, shouldPushToMatching, getClusterAttributesForTask) | identical | audited 2026-11-07; Rust WorkflowIdentifier also carries domain_id (superset) |
| fetcher, redispatcher, processor, priority | not yet audited | outside transfer executor scope |

## Evidence
- 2026-11-07: merged origin/master; added `target_workflow_id` and `task_list` to `StartChildExecutionTask` in `task/deps.rs` (Go `persistence.StartChildExecutionTask` parity; the field was missing, so run 42234619633 failed to compile).
- CI (rust-ci, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/42337445980 head a9e41851b conclusion success.
- Re-checked the standby function list against Go: no new gaps. Still open: header argument not forwarded in recordChildExecutionStarted; task_util/fetcher/redispatcher/processor/priority not audited.

## 2026-11-07 (branch history-gap-transferexec2)
- recordChildExecutionStarted header: closed. Rust passes the full StartChildAttributes (incl. header) to add_child_workflow_execution_started_event, so Go's initiatedAttributes.Header is available to the implementation.
- task_util.go audited line by line: no gaps, no code changes.
