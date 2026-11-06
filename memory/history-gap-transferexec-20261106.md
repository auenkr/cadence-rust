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
| task_util.go, fetcher, redispatcher, processor, priority | not yet audited | see remaining gaps |

## Evidence
CI run: see below once recorded.
