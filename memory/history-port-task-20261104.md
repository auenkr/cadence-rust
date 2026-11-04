# History task/ and taskdlq/ port checklist (branch history-port-task, 2026-11-04)

Rust dir: `rust/crates/cadence-history/src/`. Go dir: `go/service/history/`. Verification of non-active files: Go func names were
snake_cased and grepped in the Rust crate (script in session, no misses except the idiomatic cases listed below).

| Go file | Rust file | Status |
|---|---|---|
| task/constants.go | task/constants.rs | ported |
| task/event_logger.go | task/event_logger.rs | ported |
| task/executor_wrapper.go | task/executor_wrapper.rs | ported |
| task/interface.go | task/interface.rs | ported |
| task/priority_assigner.go | task/priority_assigner.rs | ported |
| task/processor.go | task/processor.rs | ported |
| task/rate_limited_processor.go | task/rate_limited_processor.rs | ported |
| task/redispatcher.go | task/redispatcher.rs | ported (getOrCreatePQLocked/sizeLocked/compareLess via BinaryHeap/Ord idioms) |
| task/rescheduler.go | task/rescheduler.rs | ported (same idiom note) |
| task/standby_task_util.go | task/standby_task_util.rs | ported (standbyTaskPostActionWriteToDLQ = standby_task_post_action_write_to_dlq) |
| task/task.go | task/history_task.rs | ported (getDomainTagByID covered by get_or_create_domain_tagged_scope) |
| task/task_rate_limiter.go | task/task_rate_limiter.rs | ported |
| task/task_util.go | task/task_util.rs | ported (NewMockTaskMatcher is a gomock test helper, not ported) |
| task/timer_active_task_executor.go | task/timer_active_task_executor.rs | ported |
| task/timer_standby_task_executor.go | task/timer_standby_task_executor.rs | ported |
| task/timer_task_executor_base.go | task/timer_task_executor_base.rs | ported |
| task/transfer_active_task_executor.go | task/transfer_active_task_executor.rs | ported in this task (all 34 funcs, see below) |
| task/transfer_standby_task_executor.go | task/transfer_standby_task_executor.rs | ported |
| task/transfer_task_executor_base.go | task/transfer_task_executor_base.rs | ported |
| taskdlq/constants.go | taskdlq/constants.rs | ported |
| taskdlq/host_limiter.go | taskdlq/host_limiter.rs | ported |
| taskdlq/interface.go | taskdlq/interface.rs | ported |
| taskdlq/processor.go | taskdlq/processor.rs | ported |

## transfer_active_task_executor.rs notes
- Ported: Execute, processActivityTask, processDecisionTask, processCloseExecution/RecordWorkflowClosed/RecordChildExecutionCompleted (+ helper),
  processCancelExecution, processSignalExecution, processStartChildExecution, processRecordWorkflowStarted/UpsertWorkflowSearchAttributes (+ helper),
  processResetWorkflow, record/complete/fail helpers, updateWorkflowExecution, the *WithRetry funcs, resetWorkflow, processParentClosePolicy,
  applyParentClosePolicy, filterPendingChildExecutions, getWorkflowExecutionStatus, error vars, resetWorkflowTimeout, allowTask.
- External pieces behind dependency traits (`TODO(link)`): ActiveMutableState, ActiveTransferContext, TransferActiveDependencies.
- Reset workflow is dispatched outside the 3s task timeout; its prepare phase has a 3s timeout and the reset a 30s timeout (matches Go).
- Shared fixes made to get the crate compiling: Logger debug_tags/warn_tags, HistoryTaskKey/Category helpers, PersistenceTask::clone_arc,
  ShardContext/Service accessors, ThrottleRetry made Send (rng not held across await), clippy fixes.

## CI
Last green run: see bottom (filled in after final run).

Green CI (rust-ci, clippy + tests): https://github.com/auenkr/cadence-rust/actions/runs/41905302914
Head SHA: c9ddd83a47b1b977299bd4334ab04c20dd90d464 (conclusion: success)
