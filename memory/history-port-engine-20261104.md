# history-port-engine checklist (2026-11-05)

Branch: history-port-engine (merged origin/master, includes decision/events/query/reset/workflowcache/execution)
Last green CI: https://github.com/auenkr/cadence-rust/actions/runs/41897923483 (package=cadence-history, conclusion success)
Head SHA: see `git log` on branch (fix commit "Fix HistoryEvent field name in reset" + this checklist)

Status: PARTIAL. Merge conflicts resolved, compile error fixed (HistoryEvent.event_id), CI green.
NOT done: grpc/thrift wrappers (only ratelimited exists), workflow/ (context/util), start_workflow_execution (1050 lines),
handler.go (2289 lines; only interface.rs ported), history_engine.go, describe_workflow_execution, register_domain_failover_callback,
reapply_events, notify_tasks, get_replication_messages, terminate/request_cancel, respond_activity_task_* (partly in activity_responses.rs).

| Go file | Rust | state |
|---|---|---|
| engineimpl/describe_mutable_state.go | - | todo/partial (check engine/*.rs) |
| engineimpl/describe_queues.go | - | todo/partial (check engine/*.rs) |
| engineimpl/describe_workflow_execution.go | - | todo/partial (check engine/*.rs) |
| engineimpl/dlq_operations.go | engine/dlq_operations.rs | ported |
| engineimpl/get_replication_messages.go | - | todo/partial (check engine/*.rs) |
| engineimpl/history_engine.go | - | todo/partial (check engine/*.rs) |
| engineimpl/notify_tasks.go | - | todo/partial (check engine/*.rs) |
| engineimpl/poll_mutable_state.go | engine/poll_mutable_state.rs | ported |
| engineimpl/query_workflow.go | engine/query_workflow.rs | ported |
| engineimpl/reapply_events.go | - | todo/partial (check engine/*.rs) |
| engineimpl/record_activity_task_started.go | engine/record_activity_task_started.rs | ported |
| engineimpl/record_child_execution_completed.go | engine/record_child_execution_completed.rs | ported |
| engineimpl/record_decision_task_started.go | - | todo/partial (check engine/*.rs) |
| engineimpl/refresh_workflow_tasks.go | - | todo/partial (check engine/*.rs) |
| engineimpl/register_domain_failover_callback.go | - | todo/partial (check engine/*.rs) |
| engineimpl/remove_signal_mutable_state.go | - | todo/partial (check engine/*.rs) |
| engineimpl/request_cancel_workflow_execution.go | - | todo/partial (check engine/*.rs) |
| engineimpl/reset_queues.go | - | todo/partial (check engine/*.rs) |
| engineimpl/reset_sticky_tasklist.go | - | todo/partial (check engine/*.rs) |
| engineimpl/reset_workflow_execution.go | engine/reset_workflow_execution.rs | ported |
| engineimpl/respond_activity_task_canceled.go | - | todo/partial (check engine/*.rs) |
| engineimpl/respond_activity_task_completed.go | - | todo/partial (check engine/*.rs) |
| engineimpl/respond_activity_task_failed.go | - | todo/partial (check engine/*.rs) |
| engineimpl/respond_activity_task_heartbeat.go | - | todo/partial (check engine/*.rs) |
| engineimpl/respond_decision_task_completed.go | - | todo/partial (check engine/*.rs) |
| engineimpl/respond_decision_task_failed.go | - | todo/partial (check engine/*.rs) |
| engineimpl/signal_workflow_execution.go | engine/signal_workflow_execution.rs | ported |
| engineimpl/start_workflow_execution.go | - | todo/partial (check engine/*.rs) |
| engineimpl/terminate_workflow_execution.go | - | todo/partial (check engine/*.rs) |

## Continuation checkpoint (2026-11-05)

Fetched origin and inspected existing worktree `/mnt/storage/service/wt-port-engine`;
branch already contains newer integration commits than the previous handoff summary.
Baseline CI run 41897923483 succeeded. No Cargo or Go validation ran locally.

Added `engine/notify_tasks.rs`: NotifyNewHistoryEvent, NotifyNewTransferTasks,
NotifyNewTimerTasks, including empty batches, first-task failover version resolution,
lookup failure logging and missing processor behavior. NotifyNewReplicationTasks and
hydrateReplicationTask remain unported. This is a checkpoint, not a parity claim.
