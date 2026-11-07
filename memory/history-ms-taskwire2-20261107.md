# history-ms-taskwire2 (2026-11-07)

Branch `history-ms-taskwire2` (from master d7dfd0519).

## Wired
- replicate_workflow_execution_started_event now takes `generate_delayed_decision_tasks` and calls GenerateWorkflowStartTasks, GenerateRecordWorkflowStartedTasks and (flag && backoff>0) GenerateDelayedDecisionTasks. AddWorkflowExecutionStartedEvent passes true; continue-as-new start passes false.
- add_first_decision_task_scheduled generates delayed decision tasks when backoff != 0 (Go decision task manager).
- New (mutable_state_methods_completed.rs): add_workflow_execution_terminated_event, replicate_workflow_execution_terminated_event, replicate_workflow_execution_continued_as_new_event -> close tasks.
- mutable_state_builder.rs: close_transaction_handle_activity_user_timer_tasks (GenerateActivityTimerTasks + GenerateUserTimerTasks if running) at start of close_transaction_as_mutation/snapshot; transfer_tasks/timer_tasks drained and cleared in cleanup_transaction.
- New generated_task_encoding.rs: GeneratedTask -> pc::TransferTask/TimerTask -> thriftrw blob -> persistence `Task` in categories (1,1) transfer and (2,2) timer, merged into TypedWorkflowMutation/Snapshot.tasks_by_category.

## Not wired
- AddContinueAsNewEvent (needs start event cache, active cluster manager) not ported; only the replicate step.
- closeTransactionHandleWorkflowReset (GenerateWorkflowResetTasks; needs domain bad-binaries + FindAutoResetPoint), FlushBufferedEvents/buffered-limit/policy check in close; Rust close has no TransactionPolicy.
- Encoded tasks have task_id 0 and visibility 0 for tasks with None visibility; the shard must assign ids/visibility later.
- activity_timer_status still discarded (ActivityInfo lacks TimerTaskStatus), so activity timer generation may emit duplicate tasks.
- Task blob fields beyond the basic mapping (e.g. transfer record_visibility, target_domain_ids) are defaults; task_list set only for decision/activity tasks and decision/activity timeouts.
- SetHistoryTree, writeEventToCache, ACSP blob, logging.

## CI
https://github.com/auenkr/cadence-rust/actions/runs/42342268187 code head 9afe99e1e7bc8d6526bd314f58589b661837337e conclusion success (-p cadence-history, clippy+tests).

Encoding notes: thrift blob needs UUIDs, so transfer tasks default target_domain_id to own domain and target_run_id to Go's dummy `30000000-0000-f000-f000-000000000002`.
