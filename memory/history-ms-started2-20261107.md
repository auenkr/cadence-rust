# history-ms-started2 (2026-11-07)

Branch `history-ms-started2`. CI (rust-ci, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/42339101454 - success on code head 243de3397d268da96459dc223efb05287fa32d6e.

## Ported (mutable_state_methods_started.rs)
add_workflow_execution_started_event, add_workflow_execution_started_event_for_continue_as_new, replicate_workflow_execution_started_event (now with StartedEnv: events fields first_execution_run_id fallback, task_list_kind, cron_overlap_policy, last_first_event_id, insert workflow request, auto-reset-points rollover, expiration only when !=0), build_started_event_attributes (Go HistoryBuilder.AddWorkflowExecutionStartedEvent), rollover_auto_reset_points_with_expiring_time. StartedEnv carries now/domain id+name/retention days/decoded auto_reset_points.
Signature of replicate changed (takes &mut StartedEnv); no other callers (workflow_started.rs/state_builder.rs use a different method).

## Gaps
- SetHistoryTree, writeEventToCache, GenerateWorkflowStartTasks/RecordWorkflowStarted/DelayedDecisionTasks not wired; ACSP blob not stored; insertWorkflowRequest keeps only request id; logging not wired.
- Continue-as-new first decision schedule goes through add_first_decision_task_scheduled (returns false when backoff != 0, caller must generate delayed task).
- No unit tests for the started methods themselves.

## Tests added (mutable_state_methods_decision.rs mod decision_tests)
schedule happy/double-schedule error, start happy/unknown id, complete happy/wrong started id, timeout, fail, schedule-to-start timeout.
