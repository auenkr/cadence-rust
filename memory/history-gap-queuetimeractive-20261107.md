# History gap: TimerActiveDependencies adapter (branch history-gap-queuetimeractive, 2026-11-07)

New file `rust/crates/cadence-history/src/wiring/assembly/queue_deps_timer_active.rs`: `TimerActiveDepsImpl` (+ private `EngineTimerWorkflow`).

## Real
- Config accessors (resurrection delay, activity schedule-to-start retry max, decision retry attempts/enforce, delete-history-event timeout, advanced visibility flag derived from `write_visibility_store_name`).
- `add_activity_task` delegates to `TransferDepsImpl` (matching gRPC).
- `load_workflow` via shard `EngineExecutionCache` (EntityNotExists -> None).
- TimerWorkflow: release, running, domain_name, workflow_type, activity_info, decision_info, resurrected_activities, delete_activity, sticky, decision attempt/schedule id, has_processed_or_pending_decision, schedule_decision, update_workflow_execution_as_active, start_version, next_event_id, cancel_requested.

## TODO(link) (errors or empty), blocked on EngineMutableState lacking timer/timeout support
- user_timers/activity_timers (empty, infallible in trait: DO NOT wire into launcher until real), user_timer_info (None), resurrected_timers, delete_user_timer, add_timer_fired_event, update_activity, retry_activity, add_activity_task_timed_out_event, add_decision_task_timed_out_event, add_decision_task_schedule_to_start_timeout_event, terminate_decision_attempts_exceeded, add_upsert_workflow_search_attributes_task, retry_backoff_duration (None), cron_backoff_duration, timeout_workflow, retry_workflow, update_workflow_execution_with_new_as_active.
- Approximations: TimerActivityInfo.schedule_to_start_timeout/timer_task_status = 0, activity_scheduled_domain = own domain, partition_config empty.
- Not unit tested; CI = clippy + existing tests.

## Evidence
(filled after CI)
