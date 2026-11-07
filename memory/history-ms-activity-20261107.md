# history-ms-activity (2026-11-07)
Port of go/service/history/execution/mutable_state_builder_methods_activity.go as
`rust/crates/cadence-history/src/execution/mutable_state_methods_activity.rs` (impl TypedMutableState).

Ported: GetActivityInfo, GetPendingActivityInfos, GetActivityByActivityID, UpdateActivityProgress,
ReplicateActivityInfo, UpdateActivity (update_activity_checked), DeleteActivity (delete_activity_tolerant),
AddActivityTaskScheduledEvent/Replicate, addTransientActivityStartedEvent, AddActivityTaskStartedEvent/Replicate,
Add/Replicate Completed, Failed, TimedOut, CancelRequested, Canceled, AddRequestCancelActivityTaskFailedEvent, RetryActivity.

Gaps (not wired in TypedMutableState / ActivityInfo): GetActivityScheduledEvent (events cache),
pending-activity count limits + local dispatch + GenerateActivityTransferTasks/RetryTasks, domain-cache lookup
(caller passes target_domain_id), logging/data-inconsistency, ActivityInfo lacks last_failure_category,
task_list_kind, timer_task_status, scheduled_event; FailureOptions.failure_category emitted as None.
Clock/jitter passed by caller.
