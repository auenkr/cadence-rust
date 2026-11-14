//! History service assembly: shared environment, shard factory, per-shard
//! engine factory, shard registry, queues, replication and failover.
pub mod active_cluster_manager;
pub mod archiver_client;
pub mod archiver_wiring;
pub mod visibility_wiring;
pub mod controller;
pub mod engine_cache;
pub mod engine_checksum;
pub mod engine_close;
pub mod engine_failover;
pub mod engine_continue;
pub mod engine_decision;
pub mod engine_decision_ops;
pub mod engine_events;
pub mod engine_events_transfer;
pub mod engine_factory;
pub mod engine_start;
pub mod engine_refresher;
pub mod engine_resetter;
pub mod engine_semaphore;
pub mod engine_state;
pub mod engine_tasks;
pub mod engine_timer;
pub mod env;
pub mod failover;
pub mod failover_client;
pub mod ndc_adapters;
pub mod parent_close_policy_client;
#[cfg(test)]
pub mod real_engine_harness;
#[cfg(test)]
mod real_engine_tests;
#[cfg(test)]
mod real_engine_v2_tests;
pub mod ndc_context_hooks;
pub mod ndc_rebuild;
pub mod ndc_state;
pub mod ndc_workflow;
pub mod queue_deps;
pub mod queue_deps_standby;
pub mod queue_deps_timer_active;
pub mod queue_deps_transfer_active;
pub mod queue_exec;
pub mod queue_factory;
pub mod queue_policy;
pub mod queue_provider;
pub mod queue_allocator_adapters;
pub mod queue_state_string;
pub mod queues;
pub mod replication;
pub mod replication_builders;
pub mod resender;
pub mod shard_adapters;
pub mod shard_factory;

/// Go `log/tag/values.go` workflow action tag for an engine `what` label.
/// `createInternalServerError(tag)` yields `"<tag> operation failed"`.
pub(crate) fn go_action_tag(what: &str) -> Option<&'static str> {
    let key = what.split([' ', '(']).next().unwrap_or(what);
    Some(match key {
        "DecisionTaskScheduled" => "add-decisiontask-scheduled-event",
        "DecisionTaskStarted" => "add-decisiontask-started-event",
        "DecisionTaskCompleted" => "add-decisiontask-completed-event",
        "DecisionTaskTimedOut" => "add-decisiontask-timedout-event",
        "DecisionTaskFailed" => "add-decisiontask-failed-event",
        "ActivityTaskStarted" => "add-activitytask-started-event",
        "ActivityTaskCompleted" => "add-activitytask-completed-event",
        "ActivityTaskFailed" => "add-activitytask-failed-event",
        "ActivityTaskTimedOut" => "add-activitytask-timed-event",
        "ActivityTaskCanceled" => "add-activitytask-canceled-event",
        "RetryActivity" => "add-activitytask-retry-event",
        "TimerFired" => "add-timer-fired-event",
        "ChildWorkflowExecutionCompleted" => "add-childworkflow-completed-event",
        "ChildWorkflowExecutionFailed" => "add-childworkflow-failed-event",
        "ChildWorkflowExecutionCanceled" => "add-childworkflow-canceled-event",
        "ChildWorkflowExecutionTerminated" => "add-childworkflow-terminated-event",
        "ChildWorkflowExecutionTimedOut" => "add-childworkflow-timedout-event",
        "WorkflowExecutionCompleted" => "add-workflow-completed--event",
        "WorkflowExecutionFailed" => "add-workflow-failed-event",
        "WorkflowExecutionCanceled" => "add-workflow-canceled-event",
        "WorkflowExecutionTimedOut" => "add-workflow-timeout-event",
        "WorkflowExecutionTerminated" => "add-workflow-terminated-event",
        "WorkflowExecutionContinuedAsNew" => "add-workflow-continue-as-new-event",
        "WorkflowExecutionCancelRequested" => "add-workflow-cancel-requested-event",
        _ => return None,
    })
}

/// Go `createInternalServerError(actionTag)` text for an invalid history action.
pub(crate) fn invalid_history_action_message(what: &str, next_event_id: i64) -> String {
    match go_action_tag(what) {
        Some(tag) => format!("{tag} operation failed"),
        None => format!("invalid history action {what} (next event id {next_event_id})"),
    }
}

#[cfg(test)]
mod action_tag_tests {
    use super::*;

    #[test]
    fn invalid_action_uses_go_tag_text() {
        assert_eq!(invalid_history_action_message("ActivityTaskCanceled (cancel not requested)", 5), "add-activitytask-canceled-event operation failed");
        assert_eq!(invalid_history_action_message("ActivityTaskTimedOut", 5), "add-activitytask-timed-event operation failed");
        assert_eq!(invalid_history_action_message("Other", 5), "invalid history action Other (next event id 5)");
    }
}
