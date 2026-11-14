//! In-memory history event builder from `history_builder.go`.

use cadence_api::service_types::{HistoryEvent, WorkflowExecutionCancelRequestedEventAttributes, WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes, WorkflowExecutionTerminatedEventAttributes};

#[derive(Debug, Clone, PartialEq)]
pub struct HistoryBuilder {
    next_event_id: i64,
    version: i64,
    timestamp_unix_nanos: i64,
    events: Vec<HistoryEvent>,
    transient_history: Vec<HistoryEvent>,
}

impl HistoryBuilder {
    pub fn new(next_event_id: i64, version: i64, timestamp_unix_nanos: i64) -> Self {
        Self { next_event_id, version, timestamp_unix_nanos, events: Vec::new(), transient_history: Vec::new() }
    }
    pub fn next_event_id(&self) -> i64 { self.next_event_id }
    pub fn set_time_and_version(&mut self, timestamp_unix_nanos: i64, version: i64) { self.timestamp_unix_nanos = timestamp_unix_nanos; self.version = version; }
    pub fn add_event(&mut self, mut event: HistoryEvent) -> HistoryEvent {
        event.event_id = self.next_event_id;
        event.timestamp.get_or_insert(self.timestamp_unix_nanos);
        event.version = self.version;
        self.next_event_id += 1;
        self.events.push(event.clone());
        event
    }
    pub fn events(&self) -> &[HistoryEvent] { &self.events }
    pub fn take_events(&mut self) -> Vec<HistoryEvent> { std::mem::take(&mut self.events) }
}


/// These helpers deliberately leave version at its zero default, matching Go's
/// createNewHistoryEvent helper used to reconstruct transient decision events.
pub fn new_decision_task_scheduled_event_with_info(event_id: i64, timestamp: i64, task_list: String, timeout: i32, attempt: i64) -> HistoryEvent {
    HistoryEvent { event_id, timestamp: Some(timestamp), event_type: Some(cadence_api::enums::EventType::DecisionTaskScheduled), decision_task_scheduled_event_attributes: Some(cadence_api::tasks::DecisionTaskScheduledEventAttributes { task_list: Some(cadence_api::types::TaskList { name: task_list, ..Default::default() }), start_to_close_timeout_seconds: Some(timeout), attempt }), ..Default::default() }
}
pub fn new_decision_task_started_event_with_info(event_id: i64, timestamp: i64, scheduled_event_id: i64, request_id: String, identity: String) -> HistoryEvent {
    HistoryEvent { event_id, timestamp: Some(timestamp), event_type: Some(cadence_api::enums::EventType::DecisionTaskStarted), decision_task_started_event_attributes: Some(cadence_api::tasks::DecisionTaskStartedEventAttributes { scheduled_event_id, request_id, identity }), ..Default::default() }
}
impl HistoryBuilder {
    pub fn from_events(events: Vec<HistoryEvent>, next_event_id: i64, version: i64, timestamp: i64) -> Self {
        Self { events, next_event_id, version, timestamp_unix_nanos: timestamp, transient_history: Vec::new() }
    }
    pub fn transient_history(&self) -> &[HistoryEvent] { &self.transient_history }
    pub fn take_transient_history(&mut self) -> Vec<HistoryEvent> { std::mem::take(&mut self.transient_history) }
    pub fn add_transient_event(&mut self, mut event: HistoryEvent) -> HistoryEvent {
        event.event_id = self.next_event_id; event.version = self.version;
        event.timestamp.get_or_insert(self.timestamp_unix_nanos); self.next_event_id += 1;
        self.transient_history.push(event.clone()); event
    }
    pub fn add_transient_decision_task_scheduled_event(&mut self, task_list: String, timeout: i32, attempt: i64, timestamp: i64) -> HistoryEvent {
        let event = new_decision_task_scheduled_event_with_info(0, timestamp, task_list, timeout, attempt); self.add_transient_event(event)
    }
    pub fn add_transient_decision_task_started_event(&mut self, scheduled: i64, request_id: String, identity: String, timestamp: i64) -> HistoryEvent {
        let event = new_decision_task_started_event_with_info(0, timestamp, scheduled, request_id, identity); self.add_transient_event(event)
    }
}

// Typed event constructors retain the complete attribute payload supplied by
// mutable-state transitions, including optional legacy fields.
impl HistoryBuilder {
    pub fn add_workflow_execution_started_event(&mut self, attributes: WorkflowExecutionStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionStarted), workflow_execution_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_signaled_event(&mut self, attributes: WorkflowExecutionSignaledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionSignaled), workflow_execution_signaled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_terminated_event(&mut self, attributes: WorkflowExecutionTerminatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionTerminated), workflow_execution_terminated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_cancel_requested_event(&mut self, attributes: WorkflowExecutionCancelRequestedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionCancelRequested), workflow_execution_cancel_requested_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_completed_event(&mut self, attributes: cadence_api::tasks::WorkflowExecutionCompletedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionCompleted), workflow_execution_completed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_failed_event(&mut self, attributes: cadence_api::tasks::WorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionFailed), workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_canceled_event(&mut self, attributes: cadence_api::tasks::WorkflowExecutionCanceledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionCanceled), workflow_execution_canceled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_scheduled_event(&mut self, attributes: cadence_api::tasks::DecisionTaskScheduledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskScheduled), decision_task_scheduled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_started_event(&mut self, attributes: cadence_api::tasks::DecisionTaskStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskStarted), decision_task_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_completed_event(&mut self, attributes: cadence_api::tasks::DecisionTaskCompletedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskCompleted), decision_task_completed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_failed_event(&mut self, attributes: cadence_api::tasks::DecisionTaskFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskFailed), decision_task_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_scheduled_event(&mut self, attributes: cadence_api::tasks::ActivityTaskScheduledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskScheduled), activity_task_scheduled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_started_event(&mut self, attributes: cadence_api::tasks::ActivityTaskStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskStarted), activity_task_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_completed_event(&mut self, attributes: cadence_api::tasks::ActivityTaskCompletedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskCompleted), activity_task_completed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_failed_event(&mut self, attributes: cadence_api::tasks::ActivityTaskFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskFailed), activity_task_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_cancel_requested_event(&mut self, attributes: cadence_api::tasks::ActivityTaskCancelRequestedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskCancelRequested), activity_task_cancel_requested_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_canceled_event(&mut self, attributes: cadence_api::tasks::ActivityTaskCanceledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskCanceled), activity_task_canceled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_timer_started_event(&mut self, attributes: cadence_api::tasks::TimerStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::TimerStarted), timer_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_timer_canceled_event(&mut self, attributes: cadence_api::tasks::TimerCanceledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::TimerCanceled), timer_canceled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_cancel_timer_failed_event(&mut self, attributes: cadence_api::tasks::CancelTimerFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::CancelTimerFailed), cancel_timer_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_marker_recorded_event(&mut self, attributes: cadence_api::tasks::MarkerRecordedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::MarkerRecorded), marker_recorded_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_continued_as_new_event(&mut self, attributes: cadence_api::tasks::WorkflowExecutionContinuedAsNewEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionContinuedAsNew), workflow_execution_continued_as_new_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_start_child_workflow_execution_initiated_event(&mut self, attributes: cadence_api::tasks::StartChildWorkflowExecutionInitiatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::StartChildWorkflowExecutionInitiated), start_child_workflow_execution_initiated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_start_child_workflow_execution_failed_event(&mut self, attributes: cadence_api::tasks::StartChildWorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::StartChildWorkflowExecutionFailed), start_child_workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_started_event(&mut self, attributes: cadence_api::tasks::ChildWorkflowExecutionStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionStarted), child_workflow_execution_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_completed_event(&mut self, attributes: cadence_api::tasks::ChildWorkflowExecutionCompletedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionCompleted), child_workflow_execution_completed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_failed_event(&mut self, attributes: cadence_api::tasks::ChildWorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionFailed), child_workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_canceled_event(&mut self, attributes: cadence_api::tasks::ChildWorkflowExecutionCanceledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionCanceled), child_workflow_execution_canceled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_terminated_event(&mut self, attributes: cadence_api::tasks::ChildWorkflowExecutionTerminatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionTerminated), child_workflow_execution_terminated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_signal_external_workflow_execution_initiated_event(&mut self, attributes: cadence_api::tasks::SignalExternalWorkflowExecutionInitiatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::SignalExternalWorkflowExecutionInitiated), signal_external_workflow_execution_initiated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_signal_external_workflow_execution_failed_event(&mut self, attributes: cadence_api::tasks::SignalExternalWorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::SignalExternalWorkflowExecutionFailed), signal_external_workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_request_cancel_external_workflow_execution_initiated_event(&mut self, attributes: cadence_api::tasks::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::RequestCancelExternalWorkflowExecutionInitiated), request_cancel_external_workflow_execution_initiated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_request_cancel_external_workflow_execution_failed_event(&mut self, attributes: cadence_api::tasks::RequestCancelExternalWorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::RequestCancelExternalWorkflowExecutionFailed), request_cancel_external_workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_upsert_workflow_search_attributes_event(&mut self, attributes: cadence_api::tasks::UpsertWorkflowSearchAttributesEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::UpsertWorkflowSearchAttributes), upsert_workflow_search_attributes_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_timer_fired_event(&mut self, attributes: cadence_api::tasks::TimerFiredEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::TimerFired), timer_fired_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_external_workflow_execution_signaled_event(&mut self, attributes: cadence_api::tasks::ExternalWorkflowExecutionSignaledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ExternalWorkflowExecutionSignaled), external_workflow_execution_signaled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_external_workflow_execution_cancel_requested_event(&mut self, attributes: cadence_api::tasks::ExternalWorkflowExecutionCancelRequestedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ExternalWorkflowExecutionCancelRequested), external_workflow_execution_cancel_requested_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_timed_out_event(&mut self, attributes: cadence_api::tasks::WorkflowExecutionTimedOutEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionTimedOut), workflow_execution_timed_out_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_timed_out_event(&mut self, attributes: cadence_api::tasks::DecisionTaskTimedOutEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskTimedOut), decision_task_timed_out_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_timed_out_event(&mut self, attributes: cadence_api::tasks::ActivityTaskTimedOutEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskTimedOut), activity_task_timed_out_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_timed_out_event(&mut self, attributes: cadence_api::tasks::ChildWorkflowExecutionTimedOutEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionTimedOut), child_workflow_execution_timed_out_event_attributes: Some(attributes), ..Default::default() })
    }
}

// Go-signature builders (history_builder.go): each takes the Go parameters and
// fills the attributes exactly as the Go constructor does.
use cadence_api::{enums::{DecisionTaskTimedOutCause, TimeoutType}, tasks as t, types::{FailureOptions, TaskList}};

impl HistoryBuilder {
    /// Go `AddDecisionTaskScheduledEvent`.
    pub fn add_decision_task_scheduled(&mut self, task_list: &str, start_to_close_timeout_seconds: i32, attempt: i64) -> HistoryEvent {
        self.add_decision_task_scheduled_event(t::DecisionTaskScheduledEventAttributes {
            task_list: Some(TaskList { name: task_list.to_string(), ..Default::default() }),
            start_to_close_timeout_seconds: Some(start_to_close_timeout_seconds),
            attempt,
        })
    }
    /// Go `AddDecisionTaskStartedEvent`.
    pub fn add_decision_task_started(&mut self, schedule_event_id: i64, request_id: &str, identity: &str) -> HistoryEvent {
        self.add_decision_task_started_event(t::DecisionTaskStartedEventAttributes { scheduled_event_id: schedule_event_id, identity: identity.to_string(), request_id: request_id.to_string() })
    }
    /// Go `AddDecisionTaskCompletedEvent`.
    pub fn add_decision_task_completed(&mut self, schedule_event_id: i64, started_event_id: i64, request: &t::RespondDecisionTaskCompletedRequest) -> HistoryEvent {
        self.add_decision_task_completed_event(t::DecisionTaskCompletedEventAttributes {
            execution_context: request.execution_context.clone(),
            scheduled_event_id: schedule_event_id,
            started_event_id,
            identity: request.identity.clone(),
            binary_checksum: request.binary_checksum.clone(),
        })
    }
    /// Go `AddDecisionTaskTimedOutEvent`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_decision_task_timed_out(&mut self, schedule_event_id: i64, started_event_id: i64, timeout_type: TimeoutType, base_run_id: &str, new_run_id: &str, fork_event_version: i64, reason: &str, cause: DecisionTaskTimedOutCause, reset_request_id: &str) -> HistoryEvent {
        self.add_decision_task_timed_out_event(t::DecisionTaskTimedOutEventAttributes {
            scheduled_event_id: schedule_event_id,
            started_event_id,
            timeout_type: Some(timeout_type),
            base_run_id: base_run_id.to_string(),
            new_run_id: new_run_id.to_string(),
            fork_event_version,
            reason: reason.to_string(),
            cause: Some(cause),
            request_id: reset_request_id.to_string(),
        })
    }
    /// Go `AddDecisionTaskFailedEvent`.
    pub fn add_decision_task_failed(&mut self, attr: t::DecisionTaskFailedEventAttributes) -> HistoryEvent { self.add_decision_task_failed_event(attr) }
    /// Go `AddActivityTaskCompletedEvent`.
    pub fn add_activity_task_completed(&mut self, schedule_event_id: i64, started_event_id: i64, request: &t::RespondActivityTaskCompletedRequest) -> HistoryEvent {
        self.add_activity_task_completed_event(t::ActivityTaskCompletedEventAttributes { result: request.result.clone(), scheduled_event_id: schedule_event_id, started_event_id, identity: request.identity.clone() })
    }
    /// Go `AddActivityTaskFailedEvent`; Reason defaults to an empty string pointer.
    pub fn add_activity_task_failed(&mut self, schedule_event_id: i64, started_event_id: i64, request: &t::RespondActivityTaskFailedRequest) -> HistoryEvent {
        self.add_activity_task_failed_event(t::ActivityTaskFailedEventAttributes {
            reason: Some(request.reason.clone().unwrap_or_default()),
            details: request.details.clone(),
            scheduled_event_id: schedule_event_id,
            started_event_id,
            identity: request.identity.clone(),
            failure_options: None::<FailureOptions>,
        })
    }
    /// Go `AddCompletedWorkflowEvent`.
    pub fn add_completed_workflow(&mut self, decision_completed_event_id: i64, attributes: &t::CompleteWorkflowExecutionDecisionAttributes) -> HistoryEvent {
        self.add_workflow_execution_completed_event(t::WorkflowExecutionCompletedEventAttributes { result: attributes.result.clone(), decision_task_completed_event_id: decision_completed_event_id })
    }
    /// Go `AddFailWorkflowEvent`.
    pub fn add_fail_workflow(&mut self, decision_completed_event_id: i64, attributes: &t::FailWorkflowExecutionDecisionAttributes) -> HistoryEvent {
        self.add_workflow_execution_failed_event(t::WorkflowExecutionFailedEventAttributes { reason: Some(attributes.reason.clone().unwrap_or_default()), details: attributes.details.clone(), decision_task_completed_event_id: decision_completed_event_id })
    }
    /// Go `AddTimeoutWorkflowEvent`.
    pub fn add_timeout_workflow(&mut self) -> HistoryEvent {
        self.add_workflow_execution_timed_out_event(t::WorkflowExecutionTimedOutEventAttributes { timeout_type: Some(TimeoutType::StartToClose) })
    }
    /// Go `AddWorkflowExecutionTerminatedEvent`.
    pub fn add_workflow_execution_terminated(&mut self, reason: &str, details: Vec<u8>, identity: &str) -> HistoryEvent {
        self.add_workflow_execution_terminated_event(WorkflowExecutionTerminatedEventAttributes { reason: reason.to_string(), details, identity: identity.to_string() })
    }
    /// Go `AddTimerStartedEvent`.
    pub fn add_timer_started(&mut self, decision_completed_event_id: i64, request: &t::StartTimerDecisionAttributes) -> HistoryEvent {
        self.add_timer_started_event(t::TimerStartedEventAttributes { timer_id: request.timer_id.clone(), start_to_fire_timeout_seconds: request.start_to_fire_timeout_seconds, decision_task_completed_event_id: decision_completed_event_id })
    }
    /// Go `GetHistory`.
    pub fn get_history(&self) -> Vec<HistoryEvent> { self.events.clone() }
}

// Remaining Go-signature builders from history_builder.go.
use cadence_api::{enums::{CancelExternalWorkflowExecutionFailedCause, ChildWorkflowExecutionFailedCause, ContinueAsNewInitiator, ParentClosePolicy, SignalExternalWorkflowExecutionFailedCause}, types::{Header, WorkflowExecution, WorkflowType}};
use crate::engine::types::{HistoryRequestCancelWorkflowExecutionRequest, HistoryStartWorkflowExecutionRequest};

fn we(workflow_id: &str, run_id: &str) -> Option<WorkflowExecution> { Some(WorkflowExecution::new(workflow_id, run_id)) }

impl HistoryBuilder {
    /// Go `AddWorkflowExecutionStartedEvent`.
    pub fn add_workflow_execution_started(&mut self, start_request: &HistoryStartWorkflowExecutionRequest, previous_run_id: Option<&str>, previous_auto_reset_points: Option<cadence_api::types::ResetPoints>, first_run_id: &str, original_run_id: &str, first_scheduled_time_nano: i64) -> HistoryEvent {
        let request = start_request.start_request.clone().unwrap_or_default();
        let scheduled = if request.cron_schedule.is_empty() { None } else { Some(first_scheduled_time_nano) };
        // Go carries PrevAutoResetPoints from the previous execution's AutoResetPoints.
        let mut a = WorkflowExecutionStartedEventAttributes {
            prev_auto_reset_points: previous_auto_reset_points,
            partition_config: if start_request.partition_config.is_empty() { None } else { Some(start_request.partition_config.clone()) },
            active_cluster_selection_policy: request.active_cluster_selection_policy.clone(),
            workflow_type: request.workflow_type.clone(), task_list: request.task_list.clone(), header: request.header.clone(), input: request.input.clone(),
            execution_start_to_close_timeout_seconds: request.execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: request.task_start_to_close_timeout_seconds,
            continued_execution_run_id: previous_run_id.unwrap_or_default().to_string(),
            identity: request.identity.clone(), retry_policy: request.retry_policy.clone(), attempt: start_request.attempt,
            expiration_timestamp: start_request.expiration_timestamp, cron_schedule: request.cron_schedule.clone(),
            cron_overlap_policy: request.cron_overlap_policy, last_completion_result: start_request.last_completion_result.clone(),
            continued_failure_reason: start_request.continued_failure_reason.clone().unwrap_or_default(),
            continued_failure_details: start_request.continued_failure_details.clone(), initiator: start_request.continue_as_new_initiator,
            first_decision_task_backoff_seconds: start_request.first_decision_task_backoff_seconds,
            first_execution_run_id: first_run_id.to_string(), first_scheduled_time_nano: scheduled,
            original_execution_run_id: original_run_id.to_string(), memo: request.memo.clone(), search_attributes: request.search_attributes.clone(),
            jitter_start_seconds: request.jitter_start_seconds, request_id: request.request_id.clone(), ..Default::default()
        };
        if let Some(p) = &start_request.parent_execution_info {
            a.parent_workflow_domain_id = Some(p.domain_uuid.clone());
            a.parent_workflow_domain = Some(p.domain.clone());
            a.parent_workflow_execution = p.execution.clone();
            a.parent_initiated_event_id = Some(p.initiated_id);
        }
        self.add_workflow_execution_started_event(a)
    }
    /// Go `AddActivityTaskScheduledEvent`.
    pub fn add_activity_task_scheduled(&mut self, decision_completed_event_id: i64, a: &t::ScheduleActivityTaskDecisionAttributes) -> HistoryEvent {
        let d = |v: Option<i32>| Some(v.unwrap_or_default());
        self.add_activity_task_scheduled_event(t::ActivityTaskScheduledEventAttributes {
            activity_id: a.activity_id.clone(), activity_type: a.activity_type.clone(),
            domain: if a.domain.is_empty() { None } else { Some(a.domain.clone()) },
            task_list: a.task_list.clone(), header: a.header.clone(), input: a.input.clone(),
            schedule_to_close_timeout_seconds: d(a.schedule_to_close_timeout_seconds), schedule_to_start_timeout_seconds: d(a.schedule_to_start_timeout_seconds),
            start_to_close_timeout_seconds: d(a.start_to_close_timeout_seconds), heartbeat_timeout_seconds: d(a.heartbeat_timeout_seconds),
            decision_task_completed_event_id: decision_completed_event_id, retry_policy: a.retry_policy.clone(),
        })
    }
    /// Go `AddActivityTaskStartedEvent`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_activity_task_started(&mut self, schedule_event_id: i64, attempt: i32, request_id: &str, identity: &str, last_failure_reason: &str, last_failure_details: Vec<u8>, last_failure_options: Option<FailureOptions>) -> HistoryEvent {
        self.add_activity_task_started_event(t::ActivityTaskStartedEventAttributes { scheduled_event_id: schedule_event_id, attempt, identity: identity.to_string(), request_id: request_id.to_string(), last_failure_reason: Some(last_failure_reason.to_string()), last_failure_details, last_failure_options })
    }
    /// Go `AddActivityTaskTimedOutEvent`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_activity_task_timed_out(&mut self, schedule_event_id: i64, started_event_id: i64, timeout_type: TimeoutType, last_heartbeat_details: Vec<u8>, last_failure_reason: &str, last_failure_details: Vec<u8>, last_failure_options: Option<FailureOptions>) -> HistoryEvent {
        self.add_activity_task_timed_out_event(t::ActivityTaskTimedOutEventAttributes { scheduled_event_id: schedule_event_id, started_event_id, timeout_type: Some(timeout_type), details: last_heartbeat_details, last_failure_reason: Some(last_failure_reason.to_string()), last_failure_details, last_failure_options })
    }
    /// Go `AddContinuedAsNewEvent`.
    pub fn add_continued_as_new(&mut self, decision_completed_event_id: i64, new_run_id: &str, a: &t::ContinueAsNewWorkflowExecutionDecisionAttributes) -> HistoryEvent {
        let initiator = a.initiator.unwrap_or(ContinueAsNewInitiator::Decider);
        self.add_workflow_execution_continued_as_new_event(t::WorkflowExecutionContinuedAsNewEventAttributes {
            new_execution_run_id: new_run_id.to_string(), workflow_type: a.workflow_type.clone(), task_list: a.task_list.clone(), header: a.header.clone(), input: a.input.clone(),
            execution_start_to_close_timeout_seconds: a.execution_start_to_close_timeout_seconds, task_start_to_close_timeout_seconds: a.task_start_to_close_timeout_seconds,
            decision_task_completed_event_id: decision_completed_event_id, backoff_start_interval_in_seconds: Some(a.backoff_start_interval_in_seconds.unwrap_or_default()),
            initiator: Some(initiator), failure_reason: a.failure_reason.clone(), failure_details: a.failure_details.clone(), last_completion_result: a.last_completion_result.clone(),
            memo: a.memo.clone(), search_attributes: a.search_attributes.clone(), cron_overlap_policy: a.cron_overlap_policy,
            jitter_start_seconds: a.jitter_start_seconds, active_cluster_selection_policy: a.active_cluster_selection_policy.clone(),
        })
    }
    /// Go `AddTimerFiredEvent`.
    pub fn add_timer_fired(&mut self, started_event_id: i64, timer_id: &str) -> HistoryEvent {
        self.add_timer_fired_event(t::TimerFiredEventAttributes { timer_id: timer_id.to_string(), started_event_id })
    }
    /// Go `AddActivityTaskCancelRequestedEvent`.
    pub fn add_activity_task_cancel_requested(&mut self, decision_completed_event_id: i64, activity_id: &str) -> HistoryEvent {
        self.add_activity_task_cancel_requested_event(t::ActivityTaskCancelRequestedEventAttributes { activity_id: activity_id.to_string(), decision_task_completed_event_id: decision_completed_event_id })
    }
    /// Go `AddActivityTaskCanceledEvent`.
    pub fn add_activity_task_canceled(&mut self, schedule_event_id: i64, started_event_id: i64, latest_cancel_requested_event_id: i64, details: Vec<u8>, identity: &str) -> HistoryEvent {
        self.add_activity_task_canceled_event(t::ActivityTaskCanceledEventAttributes { scheduled_event_id: schedule_event_id, started_event_id, latest_cancel_requested_event_id, details, identity: identity.to_string() })
    }
    /// Go `AddTimerCanceledEvent`.
    pub fn add_timer_canceled(&mut self, started_event_id: i64, decision_task_completed_event_id: i64, timer_id: &str, identity: &str) -> HistoryEvent {
        self.add_timer_canceled_event(t::TimerCanceledEventAttributes { started_event_id, decision_task_completed_event_id, timer_id: timer_id.to_string(), identity: identity.to_string() })
    }
    /// Go `AddCancelTimerFailedEvent`.
    pub fn add_cancel_timer_failed(&mut self, timer_id: &str, decision_task_completed_event_id: i64, cause: &str, identity: &str) -> HistoryEvent {
        self.add_cancel_timer_failed_event(t::CancelTimerFailedEventAttributes { timer_id: timer_id.to_string(), decision_task_completed_event_id, cause: cause.to_string(), identity: identity.to_string() })
    }
    /// Go `AddWorkflowExecutionCancelRequestedEvent`.
    pub fn add_workflow_execution_cancel_requested(&mut self, cause: &str, request: &HistoryRequestCancelWorkflowExecutionRequest) -> HistoryEvent {
        let c = request.cancel_request.clone().unwrap_or_default();
        self.add_workflow_execution_cancel_requested_event(WorkflowExecutionCancelRequestedEventAttributes { cause: cause.to_string(), identity: c.identity, external_initiated_event_id: request.external_initiated_event_id, external_workflow_execution: request.external_workflow_execution.clone(), request_id: c.request_id })
    }
    /// Go `AddWorkflowExecutionCanceledEvent`.
    pub fn add_workflow_execution_canceled(&mut self, decision_task_completed_event_id: i64, a: &t::CancelWorkflowExecutionDecisionAttributes) -> HistoryEvent {
        self.add_workflow_execution_canceled_event(t::WorkflowExecutionCanceledEventAttributes { decision_task_completed_event_id, details: a.details.clone() })
    }
    /// Go `AddRequestCancelExternalWorkflowExecutionInitiatedEvent`.
    pub fn add_request_cancel_external_initiated(&mut self, decision_task_completed_event_id: i64, r: &t::RequestCancelExternalWorkflowExecutionDecisionAttributes) -> HistoryEvent {
        self.add_request_cancel_external_workflow_execution_initiated_event(t::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes { decision_task_completed_event_id, domain: r.domain.clone(), workflow_execution: we(&r.workflow_id, &r.run_id), control: r.control.clone(), child_workflow_only: r.child_workflow_only })
    }
    /// Go `AddRequestCancelExternalWorkflowExecutionFailedEvent`.
    pub fn add_request_cancel_external_failed(&mut self, decision_task_completed_event_id: i64, initiated_event_id: i64, domain: &str, workflow_id: &str, run_id: &str, cause: CancelExternalWorkflowExecutionFailedCause) -> HistoryEvent {
        self.add_request_cancel_external_workflow_execution_failed_event(t::RequestCancelExternalWorkflowExecutionFailedEventAttributes { decision_task_completed_event_id, initiated_event_id, domain: domain.to_string(), workflow_execution: we(workflow_id, run_id), cause: Some(cause), control: Vec::new() })
    }
    /// Go `AddExternalWorkflowExecutionCancelRequested`.
    pub fn add_external_workflow_execution_cancel_requested(&mut self, initiated_event_id: i64, domain: &str, workflow_id: &str, run_id: &str) -> HistoryEvent {
        self.add_external_workflow_execution_cancel_requested_event(t::ExternalWorkflowExecutionCancelRequestedEventAttributes { initiated_event_id, domain: domain.to_string(), workflow_execution: we(workflow_id, run_id) })
    }
    /// Go `AddSignalExternalWorkflowExecutionInitiatedEvent`.
    pub fn add_signal_external_initiated(&mut self, decision_task_completed_event_id: i64, a: &t::SignalExternalWorkflowExecutionDecisionAttributes) -> HistoryEvent {
        let e = a.execution.clone().unwrap_or_default();
        self.add_signal_external_workflow_execution_initiated_event(t::SignalExternalWorkflowExecutionInitiatedEventAttributes { decision_task_completed_event_id, domain: a.domain.clone(), workflow_execution: we(&e.workflow_id, &e.run_id), signal_name: a.signal_name.clone(), input: a.input.clone(), control: a.control.clone(), child_workflow_only: a.child_workflow_only })
    }
    /// Go `AddSignalExternalWorkflowExecutionFailedEvent`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_signal_external_failed(&mut self, decision_task_completed_event_id: i64, initiated_event_id: i64, domain: &str, workflow_id: &str, run_id: &str, control: Vec<u8>, cause: SignalExternalWorkflowExecutionFailedCause) -> HistoryEvent {
        self.add_signal_external_workflow_execution_failed_event(t::SignalExternalWorkflowExecutionFailedEventAttributes { decision_task_completed_event_id, initiated_event_id, domain: domain.to_string(), workflow_execution: we(workflow_id, run_id), cause: Some(cause), control })
    }
    /// Go `AddExternalWorkflowExecutionSignaled`.
    pub fn add_external_workflow_execution_signaled(&mut self, initiated_event_id: i64, domain: &str, workflow_id: &str, run_id: &str, control: Vec<u8>) -> HistoryEvent {
        self.add_external_workflow_execution_signaled_event(t::ExternalWorkflowExecutionSignaledEventAttributes { initiated_event_id, domain: domain.to_string(), workflow_execution: we(workflow_id, run_id), control })
    }
    /// Go `AddUpsertWorkflowSearchAttributesEvent`.
    pub fn add_upsert_workflow_search_attributes(&mut self, decision_task_completed_event_id: i64, a: &t::UpsertWorkflowSearchAttributesDecisionAttributes) -> HistoryEvent {
        self.add_upsert_workflow_search_attributes_event(t::UpsertWorkflowSearchAttributesEventAttributes { decision_task_completed_event_id, search_attributes: a.search_attributes.clone() })
    }
    /// Go `AddMarkerRecordedEvent`.
    pub fn add_marker_recorded(&mut self, decision_completed_event_id: i64, a: &t::RecordMarkerDecisionAttributes) -> HistoryEvent {
        self.add_marker_recorded_event(t::MarkerRecordedEventAttributes { marker_name: a.marker_name.clone(), details: a.details.clone(), decision_task_completed_event_id: decision_completed_event_id, header: a.header.clone() })
    }
    /// Go `AddWorkflowExecutionSignaledEvent`.
    pub fn add_workflow_execution_signaled(&mut self, signal_name: &str, input: Vec<u8>, identity: &str, request_id: &str) -> HistoryEvent {
        self.add_workflow_execution_signaled_event(WorkflowExecutionSignaledEventAttributes { signal_name: signal_name.to_string(), input, identity: identity.to_string(), request_id: request_id.to_string() })
    }
    /// Go `AddStartChildWorkflowExecutionInitiatedEvent`.
    pub fn add_start_child_initiated(&mut self, decision_completed_event_id: i64, a: &t::StartChildWorkflowExecutionDecisionAttributes, target_domain_name: &str) -> HistoryEvent {
        let domain = if a.domain.is_empty() { target_domain_name.to_string() } else { a.domain.clone() };
        self.add_start_child_workflow_execution_initiated_event(t::StartChildWorkflowExecutionInitiatedEventAttributes {
            domain, workflow_id: a.workflow_id.clone(), workflow_type: a.workflow_type.clone(), task_list: a.task_list.clone(), header: a.header.clone(), input: a.input.clone(),
            execution_start_to_close_timeout_seconds: a.execution_start_to_close_timeout_seconds, task_start_to_close_timeout_seconds: a.task_start_to_close_timeout_seconds,
            control: a.control.clone(), decision_task_completed_event_id: decision_completed_event_id, workflow_id_reuse_policy: a.workflow_id_reuse_policy,
            retry_policy: a.retry_policy.clone(), cron_schedule: a.cron_schedule.clone(), memo: a.memo.clone(), search_attributes: a.search_attributes.clone(),
            parent_close_policy: Some(a.parent_close_policy.unwrap_or(ParentClosePolicy::Abandon)), cron_overlap_policy: a.cron_overlap_policy,
            active_cluster_selection_policy: a.active_cluster_selection_policy.clone(), ..Default::default()
        })
    }
    /// Go `AddChildWorkflowExecutionStartedEvent`.
    pub fn add_child_workflow_execution_started(&mut self, domain: &str, execution: Option<WorkflowExecution>, workflow_type: Option<WorkflowType>, initiated_id: i64, header: Option<Header>) -> HistoryEvent {
        self.add_child_workflow_execution_started_event(t::ChildWorkflowExecutionStartedEventAttributes { domain: domain.to_string(), workflow_execution: execution, workflow_type, initiated_event_id: initiated_id, header })
    }
    /// Go `AddStartChildWorkflowExecutionFailedEvent`.
    pub fn add_start_child_failed(&mut self, initiated_id: i64, cause: ChildWorkflowExecutionFailedCause, i: &t::StartChildWorkflowExecutionInitiatedEventAttributes) -> HistoryEvent {
        self.add_start_child_workflow_execution_failed_event(t::StartChildWorkflowExecutionFailedEventAttributes { domain: i.domain.clone(), workflow_id: i.workflow_id.clone(), workflow_type: i.workflow_type.clone(), initiated_event_id: initiated_id, decision_task_completed_event_id: i.decision_task_completed_event_id, control: i.control.clone(), cause: Some(cause) })
    }
    /// Go `AddChildWorkflowExecutionCompletedEvent`.
    pub fn add_child_completed(&mut self, domain: &str, execution: Option<WorkflowExecution>, workflow_type: Option<WorkflowType>, initiated_id: i64, started_id: i64, c: &t::WorkflowExecutionCompletedEventAttributes) -> HistoryEvent {
        self.add_child_workflow_execution_completed_event(t::ChildWorkflowExecutionCompletedEventAttributes { domain: domain.to_string(), workflow_execution: execution, workflow_type, initiated_event_id: initiated_id, started_event_id: started_id, result: c.result.clone() })
    }
    /// Go `AddChildWorkflowExecutionFailedEvent`.
    pub fn add_child_failed(&mut self, domain: &str, execution: Option<WorkflowExecution>, workflow_type: Option<WorkflowType>, initiated_id: i64, started_id: i64, f: &t::WorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_child_workflow_execution_failed_event(t::ChildWorkflowExecutionFailedEventAttributes { domain: domain.to_string(), workflow_execution: execution, workflow_type, initiated_event_id: initiated_id, started_event_id: started_id, reason: Some(f.reason.clone().unwrap_or_default()), details: f.details.clone() })
    }
    /// Go `AddChildWorkflowExecutionCanceledEvent`.
    pub fn add_child_canceled(&mut self, domain: &str, execution: Option<WorkflowExecution>, workflow_type: Option<WorkflowType>, initiated_id: i64, started_id: i64, c: &t::WorkflowExecutionCanceledEventAttributes) -> HistoryEvent {
        self.add_child_workflow_execution_canceled_event(t::ChildWorkflowExecutionCanceledEventAttributes { domain: domain.to_string(), workflow_execution: execution, workflow_type, initiated_event_id: initiated_id, started_event_id: started_id, details: c.details.clone() })
    }
    /// Go `AddChildWorkflowExecutionTerminatedEvent`.
    pub fn add_child_terminated(&mut self, domain: &str, execution: Option<WorkflowExecution>, workflow_type: Option<WorkflowType>, initiated_id: i64, started_id: i64) -> HistoryEvent {
        self.add_child_workflow_execution_terminated_event(t::ChildWorkflowExecutionTerminatedEventAttributes { domain: domain.to_string(), workflow_execution: execution, workflow_type, initiated_event_id: initiated_id, started_event_id: started_id })
    }
    /// Go `AddChildWorkflowExecutionTimedOutEvent`.
    pub fn add_child_timed_out(&mut self, domain: &str, execution: Option<WorkflowExecution>, workflow_type: Option<WorkflowType>, initiated_id: i64, started_id: i64, timed_out: &t::WorkflowExecutionTimedOutEventAttributes) -> HistoryEvent {
        self.add_child_workflow_execution_timed_out_event(t::ChildWorkflowExecutionTimedOutEventAttributes { domain: domain.to_string(), timeout_type: timed_out.timeout_type, workflow_execution: execution, workflow_type, initiated_event_id: initiated_id, started_event_id: started_id })
    }
}

impl HistoryBuilder {
    /// Go `AddRequestCancelActivityTaskFailedEvent`.
    pub fn add_request_cancel_activity_task_failed(&mut self, decision_completed_event_id: i64, activity_id: &str, cause: &str) -> HistoryEvent {
        self.add_event(HistoryEvent {
            event_type: Some(cadence_api::enums::EventType::RequestCancelActivityTaskFailed),
            request_cancel_activity_task_failed_event_attributes: Some(cadence_api::tasks::RequestCancelActivityTaskFailedEventAttributes { activity_id: activity_id.to_string(), cause: cause.to_string(), decision_task_completed_event_id: decision_completed_event_id }),
            ..Default::default()
        })
    }
    /// Go `AddSemaphoreAcquireInitiatedEvent`.
    pub fn add_semaphore_acquire_initiated(&mut self, decision_completed_event_id: i64, semaphore_name: &str, wait_timeout_seconds: i32) -> HistoryEvent {
        self.add_event(HistoryEvent {
            event_type: Some(cadence_api::enums::EventType::SemaphoreAcquireInitiated),
            semaphore_acquire_initiated_event_attributes: Some(cadence_api::tasks::SemaphoreAcquireInitiatedEventAttributes { semaphore_name: semaphore_name.to_string(), wait_timeout_seconds: Some(wait_timeout_seconds), decision_task_completed_event_id: decision_completed_event_id }),
            ..Default::default()
        })
    }
    /// Go `AddSemaphoreAcquiredEvent`.
    pub fn add_semaphore_acquired(&mut self, initiated_event_id: i64, token_id: i32) -> HistoryEvent {
        self.add_event(HistoryEvent {
            event_type: Some(cadence_api::enums::EventType::SemaphoreAcquired),
            semaphore_acquired_event_attributes: Some(cadence_api::tasks::SemaphoreAcquiredEventAttributes { token_id, initiated_event_id }),
            ..Default::default()
        })
    }
    /// Go `AddSemaphoreReleasedEvent`.
    pub fn add_semaphore_released(&mut self, decision_completed_event_id: i64, initiated_event_id: i64, token_id: i32) -> HistoryEvent {
        self.add_event(HistoryEvent {
            event_type: Some(cadence_api::enums::EventType::SemaphoreReleased),
            semaphore_released_event_attributes: Some(cadence_api::tasks::SemaphoreReleasedEventAttributes { token_id, initiated_event_id, decision_task_completed_event_id: decision_completed_event_id }),
            ..Default::default()
        })
    }
}
