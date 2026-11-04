//! In-memory history event builder from `history_builder.go`.

use cadence_api::service_types::HistoryEvent;

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
    pub fn add_workflow_execution_completed_event(&mut self, attributes: crate::tasks::WorkflowExecutionCompletedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionCompleted), workflow_execution_completed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_failed_event(&mut self, attributes: crate::tasks::WorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionFailed), workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_canceled_event(&mut self, attributes: crate::tasks::WorkflowExecutionCanceledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionCanceled), workflow_execution_canceled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_scheduled_event(&mut self, attributes: crate::tasks::DecisionTaskScheduledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskScheduled), decision_task_scheduled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_started_event(&mut self, attributes: crate::tasks::DecisionTaskStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskStarted), decision_task_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_completed_event(&mut self, attributes: crate::tasks::DecisionTaskCompletedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskCompleted), decision_task_completed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_failed_event(&mut self, attributes: crate::tasks::DecisionTaskFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskFailed), decision_task_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_scheduled_event(&mut self, attributes: crate::tasks::ActivityTaskScheduledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskScheduled), activity_task_scheduled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_started_event(&mut self, attributes: crate::tasks::ActivityTaskStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskStarted), activity_task_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_completed_event(&mut self, attributes: crate::tasks::ActivityTaskCompletedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskCompleted), activity_task_completed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_failed_event(&mut self, attributes: crate::tasks::ActivityTaskFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskFailed), activity_task_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_cancel_requested_event(&mut self, attributes: crate::tasks::ActivityTaskCancelRequestedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskCancelRequested), activity_task_cancel_requested_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_canceled_event(&mut self, attributes: crate::tasks::ActivityTaskCanceledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskCanceled), activity_task_canceled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_timer_started_event(&mut self, attributes: crate::tasks::TimerStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::TimerStarted), timer_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_timer_canceled_event(&mut self, attributes: crate::tasks::TimerCanceledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::TimerCanceled), timer_canceled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_cancel_timer_failed_event(&mut self, attributes: crate::tasks::CancelTimerFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::CancelTimerFailed), cancel_timer_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_marker_recorded_event(&mut self, attributes: crate::tasks::MarkerRecordedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::MarkerRecorded), marker_recorded_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_continued_as_new_event(&mut self, attributes: crate::tasks::WorkflowExecutionContinuedAsNewEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionContinuedAsNew), workflow_execution_continued_as_new_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_start_child_workflow_execution_initiated_event(&mut self, attributes: crate::tasks::StartChildWorkflowExecutionInitiatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::StartChildWorkflowExecutionInitiated), start_child_workflow_execution_initiated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_start_child_workflow_execution_failed_event(&mut self, attributes: crate::tasks::StartChildWorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::StartChildWorkflowExecutionFailed), start_child_workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_started_event(&mut self, attributes: crate::tasks::ChildWorkflowExecutionStartedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionStarted), child_workflow_execution_started_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_completed_event(&mut self, attributes: crate::tasks::ChildWorkflowExecutionCompletedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionCompleted), child_workflow_execution_completed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_failed_event(&mut self, attributes: crate::tasks::ChildWorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionFailed), child_workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_canceled_event(&mut self, attributes: crate::tasks::ChildWorkflowExecutionCanceledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionCanceled), child_workflow_execution_canceled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_terminated_event(&mut self, attributes: crate::tasks::ChildWorkflowExecutionTerminatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionTerminated), child_workflow_execution_terminated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_signal_external_workflow_execution_initiated_event(&mut self, attributes: crate::tasks::SignalExternalWorkflowExecutionInitiatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::SignalExternalWorkflowExecutionInitiated), signal_external_workflow_execution_initiated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_signal_external_workflow_execution_failed_event(&mut self, attributes: crate::tasks::SignalExternalWorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::SignalExternalWorkflowExecutionFailed), signal_external_workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_request_cancel_external_workflow_execution_initiated_event(&mut self, attributes: crate::tasks::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::RequestCancelExternalWorkflowExecutionInitiated), request_cancel_external_workflow_execution_initiated_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_request_cancel_external_workflow_execution_failed_event(&mut self, attributes: crate::tasks::RequestCancelExternalWorkflowExecutionFailedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::RequestCancelExternalWorkflowExecutionFailed), request_cancel_external_workflow_execution_failed_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_upsert_workflow_search_attributes_event(&mut self, attributes: crate::tasks::UpsertWorkflowSearchAttributesEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::UpsertWorkflowSearchAttributes), upsert_workflow_search_attributes_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_timer_fired_event(&mut self, attributes: crate::tasks::TimerFiredEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::TimerFired), timer_fired_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_external_workflow_execution_signaled_event(&mut self, attributes: crate::tasks::ExternalWorkflowExecutionSignaledEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ExternalWorkflowExecutionSignaled), external_workflow_execution_signaled_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_external_workflow_execution_cancel_requested_event(&mut self, attributes: crate::tasks::ExternalWorkflowExecutionCancelRequestedEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ExternalWorkflowExecutionCancelRequested), external_workflow_execution_cancel_requested_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_workflow_execution_timed_out_event(&mut self, attributes: crate::tasks::WorkflowExecutionTimedOutEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionTimedOut), workflow_execution_timed_out_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_decision_task_timed_out_event(&mut self, attributes: crate::tasks::DecisionTaskTimedOutEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::DecisionTaskTimedOut), decision_task_timed_out_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_activity_task_timed_out_event(&mut self, attributes: crate::tasks::ActivityTaskTimedOutEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ActivityTaskTimedOut), activity_task_timed_out_event_attributes: Some(attributes), ..Default::default() })
    }
    pub fn add_child_workflow_execution_timed_out_event(&mut self, attributes: crate::tasks::ChildWorkflowExecutionTimedOutEventAttributes) -> HistoryEvent {
        self.add_event(HistoryEvent { event_type: Some(cadence_api::enums::EventType::ChildWorkflowExecutionTimedOut), child_workflow_execution_timed_out_event_attributes: Some(attributes), ..Default::default() })
    }
}
