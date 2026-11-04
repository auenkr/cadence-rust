//! Workflow start event application from `mutable_state_builder_methods_started.go`.

use cadence_api::service_types::{HistoryEvent, WorkflowExecutionStartedEventAttributes};
use cadence_api::types::EventType;
use crate::mutable_state::MutableStateBuilder;

impl MutableStateBuilder {
    pub fn add_workflow_execution_started_event(&mut self, attributes: WorkflowExecutionStartedEventAttributes, timestamp_unix_nanos: i64, version: i64) -> HistoryEvent {
        let event = self.add_event(HistoryEvent { event_type: Some(EventType::WorkflowExecutionStarted), timestamp: Some(timestamp_unix_nanos), version, workflow_execution_started_event_attributes: Some(attributes), ..Default::default() });
        self.replicate_workflow_execution_started_event(&event);
        event
    }
    pub fn replicate_workflow_execution_started_event(&mut self, event: &HistoryEvent) -> bool {
        let Some(attrs) = event.workflow_execution_started_event_attributes.as_ref() else { return false; };
        self.execution_info["state"] = 1.into();
        self.execution_info["close_status"] = 0.into();
        self.execution_info["start_timestamp"] = event.timestamp.unwrap_or_default().into();
        self.execution_info["workflow_execution_started_event_attributes"] = serde_json::to_value(attrs).expect("start attributes are serializable");
        self.execution_info["workflow_start_version"] = event.version.into();
        self.execution_info["cancel_requested"] = false.into();
        true
    }
}
