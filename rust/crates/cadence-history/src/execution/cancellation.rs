//! Workflow and external workflow cancellation transitions.

use cadence_api::{service_types::HistoryEvent, tasks::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes, types::EventType};
use serde::{Deserialize, Serialize};
use crate::mutable_state::MutableStateBuilder;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCancelInfo { pub version: i64, pub initiated_id: i64, pub initiated_event_batch_id: i64, pub cancel_request_id: String }

impl MutableStateBuilder {
    pub fn is_cancel_requested(&self) -> (bool, String) {
        (self.execution_info.get("cancel_requested").and_then(serde_json::Value::as_bool).unwrap_or(false), self.execution_info.get("cancel_request_id").and_then(serde_json::Value::as_str).unwrap_or_default().to_owned())
    }
    pub fn add_workflow_execution_cancel_requested_event(&mut self, cause: impl Into<String>, request_id: impl Into<String>, identity: impl Into<String>, timestamp_unix_nanos: i64, version: i64) -> HistoryEvent {
        let cause = cause.into(); let request_id = request_id.into();
        let event = self.add_event(HistoryEvent { event_type: Some(EventType::WorkflowExecutionCancelRequested), timestamp: Some(timestamp_unix_nanos), version, workflow_execution_cancel_requested_event_attributes: Some(cadence_api::service_types::WorkflowExecutionCancelRequestedEventAttributes { cause, request_id: request_id.clone(), identity: identity.into(), ..Default::default() }), ..Default::default() });
        self.execution_info["cancel_requested"] = true.into();
        self.execution_info["cancel_request_id"] = request_id.into();
        event
    }
    pub fn replicate_workflow_execution_cancel_requested_event(&mut self, event: &HistoryEvent) -> bool {
        let Some(attrs) = event.workflow_execution_cancel_requested_event_attributes.as_ref() else { return false; };
        self.execution_info["cancel_requested"] = true.into();
        self.execution_info["cancel_request_id"] = attrs.request_id.clone().into();
        true
    }
    pub fn add_request_cancel_external_workflow_execution_initiated_event(&mut self, attributes: RequestCancelExternalWorkflowExecutionInitiatedEventAttributes, cancel_request_id: impl Into<String>, first_event_id: i64, timestamp_unix_nanos: i64, version: i64) -> (HistoryEvent, RequestCancelInfo) {
        let event = self.add_event(HistoryEvent { event_type: Some(EventType::RequestCancelExternalWorkflowExecutionInitiated), timestamp: Some(timestamp_unix_nanos), version, request_cancel_external_workflow_execution_initiated_event_attributes: Some(attributes), ..Default::default() });
        let info = self.replicate_request_cancel_external_workflow_execution_initiated_event(first_event_id, &event, cancel_request_id).expect("attributes were set");
        (event, info)
    }
    pub fn replicate_request_cancel_external_workflow_execution_initiated_event(&mut self, first_event_id: i64, event: &HistoryEvent, cancel_request_id: impl Into<String>) -> Option<RequestCancelInfo> {
        event.request_cancel_external_workflow_execution_initiated_event_attributes.as_ref()?;
        let info = RequestCancelInfo { version: event.version, initiated_id: event.event_id, initiated_event_batch_id: first_event_id, cancel_request_id: cancel_request_id.into() };
        let value = serde_json::to_value(&info).expect("RequestCancelInfo is serializable");
        self.pending_request_cancel_infos.insert(info.initiated_id, value.clone());
        self.update_request_cancel_infos.insert(info.initiated_id, value);
        Some(info)
    }
    pub fn replicate_external_workflow_execution_cancel_requested(&mut self, event: &HistoryEvent) -> bool {
        let Some(attrs) = event.external_workflow_execution_cancel_requested_event_attributes.as_ref() else { return false; };
        self.pending_request_cancel_infos.remove(&attrs.initiated_event_id);
        self.update_request_cancel_infos.remove(&attrs.initiated_event_id);
        self.delete_request_cancel_infos.insert(attrs.initiated_event_id);
        true
    }
    pub fn delete_pending_request_cancel(&mut self, initiated_event_id: i64) {
        self.pending_request_cancel_infos.remove(&initiated_event_id);
        self.update_request_cancel_infos.remove(&initiated_event_id);
        self.delete_request_cancel_infos.insert(initiated_event_id);
    }
    pub fn replicate_request_cancel_external_workflow_execution_failed_event(&mut self, event: &HistoryEvent) -> bool {
        let Some(attrs) = event.request_cancel_external_workflow_execution_failed_event_attributes.as_ref() else { return false; };
        self.delete_pending_request_cancel(attrs.initiated_event_id);
        true
    }
}
