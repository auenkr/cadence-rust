//! Signal transitions from `mutable_state_builder_methods_signal.go`.

use cadence_api::{service_types::HistoryEvent, tasks::SignalExternalWorkflowExecutionInitiatedEventAttributes, types::EventType};
use serde::{Deserialize, Serialize};
use crate::mutable_state::MutableStateBuilder;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalInfo {
    pub version: i64,
    pub initiated_event_batch_id: i64,
    pub initiated_id: i64,
    pub signal_request_id: String,
    pub signal_name: String,
    pub input: Vec<u8>,
    pub control: Vec<u8>,
}

impl MutableStateBuilder {
    pub fn is_signal_requested(&self, request_id: &str) -> bool { self.pending_signal_requested_ids.contains(request_id) }
    pub fn add_signal_requested(&mut self, request_id: impl Into<String>) {
        let id = request_id.into();
        self.pending_signal_requested_ids.insert(id.clone());
        self.update_signal_requested_ids.insert(id.clone());
        self.delete_signal_requested_ids.remove(&id);
    }
    pub fn delete_signal_requested(&mut self, request_id: &str) {
        self.pending_signal_requested_ids.remove(request_id);
        self.update_signal_requested_ids.remove(request_id);
        self.delete_signal_requested_ids.insert(request_id.to_owned());
    }
    pub fn get_signal_info(&self, initiated_id: i64) -> Option<SignalInfo> {
        self.pending_signal_infos.get(&initiated_id).and_then(|v| serde_json::from_value(v.clone()).ok())
    }
    pub fn delete_pending_signal(&mut self, initiated_id: i64) {
        self.pending_signal_infos.remove(&initiated_id);
        self.update_signal_infos.remove(&initiated_id);
        self.delete_signal_infos.insert(initiated_id);
    }
    pub fn add_workflow_execution_signaled_event(&mut self, signal_name: impl Into<String>, input: Vec<u8>, identity: impl Into<String>, request_id: impl Into<String>, timestamp_unix_nanos: i64, version: i64) -> HistoryEvent {
        let event = self.add_event(HistoryEvent {
            event_type: Some(EventType::WorkflowExecutionSignaled), timestamp: Some(timestamp_unix_nanos), version,
            workflow_execution_signaled_event_attributes: Some(cadence_api::service_types::WorkflowExecutionSignaledEventAttributes { signal_name: signal_name.into(), input, identity: identity.into(), request_id: request_id.into() }),
            ..Default::default()
        });
        let count = self.execution_info.get("signal_count").and_then(serde_json::Value::as_i64).unwrap_or_default().saturating_add(1);
        self.execution_info["signal_count"] = count.into();
        event
    }
    pub fn replicate_workflow_execution_signaled_event(&mut self, _event: &HistoryEvent) {
        let count = self.execution_info.get("signal_count").and_then(serde_json::Value::as_i64).unwrap_or_default().saturating_add(1);
        self.execution_info["signal_count"] = count.into();
    }
    pub fn add_signal_external_workflow_execution_initiated_event(&mut self, attributes: SignalExternalWorkflowExecutionInitiatedEventAttributes, signal_request_id: impl Into<String>, first_event_id: i64, timestamp_unix_nanos: i64, version: i64) -> (HistoryEvent, SignalInfo) {
        let event = self.add_event(HistoryEvent { event_type: Some(EventType::SignalExternalWorkflowExecutionInitiated), timestamp: Some(timestamp_unix_nanos), version, signal_external_workflow_execution_initiated_event_attributes: Some(attributes), ..Default::default() });
        let attrs = event.signal_external_workflow_execution_initiated_event_attributes.as_ref().expect("attributes were set");
        let info = SignalInfo { version, initiated_event_batch_id: first_event_id, initiated_id: event.event_id, signal_request_id: signal_request_id.into(), signal_name: attrs.signal_name.clone(), input: attrs.input.clone(), control: attrs.control.clone() };
        let value = serde_json::to_value(&info).expect("SignalInfo is serializable");
        self.pending_signal_infos.insert(info.initiated_id, value.clone());
        self.update_signal_infos.insert(info.initiated_id, value);
        (event, info)
    }
    pub fn replicate_signal_external_workflow_execution_initiated_event(&mut self, first_event_id: i64, event: &HistoryEvent, signal_request_id: impl Into<String>) -> Option<SignalInfo> {
        let attrs = event.signal_external_workflow_execution_initiated_event_attributes.as_ref()?;
        let info = SignalInfo { version: event.version, initiated_event_batch_id: first_event_id, initiated_id: event.event_id, signal_request_id: signal_request_id.into(), signal_name: attrs.signal_name.clone(), input: attrs.input.clone(), control: attrs.control.clone() };
        let value = serde_json::to_value(&info).expect("SignalInfo is serializable");
        self.pending_signal_infos.insert(info.initiated_id, value.clone());
        self.update_signal_infos.insert(info.initiated_id, value);
        Some(info)
    }
}
