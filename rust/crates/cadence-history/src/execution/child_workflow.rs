//! Child workflow initiation and start transitions from
//! `mutable_state_builder_methods_child_workflow.go`.

use cadence_api::{enums::{EventType, ParentClosePolicy}, service_types::HistoryEvent, tasks::{ChildWorkflowExecutionStartedEventAttributes, StartChildWorkflowExecutionInitiatedEventAttributes}, types::{WorkflowExecution, WorkflowType}};
use serde::{Deserialize, Serialize};

use crate::mutable_state::{MutableStateBuilder, MutableStateError};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildExecutionInfo {
    pub version: i64,
    pub initiated_id: i64,
    pub initiated_event_batch_id: i64,
    pub started_id: i64,
    pub started_workflow_id: String,
    pub started_run_id: String,
    pub create_request_id: String,
    pub domain_id: String,
    /// Legacy persistence field used by Go `fillForBackwardsCompatibility`.
    pub domain_name_deprecated: String,
    pub workflow_type_name: String,
    pub parent_close_policy: Option<ParentClosePolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChildWorkflowError {
    #[error("child workflow initiated event {0} is not pending")]
    Missing(i64),
    #[error("child workflow initiated event {0} already has a started event")]
    AlreadyStarted(i64),
    #[error("invalid child workflow event: {0}")]
    InvalidEvent(&'static str),
    #[error(transparent)]
    State(#[from] MutableStateError),
}

impl MutableStateBuilder {
    pub fn get_child_execution_info(&self, initiated_event_id: i64) -> Option<ChildExecutionInfo> {
        self.pending_child_execution_infos.get(&initiated_event_id).and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    pub fn delete_pending_child_execution(&mut self, initiated_event_id: i64) -> bool {
        self.pending_child_execution_infos.remove(&initiated_event_id);
        self.update_child_execution_infos.remove(&initiated_event_id);
        self.delete_child_execution_infos.insert(initiated_event_id)
    }

    /// Build and apply a child initiated event. Domain lookup and task
    /// generation are service dependencies and are performed by the history
    /// task executor after this transaction record is persisted.
    pub fn add_start_child_workflow_execution_initiated_event(
        &mut self,
        attributes: StartChildWorkflowExecutionInitiatedEventAttributes,
        create_request_id: impl Into<String>,
        first_event_id: i64,
        timestamp_unix_nanos: i64,
        version: i64,
    ) -> Result<(HistoryEvent, ChildExecutionInfo), ChildWorkflowError> {
        let event = self.add_event(HistoryEvent {
            event_type: Some(EventType::StartChildWorkflowExecutionInitiated),
            timestamp: Some(timestamp_unix_nanos), version,
            start_child_workflow_execution_initiated_event_attributes: Some(attributes),
            ..Default::default()
        });
        let info = self.replicate_start_child_workflow_execution_initiated_event(first_event_id, &event, create_request_id)?;
        Ok((event, info))
    }

    pub fn replicate_start_child_workflow_execution_initiated_event(
        &mut self, first_event_id: i64, event: &HistoryEvent, create_request_id: impl Into<String>,
    ) -> Result<ChildExecutionInfo, ChildWorkflowError> {
        let attrs = event.start_child_workflow_execution_initiated_event_attributes.as_ref().ok_or(ChildWorkflowError::InvalidEvent("missing initiated attributes"))?;
        if attrs.workflow_id.is_empty() { return Err(ChildWorkflowError::InvalidEvent("empty child workflow ID")); }
        let info = ChildExecutionInfo {
            version: event.version, initiated_id: event.event_id, initiated_event_batch_id: first_event_id,
            started_id: 0, started_workflow_id: attrs.workflow_id.clone(), create_request_id: create_request_id.into(),
            // Go resolves a non-empty domain name through the domain cache;
            // absent that service dependency, preserve the parent ID from the
            // execution record instead of storing a domain name in the ID field.
            domain_id: self.execution_info.get("domain_id").and_then(serde_json::Value::as_str).unwrap_or_default().to_owned(),
            workflow_type_name: attrs.workflow_type.as_ref().map(|v| v.name.clone()).unwrap_or_default(),
            parent_close_policy: attrs.parent_close_policy,
            ..Default::default()
        };
        self.pending_child_execution_infos.insert(info.initiated_id, serde_json::to_value(&info).expect("ChildExecutionInfo is serializable"));
        self.update_child_execution_infos.insert(info.initiated_id, serde_json::to_value(&info).expect("ChildExecutionInfo is serializable"));
        self.delete_child_execution_infos.remove(&info.initiated_id);
        Ok(info)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_child_workflow_execution_started_event(
        &mut self, initiated_event_id: i64, domain: impl Into<String>, execution: WorkflowExecution,
        workflow_type: Option<WorkflowType>, header: Option<cadence_api::types::Header>, timestamp_unix_nanos: i64, version: i64,
    ) -> Result<HistoryEvent, ChildWorkflowError> {
        let info = self.get_child_execution_info(initiated_event_id).ok_or(ChildWorkflowError::Missing(initiated_event_id))?;
        if info.started_id != 0 { return Err(ChildWorkflowError::AlreadyStarted(initiated_event_id)); }
        let event = self.add_event(HistoryEvent {
            event_type: Some(EventType::ChildWorkflowExecutionStarted), timestamp: Some(timestamp_unix_nanos), version,
            child_workflow_execution_started_event_attributes: Some(ChildWorkflowExecutionStartedEventAttributes {
                domain: domain.into(), initiated_event_id, workflow_execution: Some(execution), workflow_type, header,
            }), ..Default::default()
        });
        self.replicate_child_workflow_execution_started_event(&event)?;
        Ok(event)
    }

    pub fn replicate_child_workflow_execution_started_event(&mut self, event: &HistoryEvent) -> Result<(), ChildWorkflowError> {
        let attrs = event.child_workflow_execution_started_event_attributes.as_ref().ok_or(ChildWorkflowError::InvalidEvent("missing started attributes"))?;
        let mut info = self.get_child_execution_info(attrs.initiated_event_id).ok_or(ChildWorkflowError::Missing(attrs.initiated_event_id))?;
        info.started_id = event.event_id;
        info.started_run_id = attrs.workflow_execution.as_ref().map(|v| v.run_id.clone()).unwrap_or_default();
        let value = serde_json::to_value(info).expect("ChildExecutionInfo is serializable");
        self.pending_child_execution_infos.insert(attrs.initiated_event_id, value.clone());
        self.update_child_execution_infos.insert(attrs.initiated_event_id, value);
        Ok(())
    }

    /// Remove the pending child record for any terminal child event. All eight
    /// Go terminal transition methods use their event's initiated ID for this
    /// common persistence action; local event construction remains owned by
    /// the matching history handler.
    pub fn replicate_child_workflow_terminal_event(&mut self, event: &HistoryEvent) -> Result<(), ChildWorkflowError> {
        use cadence_api::enums::EventType;
        let valid = matches!(event.event_type.as_ref(), Some(EventType::StartChildWorkflowExecutionFailed | EventType::ChildWorkflowExecutionCompleted | EventType::ChildWorkflowExecutionFailed | EventType::ChildWorkflowExecutionCanceled | EventType::ChildWorkflowExecutionTimedOut | EventType::ChildWorkflowExecutionTerminated));
        if !valid { return Err(ChildWorkflowError::InvalidEvent("event is not a terminal child transition")); }
        let value = serde_json::to_value(event).map_err(|_| ChildWorkflowError::InvalidEvent("event is not serializable"))?;
        let attrs_name = event.event_type.as_ref().map(|v| match v {
            EventType::StartChildWorkflowExecutionFailed => "startChildWorkflowExecutionFailedEventAttributes",
            EventType::ChildWorkflowExecutionCompleted => "childWorkflowExecutionCompletedEventAttributes",
            EventType::ChildWorkflowExecutionFailed => "childWorkflowExecutionFailedEventAttributes",
            EventType::ChildWorkflowExecutionCanceled => "childWorkflowExecutionCanceledEventAttributes",
            EventType::ChildWorkflowExecutionTimedOut => "childWorkflowExecutionTimedOutEventAttributes",
            EventType::ChildWorkflowExecutionTerminated => "childWorkflowExecutionTerminatedEventAttributes",
            _ => "",
        }).unwrap_or_default();
        let initiated_id = value.get(attrs_name).and_then(|a| a.get("initiatedEventId")).and_then(serde_json::Value::as_i64).ok_or(ChildWorkflowError::InvalidEvent("terminal event missing initiated ID"))?;
        if !self.delete_pending_child_execution(initiated_id) { return Err(ChildWorkflowError::Missing(initiated_id)); }
        Ok(())
    }

    pub fn add_start_child_workflow_execution_failed_event(&mut self, event: HistoryEvent) -> Result<HistoryEvent, ChildWorkflowError> { self.add_child_terminal_event(event, EventType::StartChildWorkflowExecutionFailed) }
    pub fn add_child_workflow_execution_completed_event(&mut self, event: HistoryEvent) -> Result<HistoryEvent, ChildWorkflowError> { self.add_child_terminal_event(event, EventType::ChildWorkflowExecutionCompleted) }
    pub fn add_child_workflow_execution_failed_event(&mut self, event: HistoryEvent) -> Result<HistoryEvent, ChildWorkflowError> { self.add_child_terminal_event(event, EventType::ChildWorkflowExecutionFailed) }
    pub fn add_child_workflow_execution_canceled_event(&mut self, event: HistoryEvent) -> Result<HistoryEvent, ChildWorkflowError> { self.add_child_terminal_event(event, EventType::ChildWorkflowExecutionCanceled) }
    pub fn add_child_workflow_execution_timed_out_event(&mut self, event: HistoryEvent) -> Result<HistoryEvent, ChildWorkflowError> { self.add_child_terminal_event(event, EventType::ChildWorkflowExecutionTimedOut) }
    pub fn add_child_workflow_execution_terminated_event(&mut self, event: HistoryEvent) -> Result<HistoryEvent, ChildWorkflowError> { self.add_child_terminal_event(event, EventType::ChildWorkflowExecutionTerminated) }

    fn add_child_terminal_event(&mut self, event: HistoryEvent, expected: EventType) -> Result<HistoryEvent, ChildWorkflowError> {
        if event.event_type.as_ref() != Some(&expected) { return Err(ChildWorkflowError::InvalidEvent("unexpected terminal child event type")); }
        let event = self.add_event(event);
        self.replicate_child_workflow_terminal_event(&event)?;
        Ok(event)
    }
}
