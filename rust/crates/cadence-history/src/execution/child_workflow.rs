//! Child workflow initiation and start transitions from
//! `mutable_state_builder_methods_child_workflow.go`.

use cadence_api::{service_types::HistoryEvent, tasks::{ChildWorkflowExecutionStartedEventAttributes, StartChildWorkflowExecutionInitiatedEventAttributes}, types::{EventType, ParentClosePolicy, WorkflowExecution, WorkflowType}};
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
            domain_id: attrs.domain.clone(), workflow_type_name: attrs.workflow_type.as_ref().map(|v| v.name.clone()).unwrap_or_default(),
            parent_close_policy: attrs.parent_close_policy,
            ..Default::default()
        };
        self.pending_child_execution_infos.insert(info.initiated_id, serde_json::to_value(&info).expect("ChildExecutionInfo is serializable"));
        self.update_child_execution_infos.insert(info.initiated_id, serde_json::to_value(&info).expect("ChildExecutionInfo is serializable"));
        self.delete_child_execution_infos.remove(&info.initiated_id);
        Ok(info)
    }

    pub fn add_child_workflow_execution_started_event(
        &mut self, initiated_event_id: i64, domain: impl Into<String>, execution: WorkflowExecution,
        workflow_type: Option<WorkflowType>, timestamp_unix_nanos: i64, version: i64,
    ) -> Result<HistoryEvent, ChildWorkflowError> {
        let info = self.get_child_execution_info(initiated_event_id).ok_or(ChildWorkflowError::Missing(initiated_event_id))?;
        if info.started_id != 0 { return Err(ChildWorkflowError::AlreadyStarted(initiated_event_id)); }
        let event = self.add_event(HistoryEvent {
            event_type: Some(EventType::ChildWorkflowExecutionStarted), timestamp: Some(timestamp_unix_nanos), version,
            child_workflow_execution_started_event_attributes: Some(ChildWorkflowExecutionStartedEventAttributes {
                domain: domain.into(), initiated_event_id, workflow_execution: Some(execution), workflow_type, header: None,
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
}
