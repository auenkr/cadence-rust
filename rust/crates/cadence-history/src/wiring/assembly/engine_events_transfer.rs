//! group: assembly/engine_events_transfer
//!
//! History-event mutations of `EngineMutableState` used by the transfer active
//! task executor (Go `transfer_active_task_executor.go`): child started /
//! start child failed, external cancel requested / signaled and their failure
//! events, plus decoding of the child initiated event.

use cadence_api::enums::{
    CancelExternalWorkflowExecutionFailedCause, ChildWorkflowExecutionFailedCause, EventType,
    SignalExternalWorkflowExecutionFailedCause,
};
use cadence_api::service_types::HistoryEvent;
use cadence_api::tasks::*;
use cadence_api::types::{Header, WorkflowExecution, WorkflowType};
use cadence_persistence::history_contract as pc;
use cadence_thrift::FromThrift;

use crate::engine::deps::{HResult, HistoryError, EMPTY_EVENT_ID};
use crate::wiring::assembly::engine_state::{decode, EngineMutableState, Inner};

fn invalid(what: &str, next_event_id: i64) -> HistoryError {
    HistoryError::internal(format!("unable to add {what}: invalid action at next event id {next_event_id}"))
}

fn execution(workflow_id: &str, run_id: &str) -> WorkflowExecution {
    WorkflowExecution { workflow_id: workflow_id.to_string(), run_id: run_id.to_string() }
}

impl EngineMutableState {
    fn has_request_cancel(&self, g: &Inner, initiated_id: i64) -> bool {
        g.pc_cancels.contains_key(&initiated_id)
    }

    fn has_signal(&self, g: &Inner, initiated_id: i64) -> bool {
        g.pc_signals.contains_key(&initiated_id)
    }

    /// Decodes the `StartChildWorkflowExecutionInitiated` event persisted in the
    /// child execution info (Go `GetChildExecutionInitiatedEvent`).
    pub fn decode_child_initiated_event(
        &self,
        initiated_id: i64,
    ) -> HResult<StartChildWorkflowExecutionInitiatedEventAttributes> {
        let blob = {
            let g = self.inner.lock().unwrap();
            let c = g.pc_children.get(&initiated_id).ok_or_else(|| {
                HistoryError::internal(format!("unable to find child workflow with initiated id: {initiated_id}"))
            })?;
            pc::DataBlob {
                encoding: if c.initiated_event_encoding.is_empty() {
                    "thriftrw".to_string()
                } else {
                    c.initiated_event_encoding.clone()
                },
                data: c.initiated_event.clone(),
            }
        };
        let s = decode(&blob)?;
        let event = HistoryEvent::from_thrift(&s)
            .map_err(|e| HistoryError::internal(format!("decode child initiated event: {e}")))?;
        event
            .start_child_workflow_execution_initiated_event_attributes
            .ok_or_else(|| HistoryError::internal("child initiated event has no attributes"))
    }

    /// Go `AddChildWorkflowExecutionStartedEvent` + `ReplicateChildWorkflowExecutionStartedEvent`.
    pub fn add_child_started(
        &self,
        domain: &str,
        exec: &WorkflowExecution,
        initiated_id: i64,
        header: Option<Header>,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        let workflow_type = match (g.children.get(&initiated_id), g.pc_children.get(&initiated_id)) {
            (Some(ci), Some(pci)) if ci.started_id == EMPTY_EVENT_ID => {
                WorkflowType { name: pci.workflow_type_name.clone() }
            }
            _ => return Err(invalid("ChildWorkflowExecutionStarted", g.next_event_id)),
        };
        let domain = domain.to_string();
        let started_exec = exec.clone();
        let event = g.push_event(EventType::ChildWorkflowExecutionStarted, ms, |e| {
            e.child_workflow_execution_started_event_attributes = Some(ChildWorkflowExecutionStartedEventAttributes {
                domain,
                initiated_event_id: initiated_id,
                workflow_execution: Some(started_exec),
                workflow_type: Some(workflow_type),
                header,
            });
        });
        let run_id = exec.run_id.clone();
        if let Some(ci) = g.children.get_mut(&initiated_id) {
            ci.started_id = event.event_id;
        }
        if let Some(pci) = g.pc_children.get_mut(&initiated_id) {
            pci.started_id = event.event_id;
            pci.started_run_id = run_id.into_bytes();
        }
        g.upsert_children.insert(initiated_id);
        Ok(())
    }

    /// Go `AddStartChildWorkflowExecutionFailedEvent` (cause WorkflowAlreadyRunning).
    pub fn add_start_child_failed(&self, initiated_id: i64) -> HResult<()> {
        let initiated = self.decode_child_initiated_event(initiated_id)?;
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        match g.children.get(&initiated_id) {
            Some(ci) if ci.started_id == EMPTY_EVENT_ID => {}
            _ => return Err(invalid("StartChildWorkflowExecutionFailed", g.next_event_id)),
        }
        g.push_event(EventType::StartChildWorkflowExecutionFailed, ms, |e| {
            e.start_child_workflow_execution_failed_event_attributes =
                Some(StartChildWorkflowExecutionFailedEventAttributes {
                    domain: initiated.domain.clone(),
                    workflow_id: initiated.workflow_id.clone(),
                    workflow_type: initiated.workflow_type.clone(),
                    cause: Some(ChildWorkflowExecutionFailedCause::WorkflowAlreadyRunning),
                    control: initiated.control.clone(),
                    initiated_event_id: initiated_id,
                    decision_task_completed_event_id: initiated.decision_task_completed_event_id,
                });
        });
        g.remove_child(initiated_id)
    }

    /// Go `AddExternalWorkflowExecutionCancelRequested`.
    pub fn add_external_cancel_requested(
        &self,
        initiated_id: i64,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        if !self.has_request_cancel(&g, initiated_id) {
            return Err(invalid("ExternalWorkflowExecutionCancelRequested", g.next_event_id));
        }
        let (d, exec) = (domain.to_string(), execution(workflow_id, run_id));
        g.push_event(EventType::ExternalWorkflowExecutionCancelRequested, ms, |e| {
            e.external_workflow_execution_cancel_requested_event_attributes =
                Some(ExternalWorkflowExecutionCancelRequestedEventAttributes {
                    initiated_event_id: initiated_id,
                    domain: d,
                    workflow_execution: Some(exec),
                });
        });
        g.pc_cancels.remove(&initiated_id);
        g.upsert_cancels.remove(&initiated_id);
        g.deleted_request_cancels.insert(initiated_id);
        g.dirty = true;
        Ok(())
    }

    /// Go `AddExternalWorkflowExecutionSignaled`.
    pub fn add_external_signaled(
        &self,
        initiated_id: i64,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        control: &[u8],
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        if !self.has_signal(&g, initiated_id) {
            return Err(invalid("ExternalWorkflowExecutionSignaled", g.next_event_id));
        }
        let (d, exec, c) = (domain.to_string(), execution(workflow_id, run_id), control.to_vec());
        g.push_event(EventType::ExternalWorkflowExecutionSignaled, ms, |e| {
            e.external_workflow_execution_signaled_event_attributes =
                Some(ExternalWorkflowExecutionSignaledEventAttributes {
                    initiated_event_id: initiated_id,
                    domain: d,
                    workflow_execution: Some(exec),
                    control: c,
                });
        });
        g.pc_signals.remove(&initiated_id);
        g.upsert_signals.remove(&initiated_id);
        g.deleted_signals.insert(initiated_id);
        g.dirty = true;
        Ok(())
    }

    /// Go `AddRequestCancelExternalWorkflowExecutionFailedEvent`
    /// (cause UnknownExternalWorkflowExecution, decision id EmptyEventID).
    pub fn add_request_cancel_external_failed(
        &self,
        initiated_id: i64,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        if !self.has_request_cancel(&g, initiated_id) {
            return Err(invalid("RequestCancelExternalWorkflowExecutionFailed", g.next_event_id));
        }
        let (d, exec) = (domain.to_string(), execution(workflow_id, run_id));
        g.push_event(EventType::RequestCancelExternalWorkflowExecutionFailed, ms, |e| {
            e.request_cancel_external_workflow_execution_failed_event_attributes =
                Some(RequestCancelExternalWorkflowExecutionFailedEventAttributes {
                    cause: Some(CancelExternalWorkflowExecutionFailedCause::UnknownExternalWorkflowExecution),
                    decision_task_completed_event_id: EMPTY_EVENT_ID,
                    domain: d,
                    workflow_execution: Some(exec),
                    initiated_event_id: initiated_id,
                    control: Vec::new(),
                });
        });
        g.pc_cancels.remove(&initiated_id);
        g.upsert_cancels.remove(&initiated_id);
        g.deleted_request_cancels.insert(initiated_id);
        g.dirty = true;
        Ok(())
    }

    /// Go `AddSignalExternalWorkflowExecutionFailedEvent`
    /// (cause UnknownExternalWorkflowExecution, decision id EmptyEventID).
    pub fn add_signal_external_failed(
        &self,
        initiated_id: i64,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        control: &[u8],
    ) -> HResult<()> {
        let ms = self.now_millis();
        let mut g = self.inner.lock().unwrap();
        if !self.has_signal(&g, initiated_id) {
            return Err(invalid("SignalExternalWorkflowExecutionFailed", g.next_event_id));
        }
        let (d, exec, c) = (domain.to_string(), execution(workflow_id, run_id), control.to_vec());
        g.push_event(EventType::SignalExternalWorkflowExecutionFailed, ms, |e| {
            e.signal_external_workflow_execution_failed_event_attributes =
                Some(SignalExternalWorkflowExecutionFailedEventAttributes {
                    cause: Some(SignalExternalWorkflowExecutionFailedCause::UnknownExternalWorkflowExecution),
                    decision_task_completed_event_id: EMPTY_EVENT_ID,
                    domain: d,
                    workflow_execution: Some(exec),
                    initiated_event_id: initiated_id,
                    control: c,
                });
        });
        g.pc_signals.remove(&initiated_id);
        g.upsert_signals.remove(&initiated_id);
        g.deleted_signals.insert(initiated_id);
        g.dirty = true;
        Ok(())
    }
}
