//! `mutable_state_builder_methods_signal.go` on [`TypedMutableState`].
//! Not wired yet: `insertWorkflowRequest` in `ReplicateWorkflowExecutionSignaled`
//! (needs domain entry/config) and `taskGenerator.GenerateSignalExternalTasks`.

use std::collections::BTreeMap;

use cadence_api::enums::{EventType, SignalExternalWorkflowExecutionFailedCause};
use cadence_api::service_types::{HistoryEvent, WorkflowExecutionSignaledEventAttributes};
use cadence_api::tasks::{
    ExternalWorkflowExecutionSignaledEventAttributes, SignalExternalWorkflowExecutionDecisionAttributes,
    SignalExternalWorkflowExecutionFailedEventAttributes, SignalExternalWorkflowExecutionInitiatedEventAttributes,
};
use cadence_api::types::WorkflowExecution;

use super::mutable_state_builder::TypedMutableState;
use super::mutable_state_methods_common::{internal_err, Result};
use super::signal::SignalInfo;

impl TypedMutableState {
    /// Go `IsSignalRequested`.
    pub fn is_signal_requested(&self, request_id: &str) -> bool {
        self.pending_signal_requested_ids.contains(request_id)
    }

    /// Go `GetSignalInfo`.
    pub fn get_signal_info(&self, initiated_event_id: i64) -> Option<&SignalInfo> {
        self.pending_signal_info_ids.get(&initiated_event_id)
    }

    /// Go `GetPendingSignalExternalInfos`.
    pub fn get_pending_signal_external_infos(&self) -> &BTreeMap<i64, SignalInfo> {
        &self.pending_signal_info_ids
    }

    /// Go `AddWorkflowExecutionSignaled`.
    pub fn add_workflow_execution_signaled(
        &mut self,
        timestamp_unix_nanos: i64,
        signal_name: &str,
        input: &[u8],
        identity: &str,
        request_id: &str,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::WorkflowExecutionSignaled, |e| {
            e.workflow_execution_signaled_event_attributes = Some(WorkflowExecutionSignaledEventAttributes {
                signal_name: signal_name.to_string(),
                input: input.to_vec(),
                identity: identity.to_string(),
                request_id: request_id.to_string(),
            });
        })?;
        self.replicate_workflow_execution_signaled(&event)?;
        Ok(event)
    }

    /// Go `ReplicateWorkflowExecutionSignaled` (`insertWorkflowRequest` not wired).
    pub fn replicate_workflow_execution_signaled(&mut self, event: &HistoryEvent) -> Result<()> {
        self.execution_info.signal_count += 1;
        let request_id = event
            .workflow_execution_signaled_event_attributes
            .as_ref()
            .map(|a| a.request_id.clone())
            .unwrap_or_default();
        self.insert_signal_workflow_request(&request_id, event.version);
        Ok(())
    }

    /// Go `insertWorkflowRequest`: only when strong idempotency is enabled and
    /// the request id is non-empty; duplicates are kept once.
    pub(crate) fn insert_signal_workflow_request(&mut self, request_id: &str, version: i64) {
        self.insert_workflow_request(request_id, version, cadence_persistence::history_contract::WORKFLOW_REQUEST_TYPE_SIGNAL);
    }

    /// Go `insertWorkflowRequest` for any request type.
    pub(crate) fn insert_workflow_request(&mut self, request_id: &str, version: i64, signal: i32) {
        if !self.task_gen_env.enable_strong_idempotency || request_id.is_empty() {
            return;
        }
        if self.insert_workflow_requests.iter().any(|r| r.request_id == request_id && r.version == version && r.request_type == signal) {
            return;
        }
        self.insert_workflow_requests.push(cadence_persistence::history_contract::WorkflowRequest {
            request_id: request_id.to_string(),
            version,
            request_type: signal,
            data: Default::default(),
        });
    }

    /// Go `AddExternalWorkflowExecutionSignaled`.
    pub fn add_external_workflow_execution_signaled(
        &mut self,
        timestamp_unix_nanos: i64,
        initiated_id: i64,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        control: &[u8],
    ) -> Result<HistoryEvent> {
        let op = "add-external-workflow-signaled-event";
        self.check_mutability()?;
        if self.get_signal_info(initiated_id).is_none() {
            return Err(self.create_internal_server_error(op));
        }
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::ExternalWorkflowExecutionSignaled,
            |e| {
                e.external_workflow_execution_signaled_event_attributes =
                    Some(ExternalWorkflowExecutionSignaledEventAttributes {
                        initiated_event_id: initiated_id,
                        domain: domain.to_string(),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                        }),
                        control: control.to_vec(),
                    });
            },
        )?;
        self.replicate_external_workflow_execution_signaled(&event)?;
        Ok(event)
    }

    /// Go `ReplicateExternalWorkflowExecutionSignaled`.
    pub fn replicate_external_workflow_execution_signaled(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.external_workflow_execution_signaled_event_attributes.as_ref() else {
            return internal_err("ExternalWorkflowExecutionSignaled event is missing attributes");
        };
        self.delete_pending_signal(a.initiated_event_id)
    }

    /// Go `AddSignalExternalWorkflowExecutionFailedEvent`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_signal_external_workflow_execution_failed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_task_completed_event_id: i64,
        initiated_id: i64,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        _control: &[u8],
        cause: SignalExternalWorkflowExecutionFailedCause,
    ) -> Result<HistoryEvent> {
        let op = "add-signal-external-workflow-failed-event";
        self.check_mutability()?;
        if self.get_signal_info(initiated_id).is_none() {
            return Err(self.create_internal_server_error(op));
        }
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::SignalExternalWorkflowExecutionFailed,
            |e| {
                e.signal_external_workflow_execution_failed_event_attributes =
                    Some(SignalExternalWorkflowExecutionFailedEventAttributes {
                        cause: Some(cause),
                        decision_task_completed_event_id,
                        domain: domain.to_string(),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                        }),
                        initiated_event_id: initiated_id,
                        ..Default::default()
                    });
            },
        )?;
        self.replicate_signal_external_workflow_execution_failed_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateSignalExternalWorkflowExecutionFailedEvent`.
    pub fn replicate_signal_external_workflow_execution_failed_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(a) = event.signal_external_workflow_execution_failed_event_attributes.as_ref() else {
            return internal_err("SignalExternalWorkflowExecutionFailed event is missing attributes");
        };
        self.delete_pending_signal(a.initiated_event_id)
    }

    /// Go `AddSignalExternalWorkflowExecutionInitiatedEvent`.
    pub fn add_signal_external_workflow_execution_initiated_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        signal_request_id: &str,
        request: &SignalExternalWorkflowExecutionDecisionAttributes,
    ) -> Result<(HistoryEvent, SignalInfo)> {
        self.check_mutability()?;
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::SignalExternalWorkflowExecutionInitiated,
            |e| {
                e.signal_external_workflow_execution_initiated_event_attributes =
                    Some(SignalExternalWorkflowExecutionInitiatedEventAttributes {
                        decision_task_completed_event_id: decision_completed_event_id,
                        domain: request.domain.clone(),
                        workflow_execution: request.execution.clone(),
                        signal_name: request.signal_name.clone(),
                        input: request.input.clone(),
                        control: request.control.clone(),
                    });
            },
        )?;
        let si = self.replicate_signal_external_workflow_execution_initiated_event(
            decision_completed_event_id,
            &event,
            signal_request_id,
        )?;
        Ok((event, si))
    }

    /// Go `ReplicateSignalExternalWorkflowExecutionInitiatedEvent`
    /// (including `GenerateSignalExternalTasks`).
    pub fn replicate_signal_external_workflow_execution_initiated_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
        signal_request_id: &str,
    ) -> Result<SignalInfo> {
        let Some(a) = event.signal_external_workflow_execution_initiated_event_attributes.as_ref() else {
            return internal_err("SignalExternalWorkflowExecutionInitiated event is missing attributes");
        };
        let si = SignalInfo {
            version: event.version,
            initiated_event_batch_id: first_event_id,
            initiated_id: event.event_id,
            signal_request_id: signal_request_id.to_string(),
            signal_name: a.signal_name.clone(),
            input: a.input.clone(),
            control: a.control.clone(),
        };
        self.update_signal(si.clone());
        // GAP: Rust attributes lack Go ChildWorkflowOnly, so child_only is false.
        self.run_task_generator(0, |g| g.generate_signal_external_tasks(event))?;
        Ok(si)
    }

    /// Go `DeletePendingSignal` (missing entry is only logged in Go).
    pub fn delete_pending_signal(&mut self, initiated_event_id: i64) -> Result<()> {
        self.pending_signal_info_ids.remove(&initiated_event_id);
        self.update_signal_infos.remove(&initiated_event_id);
        self.delete_signal_info_ids.insert(initiated_event_id);
        Ok(())
    }
}

#[cfg(test)]
mod insert_request_tests {
    use super::*;

    fn signaled(request_id: &str) -> HistoryEvent {
        HistoryEvent {
            workflow_execution_signaled_event_attributes: Some(WorkflowExecutionSignaledEventAttributes {
                request_id: request_id.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn signal_request_not_recorded_when_strong_idempotency_off() {
        let mut s = TypedMutableState::new();
        s.replicate_workflow_execution_signaled(&signaled("r1")).unwrap();
        assert_eq!(s.execution_info.signal_count, 1);
        assert!(s.insert_workflow_requests.is_empty());
    }

    #[test]
    fn signal_request_recorded_once_when_enabled() {
        let mut s = TypedMutableState::new();
        s.task_gen_env.enable_strong_idempotency = true;
        s.replicate_workflow_execution_signaled(&signaled("r1")).unwrap();
        s.replicate_workflow_execution_signaled(&signaled("r1")).unwrap();
        s.replicate_workflow_execution_signaled(&signaled("")).unwrap();
        assert_eq!(s.execution_info.signal_count, 3);
        assert_eq!(s.insert_workflow_requests.len(), 1);
        assert_eq!(s.insert_workflow_requests[0].request_id, "r1");
        assert_eq!(s.insert_workflow_requests[0].request_type, 1);
    }

    #[test]
    fn signal_request_keeps_version_and_dedups_on_full_key() {
        let mut s = TypedMutableState::new();
        s.task_gen_env.enable_strong_idempotency = true;
        let mut e = signaled("r1");
        e.version = 7;
        s.replicate_workflow_execution_signaled(&e).unwrap();
        s.replicate_workflow_execution_signaled(&e).unwrap();
        e.version = 8;
        s.replicate_workflow_execution_signaled(&e).unwrap();
        assert_eq!(s.insert_workflow_requests.len(), 2);
        assert_eq!(s.insert_workflow_requests[0].version, 7);
        assert_eq!(s.insert_workflow_requests[1].version, 8);
    }
}
