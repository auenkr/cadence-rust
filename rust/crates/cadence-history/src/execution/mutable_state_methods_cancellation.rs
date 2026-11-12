//! `mutable_state_builder_methods_cancellation.go` on [`TypedMutableState`].
//! Not wired yet: `insertWorkflowRequest` (strong idempotency, needs domain
//! entry/config), task generation and the events cache.

use cadence_api::enums::{CancelExternalWorkflowExecutionFailedCause, EventType};
use cadence_api::service_types::{HistoryEvent, WorkflowExecutionCancelRequestedEventAttributes};
use cadence_api::tasks::{
    CancelWorkflowExecutionDecisionAttributes, ExternalWorkflowExecutionCancelRequestedEventAttributes,
    RequestCancelExternalWorkflowExecutionDecisionAttributes,
    RequestCancelExternalWorkflowExecutionFailedEventAttributes,
    RequestCancelExternalWorkflowExecutionInitiatedEventAttributes, WorkflowExecutionCanceledEventAttributes,
};
use cadence_api::types::WorkflowExecution;

use super::cancellation::RequestCancelInfo;
use super::mutable_state_builder::TypedMutableState;
use super::mutable_state_methods_common::{
    internal_err, Result, CLOSE_STATUS_CANCELED, WORKFLOW_STATE_COMPLETED,
};

impl TypedMutableState {
    /// Go `IsCancelRequested`.
    pub fn is_cancel_requested(&self) -> (bool, String) {
        if self.execution_info.cancel_requested {
            return (true, self.execution_info.cancel_request_id.clone());
        }
        (false, String::new())
    }

    /// Go `GetRequestCancelInfo`.
    pub fn get_request_cancel_info(&self, initiated_event_id: i64) -> Option<&RequestCancelInfo> {
        self.pending_request_cancel_info_ids.get(&initiated_event_id)
    }

    /// Go `AddWorkflowExecutionCancelRequestedEvent`; the request fields of
    /// `HistoryRequestCancelWorkflowExecutionRequest` are passed explicitly.
    pub fn add_workflow_execution_cancel_requested_event(
        &mut self,
        timestamp_unix_nanos: i64,
        cause: &str,
        identity: &str,
        request_id: &str,
        external_initiated_event_id: Option<i64>,
        external_workflow_execution: Option<WorkflowExecution>,
    ) -> Result<HistoryEvent> {
        let op = "add-workflow-cancel-requested-event";
        self.check_mutability()?;
        if self.execution_info.cancel_requested {
            return Err(self.create_internal_server_error(op));
        }
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::WorkflowExecutionCancelRequested,
            |e| {
                e.workflow_execution_cancel_requested_event_attributes =
                    Some(WorkflowExecutionCancelRequestedEventAttributes {
                        cause: cause.to_string(),
                        external_initiated_event_id,
                        external_workflow_execution,
                        identity: identity.to_string(),
                        request_id: request_id.to_string(),
                    });
            },
        )?;
        self.replicate_workflow_execution_cancel_requested_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateWorkflowExecutionCancelRequestedEvent`.
    pub fn replicate_workflow_execution_cancel_requested_event(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(attributes) = event.workflow_execution_cancel_requested_event_attributes.as_ref() else {
            return internal_err("WorkflowExecutionCancelRequested event is missing attributes");
        };
        self.execution_info.cancel_requested = true;
        self.execution_info.cancel_request_id = attributes.request_id.clone();
        self.insert_workflow_request(
            &attributes.request_id,
            event.version,
            cadence_persistence::history_contract::WORKFLOW_REQUEST_TYPE_CANCEL,
        );
        Ok(())
    }

    /// Go `AddWorkflowExecutionCanceledEvent`.
    pub fn add_workflow_execution_canceled_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_task_completed_event_id: i64,
        attributes: &CancelWorkflowExecutionDecisionAttributes,
    ) -> Result<HistoryEvent> {
        self.check_mutability()?;
        let event = self.add_history_event(timestamp_unix_nanos, EventType::WorkflowExecutionCanceled, |e| {
            e.workflow_execution_canceled_event_attributes = Some(WorkflowExecutionCanceledEventAttributes {
                decision_task_completed_event_id,
                details: attributes.details.clone(),
            });
        })?;
        self.replicate_workflow_execution_canceled_event(decision_task_completed_event_id, &event)?;
        Ok(event)
    }

    /// Go `ReplicateWorkflowExecutionCanceledEvent` (close tasks generated; Go `writeEventToCache` is perf-only, the executor falls back to the history reader).
    pub fn replicate_workflow_execution_canceled_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
    ) -> Result<()> {
        self.update_workflow_state_close_status(WORKFLOW_STATE_COMPLETED, CLOSE_STATUS_CANCELED)?;
        self.execution_info.completion_event_batch_id = first_event_id;
        self.clear_stickyness();
        // Go: taskGenerator.GenerateWorkflowCloseTasks (close tasks generated; Go `writeEventToCache` is perf-only, the executor falls back to the history reader)
        self.generate_workflow_close_tasks(event)
    }

    /// Go `AddRequestCancelExternalWorkflowExecutionInitiatedEvent`.
    pub fn add_request_cancel_external_workflow_execution_initiated_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_completed_event_id: i64,
        cancel_request_id: &str,
        request: &RequestCancelExternalWorkflowExecutionDecisionAttributes,
    ) -> Result<(HistoryEvent, RequestCancelInfo)> {
        self.check_mutability()?;
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::RequestCancelExternalWorkflowExecutionInitiated,
            |e| {
                e.request_cancel_external_workflow_execution_initiated_event_attributes =
                    Some(RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                        decision_task_completed_event_id: decision_completed_event_id,
                        domain: request.domain.clone(),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: request.workflow_id.clone(),
                            run_id: request.run_id.clone(),
                        }),
                        control: request.control.clone(),
                        child_workflow_only: request.child_workflow_only,
                    });
            },
        )?;
        let rci = self.replicate_request_cancel_external_workflow_execution_initiated_event(
            decision_completed_event_id,
            &event,
            cancel_request_id,
        )?;
        Ok((event, rci))
    }

    /// Go `ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent`
    /// (including `GenerateRequestCancelExternalTasks`).
    pub fn replicate_request_cancel_external_workflow_execution_initiated_event(
        &mut self,
        first_event_id: i64,
        event: &HistoryEvent,
        cancel_request_id: &str,
    ) -> Result<RequestCancelInfo> {
        let rci = RequestCancelInfo {
            version: event.version,
            initiated_event_batch_id: first_event_id,
            initiated_id: event.event_id,
            cancel_request_id: cancel_request_id.to_string(),
        };
        self.update_request_cancel(rci.clone());
        self.run_task_generator(0, |g| g.generate_request_cancel_external_tasks(event))?;
        Ok(rci)
    }

    /// Go `AddExternalWorkflowExecutionCancelRequested`.
    pub fn add_external_workflow_execution_cancel_requested(
        &mut self,
        timestamp_unix_nanos: i64,
        initiated_id: i64,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<HistoryEvent> {
        let op = "add-external-workflow-cancel-requested-event";
        self.check_mutability()?;
        if self.get_request_cancel_info(initiated_id).is_none() {
            return Err(self.create_internal_server_error(op));
        }
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::ExternalWorkflowExecutionCancelRequested,
            |e| {
                e.external_workflow_execution_cancel_requested_event_attributes =
                    Some(ExternalWorkflowExecutionCancelRequestedEventAttributes {
                        initiated_event_id: initiated_id,
                        domain: domain.to_string(),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                        }),
                    });
            },
        )?;
        self.replicate_external_workflow_execution_cancel_requested(&event)?;
        Ok(event)
    }

    /// Go `ReplicateExternalWorkflowExecutionCancelRequested`.
    pub fn replicate_external_workflow_execution_cancel_requested(&mut self, event: &HistoryEvent) -> Result<()> {
        let Some(attributes) = event.external_workflow_execution_cancel_requested_event_attributes.as_ref() else {
            return internal_err("ExternalWorkflowExecutionCancelRequested event is missing attributes");
        };
        let initiated_id = attributes.initiated_event_id;
        self.delete_request_cancel(initiated_id)
    }

    /// Go `AddRequestCancelExternalWorkflowExecutionFailedEvent`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_request_cancel_external_workflow_execution_failed_event(
        &mut self,
        timestamp_unix_nanos: i64,
        decision_task_completed_event_id: i64,
        initiated_id: i64,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        cause: CancelExternalWorkflowExecutionFailedCause,
    ) -> Result<HistoryEvent> {
        let op = "add-request-cancel-external-workflow-failed-event";
        self.check_mutability()?;
        if self.get_request_cancel_info(initiated_id).is_none() {
            return Err(self.create_internal_server_error(op));
        }
        let event = self.add_history_event(
            timestamp_unix_nanos,
            EventType::RequestCancelExternalWorkflowExecutionFailed,
            |e| {
                e.request_cancel_external_workflow_execution_failed_event_attributes =
                    Some(RequestCancelExternalWorkflowExecutionFailedEventAttributes {
                        cause: Some(cause),
                        decision_task_completed_event_id,
                        domain: domain.to_string(),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                        }),
                        initiated_event_id: initiated_id,
                        control: Vec::new(),
                    });
            },
        )?;
        self.replicate_request_cancel_external_workflow_execution_failed_event(&event)?;
        Ok(event)
    }

    /// Go `ReplicateRequestCancelExternalWorkflowExecutionFailedEvent`.
    pub fn replicate_request_cancel_external_workflow_execution_failed_event(
        &mut self,
        event: &HistoryEvent,
    ) -> Result<()> {
        let Some(attributes) = event
            .request_cancel_external_workflow_execution_failed_event_attributes
            .as_ref()
        else {
            return internal_err("RequestCancelExternalWorkflowExecutionFailed event is missing attributes");
        };
        let initiated_id = attributes.initiated_event_id;
        self.delete_request_cancel(initiated_id)
    }
}
