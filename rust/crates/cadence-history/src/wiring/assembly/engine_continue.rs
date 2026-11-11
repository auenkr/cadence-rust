//! group: assembly/engine_continue
//!
//! Go `mutableStateBuilder.AddContinueAsNewEvent`: records the
//! `WorkflowExecutionContinuedAsNew` event, closes the current run and builds the
//! mutable state of the new run (kept in `Inner::new_run` until the decision
//! handler persists both runs in one update).

use std::sync::Arc;

use cadence_api::enums::{ContinueAsNewInitiator, EventType};
use cadence_api::tasks::*;
use cadence_api::types::{StartWorkflowExecutionRequest, TaskList, WorkflowExecution, WorkflowType};

use crate::engine::deps::{
    HResult, HistoryError, WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW, WORKFLOW_STATE_COMPLETED,
};
use crate::engine::types::{HistoryStartWorkflowExecutionRequest, ParentExecutionInfo};
use crate::wiring::assembly::engine_start::ContinuedFrom;
use crate::wiring::assembly::engine_state::EngineMutableState;

const NANOS: i64 = 1_000_000_000;

impl EngineMutableState {
    /// Go `AddContinueAsNewEvent`.
    pub(super) fn add_continue_as_new_event(
        &self,
        first_event_id: i64,
        completed_id: i64,
        parent_domain_name: &str,
        a: &ContinueAsNewWorkflowExecutionDecisionAttributes,
    ) -> HResult<()> {
        let prev_attrs = {
            let g = self.inner.lock().unwrap();
            g.prev_start_event
                .clone()
                .and_then(|e| e.workflow_execution_started_event_attributes)
                .ok_or_else(|| HistoryError::internal("workflow start event is not loaded"))?
        };
        let new_run_id = uuid::Uuid::new_v4().to_string();
        let ms = self.now_millis();
        let now = self.now();
        let backoff = a.backoff_start_interval_in_seconds;
        let initiator = a.initiator.unwrap_or(ContinueAsNewInitiator::Decider);
        let params = self.params.clone();

        let mut g = self.inner.lock().unwrap();
        if !g.exec.is_running() {
            return Err(HistoryError::internal(format!(
                "invalid history action WorkflowExecutionContinuedAsNew (next event id {})",
                g.next_event_id
            )));
        }
        let workflow_id = g.pc_exec.workflow_id.clone();
        let prev_run_id = g.pc_exec.run_id.clone();
        let first_run_id = if g.pc_exec.first_execution_run_id.is_empty() {
            prev_attrs.first_execution_run_id.clone()
        } else {
            g.pc_exec.first_execution_run_id.clone()
        };
        let prev_task_list = g.pc_exec.task_list.clone();
        let prev_type = g.pc_exec.workflow_type_name.clone();
        let prev_decision_timeout = g.exec.decision_start_to_close_timeout;
        let prev_attempt = g.pc_exec.attempt;
        let prev_expiration = g.pc_exec.expiration_time_unix_nanos;
        let partition_config = g.pc_exec.partition_config.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let parent = if g.pc_exec.parent_workflow_id.is_empty() {
            None
        } else {
            Some(ParentExecutionInfo {
                domain_uuid: g.pc_exec.parent_domain_id.clone(),
                domain: parent_domain_name.to_string(),
                execution: Some(WorkflowExecution {
                    workflow_id: g.pc_exec.parent_workflow_id.clone(),
                    run_id: g.pc_exec.parent_run_id.clone(),
                }),
                initiated_id: g.pc_exec.initiated_id,
            })
        };

        // Go `AddContinuedAsNewEvent` + `ReplicateWorkflowExecutionContinuedAsNewEvent`.
        let attrs = a.clone();
        let run = new_run_id.clone();
        let event = g.push_event(EventType::WorkflowExecutionContinuedAsNew, ms, |e| {
            e.workflow_execution_continued_as_new_event_attributes =
                Some(WorkflowExecutionContinuedAsNewEventAttributes {
                    new_execution_run_id: run,
                    workflow_type: attrs.workflow_type,
                    task_list: attrs.task_list,
                    header: attrs.header,
                    input: attrs.input,
                    execution_start_to_close_timeout_seconds: attrs.execution_start_to_close_timeout_seconds,
                    task_start_to_close_timeout_seconds: attrs.task_start_to_close_timeout_seconds,
                    decision_task_completed_event_id: completed_id,
                    backoff_start_interval_in_seconds: Some(backoff.unwrap_or(0)),
                    initiator: Some(initiator),
                    failure_reason: attrs.failure_reason,
                    failure_details: attrs.failure_details,
                    last_completion_result: attrs.last_completion_result,
                    memo: attrs.memo,
                    search_attributes: attrs.search_attributes,
                    jitter_start_seconds: attrs.jitter_start_seconds,
                    cron_overlap_policy: attrs.cron_overlap_policy,
                    active_cluster_selection_policy: attrs.active_cluster_selection_policy,
                });
        });
        g.exec.state = WORKFLOW_STATE_COMPLETED;
        g.exec.close_status = WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW;
        g.pc_exec.state = WORKFLOW_STATE_COMPLETED;
        g.pc_exec.close_status = WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW;
        g.pc_exec.completion_event_batch_id = first_event_id;
        g.clear_stickyness();
        g.gen_workflow_close_tasks(&params, ms * 1_000_000, event.version);
        drop(g);

        // Go `addWorkflowExecutionStartedEventForContinueAsNew`.
        let task_list = TaskList {
            name: a.task_list.as_ref().map(|t| t.name.clone()).unwrap_or(prev_task_list),
            ..Default::default()
        };
        let workflow_type = WorkflowType {
            name: a.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or(prev_type),
        };
        let decision_timeout = a.task_start_to_close_timeout_seconds.unwrap_or(prev_decision_timeout);
        let start_request = StartWorkflowExecutionRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            domain: self.domain.info.name.clone(),
            workflow_id: workflow_id.clone(),
            task_list: Some(task_list),
            workflow_type: Some(workflow_type),
            task_start_to_close_timeout_seconds: Some(decision_timeout),
            execution_start_to_close_timeout_seconds: a.execution_start_to_close_timeout_seconds,
            input: a.input.clone(),
            header: a.header.clone(),
            retry_policy: a.retry_policy.clone(),
            cron_schedule: a.cron_schedule.clone(),
            memo: a.memo.clone(),
            search_attributes: a.search_attributes.clone(),
            jitter_start_seconds: a.jitter_start_seconds,
            cron_overlap_policy: a.cron_overlap_policy,
            active_cluster_selection_policy: a.active_cluster_selection_policy.clone(),
            ..Default::default()
        };
        // Cron / decider continue-as-new recomputes the expiration and resets attempts.
        let mut attempt = 0;
        let mut expiration_timestamp = None;
        if let Some(rp) = &a.retry_policy {
            if rp.expiration_interval_in_seconds > 0 {
                let seconds = rp.expiration_interval_in_seconds as i64 + backoff.unwrap_or(0) as i64;
                expiration_timestamp = Some(now + seconds * NANOS);
            }
        }
        // Retry continue-as-new keeps the expiration and increments the attempt.
        if a.initiator == Some(ContinueAsNewInitiator::RetryPolicy) {
            attempt = prev_attempt + 1;
            if prev_expiration != 0 {
                expiration_timestamp = Some(prev_expiration);
            }
        }
        let request = HistoryStartWorkflowExecutionRequest {
            domain_uuid: self.domain.info.id.clone(),
            start_request: Some(start_request),
            parent_execution_info: parent,
            attempt,
            expiration_timestamp,
            continue_as_new_initiator: a.initiator,
            continued_failure_reason: a.failure_reason.clone(),
            continued_failure_details: a.failure_details.clone(),
            last_completion_result: a.last_completion_result.clone(),
            first_decision_task_backoff_seconds: backoff,
            partition_config,
        };

        let new_state = Arc::new(EngineMutableState::new_for_start(
            self.domain.clone(),
            params.failover_version,
            &new_run_id,
            self.sticky_ttl_nanos,
            self.now_nanos.clone(),
            params,
        )?);
        let execution = WorkflowExecution { workflow_id, run_id: new_run_id };
        let start_event = new_state
            .start_event_with(
                &execution,
                &request,
                Some(&ContinuedFrom {
                    prev_run_id,
                    first_run_id,
                    first_scheduled_time_nano: prev_attrs.first_scheduled_time_nano.unwrap_or(0),
                }),
            )
            .map_err(|_| HistoryError::internal("Failed to add workflow execution started event."))?;
        new_state.first_decision(&start_event)?;
        self.inner.lock().unwrap().new_run = Some(new_state);
        Ok(())
    }

    /// Takes the new run created by a ContinueAsNew decision.
    pub(super) fn take_new_run(&self) -> Option<Arc<EngineMutableState>> {
        self.inner.lock().unwrap().new_run.take()
    }

    /// Loads the start event of this run for the (synchronous) decision handler.
    pub(super) async fn preload_start_event(&self) {
        if self.inner.lock().unwrap().prev_start_event.is_some() {
            return;
        }
        if let Ok(event) = self.ev_start_event().await {
            self.inner.lock().unwrap().prev_start_event = Some(event);
        }
    }
}
