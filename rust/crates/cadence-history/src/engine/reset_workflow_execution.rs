//! Port of engineimpl/reset_workflow_execution.go.
use cadence_api::{enums::EventType, service_types::HistoryEvent, types::{ResetWorkflowExecutionRequest, ResetWorkflowExecutionResponse}};
use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};
use super::{deps::*, runtime::*, types::*};
impl HistoryEngine {
    pub async fn reset_workflow_execution(&self, req: &HistoryResetWorkflowExecutionRequest) -> HResult<ResetWorkflowExecutionResponse> {
        let request = req.reset_request.clone().unwrap_or_default();
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let (base_context, base_release) = self.execution_cache.get_or_create_workflow_execution(&req.domain_uuid, &execution).await?;
        let mut current_release = None;
        let result = async {
            let base = base_context.load_workflow_execution().await?;
            if !base.has_processed_or_pending_decision() { return Err(HistoryError::bad_request("Cannot reset workflow without a decision task schedule.")); }
            let finish = request.decision_finish_event_id;
            if finish <= FIRST_EVENT_ID || finish > base.get_next_event_id() {
                return Err(HistoryError::bad_request("Decision finish ID must be > 1 && <= workflow next event ID."));
            }
            let domain_name = self.domain_cache.get_domain_name(&req.domain_uuid)?;
            let current_run = self.current_execution_reader.get_current_execution(&req.domain_uuid, &execution.workflow_id, &domain_name).await?;
            let (context, state) = if current_run == execution.run_id {
                (base_context.clone(), base.clone())
            } else {
                let current_execution = WorkflowExecution { workflow_id: execution.workflow_id.clone(), run_id: current_run.clone() };
                let (context, release) = self.execution_cache.get_or_create_workflow_execution(&req.domain_uuid, &current_execution).await?;
                current_release = Some(release);
                let state = context.load_workflow_execution().await?;
                (context, state)
            };
            if state.get_execution_info().create_request_id == request.request_id {
                tracing::info!(workflow_id = execution.workflow_id, run_id = current_run, domain_id = req.domain_uuid, "Duplicated reset request");
                return Ok(ResetWorkflowExecutionResponse { run_id: current_run });
            }
            let reset_run_id = uuid::Uuid::new_v4().to_string();
            let last_event_id = finish - 1;
            let mut branch = base.get_current_branch_token()?;
            let mut version = base.get_current_version();
            if let Some(histories) = base.get_version_histories() {
                let history = histories.current_history()?;
                version = history.event_version(last_event_id)?;
                branch = history.branch_token.clone();
            }
            self.validate_reset_point(&request, &branch, &req.domain_uuid).await?;
            let result = self.workflow_resetter.reset_workflow(ResetWorkflow {
                domain_id: req.domain_uuid.clone(), workflow_id: execution.workflow_id.clone(), base_run_id: execution.run_id.clone(),
                base_branch_token: branch, rebuild_last_event_id: last_event_id, rebuild_last_event_version: version,
                base_next_event_id: base.get_next_event_id(), reset_run_id: reset_run_id.clone(), request_id: request.request_id.clone(),
                current_context: context, current_state: state, current_release: current_release.clone(), reason: request.reason.clone(),
                events: Vec::new(), skip_signal_reapply: request.skip_signal_reapply,
            }).await;
            match result {
                Err(HistoryError::DuplicateRequest { request_type: WorkflowRequestType::Reset, run_id }) => Ok(ResetWorkflowExecutionResponse { run_id }),
                Err(err) => Err(err),
                Ok(()) => Ok(ResetWorkflowExecutionResponse { run_id: reset_run_id }),
            }
        }.await;
        if let Some(release) = current_release { release(result.as_ref().err()); }
        base_release(result.as_ref().err()); result
    }
    async fn validate_reset_point(&self, request: &ResetWorkflowExecutionRequest, branch: &[u8], domain: &str) -> HResult<()> {
        let mut token = Vec::new(); let mut events = Vec::new(); let mut found_batch = false;
        loop {
            let (batches, next) = self.history_reader.paginate_history(FIRST_EVENT_ID, request.decision_finish_event_id + 1, branch, &token, 100, domain).await?;
            for batch in batches {
                found_batch = true; events = batch;
                if events.last().is_some_and(|e| e.event_id >= request.decision_finish_event_id) {
                    return check_reset_event_type(&events, request.decision_finish_event_id);
                }
            }
            if next.is_empty() { break; } token = next;
        }
        if !found_batch { return Err(HistoryError::Other("workflow has corrupted or missing history".into())); }
        check_reset_event_type(&events, request.decision_finish_event_id)
    }
}
pub fn check_reset_event_type(events: &[HistoryEvent], reset_id: i64) -> HResult<()> {
    for event in events {
        if event.event_id == reset_id {
            return match event.event_type {
                Some(EventType::DecisionTaskStarted | EventType::DecisionTaskTimedOut | EventType::DecisionTaskFailed | EventType::DecisionTaskCompleted) => Ok(()),
                _ => Err(HistoryError::bad_request(format!("reset event must be of type DecisionTaskStarted, DecisionTaskTimedOut, DecisionTaskFailed, or DecisionTaskCompleted. Attempting to reset on event type: {}", event.event_type.map(|t| format!("{t:?}")).unwrap_or_default()))),
            };
        }
    }
    Err(HistoryError::Other(format!("reset event ID {reset_id} not found in the history events")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_rejection_uses_go_event_name() {
        let events = [HistoryEvent {
            event_id: 9,
            event_type: Some(EventType::ActivityTaskScheduled),
            ..Default::default()
        }];
        let error = check_reset_event_type(&events, 9).unwrap_err();
        assert_eq!(error.to_string(), "reset event must be of type DecisionTaskStarted, DecisionTaskTimedOut, DecisionTaskFailed, or DecisionTaskCompleted. Attempting to reset on event type: ActivityTaskScheduled");
    }
}
