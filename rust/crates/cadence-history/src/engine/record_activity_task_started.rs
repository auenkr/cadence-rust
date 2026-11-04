//! Port of engineimpl/record_activity_task_started.go.
use cadence_api::errors::EventAlreadyStartedError;
use super::{deps::*, runtime::*, types::*};
impl HistoryEngine {
    pub async fn record_activity_task_started(&self, request: &RecordActivityTaskStartedRequest) -> HResult<RecordActivityTaskStartedResponse> {
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let domain = self.get_active_domain_by_workflow(&request.domain_uuid, &execution.workflow_id, &execution.run_id).await?;
        let response = std::sync::Mutex::new(RecordActivityTaskStartedResponse::default());
        let resurrected = std::sync::atomic::AtomicBool::new(false);
        self.updater.update(&domain.info.id, &execution, false, self.time_source.now(), Box::new(|_, state| {
            let response = &response; let resurrected = &resurrected; let domain = &domain;
            Box::pin(async move {
                if !state.is_workflow_execution_running() { return Err(HistoryError::entity_not_exists("workflow execution already completed")); }
                let schedule_id = request.schedule_id;
                let info = state.get_activity_info(schedule_id);
                if let Some(ai) = &info {
                    if self.time_source.now() > ai.scheduled_time.saturating_add(i64::from(ai.schedule_to_close_timeout).saturating_mul(1_000_000_000)) {
                        let activities = state.get_resurrected_activities().await.map_err(|err| { tracing::error!(error = %err, "Activity resurrection check failed"); err })?;
                        if activities.contains(&schedule_id) {
                            tracing::error!(domain_name = domain.info.name, schedule_id, "Encounter resurrected activity, skip");
                            state.delete_activity(schedule_id)?;
                            resurrected.store(true, std::sync::atomic::Ordering::Relaxed);
                            return Ok(UpdateAction::default());
                        }
                    }
                }
                if info.is_none() && schedule_id >= state.get_next_event_id() {
                    self.metrics.stale_mutable_state("RecordActivityTaskStarted");
                    tracing::error!(schedule_id, next_event_id = state.get_next_event_id(), "Encounter stale mutable state in RecordActivityTaskStarted");
                    return Err(HistoryError::StaleState);
                }
                let ai = info.ok_or_else(|| HistoryError::entity_not_exists("activity task not found"))?;
                let scheduled_event = state.get_activity_scheduled_event(schedule_id).await?;
                let mut value = RecordActivityTaskStartedResponse {
                    scheduled_event: Some(scheduled_event), scheduled_timestamp_of_this_attempt: Some(ai.scheduled_time),
                    attempt: i64::from(ai.attempt), heartbeat_details: ai.details.clone(), workflow_type: state.get_workflow_type(),
                    workflow_domain: domain.info.name.clone(), ..Default::default()
                };
                if ai.started_id != EMPTY_EVENT_ID {
                    if ai.request_id == request.request_id {
                        value.started_timestamp = Some(ai.started_time);
                        *response.lock().unwrap() = value;
                        return Ok(UpdateAction::default());
                    }
                    return Err(EventAlreadyStartedError { message: "Activity task already started.".into() }.into());
                }
                let identity = request.poll_request.as_ref().map(|r| r.identity.as_str()).unwrap_or_default();
                let ai = state.add_activity_task_started_event(&ai, schedule_id, &request.request_id, identity)?;
                value.started_timestamp = Some(ai.started_time);
                *response.lock().unwrap() = value;
                Ok(UpdateAction::default())
            })
        })).await?;
        if resurrected.into_inner() { return Err(HistoryError::entity_not_exists("activity task not found")); }
        Ok(response.into_inner().unwrap())
    }
}
