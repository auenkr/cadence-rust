//! Port of engineimpl/respond_activity_task_{completed,failed,canceled,heartbeat}.go.
use super::{
    deps::*,
    runtime::*,
    types::*,
    workflow_mutations::{already_completed, get_schedule_id},
};
use cadence_api::{
    tasks::*,
    tasktoken::{JsonTaskTokenSerializer, TaskToken},
};
fn token(data: &[u8]) -> HResult<TaskToken> {
    JsonTaskTokenSerializer::deserialize(data)
        .map_err(|_| HistoryError::bad_request("error deserializing task token"))
}
fn activity(
    state: &dyn MutableState,
    token: &TaskToken,
    metrics: &dyn EngineMetrics,
    operation: &str,
) -> HResult<ActivityInfo> {
    if !state.is_workflow_execution_running() {
        return Err(already_completed());
    }
    let schedule_id = if token.schedule_id == EMPTY_EVENT_ID {
        get_schedule_id(&token.activity_id, state)?
    } else {
        token.schedule_id
    };
    let info = state.get_activity_info(schedule_id);
    if info.is_none() && schedule_id >= state.get_next_event_id() {
        metrics.stale_mutable_state(operation);
        tracing::error!(
            schedule_id,
            next_event_id = state.get_next_event_id(),
            "Encounter stale mutable state in {}",
            operation
        );
        return Err(HistoryError::StaleState);
    }
    match info {
        Some(ai)
            if ai.started_id != EMPTY_EVENT_ID
                && (token.schedule_id == EMPTY_EVENT_ID
                    || token.schedule_attempt == i64::from(ai.attempt)) =>
        {
            Ok(ai)
        }
        _ => Err(HistoryError::entity_not_exists("activity task not found")),
    }
}
impl HistoryEngine {
    pub async fn respond_activity_task_completed(
        &self,
        req: &HistoryRespondActivityTaskCompletedRequest,
    ) -> HResult<()> {
        let request = req.complete_request.clone().unwrap_or_default();
        let token = token(&request.task_token)?;
        let domain = self
            .get_active_domain_by_workflow(&req.domain_uuid, &token.workflow_id, &token.run_id)
            .await?;
        let execution = WorkflowExecution {
            workflow_id: token.workflow_id.clone(),
            run_id: token.run_id.clone(),
        };
        let completed = std::sync::Mutex::new(None);
        let result = self
            .updater
            .update(
                &domain.info.id,
                &execution,
                false,
                self.time_source.now(),
                Box::new(|_, state| {
                    let completed = &completed;
                    let token = &token;
                    let request = &request;
                    Box::pin(async move {
                        let ai = activity(
                            &*state,
                            token,
                            &*self.metrics,
                            "RecordActivityTaskCompleted",
                        )?;
                        state
                            .add_activity_task_completed_event(
                                ai.schedule_id,
                                ai.started_id,
                                request,
                            )
                            .map_err(|_| {
                                HistoryError::internal(
                                    "Unable to add ActivityTaskCompleted event to history.",
                                )
                            })?;
                        *completed.lock().unwrap() = Some(ai);
                        Ok(UpdateAction {
                            create_decision: true,
                            ..Default::default()
                        })
                    })
                }),
            )
            .await;
        if result.is_ok() {
            self.record_activity_latency(
                "RespondActivityTaskCompleted",
                &domain.info.name,
                &token,
                completed.into_inner().unwrap(),
            );
        }
        result
    }
    pub async fn respond_activity_task_failed(
        &self,
        req: &HistoryRespondActivityTaskFailedRequest,
    ) -> HResult<()> {
        let request = req.failed_request.clone().unwrap_or_default();
        let token = token(&request.task_token)?;
        let domain = self
            .get_active_domain_by_workflow(&req.domain_uuid, &token.workflow_id, &token.run_id)
            .await?;
        let execution = WorkflowExecution {
            workflow_id: token.workflow_id.clone(),
            run_id: token.run_id.clone(),
        };
        let completed = std::sync::Mutex::new(None);
        let result = self
            .updater
            .update(
                &domain.info.id,
                &execution,
                false,
                self.time_source.now(),
                Box::new(|_, state| {
                    let completed = &completed;
                    let token = &token;
                    let request = &request;
                    Box::pin(async move {
                        let ai =
                            activity(&*state, token, &*self.metrics, "RecordActivityTaskFailed")?;
                        let retry = state.retry_activity(
                            &ai,
                            request.reason.as_deref().unwrap_or_default(),
                            &request.details,
                            request.failure_options.as_ref(),
                        )?;
                        if !retry {
                            state
                                .add_activity_task_failed_event(
                                    ai.schedule_id,
                                    ai.started_id,
                                    request,
                                )
                                .map_err(|_| {
                                    HistoryError::internal(
                                        "Unable to add ActivityTaskFailed event to history.",
                                    )
                                })?;
                        }
                        *completed.lock().unwrap() = Some(ai);
                        Ok(UpdateAction {
                            create_decision: !retry,
                            ..Default::default()
                        })
                    })
                }),
            )
            .await;
        if result.is_ok() {
            self.record_activity_latency(
                "RespondActivityTaskFailed",
                &domain.info.name,
                &token,
                completed.into_inner().unwrap(),
            );
        }
        result
    }
    pub async fn respond_activity_task_canceled(
        &self,
        req: &HistoryRespondActivityTaskCanceledRequest,
    ) -> HResult<()> {
        let request = req.cancel_request.clone().unwrap_or_default();
        let token = token(&request.task_token)?;
        let domain = self
            .get_active_domain_by_workflow(&req.domain_uuid, &token.workflow_id, &token.run_id)
            .await?;
        let execution = WorkflowExecution {
            workflow_id: token.workflow_id.clone(),
            run_id: token.run_id.clone(),
        };
        let completed = std::sync::Mutex::new(None);
        let result = self
            .updater
            .update(
                &domain.info.id,
                &execution,
                false,
                self.time_source.now(),
                Box::new(|_, state| {
                    let completed = &completed;
                    let token = &token;
                    let request = &request;
                    Box::pin(async move {
                        let ai =
                            activity(&*state, token, &*self.metrics, "RecordActivityTaskCanceled")?;
                        state
                            .add_activity_task_canceled_event(
                                ai.schedule_id,
                                ai.started_id,
                                ai.cancel_request_id,
                                &request.details,
                                &request.identity,
                            )
                            .map_err(|_| {
                                HistoryError::internal(
                                    "Unable to add ActivityTaskCanceled event to history.",
                                )
                            })?;
                        *completed.lock().unwrap() = Some(ai);
                        Ok(UpdateAction {
                            create_decision: true,
                            ..Default::default()
                        })
                    })
                }),
            )
            .await;
        if result.is_ok() {
            self.record_activity_latency(
                "HistoryClientRespondActivityTaskCanceled",
                &domain.info.name,
                &token,
                completed.into_inner().unwrap(),
            );
        }
        result
    }
    pub async fn record_activity_task_heartbeat(
        &self,
        req: &HistoryRecordActivityTaskHeartbeatRequest,
    ) -> HResult<RecordActivityTaskHeartbeatResponse> {
        let request = req.heartbeat_request.clone().unwrap_or_default();
        let token = token(&request.task_token)?;
        let domain = self
            .get_active_domain_by_workflow(&req.domain_uuid, &token.workflow_id, &token.run_id)
            .await?;
        let execution = WorkflowExecution {
            workflow_id: token.workflow_id.clone(),
            run_id: token.run_id.clone(),
        };
        let cancel_requested = std::sync::atomic::AtomicBool::new(false);
        self.updater.update(&domain.info.id, &execution, false, self.time_source.now(), Box::new(|_, state| {
            let cancel_requested = &cancel_requested; let token = &token; let request = &request;
            Box::pin(async move {
                let ai = activity(&*state, token, &*self.metrics, "RecordActivityTaskHeartbeat")?;
                cancel_requested.store(ai.cancel_requested, std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(schedule_event_id = ai.schedule_id, activity_info = ?ai, cancel_requested = ai.cancel_requested, "Activity HeartBeat");
                state.update_activity_progress(&ai, request);
                Ok(UpdateAction::default())
            })
        })).await?;
        Ok(RecordActivityTaskHeartbeatResponse {
            cancel_requested: cancel_requested.into_inner(),
        })
    }
    fn record_activity_latency(
        &self,
        operation: &str,
        domain: &str,
        token: &TaskToken,
        info: Option<ActivityInfo>,
    ) {
        if let Some(ai) = info.filter(|a| a.started_time != 0) {
            self.metrics.activity_e2e_latency(
                operation,
                domain,
                &token.workflow_type,
                &token.activity_type,
                &ai.task_list,
                ai.started_time,
            );
        }
    }
}
