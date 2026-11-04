//! Port of engineimpl/signal_workflow_execution.go.
use super::{deps::*, runtime::*, types::*, workflow_mutations::already_completed};
use cadence_api::errors::LimitExceededError;
impl HistoryEngine {
    pub async fn signal_workflow_execution(
        &self,
        req: &HistorySignalWorkflowExecutionRequest,
    ) -> HResult<()> {
        let request = req.signal_request.clone().unwrap_or_default();
        let execution = request.workflow_execution.clone().unwrap_or_default();
        let domain = self
            .get_active_domain_by_workflow(
                &req.domain_uuid,
                &execution.workflow_id,
                &execution.run_id,
            )
            .await?;
        if domain.info.status != DOMAIN_STATUS_REGISTERED {
            return Err(HistoryError::bad_request("Domain is deprecated."));
        }
        self.updater
            .update(
                &domain.info.id,
                &execution,
                true,
                self.time_source.now(),
                Box::new(|_, state| {
                    let request = &request;
                    let domain = &domain;
                    Box::pin(async move {
                        if !request.request_id.is_empty()
                            && state.is_signal_requested(&request.request_id)
                        {
                            return Ok(UpdateAction {
                                noop: true,
                                ..Default::default()
                            });
                        }
                        if !state.is_workflow_execution_running() {
                            return Err(already_completed());
                        }
                        if self.check_for_history_corruptions(&*state).await? {
                            return Err(HistoryError::entity_not_exists(
                                "Workflow execution corrupted.",
                            ));
                        }
                        let info = state.get_execution_info();
                        let create_decision = state.has_processed_or_pending_decision();
                        let max = self.config.maximum_signals_per_execution(&domain.info.name);
                        if max > 0 && info.signal_count >= max {
                            tracing::info!(
                                signal_count = info.signal_count,
                                workflow_id = info.workflow_id,
                                run_id = info.run_id,
                                domain_id = domain.info.id,
                                "Execution limit reached for maximum signals"
                            );
                            return Err(LimitExceededError {
                                message: "exceeded workflow execution limit for signal events"
                                    .into(),
                            }
                            .into());
                        }
                        if req.child_workflow_only {
                            let parent =
                                req.external_workflow_execution.clone().unwrap_or_default();
                            if parent.workflow_id != info.parent_workflow_id
                                || parent.run_id != info.parent_run_id
                            {
                                return Err(HistoryError::entity_not_exists(
                                    "workflow parent does not match",
                                ));
                            }
                        }
                        if !request.request_id.is_empty() {
                            state.add_signal_requested(&request.request_id);
                        }
                        state
                            .add_workflow_execution_signaled(
                                &request.signal_name,
                                &request.input,
                                &request.identity,
                                &request.request_id,
                            )
                            .map_err(|_| {
                                HistoryError::internal("Unable to signal workflow execution.")
                            })?;
                        Ok(UpdateAction {
                            create_decision,
                            noop: false,
                        })
                    })
                }),
            )
            .await
    }
}
