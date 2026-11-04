//! Port of engineimpl/reapply_events.go.
use cadence_api::{service_types::HistoryEvent, types::WorkflowExecution};
use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};
use super::{deps::*, runtime::*};

impl HistoryEngine {
    pub async fn reapply_events(&self, domain_uuid: &str, workflow_id: &str, run_id: &str, events: &[HistoryEvent]) -> HResult<()> {
        let domain = match self.get_active_domain_by_workflow(domain_uuid, workflow_id, run_id).await {
            Ok(domain) => domain,
            Err(error) => {
                // Go retains the domain entry on active-domain lookup failure.
                if self.domain_cache.get_domain_by_id(domain_uuid)
                    .is_ok_and(|entry| entry.is_domain_pending_active) { return Ok(()); }
                return Err(error);
            }
        };
        // Always reapply to the current execution, irrespective of the source run.
        let execution = WorkflowExecution { workflow_id: workflow_id.into(), run_id: String::new() };
        self.updater.update(&domain.info.id, &execution, false, self.time_source.now(), Box::new(|context, state| {
            let domain = &domain; let execution = &execution;
            Box::pin(async move {
                let last_version = state.get_last_write_version()?;
                let to_reapply: Vec<_> = events.iter().filter(|event| {
                    event.version != last_version && !state.is_event_reapplied(run_id, event.event_id, event.version)
                }).cloned().collect();
                if to_reapply.is_empty() { return Ok(UpdateAction { noop: true, ..Default::default() }); }
                if !state.is_workflow_execution_running() {
                    let info = state.get_execution_info();
                    let last_event_id = state.get_previous_started_event_id();
                    if last_event_id == EMPTY_EVENT_ID {
                        tracing::warn!(domain_id = domain.info.id, workflow_id = execution.workflow_id,
                            "cannot reapply event to a finished workflow");
                        self.metrics.event_reapply_skipped();
                        return Ok(UpdateAction { noop: true, ..Default::default() });
                    }
                    let histories = state.get_version_histories().ok_or(HistoryError::MissingVersionHistories)?;
                    let history = histories.current_history()?;
                    let last_event_version = history.event_version(last_event_id)?;
                    self.workflow_resetter.reset_workflow(ResetWorkflow {
                        domain_id: domain.info.id.clone(), workflow_id: workflow_id.into(), base_run_id: info.run_id,
                        base_branch_token: history.branch_token.clone(), rebuild_last_event_id: last_event_id,
                        rebuild_last_event_version: last_event_version, base_next_event_id: state.get_next_event_id(),
                        reset_run_id: uuid::Uuid::new_v4().to_string(), request_id: uuid::Uuid::new_v4().to_string(),
                        current_context: context, current_state: state, current_release: None,
                        reason: "events-reapplication".into(), events: to_reapply, skip_signal_reapply: false,
                    }).await?;
                    return Ok(UpdateAction { noop: true, ..Default::default() });
                }
                let create_decision = state.get_execution_info().cron_schedule.is_empty()
                    || state.has_processed_or_pending_decision();
                let reapplied = self.events_reapplier.reapply_events(state, &to_reapply, run_id).await.map_err(|error| {
                    tracing::error!(error = %error, "failed to re-apply stale events");
                    HistoryError::internal("unable to re-apply stale events")
                })?;
                if reapplied.is_empty() { return Ok(UpdateAction { noop: true, ..Default::default() }); }
                Ok(UpdateAction { create_decision, noop: false })
            })
        })).await
    }
}
