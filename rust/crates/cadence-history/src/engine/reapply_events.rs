//! Port of engineimpl/reapply_events.go.
use cadence_api::service_types::HistoryEvent;
use super::{deps::*, runtime::*};

/// `constants.EmptyEventID`
const EMPTY_EVENT_ID_C: i64 = EMPTY_EVENT_ID;
/// `ndc.EventsReapplicationResetWorkflowReason`
pub const EVENTS_REAPPLICATION_RESET_WORKFLOW_REASON: &str = "events-reapplication";

/// `definition.NewEventReappliedID(runID, eventID, version).GetID()`
pub(crate) fn event_reapplied_id(run_id: &str, event_id: i64, version: i64) -> String {
    format!("{run_id}::{event_id}::{version}")
}

impl HistoryEngine {
    /// `ReapplyEvents`
    pub async fn reapply_events(&self, domain_uuid: &str, workflow_id: &str, run_id: &str, reapply_events: &[HistoryEvent]) -> HResult<()> {
        let domain_entry = match self.get_active_domain_by_workflow(domain_uuid, workflow_id, run_id).await {
            Ok(entry) => entry,
            Err(err) => {
                // TODO(active-active): should we remove this check for active-active domain?
                let pending_active = self.domain_cache.get_domain_by_id(domain_uuid).map(|d| d.is_domain_pending_active()).unwrap_or(false);
                return if pending_active { Ok(()) } else { Err(err) };
            }
        };
        let domain_id = domain_entry.get_info().id.clone();
        // remove run id from the execution so that reapply events to the current run
        let current_execution = WorkflowExecution { workflow_id: workflow_id.to_string(), ..Default::default() };
        let engine = self;
        let workflow_id_owned = workflow_id.to_string();
        let domain_id_c = domain_id.clone();
        self.updater.update(&domain_id, &current_execution, true, self.time_source.now(), Box::new(move |wf_context, mutable_state| {
            let domain_id = domain_id_c.clone();
            let workflow_id_owned = workflow_id_owned.clone();
            Box::pin(async move {
                // Filter out reapply event from the same cluster
                let mut to_reapply: Vec<HistoryEvent> = Vec::with_capacity(reapply_events.len());
                let last_write_version = mutable_state.get_last_write_version()?;
                for event in reapply_events {
                    if event.version == last_write_version {
                        // The reapply is from the same cluster. Ignoring.
                        continue;
                    }
                    let dedup = event_reapplied_id(run_id, event.event_id, event.version);
                    if mutable_state.is_resource_duplicated(&dedup) {
                        // already apply the signal
                        continue;
                    }
                    to_reapply.push(event.clone());
                }
                if to_reapply.is_empty() {
                    return Ok(UpdateAction { noop: true, ..Default::default() });
                }
                if !mutable_state.is_workflow_execution_running() {
                    // need to reset target workflow (which is also the current workflow)
                    // to accept events to be reapplied
                    let base_run_id = mutable_state.get_execution_info().run_id;
                    let reset_run_id = uuid::Uuid::new_v4().to_string();
                    let base_rebuild_last_event_id = mutable_state.get_previous_started_event_id();
                    // TODO when https://github.com/uber/cadence/issues/2420 is finished, remove this block,
                    //  since cannot reapply event to a finished workflow which had no decisions started
                    if base_rebuild_last_event_id == EMPTY_EVENT_ID_C {
                        tracing::warn!(domain_id = %domain_id, workflow_id = %workflow_id_owned, "cannot reapply event to a finished workflow");
                        engine.metrics.stale_mutable_state("EventReapplySkippedCount");
                        return Ok(UpdateAction { noop: true, ..Default::default() });
                    }
                    let base_version_histories = mutable_state.get_version_histories().ok_or(HistoryError::MissingVersionHistories)?;
                    let base_current = base_version_histories.histories
                        .get(base_version_histories.current_version_history_index as usize)
                        .ok_or_else(|| HistoryError::Other("current version history index out of range".into()))?;
                    // VersionHistory.GetEventVersion
                    let base_rebuild_last_event_version = base_current.items.iter()
                        .find(|item| base_rebuild_last_event_id <= item.event_id)
                        .map(|item| item.version)
                        .ok_or_else(|| HistoryError::bad_request(format!("version history event id {base_rebuild_last_event_id} not found")))?;
                    let base_current_branch_token = base_current.branch_token.clone();
                    let base_next_event_id = mutable_state.get_next_event_id();
                    engine.workflow_resetter.reset_workflow(ResetWorkflow {
                        domain_id: domain_id.clone(),
                        workflow_id: workflow_id.to_string(),
                        base_run_id,
                        base_branch_token: base_current_branch_token,
                        rebuild_last_event_id: base_rebuild_last_event_id,
                        rebuild_last_event_version: base_rebuild_last_event_version,
                        base_next_event_id,
                        reset_run_id,
                        request_id: uuid::Uuid::new_v4().to_string(),
                        current_context: wf_context,
                        current_state: mutable_state.clone(),
                        current_release: None, // execution.NoopReleaseFn
                        reason: EVENTS_REAPPLICATION_RESET_WORKFLOW_REASON.to_string(),
                        events: to_reapply,
                        skip_signal_reapply: false,
                    }).await?;
                    return Ok(UpdateAction { noop: true, ..Default::default() });
                }
                let mut post_actions = UpdateAction { create_decision: true, noop: false };
                // Do not create decision task when the workflow is cron and the cron has not been started yet
                if !mutable_state.get_execution_info().cron_schedule.is_empty() && !mutable_state.has_processed_or_pending_decision() {
                    post_actions.create_decision = false;
                }
                let reapplied = match engine.events_reapplier.reapply_events(mutable_state.clone(), &to_reapply, run_id).await {
                    Ok(r) => r,
                    Err(err) => {
                        tracing::error!(error = %err, "failed to re-apply stale events");
                        return Err(HistoryError::internal("unable to re-apply stale events"));
                    }
                };
                if reapplied.is_empty() {
                    return Ok(UpdateAction { noop: true, ..Default::default() });
                }
                Ok(post_actions)
            })
        })).await
    }
}
