//! Port of Go `ndc/activity_replicator.go`.
use cadence_api::types::VersionHistory;
use cadence_api::types::WorkflowExecution;
use cadence_persistence::history_contract::{UpdateWorkflowMode, WorkflowRequestMode};
use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};

use super::{
    new_ndc_retry_task_error_with_hint, NdcClusterMetadata, NdcError, NdcExecutionCache,
    NdcMutableState, NdcResult, SyncActivityRequest, EMPTY_EVENT_ID,
};
use crate::execution::workflow::{TransactionPolicy, WorkflowState};

pub const RESEND_MISSING_EVENT_MESSAGE: &str = "Resend missed sync activity events";
pub const RESEND_HIGHER_VERSION_MESSAGE: &str =
    "Resend sync activity events due to a higher version received";
pub const ERR_RETRY_SYNC_ACTIVITY_MSG: &str = "retry on applying sync activity";

/// Go `ActivityReplicator`.
pub trait ActivityReplicator {
    fn sync_activity(&self, request: &SyncActivityRequest) -> NdcResult<()>;
}

/// Go `activityReplicatorImpl`.
pub struct ActivityReplicatorImpl<'a> {
    pub execution_cache: &'a dyn NdcExecutionCache,
    pub cluster_metadata: &'a dyn NdcClusterMetadata,
}

impl<'a> ActivityReplicatorImpl<'a> {
    pub fn new(
        execution_cache: &'a dyn NdcExecutionCache,
        cluster_metadata: &'a dyn NdcClusterMetadata,
    ) -> Self {
        Self { execution_cache, cluster_metadata }
    }

    fn sync_activity_inner(
        &self,
        workflow: &mut dyn super::NdcWorkflow,
        request: &SyncActivityRequest,
    ) -> NdcResult<()> {
        match workflow.load_workflow_execution(None) {
            Ok(()) => {}
            Err(NdcError::EntityNotExists(_)) => return Ok(()),
            Err(e) => return Err(e),
        }
        let version = request.version;
        let schedule_id = request.scheduled_id;
        let should_apply = self.should_apply_sync_activity(
            &request.domain_id,
            &request.workflow_id,
            &request.run_id,
            schedule_id,
            workflow.mutable_state(),
            request.version_history.as_ref(),
        )?;
        if !should_apply {
            return Ok(());
        }
        let ai = match workflow.mutable_state().get_activity_info(schedule_id)? {
            Some(ai) => ai,
            None => return Ok(()),
        };
        if ai.version > version {
            return Ok(());
        }
        if ai.version == version {
            if ai.attempt > request.attempt {
                return Ok(());
            }
            if ai.attempt == request.attempt
                && ai.last_heartbeat_updated_time_nanos > request.last_heartbeat_time
            {
                return Ok(());
            }
        }
        let reset_activity_timer_task_status =
            !self.cluster_metadata.is_version_from_same_cluster(version, ai.version)
                || ai.attempt < request.attempt;
        let attempt_incremented = ai.attempt < request.attempt;
        let activity_not_started = request.started_id == EMPTY_EVENT_ID;

        workflow
            .mutable_state_mut()
            .replicate_activity_info(request, reset_activity_timer_task_status)?;

        let mut event_time = request.scheduled_time;
        if event_time < request.started_time {
            event_time = request.started_time;
        }
        if event_time < request.last_heartbeat_time {
            event_time = request.last_heartbeat_time;
        }
        let now = event_time;

        workflow.mutable_state_mut().create_next_activity_timer()?;

        if attempt_incremented && activity_not_started && !ai.cancel_requested {
            workflow
                .mutable_state_mut()
                .generate_activity_retry_tasks(schedule_id)?;
        }

        let mut update_mode = UpdateWorkflowMode::UpdateCurrent;
        if workflow.mutable_state().get_workflow_state() == WorkflowState::Zombie {
            update_mode = UpdateWorkflowMode::BypassCurrent;
        }
        workflow.update_workflow_execution_with_new(
            now,
            update_mode,
            None, // no new workflow
            TransactionPolicy::Passive,
            None,
            WorkflowRequestMode::Replicated,
        )
    }

    fn should_apply_sync_activity(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
        schedule_id: i64,
        mutable_state: &dyn NdcMutableState,
        incoming_raw: Option<&VersionHistory>,
    ) -> NdcResult<bool> {
        let histories = match mutable_state.get_version_histories() {
            Some(h) => h,
            None => return Err(NdcError::internal("The workflow version histories is corrupted.")),
        };
        if mutable_state.get_workflow_state() == WorkflowState::Completed {
            return Ok(false);
        }
        let current = histories.current_history()?;
        let last_local_item = current.last_item()?;
        let default_vh = VersionHistory::default();
        let incoming = incoming_raw.unwrap_or(&default_vh);
        let last_incoming_item = incoming.last_item()?;
        let lca_item = current.find_lca(incoming)?;

        if current.is_lca_appendable(&lca_item) || incoming.is_lca_appendable(&lca_item) {
            if schedule_id > lca_item.event_id {
                return Err(new_ndc_retry_task_error_with_hint(
                    RESEND_MISSING_EVENT_MESSAGE,
                    domain_id,
                    workflow_id,
                    run_id,
                    Some(lca_item.event_id),
                    Some(lca_item.version),
                    None,
                    None,
                ));
            }
        } else if last_incoming_item.version < last_local_item.version {
            return Ok(false);
        } else if last_incoming_item.version > last_local_item.version {
            return Err(new_ndc_retry_task_error_with_hint(
                RESEND_HIGHER_VERSION_MESSAGE,
                domain_id,
                workflow_id,
                run_id,
                Some(lca_item.event_id),
                Some(lca_item.version),
                None,
                None,
            ));
        }
        Ok(true)
    }
}

impl<'a> ActivityReplicator for ActivityReplicatorImpl<'a> {
    fn sync_activity(&self, request: &SyncActivityRequest) -> NdcResult<()> {
        let execution = WorkflowExecution {
            workflow_id: request.workflow_id.clone(),
            run_id: request.run_id.clone(),
        };
        let mut workflow = self
            .execution_cache
            .get_or_create_workflow_execution(&request.domain_id, &execution)?;
        let result = self.sync_activity_inner(workflow.as_mut(), request);
        workflow.release(result.as_ref().err());
        result
    }
}
