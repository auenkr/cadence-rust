//! Delegations in history_engine.go and record/respond_decision_task_*.go.
use super::{deps::*, runtime::*, types::*};
impl HistoryEngine {
    pub async fn schedule_decision_task(&self, req: &ScheduleDecisionTaskRequest) -> HResult<()> {
        self.decision_handler.handle_decision_task_scheduled(req).await
    }
    pub async fn record_decision_task_started(&self, req: &RecordDecisionTaskStartedRequest) -> HResult<RecordDecisionTaskStartedResponse> {
        self.decision_handler.handle_decision_task_started(req).await
    }
    pub async fn respond_decision_task_completed(&self, req: &HistoryRespondDecisionTaskCompletedRequest) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        self.decision_handler.handle_decision_task_completed(req).await
    }
    pub async fn respond_decision_task_failed(&self, req: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()> {
        self.decision_handler.handle_decision_task_failed(req).await
    }
    pub async fn replicate_events_v2(&self, req: &ReplicateEventsV2Request) -> HResult<()> {
        self.replicator.apply_events(req).await
    }
    pub async fn sync_activity(&self, req: &SyncActivityRequest) -> HResult<()> {
        self.replicator.sync_activity(req).await
    }
    pub async fn sync_shard_status(&self, req: &SyncShardStatusRequest) -> HResult<()> {
        self.shard_clock.set_current_time(&req.source_cluster, req.timestamp.unwrap_or_default());
        for processor in self.queue_processors.values() {
            processor.notify_new_task(&req.source_cluster, &[]);
        }
        Ok(())
    }
    pub async fn check_for_history_corruptions(&self, state: &dyn MutableState) -> HResult<bool> {
        let domain = state.get_domain_entry();
        if !self.config.enable_history_corruption_check(&domain.info.name) { return Ok(false); }
        match state.get_start_event().await {
            Ok(_) => Ok(false),
            Err(HistoryError::MissingWorkflowStartEvent) => {
                state.set_workflow_state(WORKFLOW_STATE_CORRUPTED);
                let info = state.get_execution_info();
                tracing::error!(domain_name = domain.info.name, workflow_id = info.workflow_id, run_id = info.run_id, workflow_type = info.workflow_type_name, "history corruption check failed");
                Ok(true)
            }
            Err(err) => Err(err),
        }
    }
}
