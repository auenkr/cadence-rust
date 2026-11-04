//! Port of engineimpl/refresh_workflow_tasks.go.
use cadence_api::types::WorkflowExecution;
use super::{deps::*, runtime::*};

impl HistoryEngine {
    /// `RefreshWorkflowTasks`
    pub async fn refresh_workflow_tasks(&self, domain_uuid: &str, workflow_execution: &WorkflowExecution) -> HResult<()> {
        let domain_entry = self.domain_cache.get_domain_by_id(domain_uuid)?;
        let domain_id = domain_entry.get_info().id.clone();
        let (wf_context, release) = self.execution_cache.get_or_create_workflow_execution(&domain_id, workflow_execution).await?;
        let result = async {
            let mutable_state = wf_context.load_workflow_execution().await?;
            self.task_refresher.refresh_tasks(mutable_state.get_execution_info().start_timestamp, mutable_state.clone()).await?;
            wf_context.update_workflow_execution_tasks(self.time_source.now()).await?;
            Ok(())
        }.await;
        release(result.as_ref().err());
        result
    }
}
