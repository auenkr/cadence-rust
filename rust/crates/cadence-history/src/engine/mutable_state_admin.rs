//! Port of engineimpl/{describe_mutable_state,refresh_workflow_tasks}.go.
use super::{common_util::validate_domain_uuid, deps::*, runtime::*, types::*};
impl HistoryEngine {
    pub async fn describe_mutable_state(&self, request: &DescribeMutableStateRequest) -> HResult<DescribeMutableStateResponse> {
        validate_domain_uuid(&request.domain_uuid)?;
        let execution = request.execution.clone().unwrap_or_default();
        let (cache, db, release, cache_hit) = self.execution_cache.get_and_create_workflow_execution(&request.domain_uuid, &execution).await?;
        let result = async {
            let mut response = DescribeMutableStateResponse::default();
            if cache_hit {
                if let Some(state) = cache.get_workflow_execution() { response.mutable_state_in_cache = to_mutable_state_json(&*state)?; }
            }
            response.mutable_state_in_database = to_mutable_state_json(&*db.load_workflow_execution().await?)?;
            Ok(response)
        }.await;
        release(result.as_ref().err());
        result
    }
    pub async fn refresh_workflow_tasks(&self, domain_uuid: &str, execution: &WorkflowExecution) -> HResult<()> {
        let domain = self.domain_cache.get_domain_by_id(domain_uuid)?;
        let (context, release) = self.execution_cache.get_or_create_workflow_execution(&domain.info.id, execution).await?;
        let result = async {
            let state = context.load_workflow_execution().await?;
            self.task_refresher.refresh_tasks(state.get_execution_info().start_timestamp, state).await?;
            context.update_workflow_execution_tasks(self.time_source.now()).await
        }.await;
        release(result.as_ref().err());
        result
    }
}
pub fn to_mutable_state_json(state: &dyn MutableState) -> HResult<String> {
    serde_json::to_string(&state.copy_to_persistence()?).map_err(|err| HistoryError::Other(err.to_string()))
}
