//! Port of engineimpl/poll_mutable_state.go.
use std::time::{Duration, Instant};
use cadence_api::{enums::TaskListKind, errors::EntityNotExistsError, types::{TaskList, VersionHistory, VersionHistoryItem, WorkflowType}};
use cadence_persistence::version_history::{VersionHistoriesExt, VersionHistoryExt};
use super::{common_util::validate_domain_uuid, deps::*, runtime::*, types::*};
impl HistoryEngine {
    pub async fn get_mutable_state(&self, request: &GetMutableStateRequest, deadline: Option<Instant>) -> HResult<GetMutableStateResponse> {
        self.get_mutable_state_or_long_poll(request, deadline).await
    }
    pub async fn poll_mutable_state(&self, request: &PollMutableStateRequest, deadline: Option<Instant>) -> HResult<PollMutableStateResponse> {
        let req = GetMutableStateRequest { domain_uuid: request.domain_uuid.clone(), execution: request.execution.clone(),
            expected_next_event_id: request.expected_next_event_id, current_branch_token: request.current_branch_token.clone(), version_history_item: request.version_history_item.clone() };
        let response = match self.get_mutable_state_or_long_poll(&req, deadline).await {
            Ok(response) => response,
            Err(err) => return Err(self.update_entity_not_exists_error_on_passive_cluster(err, &request.domain_uuid, &request.execution.clone().unwrap_or_default()).await),
        };
        Ok(PollMutableStateResponse { execution: response.execution, workflow_type: response.workflow_type,
            next_event_id: response.next_event_id, previous_started_event_id: response.previous_started_event_id,
            last_first_event_id: response.last_first_event_id, task_list: response.task_list, sticky_task_list: response.sticky_task_list,
            client_library_version: response.client_library_version, client_feature_version: response.client_feature_version, client_impl: response.client_impl,
            sticky_task_list_schedule_to_start_timeout: response.sticky_task_list_schedule_to_start_timeout, current_branch_token: response.current_branch_token,
            version_histories: response.version_histories, workflow_state: response.workflow_state, workflow_close_state: response.workflow_close_state })
    }
    async fn load_mutable_state(&self, domain_id: &str, execution: &WorkflowExecution) -> HResult<GetMutableStateResponse> {
        let (context, release) = self.execution_cache.get_or_create_workflow_execution(domain_id, execution).await?;
        let result = async {
            let state = context.load_workflow_execution().await?;
            let current_branch_token = state.get_current_branch_token()?;
            let info = state.get_execution_info();
            let mut execution = execution.clone(); execution.run_id = context.get_execution().run_id;
            Ok(GetMutableStateResponse {
                execution: Some(execution), workflow_type: Some(WorkflowType { name: info.workflow_type_name }),
                last_first_event_id: state.get_last_first_event_id(), next_event_id: state.get_next_event_id(), previous_started_event_id: Some(state.get_previous_started_event_id()),
                task_list: Some(TaskList { name: info.task_list, ..Default::default() }),
                sticky_task_list: Some(TaskList { name: info.sticky_task_list, kind: Some(TaskListKind::Sticky) }),
                client_library_version: info.client_library_version, client_feature_version: info.client_feature_version, client_impl: info.client_impl,
                is_workflow_running: state.is_workflow_execution_running(), sticky_task_list_schedule_to_start_timeout: Some(info.sticky_schedule_to_start_timeout),
                current_branch_token, workflow_state: Some(info.state), workflow_close_state: Some(info.close_status),
                is_sticky_task_list_enabled: state.is_sticky_task_list_enabled(), history_size: state.get_history_size(), version_histories: state.get_version_histories(), ..Default::default()
            })
        }.await;
        release(result.as_ref().err()); result
    }
    async fn update_entity_not_exists_error_on_passive_cluster(&self, err: HistoryError, domain_id: &str, execution: &WorkflowExecution) -> HistoryError {
        if err.is_entity_not_exists() {
            if let Ok(active) = self.active_cluster_manager.active_cluster_by_workflow(domain_id, &execution.workflow_id, &execution.run_id).await {
                let current = self.cluster_metadata.get_current_cluster_name();
                if active != current { return EntityNotExistsError { message: "Workflow execution not found in non-active cluster".into(), active_cluster: active, current_cluster: current }.into(); }
            }
        }
        err
    }
    async fn get_mutable_state_with_current_version_history(&self, domain_id: &str, execution: &WorkflowExecution) -> HResult<(GetMutableStateResponse, VersionHistory)> {
        let state = self.load_mutable_state(domain_id, execution).await?;
        let histories = state.version_histories.as_ref().ok_or(HistoryError::MissingVersionHistories)?;
        let history = histories.current_history().map_err(|err| HistoryError::Other(format!("unable to get version history while looking up response for mutable state: {err}")))?.clone();
        Ok((state, history))
    }
    async fn get_mutable_state_or_long_poll(&self, request: &GetMutableStateRequest, deadline: Option<Instant>) -> HResult<GetMutableStateResponse> {
        validate_domain_uuid(&request.domain_uuid)?;
        let mut execution = request.execution.clone().unwrap_or_default();
        let (state, history) = self.get_mutable_state_with_current_version_history(&request.domain_uuid, &execution).await?;
        let item = match &request.version_history_item { Some(item) => item.clone(), None => history.last_item()? };
        if !history.contains_item(&item) { return Err(branch_changed(&state, false)); }
        execution.run_id = state.execution.as_ref().map(|e| e.run_id.clone()).unwrap_or_default();
        let expected = if request.expected_next_event_id == 0 { FIRST_EVENT_ID } else { request.expected_next_event_id };
        if expected < state.next_event_id || !state.is_workflow_running { return Ok(state); }
        self.long_poll_for_event_id(expected, &request.domain_uuid, &execution, &item, deadline).await
    }
    async fn long_poll_for_event_id(&self, expected: i64, domain_id: &str, execution: &WorkflowExecution, item: &VersionHistoryItem, deadline: Option<Instant>) -> HResult<GetMutableStateResponse> {
        let (subscriber, mut channel) = self.history_event_notifier.watch_history_event(domain_id, execution)?;
        let result = async {
            // Recheck after subscribing to close the load/subscribe race.
            let (mut state, history) = self.get_mutable_state_with_current_version_history(domain_id, execution).await?;
            if !history.contains_item(item) { return Err(branch_changed(&state, false)); }
            if expected < state.next_event_id || !state.is_workflow_running { return Ok(state); }
            let domain = self.domain_cache.get_domain_name(domain_id)?;
            let mut expiration = self.config.long_poll_expiration_interval(&domain);
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let buffer = Duration::from_millis(50);
                if remaining < buffer { return Err(HistoryError::DeadlineExceeded); }
                expiration = expiration.min(remaining - buffer);
            }
            if expiration.is_zero() { return Ok(state); }
            let timer = tokio::time::sleep(expiration); tokio::pin!(timer);
            loop {
                tokio::select! {
                    event = channel.recv() => {
                        let event = event.ok_or(HistoryError::Canceled)?;
                        state.last_first_event_id = event.last_first_event_id; state.next_event_id = event.next_event_id;
                        state.is_workflow_running = event.workflow_close_state == WORKFLOW_CLOSE_STATUS_NONE;
                        state.previous_started_event_id = Some(event.previous_started_event_id);
                        state.workflow_state = Some(event.workflow_state); state.workflow_close_state = Some(event.workflow_close_state);
                        let history = event.version_histories.current_history().map_err(|err| HistoryError::Other(format!("unexpected error getting current version history while polling for mutable state: {err}")))?;
                        if !history.contains_item(item) { return Err(branch_changed(&state, true)); }
                        if expected < state.next_event_id || !state.is_workflow_running { return Ok(state); }
                    }
                    _ = &mut timer => return Ok(state),
                }
            }
        }.await;
        self.history_event_notifier.unwatch_history_event(domain_id, execution, &subscriber);
        result
    }
    pub fn notify_new_history_event(&self, event: HistoryNotification) { self.history_event_notifier.notify_new_history_event(event); }
}
fn branch_changed(state: &GetMutableStateResponse, polling: bool) -> HistoryError {
    HistoryError::CurrentBranchChanged {
        message: if polling { "current and requested version histories don't match - changed while polling" } else { "current branch token and request branch token doesn't match" }.into(),
        current_branch_token: state.current_branch_token.clone(),
    }
}
