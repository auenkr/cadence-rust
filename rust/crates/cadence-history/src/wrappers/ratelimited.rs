//! Port of wrappers/ratelimited/{handler_generated,ratelimit}.go.
use std::sync::Arc;
use async_trait::async_trait;
use crate::{handler::Handler, engine::deps::{HResult, HistoryError}};
pub trait WorkflowIdCache: Send + Sync { fn allow_external(&self, domain: &str, workflow_id: &str) -> bool; }
pub struct HistoryHandler { pub wrapped: Arc<dyn Handler>, pub workflow_id_cache: Arc<dyn WorkflowIdCache> }
impl HistoryHandler {
    fn allow_wf_id(&self, domain: &str, workflow_id: &str) -> HResult<()> {
        if domain.is_empty() { return Err(HistoryError::bad_request("Domain not set on request.")); }
        if workflow_id.is_empty() { return Err(HistoryError::bad_request("WorkflowId is not set on request.")); }
        if !self.workflow_id_cache.allow_external(domain, workflow_id) {
            return Err(cadence_api::errors::ServiceBusyError { message: "Too many requests for the workflow ID".into(), reason: Some("external-workflow-id-rate-limit".into()) }.into());
        }
        Ok(())
    }
}
#[async_trait]
impl Handler for HistoryHandler {
    fn start(&self) { self.wrapped.start(); }
    fn stop(&self) { self.wrapped.stop(); }
    async fn prepare_to_stop(&self, remaining: std::time::Duration) -> std::time::Duration { self.wrapped.prepare_to_stop(remaining).await }
    async fn health(&self) -> HResult<crate::engine::types::HealthStatus> { self.wrapped.health().await }
    async fn close_shard(&self, request: Option<&cadence_api::admin::CloseShardRequest>) -> HResult<()> {
        self.wrapped.close_shard(request).await
    }
    async fn describe_history_host(&self, request: Option<&cadence_api::admin::DescribeHistoryHostRequest>) -> HResult<cadence_api::admin::DescribeHistoryHostResponse> {
        self.wrapped.describe_history_host(request).await
    }
    async fn describe_mutable_state(&self, request: Option<&crate::engine::types::DescribeMutableStateRequest>) -> HResult<crate::engine::types::DescribeMutableStateResponse> {
        self.wrapped.describe_mutable_state(request).await
    }
    async fn describe_queue(&self, request: Option<&cadence_api::admin::DescribeQueueRequest>) -> HResult<cadence_api::admin::DescribeQueueResponse> {
        self.wrapped.describe_queue(request).await
    }
    async fn describe_workflow_execution(&self, request: Option<&crate::engine::types::HistoryDescribeWorkflowExecutionRequest>) -> HResult<cadence_api::service_types::DescribeWorkflowExecutionResponse> {
        let request = request.ok_or_else(|| HistoryError::bad_request("Request is nil."))?;
        self.allow_wf_id(&request.domain_uuid, request.request.as_ref().and_then(|r| r.execution.as_ref()).map(|e| e.workflow_id.as_str()).unwrap_or_default())?;
        self.wrapped.describe_workflow_execution(Some(request)).await
    }
    async fn get_cross_cluster_tasks(&self, request: Option<&cadence_api::admin::GetCrossClusterTasksRequest>) -> HResult<cadence_api::admin::GetCrossClusterTasksResponse> {
        self.wrapped.get_cross_cluster_tasks(request).await
    }
    async fn count_dlq_messages(&self, request: Option<&cadence_api::admin::CountDLQMessagesRequest>) -> HResult<crate::engine::types::HistoryCountDLQMessagesResponse> {
        self.wrapped.count_dlq_messages(request).await
    }
    async fn get_dlq_replication_messages(&self, request: Option<&cadence_api::admin::GetDLQReplicationMessagesRequest>) -> HResult<cadence_api::admin::GetDLQReplicationMessagesResponse> {
        self.wrapped.get_dlq_replication_messages(request).await
    }
    async fn get_mutable_state(&self, request: Option<&crate::engine::types::GetMutableStateRequest>) -> HResult<crate::engine::types::GetMutableStateResponse> {
        self.wrapped.get_mutable_state(request).await
    }
    async fn get_replication_messages(&self, request: Option<&cadence_api::admin::GetReplicationMessagesRequest>) -> HResult<cadence_api::admin::GetReplicationMessagesResponse> {
        self.wrapped.get_replication_messages(request).await
    }
    async fn merge_dlq_messages(&self, request: Option<&cadence_api::admin::MergeDLQMessagesRequest>) -> HResult<cadence_api::admin::MergeDLQMessagesResponse> {
        self.wrapped.merge_dlq_messages(request).await
    }
    async fn notify_failover_markers(&self, request: Option<&crate::engine::types::NotifyFailoverMarkersRequest>) -> HResult<()> {
        self.wrapped.notify_failover_markers(request).await
    }
    async fn poll_mutable_state(&self, request: Option<&crate::engine::types::PollMutableStateRequest>) -> HResult<crate::engine::types::PollMutableStateResponse> {
        self.wrapped.poll_mutable_state(request).await
    }
    async fn purge_dlq_messages(&self, request: Option<&cadence_api::admin::PurgeDLQMessagesRequest>) -> HResult<()> {
        self.wrapped.purge_dlq_messages(request).await
    }
    async fn query_workflow(&self, request: Option<&crate::engine::types::HistoryQueryWorkflowRequest>) -> HResult<crate::engine::types::HistoryQueryWorkflowResponse> {
        self.wrapped.query_workflow(request).await
    }
    async fn read_dlq_messages(&self, request: Option<&cadence_api::admin::ReadDLQMessagesRequest>) -> HResult<cadence_api::admin::ReadDLQMessagesResponse> {
        self.wrapped.read_dlq_messages(request).await
    }
    async fn reapply_events(&self, request: Option<&crate::engine::types::HistoryReapplyEventsRequest>) -> HResult<()> {
        self.wrapped.reapply_events(request).await
    }
    async fn record_activity_task_heartbeat(&self, request: Option<&crate::engine::types::HistoryRecordActivityTaskHeartbeatRequest>) -> HResult<cadence_api::tasks::RecordActivityTaskHeartbeatResponse> {
        self.wrapped.record_activity_task_heartbeat(request).await
    }
    async fn record_activity_task_started(&self, request: Option<&crate::engine::types::RecordActivityTaskStartedRequest>) -> HResult<crate::engine::types::RecordActivityTaskStartedResponse> {
        self.wrapped.record_activity_task_started(request).await
    }
    async fn record_child_execution_completed(&self, request: Option<&crate::engine::types::RecordChildExecutionCompletedRequest>) -> HResult<()> {
        self.wrapped.record_child_execution_completed(request).await
    }
    async fn record_decision_task_started(&self, request: Option<&crate::engine::types::RecordDecisionTaskStartedRequest>) -> HResult<crate::engine::types::RecordDecisionTaskStartedResponse> {
        self.wrapped.record_decision_task_started(request).await
    }
    async fn refresh_workflow_tasks(&self, request: Option<&crate::engine::types::HistoryRefreshWorkflowTasksRequest>) -> HResult<()> {
        self.wrapped.refresh_workflow_tasks(request).await
    }
    async fn remove_signal_mutable_state(&self, request: Option<&crate::engine::types::RemoveSignalMutableStateRequest>) -> HResult<()> {
        self.wrapped.remove_signal_mutable_state(request).await
    }
    async fn remove_task(&self, request: Option<&cadence_api::admin::RemoveTaskRequest>) -> HResult<()> {
        self.wrapped.remove_task(request).await
    }
    async fn replicate_events_v2(&self, request: Option<&crate::engine::types::ReplicateEventsV2Request>) -> HResult<()> {
        self.wrapped.replicate_events_v2(request).await
    }
    async fn request_cancel_workflow_execution(&self, request: Option<&crate::engine::types::HistoryRequestCancelWorkflowExecutionRequest>) -> HResult<()> {
        self.wrapped.request_cancel_workflow_execution(request).await
    }
    async fn reset_queue(&self, request: Option<&cadence_api::admin::ResetQueueRequest>) -> HResult<()> {
        self.wrapped.reset_queue(request).await
    }
    async fn reset_sticky_task_list(&self, request: Option<&crate::engine::types::HistoryResetStickyTaskListRequest>) -> HResult<crate::engine::types::HistoryResetStickyTaskListResponse> {
        self.wrapped.reset_sticky_task_list(request).await
    }
    async fn reset_workflow_execution(&self, request: Option<&crate::engine::types::HistoryResetWorkflowExecutionRequest>) -> HResult<cadence_api::types::ResetWorkflowExecutionResponse> {
        self.wrapped.reset_workflow_execution(request).await
    }
    async fn respond_activity_task_canceled(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCanceledRequest>) -> HResult<()> {
        self.wrapped.respond_activity_task_canceled(request).await
    }
    async fn respond_activity_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCompletedRequest>) -> HResult<()> {
        self.wrapped.respond_activity_task_completed(request).await
    }
    async fn respond_activity_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskFailedRequest>) -> HResult<()> {
        self.wrapped.respond_activity_task_failed(request).await
    }
    async fn respond_cross_cluster_tasks_completed(&self, request: Option<&cadence_api::admin::RespondCrossClusterTasksCompletedRequest>) -> HResult<cadence_api::admin::RespondCrossClusterTasksCompletedResponse> {
        self.wrapped.respond_cross_cluster_tasks_completed(request).await
    }
    async fn respond_decision_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskCompletedRequest>) -> HResult<crate::engine::types::HistoryRespondDecisionTaskCompletedResponse> {
        self.wrapped.respond_decision_task_completed(request).await
    }
    async fn respond_decision_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskFailedRequest>) -> HResult<()> {
        self.wrapped.respond_decision_task_failed(request).await
    }
    async fn schedule_decision_task(&self, request: Option<&crate::engine::types::ScheduleDecisionTaskRequest>) -> HResult<()> {
        self.wrapped.schedule_decision_task(request).await
    }
    async fn signal_with_start_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWithStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> {
        let request = request.ok_or_else(|| HistoryError::bad_request("Request is nil."))?;
        self.allow_wf_id(&request.domain_uuid, request.signal_with_start_request.as_ref().map(|r| r.workflow_id.as_str()).unwrap_or_default())?;
        self.wrapped.signal_with_start_workflow_execution(Some(request)).await
    }
    async fn signal_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWorkflowExecutionRequest>) -> HResult<()> {
        let request = request.ok_or_else(|| HistoryError::bad_request("Request is nil."))?;
        self.allow_wf_id(&request.domain_uuid, request.signal_request.as_ref().and_then(|r| r.workflow_execution.as_ref()).map(|e| e.workflow_id.as_str()).unwrap_or_default())?;
        self.wrapped.signal_workflow_execution(Some(request)).await
    }
    async fn start_workflow_execution(&self, request: Option<&crate::engine::types::HistoryStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> {
        let request = request.ok_or_else(|| HistoryError::bad_request("Request is nil."))?;
        self.allow_wf_id(&request.domain_uuid, request.start_request.as_ref().map(|r| r.workflow_id.as_str()).unwrap_or_default())?;
        self.wrapped.start_workflow_execution(Some(request)).await
    }
    async fn sync_activity(&self, request: Option<&crate::engine::types::SyncActivityRequest>) -> HResult<()> {
        self.wrapped.sync_activity(request).await
    }
    async fn sync_shard_status(&self, request: Option<&crate::engine::types::SyncShardStatusRequest>) -> HResult<()> {
        self.wrapped.sync_shard_status(request).await
    }
    async fn terminate_workflow_execution(&self, request: Option<&crate::engine::types::HistoryTerminateWorkflowExecutionRequest>) -> HResult<()> {
        self.wrapped.terminate_workflow_execution(request).await
    }
    async fn get_failover_info(&self, request: Option<&crate::engine::types::GetFailoverInfoRequest>) -> HResult<crate::engine::types::GetFailoverInfoResponse> {
        self.wrapped.get_failover_info(request).await
    }
    async fn ratelimit_update(&self, request: Option<&crate::engine::types::RatelimitUpdateRequest>) -> HResult<crate::engine::types::RatelimitUpdateResponse> {
        self.wrapped.ratelimit_update(request).await
    }
}
