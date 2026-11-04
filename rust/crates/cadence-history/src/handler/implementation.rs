//! Concrete implementation of the complete History handler interface.
use async_trait::async_trait;
use crate::engine::deps::HResult;
use super::{Handler, runtime::HistoryHandler};
#[async_trait]
impl Handler for HistoryHandler {
    fn start(&self) { HistoryHandler::start(self); }
    fn stop(&self) { HistoryHandler::stop(self); }
    async fn prepare_to_stop(&self, remaining: std::time::Duration) -> std::time::Duration { HistoryHandler::prepare_to_stop(self, remaining).await }
    async fn health(&self) -> HResult<crate::engine::types::HealthStatus> { HistoryHandler::health(self).await }
    async fn close_shard(&self, request: Option<&cadence_api::admin::CloseShardRequest>) -> HResult<()> { HistoryHandler::close_shard(self, request).await }
    async fn describe_history_host(&self, request: Option<&cadence_api::admin::DescribeHistoryHostRequest>) -> HResult<cadence_api::admin::DescribeHistoryHostResponse> { HistoryHandler::describe_history_host(self, request).await }
    async fn describe_mutable_state(&self, request: Option<&crate::engine::types::DescribeMutableStateRequest>) -> HResult<crate::engine::types::DescribeMutableStateResponse> { HistoryHandler::describe_mutable_state(self, request).await }
    async fn describe_queue(&self, request: Option<&cadence_api::admin::DescribeQueueRequest>) -> HResult<cadence_api::admin::DescribeQueueResponse> { HistoryHandler::describe_queue(self, request).await }
    async fn describe_workflow_execution(&self, request: Option<&crate::engine::types::HistoryDescribeWorkflowExecutionRequest>) -> HResult<cadence_api::service_types::DescribeWorkflowExecutionResponse> { HistoryHandler::describe_workflow_execution(self, request).await }
    async fn get_cross_cluster_tasks(&self, request: Option<&cadence_api::admin::GetCrossClusterTasksRequest>) -> HResult<cadence_api::admin::GetCrossClusterTasksResponse> { HistoryHandler::get_cross_cluster_tasks(self, request).await }
    async fn count_dlq_messages(&self, request: Option<&cadence_api::admin::CountDLQMessagesRequest>) -> HResult<crate::engine::types::HistoryCountDLQMessagesResponse> { HistoryHandler::count_dlq_messages(self, request).await }
    async fn get_dlq_replication_messages(&self, request: Option<&cadence_api::admin::GetDLQReplicationMessagesRequest>) -> HResult<cadence_api::admin::GetDLQReplicationMessagesResponse> { HistoryHandler::get_dlq_replication_messages(self, request).await }
    async fn get_mutable_state(&self, request: Option<&crate::engine::types::GetMutableStateRequest>) -> HResult<crate::engine::types::GetMutableStateResponse> { HistoryHandler::get_mutable_state(self, request).await }
    async fn get_replication_messages(&self, request: Option<&cadence_api::admin::GetReplicationMessagesRequest>) -> HResult<cadence_api::admin::GetReplicationMessagesResponse> { HistoryHandler::get_replication_messages(self, request).await }
    async fn merge_dlq_messages(&self, request: Option<&cadence_api::admin::MergeDLQMessagesRequest>) -> HResult<cadence_api::admin::MergeDLQMessagesResponse> { HistoryHandler::merge_dlq_messages(self, request).await }
    async fn notify_failover_markers(&self, request: Option<&crate::engine::types::NotifyFailoverMarkersRequest>) -> HResult<()> { HistoryHandler::notify_failover_markers(self, request).await }
    async fn poll_mutable_state(&self, request: Option<&crate::engine::types::PollMutableStateRequest>) -> HResult<crate::engine::types::PollMutableStateResponse> { HistoryHandler::poll_mutable_state(self, request).await }
    async fn purge_dlq_messages(&self, request: Option<&cadence_api::admin::PurgeDLQMessagesRequest>) -> HResult<()> { HistoryHandler::purge_dlq_messages(self, request).await }
    async fn query_workflow(&self, request: Option<&crate::engine::types::HistoryQueryWorkflowRequest>) -> HResult<crate::engine::types::HistoryQueryWorkflowResponse> { HistoryHandler::query_workflow(self, request).await }
    async fn read_dlq_messages(&self, request: Option<&cadence_api::admin::ReadDLQMessagesRequest>) -> HResult<cadence_api::admin::ReadDLQMessagesResponse> { HistoryHandler::read_dlq_messages(self, request).await }
    async fn reapply_events(&self, request: Option<&crate::engine::types::HistoryReapplyEventsRequest>) -> HResult<()> { HistoryHandler::reapply_events(self, request).await }
    async fn record_activity_task_heartbeat(&self, request: Option<&crate::engine::types::HistoryRecordActivityTaskHeartbeatRequest>) -> HResult<cadence_api::tasks::RecordActivityTaskHeartbeatResponse> { HistoryHandler::record_activity_task_heartbeat(self, request).await }
    async fn record_activity_task_started(&self, request: Option<&crate::engine::types::RecordActivityTaskStartedRequest>) -> HResult<crate::engine::types::RecordActivityTaskStartedResponse> { HistoryHandler::record_activity_task_started(self, request).await }
    async fn record_child_execution_completed(&self, request: Option<&crate::engine::types::RecordChildExecutionCompletedRequest>) -> HResult<()> { HistoryHandler::record_child_execution_completed(self, request).await }
    async fn record_decision_task_started(&self, request: Option<&crate::engine::types::RecordDecisionTaskStartedRequest>) -> HResult<crate::engine::types::RecordDecisionTaskStartedResponse> { HistoryHandler::record_decision_task_started(self, request).await }
    async fn refresh_workflow_tasks(&self, request: Option<&crate::engine::types::HistoryRefreshWorkflowTasksRequest>) -> HResult<()> { HistoryHandler::refresh_workflow_tasks(self, request).await }
    async fn remove_signal_mutable_state(&self, request: Option<&crate::engine::types::RemoveSignalMutableStateRequest>) -> HResult<()> { HistoryHandler::remove_signal_mutable_state(self, request).await }
    async fn remove_task(&self, request: Option<&cadence_api::admin::RemoveTaskRequest>) -> HResult<()> { HistoryHandler::remove_task(self, request).await }
    async fn replicate_events_v2(&self, request: Option<&crate::engine::types::ReplicateEventsV2Request>) -> HResult<()> { HistoryHandler::replicate_events_v2(self, request).await }
    async fn request_cancel_workflow_execution(&self, request: Option<&crate::engine::types::HistoryRequestCancelWorkflowExecutionRequest>) -> HResult<()> { HistoryHandler::request_cancel_workflow_execution(self, request).await }
    async fn reset_queue(&self, request: Option<&cadence_api::admin::ResetQueueRequest>) -> HResult<()> { HistoryHandler::reset_queue(self, request).await }
    async fn reset_sticky_task_list(&self, request: Option<&crate::engine::types::HistoryResetStickyTaskListRequest>) -> HResult<crate::engine::types::HistoryResetStickyTaskListResponse> { HistoryHandler::reset_sticky_task_list(self, request).await }
    async fn reset_workflow_execution(&self, request: Option<&crate::engine::types::HistoryResetWorkflowExecutionRequest>) -> HResult<cadence_api::types::ResetWorkflowExecutionResponse> { HistoryHandler::reset_workflow_execution(self, request).await }
    async fn respond_activity_task_canceled(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCanceledRequest>) -> HResult<()> { HistoryHandler::respond_activity_task_canceled(self, request).await }
    async fn respond_activity_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCompletedRequest>) -> HResult<()> { HistoryHandler::respond_activity_task_completed(self, request).await }
    async fn respond_activity_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskFailedRequest>) -> HResult<()> { HistoryHandler::respond_activity_task_failed(self, request).await }
    async fn respond_cross_cluster_tasks_completed(&self, request: Option<&cadence_api::admin::RespondCrossClusterTasksCompletedRequest>) -> HResult<cadence_api::admin::RespondCrossClusterTasksCompletedResponse> { HistoryHandler::respond_cross_cluster_tasks_completed(self, request).await }
    async fn respond_decision_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskCompletedRequest>) -> HResult<crate::engine::types::HistoryRespondDecisionTaskCompletedResponse> { HistoryHandler::respond_decision_task_completed(self, request).await }
    async fn respond_decision_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskFailedRequest>) -> HResult<()> { HistoryHandler::respond_decision_task_failed(self, request).await }
    async fn schedule_decision_task(&self, request: Option<&crate::engine::types::ScheduleDecisionTaskRequest>) -> HResult<()> { HistoryHandler::schedule_decision_task(self, request).await }
    async fn signal_with_start_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWithStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> { HistoryHandler::signal_with_start_workflow_execution(self, request).await }
    async fn signal_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWorkflowExecutionRequest>) -> HResult<()> { HistoryHandler::signal_workflow_execution(self, request).await }
    async fn start_workflow_execution(&self, request: Option<&crate::engine::types::HistoryStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> { HistoryHandler::start_workflow_execution(self, request).await }
    async fn sync_activity(&self, request: Option<&crate::engine::types::SyncActivityRequest>) -> HResult<()> { HistoryHandler::sync_activity(self, request).await }
    async fn sync_shard_status(&self, request: Option<&crate::engine::types::SyncShardStatusRequest>) -> HResult<()> { HistoryHandler::sync_shard_status(self, request).await }
    async fn terminate_workflow_execution(&self, request: Option<&crate::engine::types::HistoryTerminateWorkflowExecutionRequest>) -> HResult<()> { HistoryHandler::terminate_workflow_execution(self, request).await }
    async fn get_failover_info(&self, request: Option<&crate::engine::types::GetFailoverInfoRequest>) -> HResult<crate::engine::types::GetFailoverInfoResponse> { HistoryHandler::get_failover_info(self, request).await }
    async fn ratelimit_update(&self, request: Option<&crate::engine::types::RatelimitUpdateRequest>) -> HResult<crate::engine::types::RatelimitUpdateResponse> { HistoryHandler::ratelimit_update(self, request).await }
}
