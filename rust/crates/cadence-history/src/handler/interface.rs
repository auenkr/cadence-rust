//! Port of go/service/history/handler/interface.go.
use crate::engine::deps::HResult;
use async_trait::async_trait;
#[async_trait]
pub trait Handler: Send + Sync {
    fn start(&self);
    fn stop(&self);
    async fn prepare_to_stop(&self, remaining: std::time::Duration) -> std::time::Duration;
    async fn health(&self) -> HResult<crate::engine::types::HealthStatus>;
    async fn close_shard(
        &self,
        request: Option<&cadence_api::admin::CloseShardRequest>,
    ) -> HResult<()>;
    async fn describe_history_host(
        &self,
        request: Option<&cadence_api::admin::DescribeHistoryHostRequest>,
    ) -> HResult<cadence_api::admin::DescribeHistoryHostResponse>;
    async fn describe_mutable_state(
        &self,
        request: Option<&crate::engine::types::DescribeMutableStateRequest>,
    ) -> HResult<crate::engine::types::DescribeMutableStateResponse>;
    async fn describe_queue(
        &self,
        request: Option<&cadence_api::admin::DescribeQueueRequest>,
    ) -> HResult<cadence_api::admin::DescribeQueueResponse>;
    async fn describe_workflow_execution(
        &self,
        request: Option<&crate::engine::types::HistoryDescribeWorkflowExecutionRequest>,
    ) -> HResult<cadence_api::service_types::DescribeWorkflowExecutionResponse>;
    async fn get_cross_cluster_tasks(
        &self,
        request: Option<&cadence_api::admin::GetCrossClusterTasksRequest>,
    ) -> HResult<cadence_api::admin::GetCrossClusterTasksResponse>;
    async fn count_dlq_messages(
        &self,
        request: Option<&cadence_api::admin::CountDLQMessagesRequest>,
    ) -> HResult<crate::engine::types::HistoryCountDLQMessagesResponse>;
    async fn get_dlq_replication_messages(
        &self,
        request: Option<&cadence_api::admin::GetDLQReplicationMessagesRequest>,
    ) -> HResult<cadence_api::admin::GetDLQReplicationMessagesResponse>;
    async fn get_mutable_state(
        &self,
        request: Option<&crate::engine::types::GetMutableStateRequest>,
    ) -> HResult<crate::engine::types::GetMutableStateResponse>;
    async fn get_replication_messages(
        &self,
        request: Option<&cadence_api::admin::GetReplicationMessagesRequest>,
    ) -> HResult<cadence_api::admin::GetReplicationMessagesResponse>;
    async fn merge_dlq_messages(
        &self,
        request: Option<&cadence_api::admin::MergeDLQMessagesRequest>,
    ) -> HResult<cadence_api::admin::MergeDLQMessagesResponse>;
    async fn notify_failover_markers(
        &self,
        request: Option<&crate::engine::types::NotifyFailoverMarkersRequest>,
    ) -> HResult<()>;
    async fn poll_mutable_state(
        &self,
        request: Option<&crate::engine::types::PollMutableStateRequest>,
    ) -> HResult<crate::engine::types::PollMutableStateResponse>;
    async fn purge_dlq_messages(
        &self,
        request: Option<&cadence_api::admin::PurgeDLQMessagesRequest>,
    ) -> HResult<()>;
    async fn query_workflow(
        &self,
        request: Option<&crate::engine::types::HistoryQueryWorkflowRequest>,
    ) -> HResult<crate::engine::types::HistoryQueryWorkflowResponse>;
    async fn read_dlq_messages(
        &self,
        request: Option<&cadence_api::admin::ReadDLQMessagesRequest>,
    ) -> HResult<cadence_api::admin::ReadDLQMessagesResponse>;
    async fn reapply_events(
        &self,
        request: Option<&crate::engine::types::HistoryReapplyEventsRequest>,
    ) -> HResult<()>;
    async fn record_activity_task_heartbeat(
        &self,
        request: Option<&crate::engine::types::HistoryRecordActivityTaskHeartbeatRequest>,
    ) -> HResult<cadence_api::tasks::RecordActivityTaskHeartbeatResponse>;
    async fn record_activity_task_started(
        &self,
        request: Option<&crate::engine::types::RecordActivityTaskStartedRequest>,
    ) -> HResult<crate::engine::types::RecordActivityTaskStartedResponse>;
    async fn record_child_execution_completed(
        &self,
        request: Option<&crate::engine::types::RecordChildExecutionCompletedRequest>,
    ) -> HResult<()>;
    async fn record_decision_task_started(
        &self,
        request: Option<&crate::engine::types::RecordDecisionTaskStartedRequest>,
    ) -> HResult<crate::engine::types::RecordDecisionTaskStartedResponse>;
    async fn refresh_workflow_tasks(
        &self,
        request: Option<&crate::engine::types::HistoryRefreshWorkflowTasksRequest>,
    ) -> HResult<()>;
    async fn remove_signal_mutable_state(
        &self,
        request: Option<&crate::engine::types::RemoveSignalMutableStateRequest>,
    ) -> HResult<()>;
    async fn remove_task(
        &self,
        request: Option<&cadence_api::admin::RemoveTaskRequest>,
    ) -> HResult<()>;
    async fn replicate_events_v2(
        &self,
        request: Option<&crate::engine::types::ReplicateEventsV2Request>,
    ) -> HResult<()>;
    async fn request_cancel_workflow_execution(
        &self,
        request: Option<&crate::engine::types::HistoryRequestCancelWorkflowExecutionRequest>,
    ) -> HResult<()>;
    async fn reset_queue(
        &self,
        request: Option<&cadence_api::admin::ResetQueueRequest>,
    ) -> HResult<()>;
    async fn reset_sticky_task_list(
        &self,
        request: Option<&crate::engine::types::HistoryResetStickyTaskListRequest>,
    ) -> HResult<crate::engine::types::HistoryResetStickyTaskListResponse>;
    async fn reset_workflow_execution(
        &self,
        request: Option<&crate::engine::types::HistoryResetWorkflowExecutionRequest>,
    ) -> HResult<cadence_api::types::ResetWorkflowExecutionResponse>;
    async fn respond_activity_task_canceled(
        &self,
        request: Option<&crate::engine::types::HistoryRespondActivityTaskCanceledRequest>,
    ) -> HResult<()>;
    async fn respond_activity_task_completed(
        &self,
        request: Option<&crate::engine::types::HistoryRespondActivityTaskCompletedRequest>,
    ) -> HResult<()>;
    async fn respond_activity_task_failed(
        &self,
        request: Option<&crate::engine::types::HistoryRespondActivityTaskFailedRequest>,
    ) -> HResult<()>;
    async fn respond_cross_cluster_tasks_completed(
        &self,
        request: Option<&cadence_api::admin::RespondCrossClusterTasksCompletedRequest>,
    ) -> HResult<cadence_api::admin::RespondCrossClusterTasksCompletedResponse>;
    async fn respond_decision_task_completed(
        &self,
        request: Option<&crate::engine::types::HistoryRespondDecisionTaskCompletedRequest>,
    ) -> HResult<crate::engine::types::HistoryRespondDecisionTaskCompletedResponse>;
    async fn respond_decision_task_failed(
        &self,
        request: Option<&crate::engine::types::HistoryRespondDecisionTaskFailedRequest>,
    ) -> HResult<()>;
    async fn schedule_decision_task(
        &self,
        request: Option<&crate::engine::types::ScheduleDecisionTaskRequest>,
    ) -> HResult<()>;
    async fn signal_with_start_workflow_execution(
        &self,
        request: Option<&crate::engine::types::HistorySignalWithStartWorkflowExecutionRequest>,
    ) -> HResult<cadence_api::types::StartWorkflowExecutionResponse>;
    async fn signal_workflow_execution(
        &self,
        request: Option<&crate::engine::types::HistorySignalWorkflowExecutionRequest>,
    ) -> HResult<()>;
    async fn start_workflow_execution(
        &self,
        request: Option<&crate::engine::types::HistoryStartWorkflowExecutionRequest>,
    ) -> HResult<cadence_api::types::StartWorkflowExecutionResponse>;
    async fn sync_activity(
        &self,
        request: Option<&crate::engine::types::SyncActivityRequest>,
    ) -> HResult<()>;
    async fn sync_shard_status(
        &self,
        request: Option<&crate::engine::types::SyncShardStatusRequest>,
    ) -> HResult<()>;
    async fn terminate_workflow_execution(
        &self,
        request: Option<&crate::engine::types::HistoryTerminateWorkflowExecutionRequest>,
    ) -> HResult<()>;
    async fn get_failover_info(
        &self,
        request: Option<&crate::engine::types::GetFailoverInfoRequest>,
    ) -> HResult<crate::engine::types::GetFailoverInfoResponse>;
    async fn ratelimit_update(
        &self,
        request: Option<&crate::engine::types::RatelimitUpdateRequest>,
    ) -> HResult<crate::engine::types::RatelimitUpdateResponse>;
}
