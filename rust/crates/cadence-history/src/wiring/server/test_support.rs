//! Test mock Handler.
#![allow(unused_variables)]
use crate::engine::deps::{HResult, HistoryError};
use crate::handler::Handler;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};

#[derive(Default)]
pub struct MockHandler {
    pub started: AtomicBool,
    pub stopped: AtomicBool,
    pub prepared: AtomicBool,
    pub closed_shard: AtomicI32,
    pub removed: AtomicUsize,
}

#[async_trait]
impl Handler for MockHandler {
    fn start(&self) { self.started.store(true, Ordering::SeqCst); }
    fn stop(&self) { self.stopped.store(true, Ordering::SeqCst); }
    async fn prepare_to_stop(&self, remaining: std::time::Duration) -> std::time::Duration { self.prepared.store(true, Ordering::SeqCst); remaining }
    async fn health(&self) -> HResult<crate::engine::types::HealthStatus> { Ok(crate::engine::types::HealthStatus { ok: true, msg: "OK".into() }) }
    async fn close_shard(&self, request: Option<&cadence_api::admin::CloseShardRequest>) -> HResult<()> {
        let r = request.ok_or_else(|| HistoryError::bad_request("nil"))?;
        if r.shard_id < 0 { return Err(HistoryError::bad_request("bad shard")); }
        self.closed_shard.store(r.shard_id, Ordering::SeqCst);
        Ok(())
    }
    async fn remove_task(&self, _request: Option<&cadence_api::admin::RemoveTaskRequest>) -> HResult<()> { self.removed.fetch_add(1, Ordering::SeqCst); Ok(()) }
    async fn reset_queue(&self, _request: Option<&cadence_api::admin::ResetQueueRequest>) -> HResult<()> { Ok(()) }
    async fn describe_queue(&self, request: Option<&cadence_api::admin::DescribeQueueRequest>) -> HResult<cadence_api::admin::DescribeQueueResponse> {
        let r = request.ok_or_else(|| HistoryError::bad_request("nil"))?;
        Ok(cadence_api::admin::DescribeQueueResponse { processing_queue_states: vec![format!("{}:{}", r.shard_id, r.cluster_name)] })
    }
    async fn describe_history_host(&self, request: Option<&cadence_api::admin::DescribeHistoryHostRequest>) -> HResult<cadence_api::admin::DescribeHistoryHostResponse> { Err(HistoryError::internal("mock")) }
    async fn describe_mutable_state(&self, request: Option<&crate::engine::types::DescribeMutableStateRequest>) -> HResult<crate::engine::types::DescribeMutableStateResponse> { Err(HistoryError::internal("mock")) }
    async fn describe_workflow_execution(&self, request: Option<&crate::engine::types::HistoryDescribeWorkflowExecutionRequest>) -> HResult<cadence_api::service_types::DescribeWorkflowExecutionResponse> { Err(HistoryError::internal("mock")) }
    async fn get_cross_cluster_tasks(&self, request: Option<&cadence_api::admin::GetCrossClusterTasksRequest>) -> HResult<cadence_api::admin::GetCrossClusterTasksResponse> { Err(HistoryError::internal("mock")) }
    async fn count_dlq_messages(&self, request: Option<&cadence_api::admin::CountDLQMessagesRequest>) -> HResult<crate::engine::types::HistoryCountDLQMessagesResponse> { Err(HistoryError::internal("mock")) }
    async fn get_dlq_replication_messages(&self, request: Option<&cadence_api::admin::GetDLQReplicationMessagesRequest>) -> HResult<cadence_api::admin::GetDLQReplicationMessagesResponse> { Err(HistoryError::internal("mock")) }
    async fn get_mutable_state(&self, request: Option<&crate::engine::types::GetMutableStateRequest>) -> HResult<crate::engine::types::GetMutableStateResponse> { Err(HistoryError::internal("mock")) }
    async fn get_replication_messages(&self, request: Option<&cadence_api::admin::GetReplicationMessagesRequest>) -> HResult<cadence_api::admin::GetReplicationMessagesResponse> { Err(HistoryError::internal("mock")) }
    async fn merge_dlq_messages(&self, request: Option<&cadence_api::admin::MergeDLQMessagesRequest>) -> HResult<cadence_api::admin::MergeDLQMessagesResponse> { Err(HistoryError::internal("mock")) }
    async fn notify_failover_markers(&self, request: Option<&crate::engine::types::NotifyFailoverMarkersRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn poll_mutable_state(&self, request: Option<&crate::engine::types::PollMutableStateRequest>) -> HResult<crate::engine::types::PollMutableStateResponse> { Err(HistoryError::internal("mock")) }
    async fn purge_dlq_messages(&self, request: Option<&cadence_api::admin::PurgeDLQMessagesRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn query_workflow(&self, request: Option<&crate::engine::types::HistoryQueryWorkflowRequest>) -> HResult<crate::engine::types::HistoryQueryWorkflowResponse> { Err(HistoryError::internal("mock")) }
    async fn read_dlq_messages(&self, request: Option<&cadence_api::admin::ReadDLQMessagesRequest>) -> HResult<cadence_api::admin::ReadDLQMessagesResponse> { Err(HistoryError::internal("mock")) }
    async fn reapply_events(&self, request: Option<&crate::engine::types::HistoryReapplyEventsRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn record_activity_task_heartbeat(&self, request: Option<&crate::engine::types::HistoryRecordActivityTaskHeartbeatRequest>) -> HResult<cadence_api::tasks::RecordActivityTaskHeartbeatResponse> { Err(HistoryError::internal("mock")) }
    async fn record_activity_task_started(&self, request: Option<&crate::engine::types::RecordActivityTaskStartedRequest>) -> HResult<crate::engine::types::RecordActivityTaskStartedResponse> { Err(HistoryError::internal("mock")) }
    async fn record_child_execution_completed(&self, request: Option<&crate::engine::types::RecordChildExecutionCompletedRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn record_decision_task_started(&self, request: Option<&crate::engine::types::RecordDecisionTaskStartedRequest>) -> HResult<crate::engine::types::RecordDecisionTaskStartedResponse> { Err(HistoryError::internal("mock")) }
    async fn refresh_workflow_tasks(&self, request: Option<&crate::engine::types::HistoryRefreshWorkflowTasksRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn remove_signal_mutable_state(&self, request: Option<&crate::engine::types::RemoveSignalMutableStateRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn replicate_events_v2(&self, request: Option<&crate::engine::types::ReplicateEventsV2Request>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn request_cancel_workflow_execution(&self, request: Option<&crate::engine::types::HistoryRequestCancelWorkflowExecutionRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn reset_sticky_task_list(&self, request: Option<&crate::engine::types::HistoryResetStickyTaskListRequest>) -> HResult<crate::engine::types::HistoryResetStickyTaskListResponse> { Err(HistoryError::internal("mock")) }
    async fn reset_workflow_execution(&self, request: Option<&crate::engine::types::HistoryResetWorkflowExecutionRequest>) -> HResult<cadence_api::types::ResetWorkflowExecutionResponse> { Err(HistoryError::internal("mock")) }
    async fn respond_activity_task_canceled(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCanceledRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn respond_activity_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskCompletedRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn respond_activity_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondActivityTaskFailedRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn respond_cross_cluster_tasks_completed(&self, request: Option<&cadence_api::admin::RespondCrossClusterTasksCompletedRequest>) -> HResult<cadence_api::admin::RespondCrossClusterTasksCompletedResponse> { Err(HistoryError::internal("mock")) }
    async fn respond_decision_task_completed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskCompletedRequest>) -> HResult<crate::engine::types::HistoryRespondDecisionTaskCompletedResponse> { Err(HistoryError::internal("mock")) }
    async fn respond_decision_task_failed(&self, request: Option<&crate::engine::types::HistoryRespondDecisionTaskFailedRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn schedule_decision_task(&self, request: Option<&crate::engine::types::ScheduleDecisionTaskRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn signal_with_start_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWithStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> { Err(HistoryError::internal("mock")) }
    async fn signal_workflow_execution(&self, request: Option<&crate::engine::types::HistorySignalWorkflowExecutionRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn start_workflow_execution(&self, request: Option<&crate::engine::types::HistoryStartWorkflowExecutionRequest>) -> HResult<cadence_api::types::StartWorkflowExecutionResponse> { Err(HistoryError::internal("mock")) }
    async fn sync_activity(&self, request: Option<&crate::engine::types::SyncActivityRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn sync_shard_status(&self, request: Option<&crate::engine::types::SyncShardStatusRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn terminate_workflow_execution(&self, request: Option<&crate::engine::types::HistoryTerminateWorkflowExecutionRequest>) -> HResult<()> { Err(HistoryError::internal("mock")) }
    async fn get_failover_info(&self, request: Option<&crate::engine::types::GetFailoverInfoRequest>) -> HResult<crate::engine::types::GetFailoverInfoResponse> { Err(HistoryError::internal("mock")) }
    async fn ratelimit_update(&self, request: Option<&crate::engine::types::RatelimitUpdateRequest>) -> HResult<crate::engine::types::RatelimitUpdateResponse> { Err(HistoryError::internal("mock")) }
}
