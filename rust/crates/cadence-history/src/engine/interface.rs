//! Complete engine interface ported from go/service/history/engine/interface.go.
use async_trait::async_trait;
use cadence_api::service_types::DescribeWorkflowExecutionResponse;
use std::{collections::HashMap, time::Instant};
use cadence_api::{service_types::HistoryEvent, tasks::RecordActivityTaskHeartbeatResponse};
use cadence_api::{admin::*, types::*};
use super::{deps::*, runtime::{HistoryEngine, HistoryNotification, Task}, types::*, notify_tasks::ReplicationNotification};
#[async_trait]
pub trait Engine: Send + Sync {
    async fn count_dlq_messages(&self, force_fetch: bool) -> HResult<HashMap<String, i64>>;
    async fn describe_mutable_state(&self, request: &DescribeMutableStateRequest) -> HResult<DescribeMutableStateResponse>;
    async fn describe_timer_queue(&self, cluster: &str) -> HResult<DescribeQueueResponse>;
    async fn describe_transfer_queue(&self, cluster: &str) -> HResult<DescribeQueueResponse>;
    async fn describe_workflow_execution(&self, request: &HistoryDescribeWorkflowExecutionRequest) -> HResult<DescribeWorkflowExecutionResponse>;
    async fn get_dlq_replication_messages(&self, infos: &[ReplicationTaskInfo]) -> HResult<Vec<ReplicationTask>>;
    async fn get_mutable_state(&self, request: &GetMutableStateRequest, deadline: Option<Instant>) -> HResult<GetMutableStateResponse>;
    async fn get_replication_messages(&self, cluster: &str, last_read_id: i64) -> HResult<ReplicationMessages>;
    async fn merge_dlq_messages(&self, request: &MergeDLQMessagesRequest) -> HResult<MergeDLQMessagesResponse>;
    fn notify_new_history_event(&self, event: HistoryNotification);
    async fn notify_new_replication_tasks(&self, notification: &ReplicationNotification);
    fn notify_new_timer_tasks(&self, tasks: &[Task]);
    fn notify_new_transfer_tasks(&self, tasks: &[Task]);
    async fn poll_mutable_state(&self, request: &PollMutableStateRequest, deadline: Option<Instant>) -> HResult<PollMutableStateResponse>;
    async fn purge_dlq_messages(&self, request: &PurgeDLQMessagesRequest) -> HResult<()>;
    async fn query_workflow(&self, request: &HistoryQueryWorkflowRequest, deadline: Option<Instant>) -> HResult<HistoryQueryWorkflowResponse>;
    async fn read_dlq_messages(&self, request: &ReadDLQMessagesRequest) -> HResult<ReadDLQMessagesResponse>;
    async fn reapply_events(&self, domain_uuid: &str, workflow_id: &str, run_id: &str, events: &[HistoryEvent]) -> HResult<()>;
    async fn record_activity_task_heartbeat(&self, req: &HistoryRecordActivityTaskHeartbeatRequest) -> HResult<RecordActivityTaskHeartbeatResponse>;
    async fn record_activity_task_started(&self, request: &RecordActivityTaskStartedRequest) -> HResult<RecordActivityTaskStartedResponse>;
    async fn record_child_execution_completed(&self, request: &RecordChildExecutionCompletedRequest) -> HResult<()>;
    async fn record_decision_task_started(&self, req: &RecordDecisionTaskStartedRequest) -> HResult<RecordDecisionTaskStartedResponse>;
    async fn refresh_workflow_tasks(&self, domain_uuid: &str, execution: &WorkflowExecution) -> HResult<()>;
    async fn remove_signal_mutable_state(&self, request: &RemoveSignalMutableStateRequest) -> HResult<()>;
    async fn replicate_events_v2(&self, req: &ReplicateEventsV2Request) -> HResult<()>;
    async fn request_cancel_workflow_execution(&self, req: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()>;
    async fn reset_sticky_task_list(&self, request: &HistoryResetStickyTaskListRequest) -> HResult<HistoryResetStickyTaskListResponse>;
    async fn reset_timer_queue(&self, cluster: &str) -> HResult<()>;
    async fn reset_transfer_queue(&self, cluster: &str) -> HResult<()>;
    async fn reset_workflow_execution(&self, req: &HistoryResetWorkflowExecutionRequest) -> HResult<ResetWorkflowExecutionResponse>;
    async fn respond_activity_task_canceled(&self, req: &HistoryRespondActivityTaskCanceledRequest) -> HResult<()>;
    async fn respond_activity_task_completed(&self, req: &HistoryRespondActivityTaskCompletedRequest) -> HResult<()>;
    async fn respond_activity_task_failed(&self, req: &HistoryRespondActivityTaskFailedRequest) -> HResult<()>;
    async fn respond_decision_task_completed(&self, req: &HistoryRespondDecisionTaskCompletedRequest) -> HResult<HistoryRespondDecisionTaskCompletedResponse>;
    async fn respond_decision_task_failed(&self, req: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()>;
    async fn schedule_decision_task(&self, req: &ScheduleDecisionTaskRequest) -> HResult<()>;
    async fn signal_with_start_workflow_execution(&self, request: &HistorySignalWithStartWorkflowExecutionRequest,
        deadline: Option<Instant>) -> HResult<StartWorkflowExecutionResponse>;
    async fn signal_workflow_execution(&self, req: &HistorySignalWorkflowExecutionRequest) -> HResult<()>;
    async fn start(&self);
    async fn start_workflow_execution(&self, request: &mut HistoryStartWorkflowExecutionRequest, deadline: Option<Instant>) -> HResult<StartWorkflowExecutionResponse>;
    async fn stop(&self);
    async fn sync_activity(&self, req: &SyncActivityRequest) -> HResult<()>;
    async fn sync_shard_status(&self, req: &SyncShardStatusRequest) -> HResult<()>;
    async fn terminate_workflow_execution(&self, req: &HistoryTerminateWorkflowExecutionRequest) -> HResult<()>;
}
#[async_trait]
impl Engine for HistoryEngine {
    async fn count_dlq_messages(&self, force_fetch: bool) -> HResult<HashMap<String, i64>> { HistoryEngine::count_dlq_messages(self, force_fetch).await }
    async fn describe_mutable_state(&self, request: &DescribeMutableStateRequest) -> HResult<DescribeMutableStateResponse> { HistoryEngine::describe_mutable_state(self, request).await }
    async fn describe_timer_queue(&self, cluster: &str) -> HResult<DescribeQueueResponse> { HistoryEngine::describe_timer_queue(self, cluster).await }
    async fn describe_transfer_queue(&self, cluster: &str) -> HResult<DescribeQueueResponse> { HistoryEngine::describe_transfer_queue(self, cluster).await }
    async fn describe_workflow_execution(&self, request: &HistoryDescribeWorkflowExecutionRequest) -> HResult<DescribeWorkflowExecutionResponse> { HistoryEngine::describe_workflow_execution(self, request).await }
    async fn get_dlq_replication_messages(&self, infos: &[ReplicationTaskInfo]) -> HResult<Vec<ReplicationTask>> { HistoryEngine::get_dlq_replication_messages(self, infos).await }
    async fn get_mutable_state(&self, request: &GetMutableStateRequest, deadline: Option<Instant>) -> HResult<GetMutableStateResponse> { HistoryEngine::get_mutable_state(self, request, deadline).await }
    async fn get_replication_messages(&self, cluster: &str, last_read_id: i64) -> HResult<ReplicationMessages> { HistoryEngine::get_replication_messages(self, cluster, last_read_id).await }
    async fn merge_dlq_messages(&self, request: &MergeDLQMessagesRequest) -> HResult<MergeDLQMessagesResponse> { HistoryEngine::merge_dlq_messages(self, request).await }
    fn notify_new_history_event(&self, event: HistoryNotification) { HistoryEngine::notify_new_history_event(self, event) }
    async fn notify_new_replication_tasks(&self, notification: &ReplicationNotification) { HistoryEngine::notify_new_replication_tasks(self, notification).await }
    fn notify_new_timer_tasks(&self, tasks: &[Task]) { HistoryEngine::notify_new_timer_tasks(self, tasks) }
    fn notify_new_transfer_tasks(&self, tasks: &[Task]) { HistoryEngine::notify_new_transfer_tasks(self, tasks) }
    async fn poll_mutable_state(&self, request: &PollMutableStateRequest, deadline: Option<Instant>) -> HResult<PollMutableStateResponse> { HistoryEngine::poll_mutable_state(self, request, deadline).await }
    async fn purge_dlq_messages(&self, request: &PurgeDLQMessagesRequest) -> HResult<()> { HistoryEngine::purge_dlq_messages(self, request).await }
    async fn query_workflow(&self, request: &HistoryQueryWorkflowRequest, deadline: Option<Instant>) -> HResult<HistoryQueryWorkflowResponse> { HistoryEngine::query_workflow(self, request, deadline).await }
    async fn read_dlq_messages(&self, request: &ReadDLQMessagesRequest) -> HResult<ReadDLQMessagesResponse> { HistoryEngine::read_dlq_messages(self, request).await }
    async fn reapply_events(&self, domain_uuid: &str, workflow_id: &str, run_id: &str, events: &[HistoryEvent]) -> HResult<()> { HistoryEngine::reapply_events(self, domain_uuid, workflow_id, run_id, events).await }
    async fn record_activity_task_heartbeat(&self, req: &HistoryRecordActivityTaskHeartbeatRequest) -> HResult<RecordActivityTaskHeartbeatResponse> { HistoryEngine::record_activity_task_heartbeat(self, req).await }
    async fn record_activity_task_started(&self, request: &RecordActivityTaskStartedRequest) -> HResult<RecordActivityTaskStartedResponse> { HistoryEngine::record_activity_task_started(self, request).await }
    async fn record_child_execution_completed(&self, request: &RecordChildExecutionCompletedRequest) -> HResult<()> { HistoryEngine::record_child_execution_completed(self, request).await }
    async fn record_decision_task_started(&self, req: &RecordDecisionTaskStartedRequest) -> HResult<RecordDecisionTaskStartedResponse> { HistoryEngine::record_decision_task_started(self, req).await }
    async fn refresh_workflow_tasks(&self, domain_uuid: &str, execution: &WorkflowExecution) -> HResult<()> { HistoryEngine::refresh_workflow_tasks(self, domain_uuid, execution).await }
    async fn remove_signal_mutable_state(&self, request: &RemoveSignalMutableStateRequest) -> HResult<()> { HistoryEngine::remove_signal_mutable_state(self, request).await }
    async fn replicate_events_v2(&self, req: &ReplicateEventsV2Request) -> HResult<()> { HistoryEngine::replicate_events_v2(self, req).await }
    async fn request_cancel_workflow_execution(&self, req: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()> { HistoryEngine::request_cancel_workflow_execution(self, req).await }
    async fn reset_sticky_task_list(&self, request: &HistoryResetStickyTaskListRequest) -> HResult<HistoryResetStickyTaskListResponse> { HistoryEngine::reset_sticky_task_list(self, request).await }
    async fn reset_timer_queue(&self, cluster: &str) -> HResult<()> { HistoryEngine::reset_timer_queue(self, cluster).await }
    async fn reset_transfer_queue(&self, cluster: &str) -> HResult<()> { HistoryEngine::reset_transfer_queue(self, cluster).await }
    async fn reset_workflow_execution(&self, req: &HistoryResetWorkflowExecutionRequest) -> HResult<ResetWorkflowExecutionResponse> { HistoryEngine::reset_workflow_execution(self, req).await }
    async fn respond_activity_task_canceled(&self, req: &HistoryRespondActivityTaskCanceledRequest) -> HResult<()> { HistoryEngine::respond_activity_task_canceled(self, req).await }
    async fn respond_activity_task_completed(&self, req: &HistoryRespondActivityTaskCompletedRequest) -> HResult<()> { HistoryEngine::respond_activity_task_completed(self, req).await }
    async fn respond_activity_task_failed(&self, req: &HistoryRespondActivityTaskFailedRequest) -> HResult<()> { HistoryEngine::respond_activity_task_failed(self, req).await }
    async fn respond_decision_task_completed(&self, req: &HistoryRespondDecisionTaskCompletedRequest) -> HResult<HistoryRespondDecisionTaskCompletedResponse> { HistoryEngine::respond_decision_task_completed(self, req).await }
    async fn respond_decision_task_failed(&self, req: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()> { HistoryEngine::respond_decision_task_failed(self, req).await }
    async fn schedule_decision_task(&self, req: &ScheduleDecisionTaskRequest) -> HResult<()> { HistoryEngine::schedule_decision_task(self, req).await }
    async fn signal_with_start_workflow_execution(&self, request: &HistorySignalWithStartWorkflowExecutionRequest,
        deadline: Option<Instant>) -> HResult<StartWorkflowExecutionResponse> { HistoryEngine::signal_with_start_workflow_execution(self, request, deadline).await }
    async fn signal_workflow_execution(&self, req: &HistorySignalWorkflowExecutionRequest) -> HResult<()> { HistoryEngine::signal_workflow_execution(self, req).await }
    async fn start(&self) { HistoryEngine::start(self).await }
    async fn start_workflow_execution(&self, request: &mut HistoryStartWorkflowExecutionRequest, deadline: Option<Instant>) -> HResult<StartWorkflowExecutionResponse> { HistoryEngine::start_workflow_execution(self, request, deadline).await }
    async fn stop(&self) { HistoryEngine::stop(self).await }
    async fn sync_activity(&self, req: &SyncActivityRequest) -> HResult<()> { HistoryEngine::sync_activity(self, req).await }
    async fn sync_shard_status(&self, req: &SyncShardStatusRequest) -> HResult<()> { HistoryEngine::sync_shard_status(self, req).await }
    async fn terminate_workflow_execution(&self, req: &HistoryTerminateWorkflowExecutionRequest) -> HResult<()> { HistoryEngine::terminate_workflow_execution(self, req).await }
}
