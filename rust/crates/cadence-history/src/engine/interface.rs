//! Port of `go/service/history/engine/interface.go` (`Engine`, including the embedded
//! `common.Daemon`). `HistoryEngine` implements it by delegating to its inherent methods.
//! Rust adaptations: the Go `ctx` is dropped (request deadlines are passed where the inherent
//! method already takes one) and `NotifyNewHistoryEvent(*events.Notification)` takes the
//! engine-side `HistoryNotification`.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use cadence_api::admin::*;
use cadence_api::tasks::RecordActivityTaskHeartbeatResponse;
use cadence_api::service_types::{DescribeWorkflowExecutionResponse, HistoryEvent};
use cadence_api::types::{ReplicationTask, ResetWorkflowExecutionResponse, StartWorkflowExecutionResponse};

use super::notify_tasks::NotifyTaskInfo;
use super::{deps::*, runtime::*, types::*};

#[async_trait]
pub trait Engine: Send + Sync {
    // common.Daemon
    async fn start(self: &Arc<Self>);
    async fn stop(&self);

    async fn start_workflow_execution(&self, request: &HistoryStartWorkflowExecutionRequest) -> HResult<StartWorkflowExecutionResponse>;
    async fn get_mutable_state(&self, request: &GetMutableStateRequest, deadline: Option<Instant>) -> HResult<GetMutableStateResponse>;
    async fn poll_mutable_state(&self, request: &PollMutableStateRequest, deadline: Option<Instant>) -> HResult<PollMutableStateResponse>;
    async fn describe_mutable_state(&self, request: &DescribeMutableStateRequest) -> HResult<DescribeMutableStateResponse>;
    async fn reset_sticky_task_list(&self, request: &HistoryResetStickyTaskListRequest) -> HResult<HistoryResetStickyTaskListResponse>;
    async fn describe_workflow_execution(&self, request: &HistoryDescribeWorkflowExecutionRequest) -> HResult<DescribeWorkflowExecutionResponse>;
    async fn record_decision_task_started(&self, request: &RecordDecisionTaskStartedRequest) -> HResult<RecordDecisionTaskStartedResponse>;
    async fn record_activity_task_started(&self, request: &RecordActivityTaskStartedRequest) -> HResult<RecordActivityTaskStartedResponse>;
    async fn respond_decision_task_completed(&self, request: &HistoryRespondDecisionTaskCompletedRequest) -> HResult<HistoryRespondDecisionTaskCompletedResponse>;
    async fn respond_decision_task_failed(&self, request: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()>;
    async fn respond_activity_task_completed(&self, request: &HistoryRespondActivityTaskCompletedRequest) -> HResult<()>;
    async fn respond_activity_task_failed(&self, request: &HistoryRespondActivityTaskFailedRequest) -> HResult<()>;
    async fn respond_activity_task_canceled(&self, request: &HistoryRespondActivityTaskCanceledRequest) -> HResult<()>;
    async fn record_activity_task_heartbeat(&self, request: &HistoryRecordActivityTaskHeartbeatRequest) -> HResult<RecordActivityTaskHeartbeatResponse>;
    async fn request_cancel_workflow_execution(&self, request: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()>;
    async fn signal_workflow_execution(&self, request: &HistorySignalWorkflowExecutionRequest) -> HResult<()>;
    async fn signal_with_start_workflow_execution(&self, request: &HistorySignalWithStartWorkflowExecutionRequest) -> HResult<StartWorkflowExecutionResponse>;
    async fn remove_signal_mutable_state(&self, request: &RemoveSignalMutableStateRequest) -> HResult<()>;
    async fn terminate_workflow_execution(&self, request: &HistoryTerminateWorkflowExecutionRequest) -> HResult<()>;
    async fn reset_workflow_execution(&self, request: &HistoryResetWorkflowExecutionRequest) -> HResult<ResetWorkflowExecutionResponse>;
    async fn schedule_decision_task(&self, request: &ScheduleDecisionTaskRequest) -> HResult<()>;
    async fn record_child_execution_completed(&self, request: &RecordChildExecutionCompletedRequest) -> HResult<()>;
    async fn replicate_events_v2(&self, request: &ReplicateEventsV2Request) -> HResult<()>;
    async fn sync_shard_status(&self, request: &SyncShardStatusRequest) -> HResult<()>;
    async fn sync_activity(&self, request: &SyncActivityRequest) -> HResult<()>;
    async fn get_replication_messages(&self, polling_cluster: &str, last_read_message_id: i64) -> HResult<ReplicationMessages>;
    async fn get_dlq_replication_messages(&self, task_infos: &[ReplicationTaskInfo]) -> HResult<Vec<ReplicationTask>>;
    async fn query_workflow(&self, request: &HistoryQueryWorkflowRequest, deadline: Option<Instant>) -> HResult<HistoryQueryWorkflowResponse>;
    async fn reapply_events(&self, domain_uuid: &str, workflow_id: &str, run_id: &str, events: &[HistoryEvent]) -> HResult<()>;
    async fn count_dlq_messages(&self, force_fetch: bool) -> HResult<HashMap<String, i64>>;
    async fn read_dlq_messages(&self, request: &ReadDLQMessagesRequest) -> HResult<ReadDLQMessagesResponse>;
    async fn purge_dlq_messages(&self, request: &PurgeDLQMessagesRequest) -> HResult<()>;
    async fn merge_dlq_messages(&self, request: &MergeDLQMessagesRequest) -> HResult<MergeDLQMessagesResponse>;
    async fn refresh_workflow_tasks(&self, domain_uuid: &str, execution: &WorkflowExecution) -> HResult<()>;
    async fn reset_transfer_queue(&self, cluster_name: &str) -> HResult<()>;
    async fn reset_timer_queue(&self, cluster_name: &str) -> HResult<()>;
    async fn describe_transfer_queue(&self, cluster_name: &str) -> HResult<DescribeQueueResponse>;
    async fn describe_timer_queue(&self, cluster_name: &str) -> HResult<DescribeQueueResponse>;

    fn notify_new_history_event(&self, event: HistoryNotification);
    fn notify_new_transfer_tasks(&self, info: &NotifyTaskInfo);
    fn notify_new_timer_tasks(&self, info: &NotifyTaskInfo);
    async fn notify_new_replication_tasks(&self, info: &NotifyTaskInfo);
}

#[async_trait]
impl Engine for HistoryEngine {
    async fn start(self: &Arc<Self>) { HistoryEngine::start(self).await }
    async fn stop(&self) { HistoryEngine::stop(self).await }
    async fn start_workflow_execution(&self, r: &HistoryStartWorkflowExecutionRequest) -> HResult<StartWorkflowExecutionResponse> { HistoryEngine::start_workflow_execution(self, r).await }
    async fn get_mutable_state(&self, r: &GetMutableStateRequest, d: Option<Instant>) -> HResult<GetMutableStateResponse> { HistoryEngine::get_mutable_state(self, r, d).await }
    async fn poll_mutable_state(&self, r: &PollMutableStateRequest, d: Option<Instant>) -> HResult<PollMutableStateResponse> { HistoryEngine::poll_mutable_state(self, r, d).await }
    async fn describe_mutable_state(&self, r: &DescribeMutableStateRequest) -> HResult<DescribeMutableStateResponse> { HistoryEngine::describe_mutable_state(self, r).await }
    async fn reset_sticky_task_list(&self, r: &HistoryResetStickyTaskListRequest) -> HResult<HistoryResetStickyTaskListResponse> { HistoryEngine::reset_sticky_task_list(self, r).await }
    async fn describe_workflow_execution(&self, r: &HistoryDescribeWorkflowExecutionRequest) -> HResult<DescribeWorkflowExecutionResponse> { HistoryEngine::describe_workflow_execution(self, r).await }
    async fn record_decision_task_started(&self, r: &RecordDecisionTaskStartedRequest) -> HResult<RecordDecisionTaskStartedResponse> { HistoryEngine::record_decision_task_started(self, r).await }
    async fn record_activity_task_started(&self, r: &RecordActivityTaskStartedRequest) -> HResult<RecordActivityTaskStartedResponse> { HistoryEngine::record_activity_task_started(self, r).await }
    async fn respond_decision_task_completed(&self, r: &HistoryRespondDecisionTaskCompletedRequest) -> HResult<HistoryRespondDecisionTaskCompletedResponse> { HistoryEngine::respond_decision_task_completed(self, r).await }
    async fn respond_decision_task_failed(&self, r: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()> { HistoryEngine::respond_decision_task_failed(self, r).await }
    async fn respond_activity_task_completed(&self, r: &HistoryRespondActivityTaskCompletedRequest) -> HResult<()> { HistoryEngine::respond_activity_task_completed(self, r).await }
    async fn respond_activity_task_failed(&self, r: &HistoryRespondActivityTaskFailedRequest) -> HResult<()> { HistoryEngine::respond_activity_task_failed(self, r).await }
    async fn respond_activity_task_canceled(&self, r: &HistoryRespondActivityTaskCanceledRequest) -> HResult<()> { HistoryEngine::respond_activity_task_canceled(self, r).await }
    async fn record_activity_task_heartbeat(&self, r: &HistoryRecordActivityTaskHeartbeatRequest) -> HResult<RecordActivityTaskHeartbeatResponse> { HistoryEngine::record_activity_task_heartbeat(self, r).await }
    async fn request_cancel_workflow_execution(&self, r: &HistoryRequestCancelWorkflowExecutionRequest) -> HResult<()> { HistoryEngine::request_cancel_workflow_execution(self, r).await }
    async fn signal_workflow_execution(&self, r: &HistorySignalWorkflowExecutionRequest) -> HResult<()> { HistoryEngine::signal_workflow_execution(self, r).await }
    async fn signal_with_start_workflow_execution(&self, r: &HistorySignalWithStartWorkflowExecutionRequest) -> HResult<StartWorkflowExecutionResponse> { HistoryEngine::signal_with_start_workflow_execution(self, r).await }
    async fn remove_signal_mutable_state(&self, r: &RemoveSignalMutableStateRequest) -> HResult<()> { HistoryEngine::remove_signal_mutable_state(self, r).await }
    async fn terminate_workflow_execution(&self, r: &HistoryTerminateWorkflowExecutionRequest) -> HResult<()> { HistoryEngine::terminate_workflow_execution(self, r).await }
    async fn reset_workflow_execution(&self, r: &HistoryResetWorkflowExecutionRequest) -> HResult<ResetWorkflowExecutionResponse> { HistoryEngine::reset_workflow_execution(self, r).await }
    async fn schedule_decision_task(&self, r: &ScheduleDecisionTaskRequest) -> HResult<()> { HistoryEngine::schedule_decision_task(self, r).await }
    async fn record_child_execution_completed(&self, r: &RecordChildExecutionCompletedRequest) -> HResult<()> { HistoryEngine::record_child_execution_completed(self, r).await }
    async fn replicate_events_v2(&self, r: &ReplicateEventsV2Request) -> HResult<()> { HistoryEngine::replicate_events_v2(self, r).await }
    async fn sync_shard_status(&self, r: &SyncShardStatusRequest) -> HResult<()> { HistoryEngine::sync_shard_status(self, r).await }
    async fn sync_activity(&self, r: &SyncActivityRequest) -> HResult<()> { HistoryEngine::sync_activity(self, r).await }
    async fn get_replication_messages(&self, c: &str, id: i64) -> HResult<ReplicationMessages> { HistoryEngine::get_replication_messages(self, c, id).await }
    async fn get_dlq_replication_messages(&self, i: &[ReplicationTaskInfo]) -> HResult<Vec<ReplicationTask>> { HistoryEngine::get_dlq_replication_messages(self, i).await }
    async fn query_workflow(&self, r: &HistoryQueryWorkflowRequest, d: Option<Instant>) -> HResult<HistoryQueryWorkflowResponse> { HistoryEngine::query_workflow(self, r, d).await }
    async fn reapply_events(&self, d: &str, w: &str, r: &str, e: &[HistoryEvent]) -> HResult<()> { HistoryEngine::reapply_events(self, d, w, r, e).await }
    async fn count_dlq_messages(&self, f: bool) -> HResult<HashMap<String, i64>> { HistoryEngine::count_dlq_messages(self, f).await }
    async fn read_dlq_messages(&self, r: &ReadDLQMessagesRequest) -> HResult<ReadDLQMessagesResponse> { HistoryEngine::read_dlq_messages(self, r).await }
    async fn purge_dlq_messages(&self, r: &PurgeDLQMessagesRequest) -> HResult<()> { HistoryEngine::purge_dlq_messages(self, r).await }
    async fn merge_dlq_messages(&self, r: &MergeDLQMessagesRequest) -> HResult<MergeDLQMessagesResponse> { HistoryEngine::merge_dlq_messages(self, r).await }
    async fn refresh_workflow_tasks(&self, d: &str, e: &WorkflowExecution) -> HResult<()> { HistoryEngine::refresh_workflow_tasks(self, d, e).await }
    async fn reset_transfer_queue(&self, c: &str) -> HResult<()> { HistoryEngine::reset_transfer_queue(self, c).await }
    async fn reset_timer_queue(&self, c: &str) -> HResult<()> { HistoryEngine::reset_timer_queue(self, c).await }
    async fn describe_transfer_queue(&self, c: &str) -> HResult<DescribeQueueResponse> { HistoryEngine::describe_transfer_queue(self, c).await }
    async fn describe_timer_queue(&self, c: &str) -> HResult<DescribeQueueResponse> { HistoryEngine::describe_timer_queue(self, c).await }
    fn notify_new_history_event(&self, e: HistoryNotification) { HistoryEngine::notify_new_history_event(self, e) }
    fn notify_new_transfer_tasks(&self, i: &NotifyTaskInfo) { HistoryEngine::notify_new_transfer_tasks(self, i) }
    fn notify_new_timer_tasks(&self, i: &NotifyTaskInfo) { HistoryEngine::notify_new_timer_tasks(self, i) }
    async fn notify_new_replication_tasks(&self, i: &NotifyTaskInfo) { HistoryEngine::notify_new_replication_tasks(self, i).await }
}
