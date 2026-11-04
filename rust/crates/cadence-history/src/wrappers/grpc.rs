//! Port of wrappers/grpc/{grpc_handler,grpc_handler_generated}.go.
//! TODO(link): provide the upstream wire mapper and dispatcher adapters.
use crate::{engine::deps::HistoryError, handler::Handler};
use std::sync::Arc;

/// Associated types are upstream grpc wire types; each mapper is the corresponding
/// common/types/mapper/proto conversion, including typed transport errors.
pub trait WireMapper: Send + Sync {
    type Error;
    type HealthResponse;
    fn health_response(&self, response: crate::engine::types::HealthStatus)
        -> Self::HealthResponse;
    fn error(&self, error: HistoryError) -> Self::Error;
    type CloseShardRequest;
    type CloseShardResponse;
    fn to_close_shard_request(
        &self,
        request: Self::CloseShardRequest,
    ) -> Option<cadence_api::admin::CloseShardRequest>;
    fn encode_close_shard_response(&self, response: ()) -> Self::CloseShardResponse;
    type DescribeHistoryHostRequest;
    type DescribeHistoryHostResponse;
    fn to_describe_history_host_request(
        &self,
        request: Self::DescribeHistoryHostRequest,
    ) -> Option<cadence_api::admin::DescribeHistoryHostRequest>;
    fn encode_describe_history_host_response(
        &self,
        response: cadence_api::admin::DescribeHistoryHostResponse,
    ) -> Self::DescribeHistoryHostResponse;
    type DescribeMutableStateRequest;
    type DescribeMutableStateResponse;
    fn to_describe_mutable_state_request(
        &self,
        request: Self::DescribeMutableStateRequest,
    ) -> Option<crate::engine::types::DescribeMutableStateRequest>;
    fn encode_describe_mutable_state_response(
        &self,
        response: crate::engine::types::DescribeMutableStateResponse,
    ) -> Self::DescribeMutableStateResponse;
    type DescribeQueueRequest;
    type DescribeQueueResponse;
    fn to_describe_queue_request(
        &self,
        request: Self::DescribeQueueRequest,
    ) -> Option<cadence_api::admin::DescribeQueueRequest>;
    fn encode_describe_queue_response(
        &self,
        response: cadence_api::admin::DescribeQueueResponse,
    ) -> Self::DescribeQueueResponse;
    type DescribeWorkflowExecutionRequest;
    type DescribeWorkflowExecutionResponse;
    fn to_describe_workflow_execution_request(
        &self,
        request: Self::DescribeWorkflowExecutionRequest,
    ) -> Option<crate::engine::types::HistoryDescribeWorkflowExecutionRequest>;
    fn encode_describe_workflow_execution_response(
        &self,
        response: cadence_api::service_types::DescribeWorkflowExecutionResponse,
    ) -> Self::DescribeWorkflowExecutionResponse;
    type GetCrossClusterTasksRequest;
    type GetCrossClusterTasksResponse;
    fn to_get_cross_cluster_tasks_request(
        &self,
        request: Self::GetCrossClusterTasksRequest,
    ) -> Option<cadence_api::admin::GetCrossClusterTasksRequest>;
    fn encode_get_cross_cluster_tasks_response(
        &self,
        response: cadence_api::admin::GetCrossClusterTasksResponse,
    ) -> Self::GetCrossClusterTasksResponse;
    type CountDlqMessagesRequest;
    type CountDlqMessagesResponse;
    fn to_count_dlq_messages_request(
        &self,
        request: Self::CountDlqMessagesRequest,
    ) -> Option<cadence_api::admin::CountDLQMessagesRequest>;
    fn encode_count_dlq_messages_response(
        &self,
        response: crate::engine::types::HistoryCountDLQMessagesResponse,
    ) -> Self::CountDlqMessagesResponse;
    type GetDlqReplicationMessagesRequest;
    type GetDlqReplicationMessagesResponse;
    fn to_get_dlq_replication_messages_request(
        &self,
        request: Self::GetDlqReplicationMessagesRequest,
    ) -> Option<cadence_api::admin::GetDLQReplicationMessagesRequest>;
    fn encode_get_dlq_replication_messages_response(
        &self,
        response: cadence_api::admin::GetDLQReplicationMessagesResponse,
    ) -> Self::GetDlqReplicationMessagesResponse;
    type GetMutableStateRequest;
    type GetMutableStateResponse;
    fn to_get_mutable_state_request(
        &self,
        request: Self::GetMutableStateRequest,
    ) -> Option<crate::engine::types::GetMutableStateRequest>;
    fn encode_get_mutable_state_response(
        &self,
        response: crate::engine::types::GetMutableStateResponse,
    ) -> Self::GetMutableStateResponse;
    type GetReplicationMessagesRequest;
    type GetReplicationMessagesResponse;
    fn to_get_replication_messages_request(
        &self,
        request: Self::GetReplicationMessagesRequest,
    ) -> Option<cadence_api::admin::GetReplicationMessagesRequest>;
    fn encode_get_replication_messages_response(
        &self,
        response: cadence_api::admin::GetReplicationMessagesResponse,
    ) -> Self::GetReplicationMessagesResponse;
    type MergeDlqMessagesRequest;
    type MergeDlqMessagesResponse;
    fn to_merge_dlq_messages_request(
        &self,
        request: Self::MergeDlqMessagesRequest,
    ) -> Option<cadence_api::admin::MergeDLQMessagesRequest>;
    fn encode_merge_dlq_messages_response(
        &self,
        response: cadence_api::admin::MergeDLQMessagesResponse,
    ) -> Self::MergeDlqMessagesResponse;
    type NotifyFailoverMarkersRequest;
    type NotifyFailoverMarkersResponse;
    fn to_notify_failover_markers_request(
        &self,
        request: Self::NotifyFailoverMarkersRequest,
    ) -> Option<crate::engine::types::NotifyFailoverMarkersRequest>;
    fn encode_notify_failover_markers_response(
        &self,
        response: (),
    ) -> Self::NotifyFailoverMarkersResponse;
    type PollMutableStateRequest;
    type PollMutableStateResponse;
    fn to_poll_mutable_state_request(
        &self,
        request: Self::PollMutableStateRequest,
    ) -> Option<crate::engine::types::PollMutableStateRequest>;
    fn encode_poll_mutable_state_response(
        &self,
        response: crate::engine::types::PollMutableStateResponse,
    ) -> Self::PollMutableStateResponse;
    type PurgeDlqMessagesRequest;
    type PurgeDlqMessagesResponse;
    fn to_purge_dlq_messages_request(
        &self,
        request: Self::PurgeDlqMessagesRequest,
    ) -> Option<cadence_api::admin::PurgeDLQMessagesRequest>;
    fn encode_purge_dlq_messages_response(&self, response: ()) -> Self::PurgeDlqMessagesResponse;
    type QueryWorkflowRequest;
    type QueryWorkflowResponse;
    fn to_query_workflow_request(
        &self,
        request: Self::QueryWorkflowRequest,
    ) -> Option<crate::engine::types::HistoryQueryWorkflowRequest>;
    fn encode_query_workflow_response(
        &self,
        response: crate::engine::types::HistoryQueryWorkflowResponse,
    ) -> Self::QueryWorkflowResponse;
    type ReadDlqMessagesRequest;
    type ReadDlqMessagesResponse;
    fn to_read_dlq_messages_request(
        &self,
        request: Self::ReadDlqMessagesRequest,
    ) -> Option<cadence_api::admin::ReadDLQMessagesRequest>;
    fn encode_read_dlq_messages_response(
        &self,
        response: cadence_api::admin::ReadDLQMessagesResponse,
    ) -> Self::ReadDlqMessagesResponse;
    type ReapplyEventsRequest;
    type ReapplyEventsResponse;
    fn to_reapply_events_request(
        &self,
        request: Self::ReapplyEventsRequest,
    ) -> Option<crate::engine::types::HistoryReapplyEventsRequest>;
    fn encode_reapply_events_response(&self, response: ()) -> Self::ReapplyEventsResponse;
    type RecordActivityTaskHeartbeatRequest;
    type RecordActivityTaskHeartbeatResponse;
    fn to_record_activity_task_heartbeat_request(
        &self,
        request: Self::RecordActivityTaskHeartbeatRequest,
    ) -> Option<crate::engine::types::HistoryRecordActivityTaskHeartbeatRequest>;
    fn encode_record_activity_task_heartbeat_response(
        &self,
        response: cadence_api::tasks::RecordActivityTaskHeartbeatResponse,
    ) -> Self::RecordActivityTaskHeartbeatResponse;
    type RecordActivityTaskStartedRequest;
    type RecordActivityTaskStartedResponse;
    fn to_record_activity_task_started_request(
        &self,
        request: Self::RecordActivityTaskStartedRequest,
    ) -> Option<crate::engine::types::RecordActivityTaskStartedRequest>;
    fn encode_record_activity_task_started_response(
        &self,
        response: crate::engine::types::RecordActivityTaskStartedResponse,
    ) -> Self::RecordActivityTaskStartedResponse;
    type RecordChildExecutionCompletedRequest;
    type RecordChildExecutionCompletedResponse;
    fn to_record_child_execution_completed_request(
        &self,
        request: Self::RecordChildExecutionCompletedRequest,
    ) -> Option<crate::engine::types::RecordChildExecutionCompletedRequest>;
    fn encode_record_child_execution_completed_response(
        &self,
        response: (),
    ) -> Self::RecordChildExecutionCompletedResponse;
    type RecordDecisionTaskStartedRequest;
    type RecordDecisionTaskStartedResponse;
    fn to_record_decision_task_started_request(
        &self,
        request: Self::RecordDecisionTaskStartedRequest,
    ) -> Option<crate::engine::types::RecordDecisionTaskStartedRequest>;
    fn encode_record_decision_task_started_response(
        &self,
        response: crate::engine::types::RecordDecisionTaskStartedResponse,
    ) -> Self::RecordDecisionTaskStartedResponse;
    type RefreshWorkflowTasksRequest;
    type RefreshWorkflowTasksResponse;
    fn to_refresh_workflow_tasks_request(
        &self,
        request: Self::RefreshWorkflowTasksRequest,
    ) -> Option<crate::engine::types::HistoryRefreshWorkflowTasksRequest>;
    fn encode_refresh_workflow_tasks_response(
        &self,
        response: (),
    ) -> Self::RefreshWorkflowTasksResponse;
    type RemoveSignalMutableStateRequest;
    type RemoveSignalMutableStateResponse;
    fn to_remove_signal_mutable_state_request(
        &self,
        request: Self::RemoveSignalMutableStateRequest,
    ) -> Option<crate::engine::types::RemoveSignalMutableStateRequest>;
    fn encode_remove_signal_mutable_state_response(
        &self,
        response: (),
    ) -> Self::RemoveSignalMutableStateResponse;
    type RemoveTaskRequest;
    type RemoveTaskResponse;
    fn to_remove_task_request(
        &self,
        request: Self::RemoveTaskRequest,
    ) -> Option<cadence_api::admin::RemoveTaskRequest>;
    fn encode_remove_task_response(&self, response: ()) -> Self::RemoveTaskResponse;
    type ReplicateEventsV2Request;
    type ReplicateEventsV2Response;
    fn to_replicate_events_v2_request(
        &self,
        request: Self::ReplicateEventsV2Request,
    ) -> Option<crate::engine::types::ReplicateEventsV2Request>;
    fn encode_replicate_events_v2_response(&self, response: ()) -> Self::ReplicateEventsV2Response;
    type RequestCancelWorkflowExecutionRequest;
    type RequestCancelWorkflowExecutionResponse;
    fn to_request_cancel_workflow_execution_request(
        &self,
        request: Self::RequestCancelWorkflowExecutionRequest,
    ) -> Option<crate::engine::types::HistoryRequestCancelWorkflowExecutionRequest>;
    fn encode_request_cancel_workflow_execution_response(
        &self,
        response: (),
    ) -> Self::RequestCancelWorkflowExecutionResponse;
    type ResetQueueRequest;
    type ResetQueueResponse;
    fn to_reset_queue_request(
        &self,
        request: Self::ResetQueueRequest,
    ) -> Option<cadence_api::admin::ResetQueueRequest>;
    fn encode_reset_queue_response(&self, response: ()) -> Self::ResetQueueResponse;
    type ResetStickyTaskListRequest;
    type ResetStickyTaskListResponse;
    fn to_reset_sticky_task_list_request(
        &self,
        request: Self::ResetStickyTaskListRequest,
    ) -> Option<crate::engine::types::HistoryResetStickyTaskListRequest>;
    fn encode_reset_sticky_task_list_response(
        &self,
        response: crate::engine::types::HistoryResetStickyTaskListResponse,
    ) -> Self::ResetStickyTaskListResponse;
    type ResetWorkflowExecutionRequest;
    type ResetWorkflowExecutionResponse;
    fn to_reset_workflow_execution_request(
        &self,
        request: Self::ResetWorkflowExecutionRequest,
    ) -> Option<crate::engine::types::HistoryResetWorkflowExecutionRequest>;
    fn encode_reset_workflow_execution_response(
        &self,
        response: cadence_api::types::ResetWorkflowExecutionResponse,
    ) -> Self::ResetWorkflowExecutionResponse;
    type RespondActivityTaskCanceledRequest;
    type RespondActivityTaskCanceledResponse;
    fn to_respond_activity_task_canceled_request(
        &self,
        request: Self::RespondActivityTaskCanceledRequest,
    ) -> Option<crate::engine::types::HistoryRespondActivityTaskCanceledRequest>;
    fn encode_respond_activity_task_canceled_response(
        &self,
        response: (),
    ) -> Self::RespondActivityTaskCanceledResponse;
    type RespondActivityTaskCompletedRequest;
    type RespondActivityTaskCompletedResponse;
    fn to_respond_activity_task_completed_request(
        &self,
        request: Self::RespondActivityTaskCompletedRequest,
    ) -> Option<crate::engine::types::HistoryRespondActivityTaskCompletedRequest>;
    fn encode_respond_activity_task_completed_response(
        &self,
        response: (),
    ) -> Self::RespondActivityTaskCompletedResponse;
    type RespondActivityTaskFailedRequest;
    type RespondActivityTaskFailedResponse;
    fn to_respond_activity_task_failed_request(
        &self,
        request: Self::RespondActivityTaskFailedRequest,
    ) -> Option<crate::engine::types::HistoryRespondActivityTaskFailedRequest>;
    fn encode_respond_activity_task_failed_response(
        &self,
        response: (),
    ) -> Self::RespondActivityTaskFailedResponse;
    type RespondCrossClusterTasksCompletedRequest;
    type RespondCrossClusterTasksCompletedResponse;
    fn to_respond_cross_cluster_tasks_completed_request(
        &self,
        request: Self::RespondCrossClusterTasksCompletedRequest,
    ) -> Option<cadence_api::admin::RespondCrossClusterTasksCompletedRequest>;
    fn encode_respond_cross_cluster_tasks_completed_response(
        &self,
        response: cadence_api::admin::RespondCrossClusterTasksCompletedResponse,
    ) -> Self::RespondCrossClusterTasksCompletedResponse;
    type RespondDecisionTaskCompletedRequest;
    type RespondDecisionTaskCompletedResponse;
    fn to_respond_decision_task_completed_request(
        &self,
        request: Self::RespondDecisionTaskCompletedRequest,
    ) -> Option<crate::engine::types::HistoryRespondDecisionTaskCompletedRequest>;
    fn encode_respond_decision_task_completed_response(
        &self,
        response: crate::engine::types::HistoryRespondDecisionTaskCompletedResponse,
    ) -> Self::RespondDecisionTaskCompletedResponse;
    type RespondDecisionTaskFailedRequest;
    type RespondDecisionTaskFailedResponse;
    fn to_respond_decision_task_failed_request(
        &self,
        request: Self::RespondDecisionTaskFailedRequest,
    ) -> Option<crate::engine::types::HistoryRespondDecisionTaskFailedRequest>;
    fn encode_respond_decision_task_failed_response(
        &self,
        response: (),
    ) -> Self::RespondDecisionTaskFailedResponse;
    type ScheduleDecisionTaskRequest;
    type ScheduleDecisionTaskResponse;
    fn to_schedule_decision_task_request(
        &self,
        request: Self::ScheduleDecisionTaskRequest,
    ) -> Option<crate::engine::types::ScheduleDecisionTaskRequest>;
    fn encode_schedule_decision_task_response(
        &self,
        response: (),
    ) -> Self::ScheduleDecisionTaskResponse;
    type SignalWithStartWorkflowExecutionRequest;
    type SignalWithStartWorkflowExecutionResponse;
    fn to_signal_with_start_workflow_execution_request(
        &self,
        request: Self::SignalWithStartWorkflowExecutionRequest,
    ) -> Option<crate::engine::types::HistorySignalWithStartWorkflowExecutionRequest>;
    fn encode_signal_with_start_workflow_execution_response(
        &self,
        response: cadence_api::types::StartWorkflowExecutionResponse,
    ) -> Self::SignalWithStartWorkflowExecutionResponse;
    type SignalWorkflowExecutionRequest;
    type SignalWorkflowExecutionResponse;
    fn to_signal_workflow_execution_request(
        &self,
        request: Self::SignalWorkflowExecutionRequest,
    ) -> Option<crate::engine::types::HistorySignalWorkflowExecutionRequest>;
    fn encode_signal_workflow_execution_response(
        &self,
        response: (),
    ) -> Self::SignalWorkflowExecutionResponse;
    type StartWorkflowExecutionRequest;
    type StartWorkflowExecutionResponse;
    fn to_start_workflow_execution_request(
        &self,
        request: Self::StartWorkflowExecutionRequest,
    ) -> Option<crate::engine::types::HistoryStartWorkflowExecutionRequest>;
    fn encode_start_workflow_execution_response(
        &self,
        response: cadence_api::types::StartWorkflowExecutionResponse,
    ) -> Self::StartWorkflowExecutionResponse;
    type SyncActivityRequest;
    type SyncActivityResponse;
    fn to_sync_activity_request(
        &self,
        request: Self::SyncActivityRequest,
    ) -> Option<crate::engine::types::SyncActivityRequest>;
    fn encode_sync_activity_response(&self, response: ()) -> Self::SyncActivityResponse;
    type SyncShardStatusRequest;
    type SyncShardStatusResponse;
    fn to_sync_shard_status_request(
        &self,
        request: Self::SyncShardStatusRequest,
    ) -> Option<crate::engine::types::SyncShardStatusRequest>;
    fn encode_sync_shard_status_response(&self, response: ()) -> Self::SyncShardStatusResponse;
    type TerminateWorkflowExecutionRequest;
    type TerminateWorkflowExecutionResponse;
    fn to_terminate_workflow_execution_request(
        &self,
        request: Self::TerminateWorkflowExecutionRequest,
    ) -> Option<crate::engine::types::HistoryTerminateWorkflowExecutionRequest>;
    fn encode_terminate_workflow_execution_response(
        &self,
        response: (),
    ) -> Self::TerminateWorkflowExecutionResponse;
    type GetFailoverInfoRequest;
    type GetFailoverInfoResponse;
    fn to_get_failover_info_request(
        &self,
        request: Self::GetFailoverInfoRequest,
    ) -> Option<crate::engine::types::GetFailoverInfoRequest>;
    fn encode_get_failover_info_response(
        &self,
        response: crate::engine::types::GetFailoverInfoResponse,
    ) -> Self::GetFailoverInfoResponse;
    type RatelimitUpdateRequest;
    type RatelimitUpdateResponse;
    fn to_ratelimit_update_request(
        &self,
        request: Self::RatelimitUpdateRequest,
    ) -> Option<crate::engine::types::RatelimitUpdateRequest>;
    fn encode_ratelimit_update_response(
        &self,
        response: crate::engine::types::RatelimitUpdateResponse,
    ) -> Self::RatelimitUpdateResponse;
}

pub trait Dispatcher<M: WireMapper> {
    fn register_history_api(&mut self, handler: Arc<GrpcHandler<M>>);
    fn register_meta_api(&mut self, handler: Arc<GrpcHandler<M>>);
}
pub struct GrpcHandler<M: WireMapper> {
    pub handler: Arc<dyn Handler>,
    pub mapper: M,
}
impl<M: WireMapper> GrpcHandler<M> {
    pub fn new(handler: Arc<dyn Handler>, mapper: M) -> Self {
        Self { handler, mapper }
    }
    pub fn register(self: &Arc<Self>, dispatcher: &mut impl Dispatcher<M>) {
        dispatcher.register_history_api(self.clone());
        dispatcher.register_meta_api(self.clone());
    }
    pub async fn health(&self) -> Result<M::HealthResponse, M::Error> {
        self.handler
            .health()
            .await
            .map(|response| self.mapper.health_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn close_shard(
        &self,
        request: M::CloseShardRequest,
    ) -> Result<M::CloseShardResponse, M::Error> {
        let request = self.mapper.to_close_shard_request(request);
        self.handler
            .close_shard(request.as_ref())
            .await
            .map(|response| self.mapper.encode_close_shard_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn describe_history_host(
        &self,
        request: M::DescribeHistoryHostRequest,
    ) -> Result<M::DescribeHistoryHostResponse, M::Error> {
        let request = self.mapper.to_describe_history_host_request(request);
        self.handler
            .describe_history_host(request.as_ref())
            .await
            .map(|response| self.mapper.encode_describe_history_host_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn describe_mutable_state(
        &self,
        request: M::DescribeMutableStateRequest,
    ) -> Result<M::DescribeMutableStateResponse, M::Error> {
        let request = self.mapper.to_describe_mutable_state_request(request);
        self.handler
            .describe_mutable_state(request.as_ref())
            .await
            .map(|response| self.mapper.encode_describe_mutable_state_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn describe_queue(
        &self,
        request: M::DescribeQueueRequest,
    ) -> Result<M::DescribeQueueResponse, M::Error> {
        let request = self.mapper.to_describe_queue_request(request);
        self.handler
            .describe_queue(request.as_ref())
            .await
            .map(|response| self.mapper.encode_describe_queue_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn describe_workflow_execution(
        &self,
        request: M::DescribeWorkflowExecutionRequest,
    ) -> Result<M::DescribeWorkflowExecutionResponse, M::Error> {
        let request = self.mapper.to_describe_workflow_execution_request(request);
        self.handler
            .describe_workflow_execution(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_describe_workflow_execution_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn get_cross_cluster_tasks(
        &self,
        request: M::GetCrossClusterTasksRequest,
    ) -> Result<M::GetCrossClusterTasksResponse, M::Error> {
        let request = self.mapper.to_get_cross_cluster_tasks_request(request);
        self.handler
            .get_cross_cluster_tasks(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_get_cross_cluster_tasks_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn count_dlq_messages(
        &self,
        request: M::CountDlqMessagesRequest,
    ) -> Result<M::CountDlqMessagesResponse, M::Error> {
        let request = self.mapper.to_count_dlq_messages_request(request);
        self.handler
            .count_dlq_messages(request.as_ref())
            .await
            .map(|response| self.mapper.encode_count_dlq_messages_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn get_dlq_replication_messages(
        &self,
        request: M::GetDlqReplicationMessagesRequest,
    ) -> Result<M::GetDlqReplicationMessagesResponse, M::Error> {
        let request = self.mapper.to_get_dlq_replication_messages_request(request);
        self.handler
            .get_dlq_replication_messages(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_get_dlq_replication_messages_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn get_mutable_state(
        &self,
        request: M::GetMutableStateRequest,
    ) -> Result<M::GetMutableStateResponse, M::Error> {
        let request = self.mapper.to_get_mutable_state_request(request);
        self.handler
            .get_mutable_state(request.as_ref())
            .await
            .map(|response| self.mapper.encode_get_mutable_state_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn get_replication_messages(
        &self,
        request: M::GetReplicationMessagesRequest,
    ) -> Result<M::GetReplicationMessagesResponse, M::Error> {
        let request = self.mapper.to_get_replication_messages_request(request);
        self.handler
            .get_replication_messages(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_get_replication_messages_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn merge_dlq_messages(
        &self,
        request: M::MergeDlqMessagesRequest,
    ) -> Result<M::MergeDlqMessagesResponse, M::Error> {
        let request = self.mapper.to_merge_dlq_messages_request(request);
        self.handler
            .merge_dlq_messages(request.as_ref())
            .await
            .map(|response| self.mapper.encode_merge_dlq_messages_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn notify_failover_markers(
        &self,
        request: M::NotifyFailoverMarkersRequest,
    ) -> Result<M::NotifyFailoverMarkersResponse, M::Error> {
        let request = self.mapper.to_notify_failover_markers_request(request);
        self.handler
            .notify_failover_markers(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_notify_failover_markers_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn poll_mutable_state(
        &self,
        request: M::PollMutableStateRequest,
    ) -> Result<M::PollMutableStateResponse, M::Error> {
        let request = self.mapper.to_poll_mutable_state_request(request);
        self.handler
            .poll_mutable_state(request.as_ref())
            .await
            .map(|response| self.mapper.encode_poll_mutable_state_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn purge_dlq_messages(
        &self,
        request: M::PurgeDlqMessagesRequest,
    ) -> Result<M::PurgeDlqMessagesResponse, M::Error> {
        let request = self.mapper.to_purge_dlq_messages_request(request);
        self.handler
            .purge_dlq_messages(request.as_ref())
            .await
            .map(|response| self.mapper.encode_purge_dlq_messages_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn query_workflow(
        &self,
        request: M::QueryWorkflowRequest,
    ) -> Result<M::QueryWorkflowResponse, M::Error> {
        let request = self.mapper.to_query_workflow_request(request);
        self.handler
            .query_workflow(request.as_ref())
            .await
            .map(|response| self.mapper.encode_query_workflow_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn read_dlq_messages(
        &self,
        request: M::ReadDlqMessagesRequest,
    ) -> Result<M::ReadDlqMessagesResponse, M::Error> {
        let request = self.mapper.to_read_dlq_messages_request(request);
        self.handler
            .read_dlq_messages(request.as_ref())
            .await
            .map(|response| self.mapper.encode_read_dlq_messages_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn reapply_events(
        &self,
        request: M::ReapplyEventsRequest,
    ) -> Result<M::ReapplyEventsResponse, M::Error> {
        let request = self.mapper.to_reapply_events_request(request);
        self.handler
            .reapply_events(request.as_ref())
            .await
            .map(|response| self.mapper.encode_reapply_events_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn record_activity_task_heartbeat(
        &self,
        request: M::RecordActivityTaskHeartbeatRequest,
    ) -> Result<M::RecordActivityTaskHeartbeatResponse, M::Error> {
        let request = self
            .mapper
            .to_record_activity_task_heartbeat_request(request);
        self.handler
            .record_activity_task_heartbeat(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_record_activity_task_heartbeat_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn record_activity_task_started(
        &self,
        request: M::RecordActivityTaskStartedRequest,
    ) -> Result<M::RecordActivityTaskStartedResponse, M::Error> {
        let request = self.mapper.to_record_activity_task_started_request(request);
        self.handler
            .record_activity_task_started(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_record_activity_task_started_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn record_child_execution_completed(
        &self,
        request: M::RecordChildExecutionCompletedRequest,
    ) -> Result<M::RecordChildExecutionCompletedResponse, M::Error> {
        let request = self
            .mapper
            .to_record_child_execution_completed_request(request);
        self.handler
            .record_child_execution_completed(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_record_child_execution_completed_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn record_decision_task_started(
        &self,
        request: M::RecordDecisionTaskStartedRequest,
    ) -> Result<M::RecordDecisionTaskStartedResponse, M::Error> {
        let request = self.mapper.to_record_decision_task_started_request(request);
        self.handler
            .record_decision_task_started(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_record_decision_task_started_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn refresh_workflow_tasks(
        &self,
        request: M::RefreshWorkflowTasksRequest,
    ) -> Result<M::RefreshWorkflowTasksResponse, M::Error> {
        let request = self.mapper.to_refresh_workflow_tasks_request(request);
        self.handler
            .refresh_workflow_tasks(request.as_ref())
            .await
            .map(|response| self.mapper.encode_refresh_workflow_tasks_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn remove_signal_mutable_state(
        &self,
        request: M::RemoveSignalMutableStateRequest,
    ) -> Result<M::RemoveSignalMutableStateResponse, M::Error> {
        let request = self.mapper.to_remove_signal_mutable_state_request(request);
        self.handler
            .remove_signal_mutable_state(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_remove_signal_mutable_state_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn remove_task(
        &self,
        request: M::RemoveTaskRequest,
    ) -> Result<M::RemoveTaskResponse, M::Error> {
        let request = self.mapper.to_remove_task_request(request);
        self.handler
            .remove_task(request.as_ref())
            .await
            .map(|response| self.mapper.encode_remove_task_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn replicate_events_v2(
        &self,
        request: M::ReplicateEventsV2Request,
    ) -> Result<M::ReplicateEventsV2Response, M::Error> {
        let request = self.mapper.to_replicate_events_v2_request(request);
        self.handler
            .replicate_events_v2(request.as_ref())
            .await
            .map(|response| self.mapper.encode_replicate_events_v2_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn request_cancel_workflow_execution(
        &self,
        request: M::RequestCancelWorkflowExecutionRequest,
    ) -> Result<M::RequestCancelWorkflowExecutionResponse, M::Error> {
        let request = self
            .mapper
            .to_request_cancel_workflow_execution_request(request);
        self.handler
            .request_cancel_workflow_execution(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_request_cancel_workflow_execution_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn reset_queue(
        &self,
        request: M::ResetQueueRequest,
    ) -> Result<M::ResetQueueResponse, M::Error> {
        let request = self.mapper.to_reset_queue_request(request);
        self.handler
            .reset_queue(request.as_ref())
            .await
            .map(|response| self.mapper.encode_reset_queue_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn reset_sticky_task_list(
        &self,
        request: M::ResetStickyTaskListRequest,
    ) -> Result<M::ResetStickyTaskListResponse, M::Error> {
        let request = self.mapper.to_reset_sticky_task_list_request(request);
        self.handler
            .reset_sticky_task_list(request.as_ref())
            .await
            .map(|response| self.mapper.encode_reset_sticky_task_list_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn reset_workflow_execution(
        &self,
        request: M::ResetWorkflowExecutionRequest,
    ) -> Result<M::ResetWorkflowExecutionResponse, M::Error> {
        let request = self.mapper.to_reset_workflow_execution_request(request);
        self.handler
            .reset_workflow_execution(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_reset_workflow_execution_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn respond_activity_task_canceled(
        &self,
        request: M::RespondActivityTaskCanceledRequest,
    ) -> Result<M::RespondActivityTaskCanceledResponse, M::Error> {
        let request = self
            .mapper
            .to_respond_activity_task_canceled_request(request);
        self.handler
            .respond_activity_task_canceled(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_respond_activity_task_canceled_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn respond_activity_task_completed(
        &self,
        request: M::RespondActivityTaskCompletedRequest,
    ) -> Result<M::RespondActivityTaskCompletedResponse, M::Error> {
        let request = self
            .mapper
            .to_respond_activity_task_completed_request(request);
        self.handler
            .respond_activity_task_completed(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_respond_activity_task_completed_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn respond_activity_task_failed(
        &self,
        request: M::RespondActivityTaskFailedRequest,
    ) -> Result<M::RespondActivityTaskFailedResponse, M::Error> {
        let request = self.mapper.to_respond_activity_task_failed_request(request);
        self.handler
            .respond_activity_task_failed(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_respond_activity_task_failed_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn respond_cross_cluster_tasks_completed(
        &self,
        request: M::RespondCrossClusterTasksCompletedRequest,
    ) -> Result<M::RespondCrossClusterTasksCompletedResponse, M::Error> {
        let request = self
            .mapper
            .to_respond_cross_cluster_tasks_completed_request(request);
        self.handler
            .respond_cross_cluster_tasks_completed(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_respond_cross_cluster_tasks_completed_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn respond_decision_task_completed(
        &self,
        request: M::RespondDecisionTaskCompletedRequest,
    ) -> Result<M::RespondDecisionTaskCompletedResponse, M::Error> {
        let request = self
            .mapper
            .to_respond_decision_task_completed_request(request);
        self.handler
            .respond_decision_task_completed(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_respond_decision_task_completed_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn respond_decision_task_failed(
        &self,
        request: M::RespondDecisionTaskFailedRequest,
    ) -> Result<M::RespondDecisionTaskFailedResponse, M::Error> {
        let request = self.mapper.to_respond_decision_task_failed_request(request);
        self.handler
            .respond_decision_task_failed(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_respond_decision_task_failed_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn schedule_decision_task(
        &self,
        request: M::ScheduleDecisionTaskRequest,
    ) -> Result<M::ScheduleDecisionTaskResponse, M::Error> {
        let request = self.mapper.to_schedule_decision_task_request(request);
        self.handler
            .schedule_decision_task(request.as_ref())
            .await
            .map(|response| self.mapper.encode_schedule_decision_task_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn signal_with_start_workflow_execution(
        &self,
        request: M::SignalWithStartWorkflowExecutionRequest,
    ) -> Result<M::SignalWithStartWorkflowExecutionResponse, M::Error> {
        let request = self
            .mapper
            .to_signal_with_start_workflow_execution_request(request);
        self.handler
            .signal_with_start_workflow_execution(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_signal_with_start_workflow_execution_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn signal_workflow_execution(
        &self,
        request: M::SignalWorkflowExecutionRequest,
    ) -> Result<M::SignalWorkflowExecutionResponse, M::Error> {
        let request = self.mapper.to_signal_workflow_execution_request(request);
        self.handler
            .signal_workflow_execution(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_signal_workflow_execution_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn start_workflow_execution(
        &self,
        request: M::StartWorkflowExecutionRequest,
    ) -> Result<M::StartWorkflowExecutionResponse, M::Error> {
        let request = self.mapper.to_start_workflow_execution_request(request);
        self.handler
            .start_workflow_execution(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_start_workflow_execution_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn sync_activity(
        &self,
        request: M::SyncActivityRequest,
    ) -> Result<M::SyncActivityResponse, M::Error> {
        let request = self.mapper.to_sync_activity_request(request);
        self.handler
            .sync_activity(request.as_ref())
            .await
            .map(|response| self.mapper.encode_sync_activity_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn sync_shard_status(
        &self,
        request: M::SyncShardStatusRequest,
    ) -> Result<M::SyncShardStatusResponse, M::Error> {
        let request = self.mapper.to_sync_shard_status_request(request);
        self.handler
            .sync_shard_status(request.as_ref())
            .await
            .map(|response| self.mapper.encode_sync_shard_status_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn terminate_workflow_execution(
        &self,
        request: M::TerminateWorkflowExecutionRequest,
    ) -> Result<M::TerminateWorkflowExecutionResponse, M::Error> {
        let request = self.mapper.to_terminate_workflow_execution_request(request);
        self.handler
            .terminate_workflow_execution(request.as_ref())
            .await
            .map(|response| {
                self.mapper
                    .encode_terminate_workflow_execution_response(response)
            })
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn get_failover_info(
        &self,
        request: M::GetFailoverInfoRequest,
    ) -> Result<M::GetFailoverInfoResponse, M::Error> {
        let request = self.mapper.to_get_failover_info_request(request);
        self.handler
            .get_failover_info(request.as_ref())
            .await
            .map(|response| self.mapper.encode_get_failover_info_response(response))
            .map_err(|error| self.mapper.error(error))
    }
    pub async fn ratelimit_update(
        &self,
        request: M::RatelimitUpdateRequest,
    ) -> Result<M::RatelimitUpdateResponse, M::Error> {
        let request = self.mapper.to_ratelimit_update_request(request);
        self.handler
            .ratelimit_update(request.as_ref())
            .await
            .map(|response| self.mapper.encode_ratelimit_update_response(response))
            .map_err(|error| self.mapper.error(error))
    }
}
