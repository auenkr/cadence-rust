//! gRPC `uber.cadence.history.v1.HistoryAPI` server (port of
//! go/service/history/wrappers/grpc + handler wiring).
//!
//! Methods whose proto<->engine type conversions exist are served; all other
//! RPCs answer `UNIMPLEMENTED` until their converters land (see memory doc).
use super::{errors, ServerState};
use cadence_proto::history as pb;
use cadence_proto::history::history_api_server::{HistoryApi, HistoryApiServer};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::{Request, Response, Status};

pub struct GrpcHistory {
    pub state: ServerState,
}

fn gate(state: &ServerState) -> Result<(), Status> {
    if state.limiter.allow() {
        Ok(())
    } else {
        Err(errors::grpc_status(&errors::busy()))
    }
}

fn task_type(v: i32) -> Option<i32> {
    if v == 0 { None } else { Some(v) }
}

#[tonic::async_trait]
impl HistoryApi for GrpcHistory {
    async fn start_workflow_execution(&self, request: Request<pb::StartWorkflowExecutionRequest>) -> Result<Response<pb::StartWorkflowExecutionResponse>, Status> {
        super::grpc_g1::start_workflow_execution(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn signal_with_start_workflow_execution(&self, request: Request<pb::SignalWithStartWorkflowExecutionRequest>) -> Result<Response<pb::SignalWithStartWorkflowExecutionResponse>, Status> {
        super::grpc_g1::signal_with_start_workflow_execution(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn signal_workflow_execution(&self, request: Request<pb::SignalWorkflowExecutionRequest>) -> Result<Response<pb::SignalWorkflowExecutionResponse>, Status> {
        super::grpc_g1::signal_workflow_execution(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn terminate_workflow_execution(&self, request: Request<pb::TerminateWorkflowExecutionRequest>) -> Result<Response<pb::TerminateWorkflowExecutionResponse>, Status> {
        super::grpc_g1::terminate_workflow_execution(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn request_cancel_workflow_execution(&self, request: Request<pb::RequestCancelWorkflowExecutionRequest>) -> Result<Response<pb::RequestCancelWorkflowExecutionResponse>, Status> {
        super::grpc_g1::request_cancel_workflow_execution(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn reset_sticky_task_list(&self, request: Request<pb::ResetStickyTaskListRequest>) -> Result<Response<pb::ResetStickyTaskListResponse>, Status> {
        super::grpc_g1::reset_sticky_task_list(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn query_workflow(&self, request: Request<pb::QueryWorkflowRequest>) -> Result<Response<pb::QueryWorkflowResponse>, Status> {
        super::grpc_g1::query_workflow(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn describe_workflow_execution(&self, request: Request<pb::DescribeWorkflowExecutionRequest>) -> Result<Response<pb::DescribeWorkflowExecutionResponse>, Status> {
        super::grpc_g1::describe_workflow_execution(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn reapply_events(&self, request: Request<pb::ReapplyEventsRequest>) -> Result<Response<pb::ReapplyEventsResponse>, Status> {
        super::grpc_g1::reapply_events(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn refresh_workflow_tasks(&self, request: Request<pb::RefreshWorkflowTasksRequest>) -> Result<Response<pb::RefreshWorkflowTasksResponse>, Status> {
        super::grpc_g1::refresh_workflow_tasks(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn schedule_decision_task(&self, request: Request<pb::ScheduleDecisionTaskRequest>) -> Result<Response<pb::ScheduleDecisionTaskResponse>, Status> {
        super::grpc_g1::schedule_decision_task(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn get_mutable_state(&self, request: Request<pb::GetMutableStateRequest>) -> Result<Response<pb::GetMutableStateResponse>, Status> {
        super::grpc_g2::get_mutable_state(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn poll_mutable_state(&self, request: Request<pb::PollMutableStateRequest>) -> Result<Response<pb::PollMutableStateResponse>, Status> {
        super::grpc_g2::poll_mutable_state(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn describe_mutable_state(&self, request: Request<pb::DescribeMutableStateRequest>) -> Result<Response<pb::DescribeMutableStateResponse>, Status> {
        super::grpc_g2::describe_mutable_state(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn record_decision_task_started(&self, request: Request<pb::RecordDecisionTaskStartedRequest>) -> Result<Response<pb::RecordDecisionTaskStartedResponse>, Status> {
        super::grpc_g2::record_decision_task_started(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn record_activity_task_started(&self, request: Request<pb::RecordActivityTaskStartedRequest>) -> Result<Response<pb::RecordActivityTaskStartedResponse>, Status> {
        super::grpc_g2::record_activity_task_started(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn respond_decision_task_completed(&self, request: Request<pb::RespondDecisionTaskCompletedRequest>) -> Result<Response<pb::RespondDecisionTaskCompletedResponse>, Status> {
        super::grpc_g2::respond_decision_task_completed(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn respond_decision_task_failed(&self, request: Request<pb::RespondDecisionTaskFailedRequest>) -> Result<Response<pb::RespondDecisionTaskFailedResponse>, Status> {
        super::grpc_g2::respond_decision_task_failed(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn respond_activity_task_completed(&self, request: Request<pb::RespondActivityTaskCompletedRequest>) -> Result<Response<pb::RespondActivityTaskCompletedResponse>, Status> {
        super::grpc_g2::respond_activity_task_completed(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn respond_activity_task_failed(&self, request: Request<pb::RespondActivityTaskFailedRequest>) -> Result<Response<pb::RespondActivityTaskFailedResponse>, Status> {
        super::grpc_g2::respond_activity_task_failed(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn respond_activity_task_canceled(&self, request: Request<pb::RespondActivityTaskCanceledRequest>) -> Result<Response<pb::RespondActivityTaskCanceledResponse>, Status> {
        super::grpc_g2::respond_activity_task_canceled(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn record_activity_task_heartbeat(&self, request: Request<pb::RecordActivityTaskHeartbeatRequest>) -> Result<Response<pb::RecordActivityTaskHeartbeatResponse>, Status> {
        super::grpc_g2::record_activity_task_heartbeat(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn record_child_execution_completed(&self, request: Request<pb::RecordChildExecutionCompletedRequest>) -> Result<Response<pb::RecordChildExecutionCompletedResponse>, Status> {
        super::grpc_g2::record_child_execution_completed(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn remove_signal_mutable_state(&self, request: Request<pb::RemoveSignalMutableStateRequest>) -> Result<Response<pb::RemoveSignalMutableStateResponse>, Status> {
        super::grpc_g2::remove_signal_mutable_state(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn replicate_events_v2(&self, request: Request<pb::ReplicateEventsV2Request>) -> Result<Response<pb::ReplicateEventsV2Response>, Status> {
        super::grpc_g3::replicate_events_v2(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn sync_activity(&self, request: Request<pb::SyncActivityRequest>) -> Result<Response<pb::SyncActivityResponse>, Status> {
        super::grpc_g3::sync_activity(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn sync_shard_status(&self, request: Request<pb::SyncShardStatusRequest>) -> Result<Response<pb::SyncShardStatusResponse>, Status> {
        super::grpc_g3::sync_shard_status(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn notify_failover_markers(&self, request: Request<pb::NotifyFailoverMarkersRequest>) -> Result<Response<pb::NotifyFailoverMarkersResponse>, Status> {
        super::grpc_g3::notify_failover_markers(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn get_failover_info(&self, request: Request<pb::GetFailoverInfoRequest>) -> Result<Response<pb::GetFailoverInfoResponse>, Status> {
        super::grpc_g3::get_failover_info(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn ratelimit_update(&self, request: Request<pb::RatelimitUpdateRequest>) -> Result<Response<pb::RatelimitUpdateResponse>, Status> {
        super::grpc_g3::ratelimit_update(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn describe_history_host(&self, request: Request<pb::DescribeHistoryHostRequest>) -> Result<Response<pb::DescribeHistoryHostResponse>, Status> {
        super::grpc_g3::describe_history_host(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn count_dlq_messages(&self, request: Request<pb::CountDlqMessagesRequest>) -> Result<Response<pb::CountDlqMessagesResponse>, Status> {
        super::grpc_g3::count_dlq_messages(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn get_cross_cluster_tasks(&self, request: Request<pb::GetCrossClusterTasksRequest>) -> Result<Response<pb::GetCrossClusterTasksResponse>, Status> {
        super::grpc_g3::get_cross_cluster_tasks(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn respond_cross_cluster_tasks_completed(&self, request: Request<pb::RespondCrossClusterTasksCompletedRequest>) -> Result<Response<pb::RespondCrossClusterTasksCompletedResponse>, Status> {
        super::grpc_g3::respond_cross_cluster_tasks_completed(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn get_replication_messages(&self, request: Request<pb::GetReplicationMessagesRequest>) -> Result<Response<pb::GetReplicationMessagesResponse>, Status> {
        super::grpc_g3::get_replication_messages(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn get_dlq_replication_messages(&self, request: Request<pb::GetDlqReplicationMessagesRequest>) -> Result<Response<pb::GetDlqReplicationMessagesResponse>, Status> {
        super::grpc_g3::get_dlq_replication_messages(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn read_dlq_messages(&self, request: Request<pb::ReadDlqMessagesRequest>) -> Result<Response<pb::ReadDlqMessagesResponse>, Status> {
        super::grpc_g3::read_dlq_messages(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn purge_dlq_messages(&self, request: Request<pb::PurgeDlqMessagesRequest>) -> Result<Response<pb::PurgeDlqMessagesResponse>, Status> {
        super::grpc_g3::purge_dlq_messages(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn merge_dlq_messages(&self, request: Request<pb::MergeDlqMessagesRequest>) -> Result<Response<pb::MergeDlqMessagesResponse>, Status> {
        super::grpc_g3::merge_dlq_messages(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn reset_workflow_execution(&self, request: Request<pb::ResetWorkflowExecutionRequest>) -> Result<Response<pb::ResetWorkflowExecutionResponse>, Status> {
        super::grpc_g1::reset_workflow_execution(&self.state, request.into_inner()).await.map(Response::new)
    }
    async fn close_shard(&self, request: Request<pb::CloseShardRequest>) -> Result<Response<pb::CloseShardResponse>, Status> {
        gate(&self.state)?;
        let r = request.into_inner();
        let req = cadence_api::admin::CloseShardRequest { shard_id: r.shard_id };
        self.state.handler.close_shard(Some(&req)).await.map_err(|e| errors::grpc_status(&e))?;
        Ok(Response::new(pb::CloseShardResponse {}))
    }
    async fn remove_task(&self, request: Request<pb::RemoveTaskRequest>) -> Result<Response<pb::RemoveTaskResponse>, Status> {
        gate(&self.state)?;
        let r = request.into_inner();
        let req = cadence_api::admin::RemoveTaskRequest {
            shard_id: r.shard_id,
            task_type: task_type(r.task_type),
            task_id: r.task_id,
            visibility_timestamp: r.visibility_time.map(|t| t.seconds.saturating_mul(1_000_000_000).saturating_add(i64::from(t.nanos))),
            cluster_name: r.cluster_name,
        };
        self.state.handler.remove_task(Some(&req)).await.map_err(|e| errors::grpc_status(&e))?;
        Ok(Response::new(pb::RemoveTaskResponse {}))
    }
    async fn reset_queue(&self, request: Request<pb::ResetQueueRequest>) -> Result<Response<pb::ResetQueueResponse>, Status> {
        gate(&self.state)?;
        let r = request.into_inner();
        let req = cadence_api::admin::ResetQueueRequest { shard_id: r.shard_id, cluster_name: r.cluster_name, task_type: task_type(r.task_type) };
        self.state.handler.reset_queue(Some(&req)).await.map_err(|e| errors::grpc_status(&e))?;
        Ok(Response::new(pb::ResetQueueResponse {}))
    }
    async fn describe_queue(&self, request: Request<pb::DescribeQueueRequest>) -> Result<Response<pb::DescribeQueueResponse>, Status> {
        gate(&self.state)?;
        let r = request.into_inner();
        let req = cadence_api::admin::DescribeQueueRequest { shard_id: r.shard_id, cluster_name: r.cluster_name, task_type: task_type(r.task_type) };
        let resp = self.state.handler.describe_queue(Some(&req)).await.map_err(|e| errors::grpc_status(&e))?;
        Ok(Response::new(pb::DescribeQueueResponse { processing_queue_states: resp.processing_queue_states }))
    }
}

pub async fn serve(
    listener: TcpListener,
    state: ServerState,
    mut stop: watch::Receiver<bool>,
    tls: Option<tonic::transport::ServerTlsConfig>,
    custom_tls: Option<std::sync::Arc<rustls::ServerConfig>>,
) {
    let max = state.max_message_size;
    let svc = HistoryApiServer::new(GrpcHistory { state }).max_decoding_message_size(max).max_encoding_message_size(max);
    let shutdown = async move {
        let _ = stop.wait_for(|v| *v).await;
    };
    let builder = match cadence_service_core::grpc_tls::server_builder(tls) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("history gRPC TLS config: {e}");
            return;
        }
    };
    let mut server = builder;
    let router = server.add_service(svc);
    let result = if let Some(config) = custom_tls {
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        let incoming = cadence_matching::grpc_matching::no_identity_tls_incoming(listener, acceptor);
        router.serve_with_incoming_shutdown(incoming, shutdown).await
    } else {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        router.serve_with_incoming_shutdown(incoming, shutdown).await
    };
    if let Err(e) = result {
        tracing::error!("history gRPC server stopped: {e}");
    }
}
