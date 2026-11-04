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

pub async fn serve(listener: TcpListener, state: ServerState, mut stop: watch::Receiver<bool>) {
    let max = state.max_message_size;
    let svc = HistoryApiServer::new(GrpcHistory { state }).max_decoding_message_size(max).max_encoding_message_size(max);
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let _ = tonic::transport::Server::builder()
        .add_service(svc)
        .serve_with_incoming_shutdown(incoming, async move {
            let _ = stop.wait_for(|v| *v).await;
        })
        .await;
}
