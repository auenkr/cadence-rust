//! Generated outbound gRPC client for the original Go History service.
#![allow(clippy::result_large_err)]

use cadence_proto::history::history_api_client::HistoryApiClient;
use cadence_service_core::{
    config::TlsConfig,
    grpc_tls::{self, ClientTls},
};
use std::time::Duration;
use tonic::{transport::Channel, Request};

pub const DEFAULT_HISTORY_RPC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct HistoryClient {
    channel: Channel,
    client: HistoryApiClient<Channel>,
    tls: Option<ClientTls>,
    rpc_timeout: Duration,
}

impl HistoryClient {
    pub fn from_channel(channel: Channel, max_message_size: usize) -> Self {
        let client = HistoryApiClient::new(channel.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        Self {
            channel,
            client,
            tls: None,
            rpc_timeout: DEFAULT_HISTORY_RPC_TIMEOUT,
        }
    }

    pub fn client(&self) -> HistoryApiClient<Channel> {
        self.client.clone()
    }
    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    /// Dial a discovered History peer using the same TLS config loader used by
    /// Matching's native History client (mTLS, CA roots and server-name rules).
    pub fn for_peer(
        peer: &str,
        tls: Option<&TlsConfig>,
        max_message_size: usize,
    ) -> anyhow::Result<Self> {
        let tls = tls.map(grpc_tls::client_tls).transpose()?.flatten();
        let channel = grpc_tls::endpoint_channel(peer, tls.as_ref())?;
        let mut client = Self::from_channel(channel, max_message_size);
        client.tls = tls;
        Ok(client)
    }

    pub fn tls(&self) -> Option<&ClientTls> {
        self.tls.as_ref()
    }

    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }

    /// Attach the same caller identity, encoding and per-attempt timeout used
    /// by Cadence's Go outbound RPC wrappers.
    pub fn request<T>(&self, payload: T) -> Request<T> {
        self.request_with_timeout(payload, self.rpc_timeout)
    }

    /// The Go wrappers leave several replication and admin methods governed
    /// only by the caller's context; callers can use this with no RPC deadline.
    pub fn request_with_timeout<T>(&self, payload: T, timeout: Duration) -> Request<T> {
        let mut request = Request::new(payload);
        request.metadata_mut().insert(
            "rpc-service",
            tonic::metadata::MetadataValue::from_static("cadence-history"),
        );
        request.metadata_mut().insert(
            "rpc-caller",
            tonic::metadata::MetadataValue::from_static("cadence-frontend"),
        );
        request.metadata_mut().insert(
            "rpc-encoding",
            tonic::metadata::MetadataValue::from_static("proto"),
        );
        request.set_timeout(timeout);
        request
    }

    /// Typed History boundary methods used by the public WorkflowHandler.
    /// Request mapping stays at the handler boundary; these methods preserve
    /// generated protobuf types and Go's caller/deadline metadata.
    pub async fn start_workflow_execution(
        &self,
        request: cadence_proto::history::StartWorkflowExecutionRequest,
    ) -> Result<cadence_proto::history::StartWorkflowExecutionResponse, tonic::Status> {
        self.client
            .clone()
            .start_workflow_execution(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn signal_workflow_execution(
        &self,
        request: cadence_proto::history::SignalWorkflowExecutionRequest,
    ) -> Result<cadence_proto::history::SignalWorkflowExecutionResponse, tonic::Status> {
        self.client
            .clone()
            .signal_workflow_execution(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn signal_with_start_workflow_execution(
        &self,
        request: cadence_proto::history::SignalWithStartWorkflowExecutionRequest,
    ) -> Result<cadence_proto::history::SignalWithStartWorkflowExecutionResponse, tonic::Status>
    {
        self.client
            .clone()
            .signal_with_start_workflow_execution(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn terminate_workflow_execution(
        &self,
        request: cadence_proto::history::TerminateWorkflowExecutionRequest,
    ) -> Result<cadence_proto::history::TerminateWorkflowExecutionResponse, tonic::Status> {
        self.client
            .clone()
            .terminate_workflow_execution(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn reset_workflow_execution(
        &self,
        request: cadence_proto::history::ResetWorkflowExecutionRequest,
    ) -> Result<cadence_proto::history::ResetWorkflowExecutionResponse, tonic::Status> {
        self.client
            .clone()
            .reset_workflow_execution(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn describe_workflow_execution(
        &self,
        request: cadence_proto::history::DescribeWorkflowExecutionRequest,
    ) -> Result<cadence_proto::history::DescribeWorkflowExecutionResponse, tonic::Status> {
        self.client
            .clone()
            .describe_workflow_execution(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn query_workflow(
        &self,
        request: cadence_proto::history::QueryWorkflowRequest,
    ) -> Result<cadence_proto::history::QueryWorkflowResponse, tonic::Status> {
        self.client
            .clone()
            .query_workflow(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn request_cancel_workflow_execution(
        &self,
        request: cadence_proto::history::RequestCancelWorkflowExecutionRequest,
    ) -> Result<cadence_proto::history::RequestCancelWorkflowExecutionResponse, tonic::Status> {
        self.client
            .clone()
            .request_cancel_workflow_execution(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn reset_sticky_task_list(
        &self,
        request: cadence_proto::history::ResetStickyTaskListRequest,
    ) -> Result<cadence_proto::history::ResetStickyTaskListResponse, tonic::Status> {
        self.client
            .clone()
            .reset_sticky_task_list(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn get_mutable_state(
        &self,
        request: cadence_proto::history::GetMutableStateRequest,
    ) -> Result<cadence_proto::history::GetMutableStateResponse, tonic::Status> {
        self.client
            .clone()
            .get_mutable_state(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn poll_mutable_state(
        &self,
        request: cadence_proto::history::PollMutableStateRequest,
    ) -> Result<cadence_proto::history::PollMutableStateResponse, tonic::Status> {
        self.client
            .clone()
            .poll_mutable_state(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Requeue the workflow tasks after Frontend's explicit refresh operation.
    /// Go sends this through the History client with the caller's normal RPC
    /// timeout and cadence-history identity metadata.
    pub async fn refresh_workflow_tasks(
        &self,
        request: cadence_proto::history::RefreshWorkflowTasksRequest,
    ) -> Result<cadence_proto::history::RefreshWorkflowTasksResponse, tonic::Status> {
        self.client
            .clone()
            .refresh_workflow_tasks(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Expose the History diagnostic operation used by workflow diagnostics
    /// and administrative callers through the typed History boundary.
    pub async fn describe_mutable_state(
        &self,
        request: cadence_proto::history::DescribeMutableStateRequest,
    ) -> Result<cadence_proto::history::DescribeMutableStateResponse, tonic::Status> {
        self.client
            .clone()
            .describe_mutable_state(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn respond_decision_task_completed(
        &self,
        request: cadence_proto::history::RespondDecisionTaskCompletedRequest,
    ) -> Result<cadence_proto::history::RespondDecisionTaskCompletedResponse, tonic::Status> {
        self.client
            .clone()
            .respond_decision_task_completed(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn respond_decision_task_failed(
        &self,
        request: cadence_proto::history::RespondDecisionTaskFailedRequest,
    ) -> Result<cadence_proto::history::RespondDecisionTaskFailedResponse, tonic::Status> {
        self.client
            .clone()
            .respond_decision_task_failed(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn respond_activity_task_completed(
        &self,
        request: cadence_proto::history::RespondActivityTaskCompletedRequest,
    ) -> Result<cadence_proto::history::RespondActivityTaskCompletedResponse, tonic::Status> {
        self.client
            .clone()
            .respond_activity_task_completed(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn respond_activity_task_failed(
        &self,
        request: cadence_proto::history::RespondActivityTaskFailedRequest,
    ) -> Result<cadence_proto::history::RespondActivityTaskFailedResponse, tonic::Status> {
        self.client
            .clone()
            .respond_activity_task_failed(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn respond_activity_task_canceled(
        &self,
        request: cadence_proto::history::RespondActivityTaskCanceledRequest,
    ) -> Result<cadence_proto::history::RespondActivityTaskCanceledResponse, tonic::Status> {
        self.client
            .clone()
            .respond_activity_task_canceled(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn record_activity_task_heartbeat(
        &self,
        request: cadence_proto::history::RecordActivityTaskHeartbeatRequest,
    ) -> Result<cadence_proto::history::RecordActivityTaskHeartbeatResponse, tonic::Status> {
        self.client
            .clone()
            .record_activity_task_heartbeat(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }
}

/// Build a lazy gRPC channel using the configured Go `services.history.rpc`
/// address. Connection establishment happens on the first RPC, matching the
/// Go client factory's lazy dialing behavior.
pub fn connect(host: &str, port: u16) -> anyhow::Result<tonic::transport::Channel> {
    let endpoint = format!("http://{host}:{port}")
        .parse::<tonic::transport::Endpoint>()?
        .connect_timeout(Duration::from_secs(10));
    Ok(endpoint.connect_lazy())
}

pub fn generated(channel: Channel, max_message_size: usize) -> HistoryApiClient<Channel> {
    HistoryApiClient::new(channel)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generated_history_calls_attach_go_caller_metadata_and_timeout() {
        let channel =
            tonic::transport::Endpoint::from_static("http://127.0.0.1:7233").connect_lazy();
        let client = HistoryClient::from_channel(channel, 1024);
        let request = client.request(());
        assert_eq!(
            request.metadata().get("rpc-service").unwrap(),
            "cadence-history"
        );
        assert_eq!(
            request.metadata().get("rpc-caller").unwrap(),
            "cadence-frontend"
        );
        assert_eq!(request.metadata().get("rpc-encoding").unwrap(), "proto");
        assert_eq!(client.rpc_timeout, DEFAULT_HISTORY_RPC_TIMEOUT);
    }
}
