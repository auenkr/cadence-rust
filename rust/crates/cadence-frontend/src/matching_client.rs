//! Generated outbound gRPC client for the original Go Matching service.
#![allow(clippy::result_large_err)]

use cadence_proto::matching::matching_api_client::MatchingApiClient;
use cadence_service_core::{
    config::TlsConfig,
    grpc_tls::{self, ClientTls},
};
use std::time::Duration;
use tonic::{transport::Channel, Request};

pub const DEFAULT_MATCHING_RPC_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_MATCHING_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(120);

/// Frontend client-pool seam. Implementations select the local or forwarded
/// Matching connection for the cluster chosen by cluster redirection.
pub trait MatchingClientResolver: Send + Sync {
    fn matching_client(&self, target_cluster: Option<&str>) -> Result<MatchingClient, String>;
}

pub struct MatchingPollCanceller<R> {
    resolver: R,
}

impl<R> MatchingPollCanceller<R> {
    pub fn new(resolver: R) -> Self {
        Self { resolver }
    }
}

impl<R: MatchingClientResolver> crate::handler_workflow::OutstandingPollCanceller
    for MatchingPollCanceller<R>
{
    fn cancel_poll<'a>(
        &'a self,
        request: serde_json::Value,
        target_cluster: Option<String>,
    ) -> crate::handler_workflow::WorkflowFuture<'a> {
        let request = match cancel_outstanding_poll_request(&request) {
            Ok(request) => request,
            Err(message) => {
                return Box::pin(async move {
                    Err(cadence_api::errors::BadRequestError::new(message).into())
                });
            }
        };
        let client = match self.resolver.matching_client(target_cluster.as_deref()) {
            Ok(client) => client,
            Err(message) => {
                return Box::pin(async move {
                    Err(cadence_api::errors::InternalServiceError::new(message).into())
                });
            }
        };
        Box::pin(async move {
            client
                .cancel_outstanding_poll(request)
                .await
                .map_err(|status| {
                    cadence_api::errors::CadenceError::from(
                        cadence_api::errors::InternalServiceError::new(status.to_string()),
                    )
                })?;
            Ok(serde_json::Value::Null)
        })
    }
}

pub fn cancel_outstanding_poll_request(
    request: &serde_json::Value,
) -> Result<cadence_proto::matching::CancelOutstandingPollRequest, String> {
    let domain_id = request
        .get("domainUUID")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "DomainUUID not set on poll cancellation.".to_owned())?;
    let poller_id = request
        .get("pollerID")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "PollerID not set on poll cancellation.".to_owned())?;
    let task_list_type = request
        .get("taskListType")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "TaskListType not set on poll cancellation.".to_owned())?
        as i32;
    let task_list = request
        .get("taskList")
        .filter(|value| !value.is_null())
        .map(|value| cadence_proto::api::TaskList {
            name: value
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            kind: value
                .get("kind")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default() as i32,
            base_name: value
                .get("baseName")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        });
    Ok(cadence_proto::matching::CancelOutstandingPollRequest {
        domain_id: domain_id.to_owned(),
        poller_id: poller_id.to_owned(),
        task_list_type,
        task_list,
    })
}

#[derive(Clone)]
pub struct MatchingClient {
    channel: Channel,
    client: MatchingApiClient<Channel>,
    tls: Option<ClientTls>,
    rpc_timeout: Duration,
}

impl MatchingClient {
    pub fn from_channel(channel: Channel, max_message_size: usize) -> Self {
        let client = MatchingApiClient::new(channel.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        Self {
            channel,
            client,
            tls: None,
            rpc_timeout: DEFAULT_MATCHING_RPC_TIMEOUT,
        }
    }

    pub fn client(&self) -> MatchingApiClient<Channel> {
        self.client.clone()
    }
    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    /// Dial a discovered Matching peer with the shared Go-compatible TLS
    /// loader, including client credentials, CA roots and server-name rules.
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

    /// Go Matching uses a two-minute deadline for long-poll calls.
    pub fn long_poll_request<T>(&self, payload: T) -> Request<T> {
        self.request_with_timeout(payload, DEFAULT_MATCHING_LONG_POLL_TIMEOUT)
    }

    pub fn request_with_timeout<T>(&self, payload: T, timeout: Duration) -> Request<T> {
        let mut request = Request::new(payload);
        request.metadata_mut().insert(
            "rpc-service",
            tonic::metadata::MetadataValue::from_static("cadence-matching"),
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

    /// Dispatch the typed internal Matching long-poll used by Go Frontend.
    /// Keeping this on the generated client makes the internal envelope and
    /// long-poll deadline explicit at the service boundary.
    pub async fn poll_for_activity_task(
        &self,
        request: cadence_proto::matching::PollForActivityTaskRequest,
    ) -> Result<cadence_proto::matching::PollForActivityTaskResponse, tonic::Status> {
        self.client
            .clone()
            .poll_for_activity_task(self.long_poll_request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn poll_for_decision_task(
        &self,
        request: cadence_proto::matching::PollForDecisionTaskRequest,
    ) -> Result<cadence_proto::matching::PollForDecisionTaskResponse, tonic::Status> {
        self.client
            .clone()
            .poll_for_decision_task(self.long_poll_request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Explicitly remove a poller after its Frontend caller disconnects. Go's
    /// request context is not propagated across this internal RPC boundary.
    pub async fn cancel_outstanding_poll(
        &self,
        request: cadence_proto::matching::CancelOutstandingPollRequest,
    ) -> Result<(), tonic::Status> {
        self.client
            .clone()
            .cancel_outstanding_poll(self.request(request))
            .await
            .map(|_| ())
    }

    pub async fn respond_query_task_completed(
        &self,
        request: cadence_proto::matching::RespondQueryTaskCompletedRequest,
    ) -> Result<cadence_proto::matching::RespondQueryTaskCompletedResponse, tonic::Status> {
        self.client
            .clone()
            .respond_query_task_completed(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn describe_task_list(
        &self,
        request: cadence_proto::matching::DescribeTaskListRequest,
    ) -> Result<cadence_proto::matching::DescribeTaskListResponse, tonic::Status> {
        self.client
            .clone()
            .describe_task_list(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn list_task_list_partitions(
        &self,
        request: cadence_proto::matching::ListTaskListPartitionsRequest,
    ) -> Result<cadence_proto::matching::ListTaskListPartitionsResponse, tonic::Status> {
        self.client
            .clone()
            .list_task_list_partitions(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn get_task_lists_by_domain(
        &self,
        request: cadence_proto::matching::GetTaskListsByDomainRequest,
    ) -> Result<cadence_proto::matching::GetTaskListsByDomainResponse, tonic::Status> {
        self.client
            .clone()
            .get_task_lists_by_domain(self.request(request))
            .await
            .map(tonic::Response::into_inner)
    }
}

/// Construct Go's internal Matching activity-poll envelope from the typed
/// public request and domain cache identity.
pub fn activity_poll_request(
    domain_id: String,
    poller_id: String,
    request: cadence_proto::api::PollForActivityTaskRequest,
    isolation_group: String,
) -> cadence_proto::matching::PollForActivityTaskRequest {
    cadence_proto::matching::PollForActivityTaskRequest {
        request: Some(request),
        domain_id,
        poller_id,
        forwarded_from: String::new(),
        isolation_group,
    }
}

pub fn decision_poll_request(
    domain_id: String,
    poller_id: String,
    request: cadence_proto::api::PollForDecisionTaskRequest,
    isolation_group: String,
) -> cadence_proto::matching::PollForDecisionTaskRequest {
    cadence_proto::matching::PollForDecisionTaskRequest {
        request: Some(request),
        domain_id,
        poller_id,
        forwarded_from: String::new(),
        isolation_group,
    }
}

/// Build a lazy gRPC channel using the configured Go
/// `services.matching.rpc.grpcPort` address.
pub fn connect(host: &str, port: u16) -> anyhow::Result<tonic::transport::Channel> {
    let endpoint = format!("http://{host}:{port}")
        .parse::<tonic::transport::Endpoint>()?
        .connect_timeout(Duration::from_secs(10));
    Ok(endpoint.connect_lazy())
}

pub fn generated(channel: Channel, max_message_size: usize) -> MatchingApiClient<Channel> {
    MatchingApiClient::new(channel)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_poll_envelopes_match_go_matching_requests() {
        let activity = activity_poll_request(
            "domain-uuid".into(),
            "poller-activity".into(),
            cadence_proto::api::PollForActivityTaskRequest {
                domain: "shop".into(),
                identity: "worker".into(),
                ..Default::default()
            },
            "group-a".into(),
        );
        assert_eq!(activity.domain_id, "domain-uuid");
        assert_eq!(activity.poller_id, "poller-activity");
        assert_eq!(activity.isolation_group, "group-a");
        assert_eq!(activity.request.unwrap().domain, "shop");

        let decision = decision_poll_request(
            "domain-uuid".into(),
            "poller-decision".into(),
            cadence_proto::api::PollForDecisionTaskRequest {
                domain: "shop".into(),
                identity: "worker".into(),
                binary_checksum: "checksum".into(),
                ..Default::default()
            },
            "group-b".into(),
        );
        assert_eq!(decision.domain_id, "domain-uuid");
        assert_eq!(decision.poller_id, "poller-decision");
        assert_eq!(decision.isolation_group, "group-b");
        let public = decision.request.unwrap();
        assert_eq!(public.domain, "shop");
        assert_eq!(public.binary_checksum, "checksum");
    }

    #[test]
    fn cancel_poll_json_maps_to_typed_go_internal_request() {
        let request = cancel_outstanding_poll_request(&serde_json::json!({
            "domainUUID": "domain-uuid",
            "pollerID": "poller-id",
            "taskListType": 2,
            "taskList": {"name": "orders", "kind": 1, "baseName": "orders-base"}
        }))
        .unwrap();
        assert_eq!(request.domain_id, "domain-uuid");
        assert_eq!(request.poller_id, "poller-id");
        assert_eq!(request.task_list_type, 2);
        let task_list = request.task_list.unwrap();
        assert_eq!(task_list.name, "orders");
        assert_eq!(task_list.kind, 1);
        assert_eq!(task_list.base_name, "orders-base");
    }
}
