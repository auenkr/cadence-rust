//! `QueryMatchingClient` over the Matching gRPC `QueryWorkflow` (Go
//! `matching.Client.QueryWorkflow`). The target channel is resolved per task
//! list through an injected resolver (Go: membership resolver keyed by the
//! task list name).
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::tasks::{QueryWorkflowRequest, QueryWorkflowResponse};
use cadence_api::types::TaskList;
use tonic::transport::Channel;

use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::QueryMatchingClient;

/// Resolves the matching host channel owning the given task list name.
pub type ChannelResolver = Arc<dyn Fn(&str) -> Result<Channel, String> + Send + Sync>;

pub struct GrpcQueryMatchingClient {
    resolver: ChannelResolver,
}

impl GrpcQueryMatchingClient {
    pub fn new(resolver: ChannelResolver) -> Self {
        Self { resolver }
    }
}

fn other(msg: impl std::fmt::Display) -> HistoryError {
    HistoryError::Cadence(Box::new(cadence_api::errors::CadenceError::Internal(
        cadence_api::errors::InternalServiceError { message: msg.to_string() },
    )))
}

#[async_trait]
impl QueryMatchingClient for GrpcQueryMatchingClient {
    async fn query_workflow(
        &self,
        domain_id: &str,
        request: &QueryWorkflowRequest,
        task_list: Option<&TaskList>,
    ) -> HResult<QueryWorkflowResponse> {
        let name = task_list.map(|t| t.name.as_str()).unwrap_or_default();
        let channel = (self.resolver)(name).map_err(other)?;
        let mut client = cadence_proto::matching::matching_api_client::MatchingApiClient::new(channel);
        let proto = cadence_proto::matching::QueryWorkflowRequest {
            domain_id: domain_id.to_string(),
            request: Some(cadence_proto::convert::client::query_request_out(request.clone())),
            task_list: task_list.cloned().map(cadence_proto::convert::task_list_out),
            forwarded_from: String::new(),
        };
        let resp = client
            .query_workflow(proto)
            .await
            .map_err(|s| {
                HistoryError::Cadence(Box::new(cadence_service_core::frontend_remote_grpc::error_from_status(&s)))
            })?
            .into_inner();
        cadence_proto::convert::client::query_response_in(cadence_proto::api::QueryWorkflowResponse {
            query_result: resp.query_result,
            query_rejected: resp.query_rejected,
        })
        .map_err(other)
    }
}
