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
    lb: Option<Arc<cadence_common::matching_lb::MatchingLoadBalancer>>,
}

impl GrpcQueryMatchingClient {
    pub fn new(resolver: ChannelResolver) -> Self {
        Self { resolver, lb: None }
    }

    /// Go matching client `loadBalancer.PickReadPartition` for QueryWorkflow.
    pub fn with_load_balancer(mut self, lb: Arc<cadence_common::matching_lb::MatchingLoadBalancer>) -> Self {
        self.lb = Some(lb);
        self
    }
}

fn other(msg: impl std::fmt::Display) -> HistoryError {
    HistoryError::Cadence(Box::new(cadence_api::errors::CadenceError::Internal(
        cadence_api::errors::InternalServiceError { message: msg.to_string() },
    )))
}

#[allow(deprecated)]
fn partition_config_from_api(
    c: cadence_proto::task_list_partition::api::TaskListPartitionConfig,
) -> cadence_common::matching_lb::PartitionConfig {
    let groups = |m: std::collections::HashMap<i32, cadence_proto::api::TaskListPartition>| {
        m.into_iter().map(|(k, v)| (k, v.isolation_groups)).collect()
    };
    cadence_common::matching_lb::PartitionConfig::from_parts(
        c.version,
        c.num_read_partitions,
        groups(c.read_partitions),
        c.num_write_partitions,
        groups(c.write_partitions),
    )
}

#[async_trait]
impl QueryMatchingClient for GrpcQueryMatchingClient {
    async fn query_workflow(
        &self,
        domain_id: &str,
        request: &QueryWorkflowRequest,
        task_list: Option<&TaskList>,
    ) -> HResult<QueryWorkflowResponse> {
        let root = task_list.map(|t| t.name.as_str()).unwrap_or_default();
        let normal = task_list.is_none_or(|t| t.kind_or_default() == cadence_api::enums::TaskListKind::Normal);
        let name = match &self.lb {
            Some(lb) => lb.pick_read_partition(
                cadence_common::matching_lb::TASK_LIST_TYPE_DECISION,
                &cadence_common::matching_lb::LbRequest {
                    domain_id,
                    task_list: root,
                    kind_normal: normal,
                    forwarded_from: "",
                    partition_config: None,
                },
                "",
            ),
            None => root.to_owned(),
        };
        let channel = (self.resolver)(&name).map_err(other)?;
        let mut client = cadence_proto::matching::matching_api_client::MatchingApiClient::new(channel);
        let proto = cadence_proto::matching::QueryWorkflowRequest {
            domain_id: domain_id.to_string(),
            request: Some(cadence_proto::convert::client::query_request_out(request.clone())),
            task_list: task_list.cloned().map(|mut t| {
                t.name = name.clone();
                cadence_proto::convert::task_list_out(t)
            }),
            forwarded_from: String::new(),
        };
        // Go's matching client goes through yarpc: the target (Go) Matching rejects a call without
        // service/caller/encoding headers or a deadline ("missing service name, caller name, encoding").
        // Go timeout wrapper: createContext(ctx, c.timeout) with the matching client timeout (60s).
        let mut grpc_request = tonic::Request::new(proto);
        {
            let metadata = grpc_request.metadata_mut();
            metadata.insert("rpc-service", tonic::metadata::MetadataValue::from_static("cadence-matching"));
            metadata.insert("rpc-caller", tonic::metadata::MetadataValue::from_static("cadence-history"));
            metadata.insert("rpc-encoding", tonic::metadata::MetadataValue::from_static("proto"));
        }
        grpc_request.set_timeout(std::time::Duration::from_secs(60));
        let resp = client
            .query_workflow(grpc_request)
            .await
            .map_err(|s| {
                HistoryError::Cadence(Box::new(cadence_service_core::frontend_remote_grpc::error_from_status(&s)))
            })?
            .into_inner();
        // Go client.go QueryWorkflow: provider.UpdatePartitionConfig on the root task list.
        if let (Some(lb), Some(tl)) = (&self.lb, task_list) {
            lb.provider().update_partition_config(
                domain_id,
                &tl.name,
                normal,
                cadence_common::matching_lb::TASK_LIST_TYPE_DECISION,
                resp.partition_config.clone().map(partition_config_from_api),
            );
        }
        cadence_proto::convert::client::query_response_in(cadence_proto::api::QueryWorkflowResponse {
            query_result: resp.query_result,
            query_rejected: resp.query_rejected,
        })
        .map_err(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)]
    fn partition_config_from_api_maps_groups_and_version() {
        let part = |g: &str| cadence_proto::api::TaskListPartition { isolation_groups: vec![g.to_owned()] };
        let c = cadence_proto::task_list_partition::api::TaskListPartitionConfig {
            version: 7,
            num_read_partitions: 2,
            num_write_partitions: 1,
            read_partitions: [(0, part("a")), (1, part("b"))].into(),
            write_partitions: [(0, part("a"))].into(),
        };
        let got = partition_config_from_api(c);
        assert_eq!(got.version, 7);
        assert_eq!(got.read_partitions.len(), 2);
        assert_eq!(got.read_partitions[&1], vec!["b".to_owned()]);
        assert_eq!(got.write_partitions.len(), 1);
    }
}
