//! Remote Frontend client used by cluster redirection (Go:
//! `resource.GetRemoteFrontendClient(cluster)` + the autoforwarding header).
//!
//! Calls go over TChannel/Thrift using the same WorkflowService codecs the
//! Frontend serves. `clusterGroup[x].rpcTransport: grpc` is not supported by
//! this client and fails with a clear error instead of silently going local.
#![allow(clippy::result_large_err)]

use std::time::Duration;

use cadence_api::errors::{CadenceError, InternalServiceError};

use crate::tchannel_json_client::{call_thrift, JsonCallError};
use crate::workflow_api::{decode_client_response, encode_client_request};

/// Go `common.AutoforwardingClusterHeaderName`.
pub const AUTOFORWARDING_CLUSTER_HEADER: &str = "cadence-forwarding-cluster";
pub const DEFAULT_REMOTE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct RemoteFrontendClient {
    pub cluster: String,
    pub address: String,
    pub service: String,
    pub transport: String,
    pub caller: String,
    pub forwarding_cluster: String,
}

impl RemoteFrontendClient {
    pub async fn call(
        &self,
        operation: &str,
        request: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, CadenceError> {
        if !self.transport.eq_ignore_ascii_case("tchannel") {
            return Err(CadenceError::Internal(InternalServiceError::new(format!(
                "remote Frontend transport {:?} for cluster {} is not supported",
                self.transport, self.cluster
            ))));
        }
        let body = encode_client_request(operation, request)
            .map_err(|e| CadenceError::Internal(InternalServiceError::new(e)))?;
        let procedure = format!("WorkflowService::{operation}");
        let headers = [(
            AUTOFORWARDING_CLUSTER_HEADER.to_owned(),
            self.forwarding_cluster.clone(),
        )];
        let response = call_thrift(
            &self.address,
            &self.service,
            &procedure,
            &self.caller,
            &headers,
            &body,
            timeout,
        )
        .await
        .map_err(|error| match error {
            JsonCallError::Application(v) => {
                CadenceError::Internal(InternalServiceError::new(v.to_string()))
            }
            JsonCallError::Transport(e) => CadenceError::Internal(InternalServiceError::new(
                format!("remote Frontend {} unavailable: {e}", self.cluster),
            )),
        })?;
        decode_client_response(operation, response.application_error, &response.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn grpc_transport_is_rejected_explicitly() {
        let client = RemoteFrontendClient {
            cluster: "b".into(),
            address: "127.0.0.1:1".into(),
            service: "cadence-frontend".into(),
            transport: "grpc".into(),
            caller: "cadence-frontend".into(),
            forwarding_cluster: "a".into(),
        };
        let err = client
            .call("SignalWorkflowExecution", serde_json::json!({}), Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn domain_not_active_exception_round_trips() {
        use cadence_thrift::value::ToThrift;
        let error = cadence_api::errors::DomainNotActiveError {
            message: "m".into(),
            domain_name: "d".into(),
            current_cluster: "a".into(),
            active_cluster: "b".into(),
            ..Default::default()
        };
        let decoded =
            crate::workflow_api::exception_from_thrift("DomainNotActiveError", &error.to_thrift());
        match decoded {
            CadenceError::DomainNotActive(e) => {
                assert_eq!((e.active_cluster.as_str(), e.domain_name.as_str()), ("b", "d"))
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
