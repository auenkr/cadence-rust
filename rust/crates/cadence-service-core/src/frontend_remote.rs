//! Remote Frontend client used by cluster redirection (Go:
//! `resource.GetRemoteFrontendClient(cluster)` + the autoforwarding header).
//!
//! Transport follows `clusterGroup[x].rpcTransport` like Go's
//! `NewCrossDCOutbounds`: `tchannel` uses TChannel/Thrift with the WorkflowService
//! codecs the Frontend serves; `grpc` uses the proto WorkflowAPI with the
//! cluster's `tls` block. Anything else is an error.
#![allow(clippy::result_large_err)]

use std::time::Duration;

use cadence_api::errors::{CadenceError, InternalServiceError};

use crate::tchannel_json_client::{call_thrift, JsonCallError};
use crate::workflow_api::{decode_client_response, encode_client_request};

/// Go `common.AutoforwardingClusterHeaderName`.
pub const AUTOFORWARDING_CLUSTER_HEADER: &str = "cadence-forwarding-cluster";
pub const DEFAULT_REMOTE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Default)]
pub struct RemoteFrontendClient {
    pub cluster: String,
    pub address: String,
    pub service: String,
    pub transport: String,
    pub caller: String,
    pub forwarding_cluster: String,
    /// Go `ClusterInformation.TLS` for the gRPC transport.
    pub tls: Option<crate::config::TlsConfig>,
    channel: std::sync::Arc<tokio::sync::OnceCell<tonic::transport::Channel>>,
}

impl RemoteFrontendClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster: String,
        address: String,
        service: String,
        transport: String,
        caller: String,
        forwarding_cluster: String,
        tls: Option<crate::config::TlsConfig>,
    ) -> Self {
        Self {
            cluster,
            address,
            service,
            transport,
            caller,
            forwarding_cluster,
            tls,
            channel: Default::default(),
        }
    }

    async fn grpc_call(
        &self,
        operation: &str,
        request: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, CadenceError> {
        let internal = |m: String| CadenceError::Internal(InternalServiceError::new(m));
        let channel = self
            .channel
            .get_or_try_init(|| async {
                let tls = match &self.tls {
                    Some(tls) => crate::grpc_tls::client_tls(tls).map_err(|e| {
                        internal(format!("remote Frontend {} TLS config: {e}", self.cluster))
                    })?,
                    None => None,
                };
                crate::grpc_tls::endpoint_channel(&self.address, tls.as_ref()).map_err(|e| {
                    internal(format!("remote Frontend {} unavailable: {e}", self.cluster))
                })
            })
            .await?
            .clone();
        crate::frontend_remote_grpc::call(
            channel,
            &self.service,
            &self.caller,
            &self.forwarding_cluster,
            operation,
            request,
            timeout,
        )
        .await
    }

    pub async fn call(
        &self,
        operation: &str,
        request: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, CadenceError> {
        if self.transport.eq_ignore_ascii_case("grpc") {
            return self.grpc_call(operation, request, timeout).await;
        }
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
    async fn unknown_transport_is_rejected_explicitly() {
        let client = RemoteFrontendClient::new(
            "b".into(),
            "127.0.0.1:1".into(),
            "cadence-frontend".into(),
            "http".into(),
            "cadence-frontend".into(),
            "a".into(),
            None,
        );
        let err = client
            .call("SignalWorkflowExecution", serde_json::json!({}), Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[tokio::test]
    async fn grpc_transport_rejects_operations_absent_from_grpc_api() {
        let client = RemoteFrontendClient::new(
            "b".into(),
            "127.0.0.1:1".into(),
            "cadence-frontend".into(),
            "grpc".into(),
            "cadence-frontend".into(),
            "a".into(),
            None,
        );
        let err = client
            .call("GetTaskListsByDomain", serde_json::json!({}), Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("gRPC remote Frontend"));
    }

    #[tokio::test]
    async fn grpc_transport_reports_unreachable_remote() {
        let client = RemoteFrontendClient::new(
            "b".into(),
            "127.0.0.1:1".into(),
            "cadence-frontend".into(),
            "grpc".into(),
            "cadence-frontend".into(),
            "a".into(),
            None,
        );
        let err = client
            .call("SignalWorkflowExecution", serde_json::json!({}), Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(matches!(err, CadenceError::Internal(_)), "{err:?}");
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
                assert_eq!(
                    (e.active_cluster.as_str(), e.domain_name.as_str()),
                    ("b", "d")
                )
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
