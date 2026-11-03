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
    /// Go `ClusterInformation.AuthorizationProvider` (outbound JWT middleware).
    authorization: Option<crate::outbound_auth::AdminJwtProvider>,
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
            authorization: None,
            channel: Default::default(),
        }
    }

    /// Go `NewCrossDCOutbounds` with `authorizationProvider.enable`: every call
    /// carries an admin JWT signed with the private key file.
    #[must_use]
    pub fn with_authorization_provider(
        mut self,
        provider: Result<crate::outbound_auth::AdminJwtProvider, String>,
    ) -> Self {
        self.authorization = Some(match provider {
            Ok(provider) => provider,
            Err(error) => crate::outbound_auth::AdminJwtProvider::invalid(error),
        });
        self
    }

    /// Applies the cluster's `authorizationProvider` block (no-op unless enabled).
    #[must_use]
    pub fn with_authorization_provider_if(self, enable: bool, private_key: &str) -> Self {
        if !enable {
            return self;
        }
        self.with_authorization_provider(crate::outbound_auth::AdminJwtProvider::from_private_key_file(
            private_key,
        ))
    }

    /// Go `authOutboundMiddleware.Call`: fetch a token per request.
    fn authorization_token(&self) -> Result<Option<String>, CadenceError> {
        match &self.authorization {
            None => Ok(None),
            Some(provider) => provider.token().map(Some).map_err(|e| {
                CadenceError::Internal(InternalServiceError::new(format!(
                    "remote Frontend {} authorization: {e}",
                    self.cluster
                )))
            }),
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
        let token = self.authorization_token()?;
        crate::frontend_remote_grpc::call(
            channel,
            &self.service,
            &self.caller,
            &self.forwarding_cluster,
            token.as_deref(),
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
        let mut headers = vec![(
            AUTOFORWARDING_CLUSTER_HEADER.to_owned(),
            self.forwarding_cluster.clone(),
        )];
        if let Some(token) = self.authorization_token()? {
            headers.push((crate::outbound_auth::AUTHORIZATION_HEADER.to_owned(), token));
        }
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
    async fn unreadable_authorization_key_fails_calls_before_dialing() {
        for transport in ["tchannel", "grpc"] {
            let client = RemoteFrontendClient::new(
                "b".into(),
                "127.0.0.1:1".into(),
                "cadence-frontend".into(),
                transport.into(),
                "cadence-frontend".into(),
                "a".into(),
                None,
            )
            .with_authorization_provider_if(true, "/nonexistent/key.pem");
            let err = client
                .call("SignalWorkflowExecution", serde_json::json!({}), Duration::from_secs(1))
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("invalid private key path /nonexistent/key.pem"),
                "{transport}: {err}"
            );
        }
    }

    #[test]
    fn authorization_disabled_adds_no_token_and_enabled_signs_one() {
        let base = || {
            RemoteFrontendClient::new(
                "b".into(),
                "x".into(),
                "s".into(),
                "tchannel".into(),
                "c".into(),
                "a".into(),
                None,
            )
        };
        assert_eq!(base().with_authorization_provider_if(false, "").authorization_token().unwrap(), None);
        let pem = include_str!("../../cadence-frontend/testdata/test_private.pem");
        let token = base()
            .with_authorization_provider(Ok(crate::outbound_auth::AdminJwtProvider::from_pem(
                pem.as_bytes().to_vec(),
            )))
            .authorization_token()
            .unwrap()
            .unwrap();
        assert_eq!(token.matches('.').count(), 2);
    }

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
