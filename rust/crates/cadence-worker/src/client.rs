//! Outbound Cadence WorkerAPI client used by internal workflow workers.

use cadence_proto::api::worker_api_client::WorkerApiClient;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Clone)]
pub struct CadenceClient {
    inner: WorkerApiClient<Channel>,
}

impl CadenceClient {
    /// Connect to the configured public Frontend gRPC endpoint.
    pub async fn connect(endpoint: impl AsRef<str>) -> anyhow::Result<Self> {
        let endpoint = Endpoint::from_shared(endpoint.as_ref().to_owned())?;
        let inner = WorkerApiClient::connect(endpoint).await?;
        Ok(Self { inner })
    }

    /// Connect with the same TLS settings used by generated Cadence clients.
    pub async fn connect_tls(
        endpoint: impl AsRef<str>,
        tls: ClientTlsConfig,
    ) -> anyhow::Result<Self> {
        let endpoint = Endpoint::from_shared(endpoint.as_ref().to_owned())?.tls_config(tls)?;
        let inner = WorkerApiClient::connect(endpoint).await?;
        Ok(Self { inner })
    }

    /// Obtain an owned generated client for poller calls.
    pub fn worker_api(&self) -> WorkerApiClient<Channel> {
        self.inner.clone()
    }
}
