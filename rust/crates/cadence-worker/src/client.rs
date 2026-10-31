//! Outbound Cadence WorkerAPI client used by internal workflow workers.

use cadence_proto::api::worker_api_client::WorkerApiClient;
use cadence_proto::api::{
    PollForActivityTaskRequest, PollForActivityTaskResponse, PollForDecisionTaskRequest,
    PollForDecisionTaskResponse, TaskList, TaskListKind,
};
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

    /// Poll one decision task from the public WorkerAPI.
    pub async fn poll_decision_task(
        &mut self,
        domain: impl Into<String>,
        task_list: impl Into<String>,
        identity: impl Into<String>,
        binary_checksum: impl Into<String>,
    ) -> Result<PollForDecisionTaskResponse, tonic::Status> {
        let response = self
            .inner
            .poll_for_decision_task(tonic::Request::new(PollForDecisionTaskRequest {
                domain: domain.into(),
                task_list: Some(TaskList {
                    name: task_list.into(),
                    kind: TaskListKind::Normal as i32,
                    base_name: String::new(),
                }),
                identity: identity.into(),
                binary_checksum: binary_checksum.into(),
            }))
            .await?;
        Ok(response.into_inner())
    }

    /// Poll one activity task from the public WorkerAPI.
    pub async fn poll_activity_task(
        &mut self,
        domain: impl Into<String>,
        task_list: impl Into<String>,
        identity: impl Into<String>,
    ) -> Result<PollForActivityTaskResponse, tonic::Status> {
        let response = self
            .inner
            .poll_for_activity_task(tonic::Request::new(PollForActivityTaskRequest {
                domain: domain.into(),
                task_list: Some(TaskList {
                    name: task_list.into(),
                    kind: TaskListKind::Normal as i32,
                    base_name: String::new(),
                }),
                identity: identity.into(),
                task_list_metadata: None,
            }))
            .await?;
        Ok(response.into_inner())
    }
}
