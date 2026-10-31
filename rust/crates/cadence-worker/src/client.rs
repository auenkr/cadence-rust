//! Outbound Cadence WorkerAPI client used by internal workflow workers.

// tonic RPC calls intentionally return `Status` directly, matching generated
// service signatures and the other Cadence service crates.
#![allow(clippy::result_large_err)]

use cadence_proto::api::worker_api_client::WorkerApiClient;
use cadence_proto::api::{
    Payload, PollForActivityTaskRequest, PollForActivityTaskResponse,
    PollForDecisionTaskRequest, PollForDecisionTaskResponse, RecordActivityTaskHeartbeatRequest,
    RecordActivityTaskHeartbeatResponse, RespondActivityTaskCompletedRequest,
    RespondActivityTaskCompletedResponse, RespondActivityTaskFailedRequest,
    RespondActivityTaskFailedResponse, RespondDecisionTaskCompletedRequest,
    RespondDecisionTaskCompletedResponse, TaskList, TaskListKind,
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

    /// Acknowledge an activity task with the public WorkerAPI. Payload bytes
    /// are already encoded by the registered activity's Go-compatible data
    /// converter.
    pub async fn complete_activity_task(
        &mut self,
        task_token: Vec<u8>,
        result: Option<Payload>,
        identity: impl Into<String>,
    ) -> Result<RespondActivityTaskCompletedResponse, tonic::Status> {
        let response = self
            .inner
            .respond_activity_task_completed(tonic::Request::new(
                RespondActivityTaskCompletedRequest {
                    task_token,
                    result,
                    identity: identity.into(),
                },
            ))
            .await?;
        Ok(response.into_inner())
    }

    /// Fail an activity task while preserving the public API's encoded
    /// failure and heartbeat-detail payloads.
    pub async fn fail_activity_task(
        &mut self,
        task_token: Vec<u8>,
        failure: Option<cadence_proto::api::Failure>,
        heartbeat_details: Option<Payload>,
        identity: impl Into<String>,
    ) -> Result<RespondActivityTaskFailedResponse, tonic::Status> {
        let response = self
            .inner
            .respond_activity_task_failed(tonic::Request::new(
                RespondActivityTaskFailedRequest {
                    task_token,
                    failure,
                    identity: identity.into(),
                    heartbeat_details,
                },
            ))
            .await?;
        Ok(response.into_inner())
    }

    /// Send an activity heartbeat. The returned flag is the server's
    /// cancellation request and must be observed by activity execution.
    pub async fn heartbeat_activity_task(
        &mut self,
        task_token: Vec<u8>,
        details: Option<Payload>,
        identity: impl Into<String>,
    ) -> Result<RecordActivityTaskHeartbeatResponse, tonic::Status> {
        let response = self
            .inner
            .record_activity_task_heartbeat(tonic::Request::new(
                RecordActivityTaskHeartbeatRequest {
                    task_token,
                    details,
                    identity: identity.into(),
                },
            ))
            .await?;
        Ok(response.into_inner())
    }

    /// Complete a decision task emitted by a workflow implementation. This
    /// transport method does not decide the workflow history; it only sends
    /// the SDK-compatible decisions produced by that executor.
    pub async fn complete_decision_task(
        &mut self,
        task_token: Vec<u8>,
        decisions: Vec<cadence_proto::api::Decision>,
        identity: impl Into<String>,
        binary_checksum: impl Into<String>,
    ) -> Result<RespondDecisionTaskCompletedResponse, tonic::Status> {
        let response = self
            .inner
            .respond_decision_task_completed(tonic::Request::new(
                RespondDecisionTaskCompletedRequest {
                    task_token,
                    decisions,
                    execution_context: Vec::new(),
                    identity: identity.into(),
                    sticky_attributes: None,
                    return_new_decision_task: false,
                    force_create_new_decision_task: false,
                    binary_checksum: binary_checksum.into(),
                    query_results: std::collections::HashMap::new(),
                },
            ))
            .await?;
        Ok(response.into_inner())
    }
}
