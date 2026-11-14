//! Admin handlers that Go's `adminHandlerImpl` proxies to the History service
//! (`go/service/frontend/admin/handler.go`: CloseShard, RemoveTask, ResetQueue,
//! DescribeQueue, DescribeHistoryHost, ReapplyEvents, replication and DLQ
//! delegation). Validation order and messages mirror Go; History errors are
//! passed through with their original typed status and details.

use crate::{handler_admin::AdminError, history_client::HistoryClient};
use cadence_api::errors::BadRequestError;
use cadence_proto::history as h;

/// Go `validate.ErrRequestNotSet`.
pub const ERR_REQUEST_NOT_SET: &str = "Request is nil.";
/// Go `validate.ErrClusterNameNotSet`.
pub const ERR_CLUSTER_NAME_NOT_SET: &str = "Cluster name is not set.";
/// Go `validate.ErrDomainNotSet`.
pub const ERR_DOMAIN_NOT_SET: &str = "Domain not set on request.";
/// Go `validate.ErrExecutionNotSet`.
pub const ERR_EXECUTION_NOT_SET: &str = "Execution is not set on request.";
/// Go `validate.ErrWorkflowIDNotSet`.
pub const ERR_WORKFLOW_ID_NOT_SET: &str = "WorkflowId is not set on request.";
/// Go `validate.ErrEmptyQueueType`.
pub const ERR_EMPTY_QUEUE_TYPE: &str = "Queue type is not set.";

fn bad(message: &str) -> AdminError {
    AdminError::BadRequest(BadRequestError::new(message))
}

fn history_error(status: tonic::Status) -> AdminError {
    AdminError::History(status)
}

/// Go `GetHistoryClient()` delegation target for Admin.
#[derive(Clone)]
pub struct HistoryProxy {
    history: HistoryClient,
}

impl HistoryProxy {
    #[must_use]
    pub fn new(history: HistoryClient) -> Self {
        Self { history }
    }

    pub async fn close_shard(
        &self,
        request: Option<h::CloseShardRequest>,
    ) -> Result<h::CloseShardResponse, AdminError> {
        let request = request.ok_or_else(|| bad(ERR_REQUEST_NOT_SET))?;
        self.history.close_shard(request).await.map_err(history_error)
    }

    /// Go rejects a nil request or nil task type; proto3 enums carry zero for
    /// "unset", which maps to the Invalid task type.
    pub async fn remove_task(
        &self,
        request: Option<h::RemoveTaskRequest>,
    ) -> Result<h::RemoveTaskResponse, AdminError> {
        let request = request.ok_or_else(|| bad(ERR_REQUEST_NOT_SET))?;
        self.history.remove_task(request).await.map_err(history_error)
    }

    pub async fn reset_queue(
        &self,
        request: Option<h::ResetQueueRequest>,
    ) -> Result<h::ResetQueueResponse, AdminError> {
        let request = request.ok_or_else(|| bad(ERR_REQUEST_NOT_SET))?;
        if request.cluster_name.is_empty() {
            return Err(bad(ERR_CLUSTER_NAME_NOT_SET));
        }
        self.history.reset_queue(request).await.map_err(history_error)
    }

    pub async fn describe_queue(
        &self,
        request: Option<h::DescribeQueueRequest>,
    ) -> Result<h::DescribeQueueResponse, AdminError> {
        let request = request.ok_or_else(|| bad(ERR_REQUEST_NOT_SET))?;
        if request.cluster_name.is_empty() {
            return Err(bad(ERR_CLUSTER_NAME_NOT_SET));
        }
        self.history.describe_queue(request).await.map_err(history_error)
    }

    pub async fn describe_history_host(
        &self,
        request: Option<h::DescribeHistoryHostRequest>,
    ) -> Result<h::DescribeHistoryHostResponse, AdminError> {
        let request = request.ok_or_else(|| bad(ERR_REQUEST_NOT_SET))?;
        self.history
            .describe_history_host(request)
            .await
            .map_err(history_error)
    }

    /// Go order: nil request, domain name, execution, workflow ID, events;
    /// then the domain lookup (done by the caller, which passes `domain_id`).
    pub async fn reapply_events(
        &self,
        request: Option<h::ReapplyEventsRequest>,
    ) -> Result<h::ReapplyEventsResponse, AdminError> {
        let request = request.ok_or_else(|| bad(ERR_REQUEST_NOT_SET))?;
        validate_reapply_events(&request)?;
        self.history.reapply_events(request).await.map_err(history_error)
    }

    pub async fn get_replication_messages(
        &self,
        request: Option<h::GetReplicationMessagesRequest>,
    ) -> Result<h::GetReplicationMessagesResponse, AdminError> {
        let request = request.ok_or_else(|| bad(ERR_REQUEST_NOT_SET))?;
        if request.cluster_name.is_empty() {
            return Err(bad(ERR_CLUSTER_NAME_NOT_SET));
        }
        self.history
            .get_replication_messages(request)
            .await
            .map_err(history_error)
    }

    pub async fn get_dlq_replication_messages(
        &self,
        request: Option<h::GetDlqReplicationMessagesRequest>,
    ) -> Result<h::GetDlqReplicationMessagesResponse, AdminError> {
        let request = request.ok_or_else(|| bad(ERR_REQUEST_NOT_SET))?;
        self.history
            .get_dlq_replication_messages(request)
            .await
            .map_err(history_error)
    }

    pub async fn count_dlq_messages(
        &self,
        request: h::CountDlqMessagesRequest,
    ) -> Result<h::CountDlqMessagesResponse, AdminError> {
        self.history
            .count_dlq_messages(request)
            .await
            .map_err(history_error)
    }

    /// Replication-type DLQ reads go straight to History after Go's request
    /// normalisation: page size defaults when <= 0 (callers pass the Go
    /// `ReadDLQMessagesPageSize` constant) and the end message ID defaults to
    /// the inclusive-maximum constant.
    pub async fn read_dlq_messages(
        &self,
        request: h::ReadDlqMessagesRequest,
    ) -> Result<h::ReadDlqMessagesResponse, AdminError> {
        self.history
            .read_dlq_messages(request)
            .await
            .map_err(history_error)
    }

    pub async fn purge_dlq_messages(
        &self,
        request: h::PurgeDlqMessagesRequest,
    ) -> Result<h::PurgeDlqMessagesResponse, AdminError> {
        self.history
            .purge_dlq_messages(request)
            .await
            .map_err(history_error)
    }

    pub async fn merge_dlq_messages(
        &self,
        request: h::MergeDlqMessagesRequest,
    ) -> Result<h::MergeDlqMessagesResponse, AdminError> {
        self.history
            .merge_dlq_messages(request)
            .await
            .map_err(history_error)
    }
}

/// Request checks Go's `ReapplyEvents` performs before touching the domain cache.
pub fn validate_reapply_events(request: &h::ReapplyEventsRequest) -> Result<(), AdminError> {
    if request.domain.is_empty() {
        return Err(bad(ERR_DOMAIN_NOT_SET));
    }
    let Some(execution) = request.workflow_execution.as_ref() else {
        return Err(bad(ERR_EXECUTION_NOT_SET));
    };
    if execution.workflow_id.is_empty() {
        return Err(bad(ERR_WORKFLOW_ID_NOT_SET));
    }
    // Go reports a missing events blob as ErrWorkflowIDNotSet.
    if request.events.is_none() {
        return Err(bad(ERR_WORKFLOW_ID_NOT_SET));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxy() -> HistoryProxy {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        HistoryProxy::new(HistoryClient::from_channel(channel, 1024))
    }

    fn message(error: AdminError) -> String {
        match error {
            AdminError::BadRequest(e) => e.message,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn nil_requests_and_cluster_names_are_rejected_before_history_is_called() {
        let proxy = proxy();
        assert_eq!(
            message(proxy.close_shard(None).await.unwrap_err()),
            ERR_REQUEST_NOT_SET
        );
        assert_eq!(
            message(proxy.reset_queue(None).await.unwrap_err()),
            ERR_REQUEST_NOT_SET
        );
        assert_eq!(
            message(
                proxy
                    .reset_queue(Some(h::ResetQueueRequest::default()))
                    .await
                    .unwrap_err()
            ),
            ERR_CLUSTER_NAME_NOT_SET
        );
        assert_eq!(
            message(
                proxy
                    .describe_queue(Some(h::DescribeQueueRequest::default()))
                    .await
                    .unwrap_err()
            ),
            ERR_CLUSTER_NAME_NOT_SET
        );
        assert_eq!(
            message(
                proxy
                    .get_replication_messages(Some(h::GetReplicationMessagesRequest::default()))
                    .await
                    .unwrap_err()
            ),
            ERR_CLUSTER_NAME_NOT_SET
        );
    }

    #[test]
    fn reapply_events_validation_follows_go_order() {
        let mut request = h::ReapplyEventsRequest::default();
        assert_eq!(
            message(validate_reapply_events(&request).unwrap_err()),
            ERR_DOMAIN_NOT_SET
        );
        request.domain = "d".into();
        assert_eq!(
            message(validate_reapply_events(&request).unwrap_err()),
            ERR_EXECUTION_NOT_SET
        );
        request.workflow_execution = Some(Default::default());
        assert_eq!(
            message(validate_reapply_events(&request).unwrap_err()),
            ERR_WORKFLOW_ID_NOT_SET
        );
        request.workflow_execution.as_mut().unwrap().workflow_id = "w".into();
        assert_eq!(
            message(validate_reapply_events(&request).unwrap_err()),
            ERR_WORKFLOW_ID_NOT_SET
        );
        request.events = Some(Default::default());
        assert!(validate_reapply_events(&request).is_ok());
    }
}

#[cfg(test)]
mod history_error_passthrough_tests {
    use super::*;
    use std::{convert::Infallible, future::Future, pin::Pin, task::{Context, Poll}};
    use tonic::codegen::{http, Service};

    #[derive(Clone)]
    struct FailingHistory(tonic::Status);
    impl tonic::server::NamedService for FailingHistory {
        const NAME: &'static str = "uber.cadence.history.v1.HistoryAPI";
    }
    impl Service<http::Request<tonic::body::BoxBody>> for FailingHistory {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
        fn call(&mut self, _: http::Request<tonic::body::BoxBody>) -> Self::Future {
            let status = self.0.clone();
            Box::pin(async move { Ok(status.into_http()) })
        }
    }

    #[tokio::test]
    async fn admin_history_forwarding_preserves_original_status_details_and_metadata() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // The service name is taken from the generated client, so the test follows
        // the same History path used by the live proxy.
        let status = tonic::Status::with_details(tonic::Code::NotFound, "history missing", vec![8, 5, 18, 0].into());
        let expected = status.clone();
        let incoming = tonic::codegen::tokio_stream::wrappers::TcpListenerStream::new(listener);
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder().add_service(FailingHistory(status)).serve_with_incoming(incoming).await.unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).unwrap().connect().await.unwrap();
        let proxy = HistoryProxy::new(HistoryClient::from_channel(channel, 1024));
        let AdminError::History(actual) = proxy.close_shard(Some(h::CloseShardRequest::default())).await.unwrap_err() else { panic!("History error was reclassified") };
        assert_eq!(actual.code(), expected.code());
        assert_eq!(actual.message(), expected.message());
        assert_eq!(actual.details(), expected.details());
        server.abort();
    }
}
