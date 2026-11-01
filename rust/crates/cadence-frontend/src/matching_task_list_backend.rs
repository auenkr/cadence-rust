//! Typed Frontend backend for the Matching task-list queries. Requests and
//! responses cross the internal Matching gRPC boundary as generated protobuf
//! types; the mapping is shared with Matching via `cadence_proto::convert`.
//! Other operations are not served by this backend.
#![allow(clippy::result_large_err)]
use crate::handler_workflow::{WorkflowBackend, WorkflowOperation};
use crate::matching_client::{MatchingClient, MatchingClientResolver};
use cadence_api::errors::{BadRequestError, CadenceError, InternalServiceError};
use cadence_api::tasks;
use cadence_proto::convert as cv;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;

pub struct MatchingTaskListBackend<R> {
    resolver: R,
}

impl<R> MatchingTaskListBackend<R> {
    pub fn new(resolver: R) -> Self {
        Self { resolver }
    }
}

impl<R: MatchingClientResolver> MatchingTaskListBackend<R> {
    async fn dispatch(
        &self,
        operation: WorkflowOperation,
        request: Value,
        target_cluster: Option<&str>,
    ) -> Result<Value, CadenceError> {
        match operation {
            WorkflowOperation::ListTaskListPartitions => {
                let request: tasks::ListTaskListPartitionsRequest = parse(request)?;
                let client = self.client(target_cluster)?;
                let response = client
                    .list_task_list_partitions(cv::list_task_list_partitions_request_out(request))
                    .await
                    .map_err(status_error)?;
                to_value(cv::list_task_list_partitions_in(response))
            }
            WorkflowOperation::GetTaskListsByDomain => {
                let request: tasks::GetTaskListsByDomainRequest = parse(request)?;
                let client = self.client(target_cluster)?;
                let response = client
                    .get_task_lists_by_domain(cv::get_task_lists_by_domain_request_out(request))
                    .await
                    .map_err(status_error)?;
                to_value(cv::get_task_lists_by_domain_in(response).map_err(status_error)?)
            }
            other => Err(CadenceError::Internal(InternalServiceError::new(format!(
                "{} is not served by the typed Matching task-list backend",
                other.name()
            )))),
        }
    }

    fn client(&self, target_cluster: Option<&str>) -> Result<MatchingClient, CadenceError> {
        self.resolver
            .matching_client(target_cluster)
            .map_err(|message| CadenceError::Internal(InternalServiceError::new(message)))
    }
}

impl<R: MatchingClientResolver> WorkflowBackend for MatchingTaskListBackend<R> {
    fn invoke<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, CadenceError>> + Send + 'a>> {
        self.invoke_on_cluster(operation, request, None)
    }

    fn invoke_on_cluster<'a>(
        &'a self,
        operation: WorkflowOperation,
        request: Value,
        target_cluster: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, CadenceError>> + Send + 'a>> {
        Box::pin(async move {
            self.dispatch(operation, request, target_cluster.as_deref())
                .await
        })
    }
}

fn parse<T: DeserializeOwned>(value: Value) -> Result<T, CadenceError> {
    serde_json::from_value(value)
        .map_err(|error| CadenceError::BadRequest(BadRequestError::new(error.to_string())))
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, CadenceError> {
    serde_json::to_value(value)
        .map_err(|error| CadenceError::Internal(InternalServiceError::new(error.to_string())))
}

fn status_error(status: tonic::Status) -> CadenceError {
    match status.code() {
        tonic::Code::InvalidArgument => {
            CadenceError::BadRequest(BadRequestError::new(status.message()))
        }
        _ => CadenceError::Internal(InternalServiceError::new(status.message())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Unavailable;

    impl MatchingClientResolver for Unavailable {
        fn matching_client(&self, _: Option<&str>) -> Result<MatchingClient, String> {
            Err("matching unavailable".to_string())
        }
    }

    #[tokio::test]
    async fn malformed_request_is_bad_request_before_dialing() {
        let backend = MatchingTaskListBackend::new(Unavailable);
        let error = backend
            .invoke(WorkflowOperation::ListTaskListPartitions, json!("oops"))
            .await
            .unwrap_err();
        assert!(matches!(error, CadenceError::BadRequest(_)));
    }

    #[tokio::test]
    async fn resolver_failure_is_internal() {
        let backend = MatchingTaskListBackend::new(Unavailable);
        let error = backend
            .invoke(
                WorkflowOperation::GetTaskListsByDomain,
                json!({"domain": "d"}),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, CadenceError::Internal(_)));
    }

    #[tokio::test]
    async fn other_operations_are_not_served() {
        let backend = MatchingTaskListBackend::new(Unavailable);
        let error = backend
            .invoke(WorkflowOperation::DescribeTaskList, json!({}))
            .await
            .unwrap_err();
        assert!(matches!(error, CadenceError::Internal(_)));
    }
}
