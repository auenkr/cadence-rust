//! Matching -> History task-start RPC boundary, using original protobuf APIs.
//! Persistence acknowledgements remain the queue controller's responsibility.
use cadence_api::tasktoken::{JsonTaskTokenSerializer, TaskToken};
use cadence_common::{backoff::NO_INTERVAL, ExponentialRetryPolicy, Retrier};
use cadence_persistence::matching_sqlite::AllocatedTask;
use cadence_proto::{api as p, history as h, matching as m};
use prost::Message;
use std::{future::Future, time::Duration};
use tonic::{transport::Channel, Code, Request, Status};
use uuid::Uuid;

#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}
/// Like Go ToError + isMatchingRetryableError: a bare gRPC code alone does
/// not prove a terminal Cadence application error. Only the first typed detail
/// is interpreted by Go's mapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartErrorKind {
    Discard,
    InactiveDomain,
    Retry,
}
pub fn start_error_kind(error: &Status) -> StartErrorKind {
    let Ok(details) = RpcStatus::decode(error.details()) else {
        return StartErrorKind::Retry;
    };
    let Some(detail) = details.details.first() else {
        return StartErrorKind::Retry;
    };
    let name = detail.type_url.rsplit('/').next().unwrap_or_default();
    match (error.code(), name) {
        (
            Code::NotFound,
            "uber.cadence.api.v1.EntityNotExistsError"
            | "uber.cadence.api.v1.WorkflowExecutionAlreadyCompletedError",
        )
        | (Code::AlreadyExists, "uber.cadence.shared.v1.EventAlreadyStartedError") => {
            StartErrorKind::Discard
        }
        (Code::FailedPrecondition, "uber.cadence.api.v1.DomainNotActiveError") => {
            StartErrorKind::InactiveDomain
        }
        _ => StartErrorKind::Retry,
    }
}
fn missing_execution(error: &Status) -> bool {
    error.code() == Code::NotFound
        && RpcStatus::decode(error.details())
            .ok()
            .and_then(|status| status.details.into_iter().next())
            .is_some_and(|detail| {
                detail.type_url.rsplit('/').next()
                    == Some("uber.cadence.api.v1.EntityNotExistsError")
            })
}
fn retryable(error: &Status) -> bool {
    start_error_kind(error) == StartErrorKind::Retry
}

#[derive(Clone)]
pub struct MatchingHistoryClient {
    history: h::history_api_client::HistoryApiClient<Channel>,
    operation_timeout: Duration,
}
impl MatchingHistoryClient {
    pub fn new(channel: Channel, operation_timeout: Duration, max_message_size: usize) -> Self {
        Self {
            history: h::history_api_client::HistoryApiClient::new(channel)
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size),
            operation_timeout,
        }
    }
    fn request<T>(&self, payload: T) -> Request<T> {
        let mut request = Request::new(payload);
        request.metadata_mut().insert(
            "rpc-service",
            tonic::metadata::MetadataValue::from_static("cadence-history"),
        );
        request.metadata_mut().insert(
            "rpc-caller",
            tonic::metadata::MetadataValue::from_static("cadence-matching"),
        );
        request.metadata_mut().insert(
            "rpc-encoding",
            tonic::metadata::MetadataValue::from_static("proto"),
        );
        request.set_timeout(self.operation_timeout);
        request
    }
    async fn retry<T: Clone + Send, R, F, Fut>(&self, payload: T, mut call: F) -> Result<R, Status>
    where
        F: FnMut(Request<T>) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let policy = ExponentialRetryPolicy::new(Duration::from_millis(250))
            .with_maximum_interval(Duration::from_secs(1))
            .with_maximum_attempts(5)
            .with_expiration_interval(NO_INTERVAL);
        let mut retrier = Retrier::new(policy);
        loop {
            match call(self.request(payload.clone())).await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if !retryable(&error) {
                        return Err(error);
                    }
                    let Some(delay) = retrier.next_backoff(&mut rand::thread_rng()) else {
                        return Err(error);
                    };
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    fn execution(task: &AllocatedTask) -> Result<p::WorkflowExecution, Status> {
        let run = task
            .data
            .run_id
            .as_deref()
            .ok_or_else(|| Status::internal("Missing run ID in persisted matching task."))?;
        let run_id = Uuid::from_slice(run)
            .map_err(|_| Status::internal("Invalid run ID in persisted matching task."))?
            .to_string();
        Ok(p::WorkflowExecution {
            workflow_id: task.data.workflow_id.clone(),
            run_id,
        })
    }
    pub async fn refresh_workflow_tasks(
        &self,
        domain_id: &str,
        task: &AllocatedTask,
    ) -> Result<(), Status> {
        let request = self.request(h::RefreshWorkflowTasksRequest {
            domain: String::new(),
            domain_id: domain_id.into(),
            workflow_execution: Some(Self::execution(task)?),
        });
        match self.history.clone().refresh_workflow_tasks(request).await {
            Ok(_) => Ok(()),
            Err(error) if missing_execution(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Go matching query polling: `historyService.GetMutableState`. A query
    /// task is delivered without recording a decision task started, but the
    /// frontend still needs the next event id and workflow metadata to load
    /// history for the non-sticky query.
    pub async fn mutable_state(
        &self,
        domain_id: &str,
        execution: &p::WorkflowExecution,
    ) -> Result<h::GetMutableStateResponse, Status> {
        let payload = h::GetMutableStateRequest {
            domain_id: domain_id.into(),
            workflow_execution: Some(execution.clone()),
            ..Default::default()
        };
        let client = self.history.clone();
        self.retry(payload, move |request| {
            let mut client = client.clone();
            async move {
                client
                    .get_mutable_state(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })
        .await
    }

    pub async fn start_decision(
        &self,
        domain_id: &str,
        task: &AllocatedTask,
        poll: p::PollForDecisionTaskRequest,
    ) -> Result<m::PollForDecisionTaskResponse, Status> {
        let execution = Self::execution(task)?;
        let payload = h::RecordDecisionTaskStartedRequest {
            domain_id: domain_id.into(),
            workflow_execution: Some(execution.clone()),
            schedule_id: task.data.schedule_id,
            task_id: task.task_id,
            request_id: Uuid::new_v4().to_string(),
            poll_request: Some(poll),
        };
        let client = self.history.clone();
        let started = self
            .retry(payload, move |request| {
                let mut client = client.clone();
                async move {
                    client
                        .record_decision_task_started(request)
                        .await
                        .map(tonic::Response::into_inner)
                }
            })
            .await?;
        let token = TaskToken {
            domain_id: domain_id.into(),
            workflow_id: execution.workflow_id.clone(),
            run_id: execution.run_id.clone(),
            schedule_id: started.scheduled_event_id,
            schedule_attempt: i64::from(started.attempt),
            ..Default::default()
        };
        Ok(m::PollForDecisionTaskResponse {
            task_token: JsonTaskTokenSerializer::serialize(&token)
                .map_err(|_| Status::internal("Failed to serialize decision task token."))?,
            workflow_execution: Some(execution),
            workflow_type: started.workflow_type,
            previous_started_event_id: started
                .previous_started_event_id
                .filter(|event| *event != -23),
            started_event_id: started.started_event_id,
            attempt: started.attempt,
            next_event_id: started.next_event_id,
            sticky_execution_enabled: started.sticky_execution_enabled,
            decision_info: started.decision_info,
            workflow_execution_task_list: started.workflow_execution_task_list,
            branch_token: started.branch_token,
            scheduled_time: started.scheduled_time,
            started_time: started.started_time,
            queries: started.queries,
            total_history_bytes: started.history_size,
            ..Default::default()
        })
    }
    pub async fn start_activity(
        &self,
        domain_id: &str,
        task: &AllocatedTask,
        poll: p::PollForActivityTaskRequest,
    ) -> Result<m::PollForActivityTaskResponse, Status> {
        let execution = Self::execution(task)?;
        let payload = h::RecordActivityTaskStartedRequest {
            domain_id: domain_id.into(),
            workflow_execution: Some(execution.clone()),
            schedule_id: task.data.schedule_id,
            task_id: task.task_id,
            request_id: Uuid::new_v4().to_string(),
            poll_request: Some(poll),
        };
        let client = self.history.clone();
        let started = self
            .retry(payload, move |request| {
                let mut client = client.clone();
                async move {
                    client
                        .record_activity_task_started(request)
                        .await
                        .map(tonic::Response::into_inner)
                }
            })
            .await?;
        let event = started.scheduled_event.ok_or_else(|| {
            Status::internal("Activity scheduled event is missing from History response.")
        })?;
        let Some(p::history_event::Attributes::ActivityTaskScheduledEventAttributes(attributes)) =
            event.attributes
        else {
            return Err(Status::internal(
                "ActivityTaskScheduledEventAttributes is not set.",
            ));
        };
        if attributes.activity_id.is_empty() {
            return Err(Status::internal(
                "ActivityTaskScheduledEventAttributes.ActivityID is not set.",
            ));
        }
        let token = TaskToken {
            domain_id: domain_id.into(),
            workflow_id: execution.workflow_id.clone(),
            run_id: execution.run_id.clone(),
            workflow_type: started
                .workflow_type
                .as_ref()
                .map(|value| value.name.clone())
                .unwrap_or_default(),
            schedule_id: task.data.schedule_id,
            schedule_attempt: i64::from(started.attempt),
            activity_id: attributes.activity_id.clone(),
            activity_type: attributes
                .activity_type
                .as_ref()
                .map(|value| value.name.clone())
                .unwrap_or_default(),
        };
        Ok(m::PollForActivityTaskResponse {
            task_token: JsonTaskTokenSerializer::serialize(&token)
                .map_err(|_| Status::internal("Failed to serialize activity task token."))?,
            workflow_execution: Some(execution),
            activity_id: attributes.activity_id,
            activity_type: attributes.activity_type,
            input: attributes.input,
            scheduled_time: event.event_time,
            started_time: started.started_time,
            schedule_to_close_timeout: attributes.schedule_to_close_timeout,
            start_to_close_timeout: attributes.start_to_close_timeout,
            heartbeat_timeout: attributes.heartbeat_timeout,
            attempt: started.attempt,
            scheduled_time_of_this_attempt: started.scheduled_time_of_this_attempt,
            heartbeat_details: started.heartbeat_details,
            workflow_type: started.workflow_type,
            workflow_domain: started.workflow_domain,
            header: attributes.header,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    fn client() -> MatchingHistoryClient {
        MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        )
    }
    fn typed(code: Code, name: &str) -> Status {
        Status::with_details(
            code,
            "fixture",
            RpcStatus {
                code: code as i32,
                message: "fixture".into(),
                details: vec![prost_types::Any {
                    type_url: format!("type.googleapis.com/{name}"),
                    value: Vec::new(),
                }],
            }
            .encode_to_vec()
            .into(),
        )
    }
    #[test]
    fn matching_history_retry_classification_requires_cadence_error_type() {
        for (code, name) in [
            (Code::NotFound, "uber.cadence.api.v1.EntityNotExistsError"),
            (
                Code::NotFound,
                "uber.cadence.api.v1.WorkflowExecutionAlreadyCompletedError",
            ),
            (
                Code::AlreadyExists,
                "uber.cadence.shared.v1.EventAlreadyStartedError",
            ),
            (
                Code::FailedPrecondition,
                "uber.cadence.api.v1.DomainNotActiveError",
            ),
        ] {
            assert!(!retryable(&typed(code, name)));
            assert!(retryable(&Status::new(code, "untyped transport error")));
        }
        assert!(retryable(&typed(
            Code::NotFound,
            "another.package.EntityNotExistsError"
        )));
        assert!(retryable(&typed(
            Code::Unavailable,
            "uber.cadence.api.v1.EntityNotExistsError"
        )));
        assert!(missing_execution(&typed(
            Code::NotFound,
            "uber.cadence.api.v1.EntityNotExistsError"
        )));
        assert!(!missing_execution(&typed(
            Code::NotFound,
            "uber.cadence.api.v1.WorkflowExecutionAlreadyCompletedError"
        )));
        assert!(!missing_execution(&Status::not_found("untyped")));
    }
    #[tokio::test]
    async fn matching_history_retries_preserve_request_id_and_yarpc_metadata() {
        let client = client();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let copy = seen.clone();
        let request = h::RecordDecisionTaskStartedRequest {
            request_id: Uuid::new_v4().to_string(),
            ..Default::default()
        };
        let expected = request.request_id.clone();
        let result = client
            .retry(request, move |request| {
                assert_eq!(
                    request.metadata().get("rpc-service").unwrap(),
                    "cadence-history"
                );
                assert_eq!(
                    request.metadata().get("rpc-caller").unwrap(),
                    "cadence-matching"
                );
                assert_eq!(request.metadata().get("rpc-encoding").unwrap(), "proto");
                assert!(request.metadata().get("grpc-timeout").is_some());
                let mut seen = copy.lock().unwrap();
                seen.push(request.into_inner().request_id);
                std::future::ready(if seen.len() < 3 {
                    Err(Status::unavailable("temporary"))
                } else {
                    Ok(42)
                })
            })
            .await
            .unwrap();
        assert_eq!(result, 42);
        assert_eq!(*seen.lock().unwrap(), vec![expected; 3]);
    }
    #[tokio::test]
    async fn matching_history_terminal_errors_and_cancellation_stop_retries() {
        let client = client();
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let result: Result<(), _> = client
            .retry((), move |_| {
                copy.fetch_add(1, Ordering::Relaxed);
                std::future::ready(Err(typed(
                    Code::AlreadyExists,
                    "uber.cadence.shared.v1.EventAlreadyStartedError",
                )))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let result = tokio::time::timeout(
            Duration::from_millis(20),
            client.retry((), move |_| {
                copy.fetch_add(1, Ordering::Relaxed);
                std::future::ready(Err::<(), _>(Status::unavailable("temporary")))
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}
