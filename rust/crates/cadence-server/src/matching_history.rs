//! Matching -> History task-start RPC boundary, using original protobuf APIs.
//! Persistence acknowledgements remain the queue controller's responsibility.
use cadence_api::tasktoken::{JsonTaskTokenSerializer, TaskToken};
use cadence_common::{backoff::NO_INTERVAL, ExponentialRetryPolicy, Retrier};
use cadence_persistence::matching_sqlite::AllocatedTask;
use cadence_proto::{api as p, history as h, matching as m, shared};
use prost::Message;
use std::{future::Future, sync::Arc, time::Duration};
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
/// Decode only the status that Go's proto mapper turns into
/// `*types.ShardOwnershipLostError`: gRPC Aborted plus the first typed detail.
fn ownership_lost_owner(error: &Status) -> Option<String> {
    if error.code() != Code::Aborted {
        return None;
    }
    let status = RpcStatus::decode(error.details()).ok()?;
    let detail = status.details.first()?;
    if detail.type_url.rsplit('/').next()? != "uber.cadence.shared.v1.ShardOwnershipLostError" {
        return None;
    }
    Some(
        shared::ShardOwnershipLostError::decode(detail.value.as_slice())
            .ok()?
            .owner,
    )
}
pub(crate) fn missing_execution(error: &Status) -> bool {
    error.code() == Code::NotFound
        && RpcStatus::decode(error.details())
            .ok()
            .and_then(|status| status.details.into_iter().next())
            .is_some_and(|detail| {
                detail.type_url.rsplit('/').next()
                    == Some("uber.cadence.api.v1.EntityNotExistsError")
            })
}
#[cfg(test)]
fn retryable(error: &Status) -> bool {
    start_error_kind(error) == StartErrorKind::Retry
}

/// Go `common.IsServiceTransientError` on the errors returned by the History
/// wrapper. Application errors remain terminal; transport failures and the
/// explicit service transient details are retried.
fn history_service_retryable(error: &Status) -> bool {
    if let Ok(status) = RpcStatus::decode(error.details()) {
        if let Some(detail) = status.details.first() {
            match detail.type_url.rsplit('/').next().unwrap_or_default() {
                "uber.cadence.api.v1.InternalServiceError"
                | "uber.cadence.api.v1.ServiceBusyError"
                | "uber.cadence.shared.v1.ShardOwnershipLostError"
                | "uber.cadence.shared.v1.TaskListNotOwnedByHostError"
                | "uber.cadence.shared.v1.SemaphoreNotOwnedByHostError" => return true,
                // Go's error mapper turns typed application errors into Go
                // errors before its YARPC status classifier sees them.
                name if name.starts_with("uber.cadence.") => return false,
                _ => {}
            }
        }
    }
    matches!(
        error.code(),
        Code::Unavailable | Code::Unknown | Code::Cancelled | Code::Internal
    )
}

fn service_busy(error: &Status) -> bool {
    RpcStatus::decode(error.details())
        .ok()
        .and_then(|status| status.details.into_iter().next())
        .is_some_and(|detail| {
            detail.type_url.rsplit('/').next() == Some("uber.cadence.api.v1.ServiceBusyError")
        })
}

#[derive(Clone)]
pub struct MatchingHistoryClient {
    history: h::history_api_client::HistoryApiClient<Channel>,
    /// Matching's default per-attempt budget for the two Record*Started RPCs.
    operation_timeout: Duration,
    /// Go factory History timeout, applied afresh for each resource retry.
    history_rpc_timeout: Duration,
    max_message_size: usize,
    /// Go `cfg.RecordTaskStartedTimeout(domain)` (`matching.recordTaskStartedTimeout`,
    /// a domain-filtered duration, default 1s): the operation timeout of each
    /// RecordDecisionTaskStarted/RecordActivityTaskStarted attempt. It is read
    /// per call because a running owner keeps its client.
    record_started_timeout: Option<RecordStartedTimeout>,
    peer_resolver: Option<Arc<crate::matching_history_peer_resolver::MatchingHistoryPeerResolver>>,
}
/// Live per-domain `matching.recordTaskStartedTimeout` read, in nanoseconds.
pub type RecordStartedTimeout = Arc<dyn Fn(&str) -> Duration + Send + Sync>;
/// Go `matching.recordTaskStartedTimeout` property default (1s), shared with the
/// Matching Record*Started attempt timeout, which is distinct from the
/// History client's configured per-RPC timeout.
pub const DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS: i64 = 1_000_000_000;
/// Go `client/wrappers/timeout.HistoryDefaultTimeout`, applied separately to
/// each History client invocation before resource retries.
pub const DEFAULT_HISTORY_RPC_TIMEOUT: Duration = Duration::from_secs(30);
impl MatchingHistoryClient {
    pub fn new(channel: Channel, operation_timeout: Duration, max_message_size: usize) -> Self {
        Self {
            history: h::history_api_client::HistoryApiClient::new(channel)
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size),
            operation_timeout,
            history_rpc_timeout: DEFAULT_HISTORY_RPC_TIMEOUT,
            max_message_size,
            record_started_timeout: None,
            peer_resolver: None,
        }
    }
    /// Override the factory's per-RPC timeout. Production uses Go's 30-second
    /// default; keeping this seam injectable makes timeout/retry tests bounded.
    pub fn with_history_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.history_rpc_timeout = timeout;
        self
    }
    /// Install Go's live `matching.recordTaskStartedTimeout` provider.
    pub fn with_record_started_timeout(mut self, provider: RecordStartedTimeout) -> Self {
        self.record_started_timeout = Some(provider);
        self
    }
    pub fn with_peer_resolver(
        mut self,
        resolver: Arc<crate::matching_history_peer_resolver::MatchingHistoryPeerResolver>,
    ) -> Self {
        self.peer_resolver = Some(resolver);
        self
    }
    pub fn peer_for_workflow(&self, workflow_id: &str) -> Result<Option<String>, Status> {
        self.peer_resolver
            .as_ref()
            .map(|resolver| resolver.workflow(workflow_id))
            .transpose()
    }
    pub fn peer_for_domain(&self, domain_id: &str) -> Result<Option<String>, Status> {
        self.peer_resolver
            .as_ref()
            .map(|resolver| resolver.domain(domain_id))
            .transpose()
    }
    pub fn peer_for_shard(&self, shard_id: u32) -> Result<Option<String>, Status> {
        self.peer_resolver
            .as_ref()
            .map(|resolver| resolver.shard_id(shard_id))
            .transpose()
    }
    pub fn peer_for_host_address(&self, address: &str) -> Result<Option<String>, Status> {
        self.peer_resolver
            .as_ref()
            .map(|resolver| resolver.host_address(address))
            .transpose()
    }
    fn client_for_peer(
        &self,
        peer: &str,
    ) -> Result<h::history_api_client::HistoryApiClient<Channel>, Status> {
        let uri = if peer.contains("://") {
            peer.to_owned()
        } else {
            format!("http://{peer}")
        };
        let channel = tonic::transport::Endpoint::from_shared(uri)
            .map_err(|error| {
                Status::unavailable(format!("Invalid resolved History endpoint: {error}"))
            })?
            .connect_lazy();
        Ok(h::history_api_client::HistoryApiClient::new(channel)
            .max_decoding_message_size(self.max_message_size)
            .max_encoding_message_size(self.max_message_size))
    }
    fn initial_peer(&self, workflow_id: &str) -> Result<Option<String>, Status> {
        self.peer_resolver
            .as_ref()
            .map(|resolver| resolver.workflow(workflow_id))
            .transpose()
    }
    /// Go `recordDecisionTaskStarted`/`recordActivityTaskStarted`: the live
    /// domain-filtered timeout for each Record*TaskStarted attempt, falling
    /// back to Go's 1s default when no dynamic provider is installed.
    pub(crate) fn record_started_timeout(&self, domain_id: &str) -> Option<Duration> {
        Some(
            self.record_started_timeout
                .as_ref()
                .map(|provider| provider(domain_id))
                .unwrap_or(Duration::from_nanos(
                    DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS as u64,
                )),
        )
    }
    fn record_started_operation_timeout(&self, domain_id: &str) -> Option<Duration> {
        let timeout = self
            .record_started_timeout(domain_id)
            .unwrap_or(self.operation_timeout);
        (!timeout.is_zero()).then_some(timeout)
    }
    #[cfg(test)]
    fn request<T>(&self, payload: T) -> Request<T> {
        self.request_with_timeout(payload, Some(self.history_rpc_timeout))
    }
    fn request_with_timeout<T>(&self, payload: T, timeout: Option<Duration>) -> Request<T> {
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
        if let Some(timeout) = timeout {
            request.set_timeout(timeout);
        }
        request
    }
    async fn retry<T: Clone + Send, R, F, Fut>(
        &self,
        payload: T,
        timeout: Option<Duration>,
        caller_deadline: Option<tokio::time::Instant>,
        workflow_id: &str,
        mut call: F,
    ) -> Result<R, Status>
    where
        F: FnMut(h::history_api_client::HistoryApiClient<Channel>, Request<T>) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        // resource/resource_impl.go wraps the 30s timeout client in
        // retryable.NewHistoryClient. Each retry gets a fresh timeout-wrapper
        // context, while the retry policy has its own 30s elapsed budget.
        let overall_deadline = match (caller_deadline, timeout) {
            (Some(deadline), Some(budget)) => {
                Some(deadline.min(tokio::time::Instant::now() + budget))
            }
            (Some(deadline), None) => Some(deadline),
            (None, Some(budget)) => Some(tokio::time::Instant::now() + budget),
            (None, None) => None,
        };
        let policy = ExponentialRetryPolicy::new(Duration::from_millis(50))
            .with_maximum_interval(Duration::from_secs(10))
            .with_expiration_interval(Duration::from_secs(30));
        let mut retrier = Retrier::new(policy);
        // NewThrottleRetry's default throttle policy remains active even when
        // resource code supplies its own retry policy and classifier.
        let throttle_policy = ExponentialRetryPolicy::new(Duration::from_secs(1))
            .with_maximum_interval(Duration::from_secs(10))
            .with_expiration_interval(NO_INTERVAL);
        let mut throttle_retrier = Retrier::new(throttle_policy);
        let mut previous_retry_error = None;
        loop {
            // Go ThrottleRetry captures the prior attempt error before it
            // invokes the next operation. If that next retry's backoff is
            // cancelled, it returns this prior error rather than the current
            // retryable error.
            let previous_error = previous_retry_error.clone();
            match self
                .execute_with_redirect(
                    payload.clone(),
                    Some(self.history_rpc_timeout),
                    overall_deadline,
                    workflow_id,
                    &mut call,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) => {
                    // The resource wrapper does not configure
                    // WithOperationTimeout. A timeout from the factory's
                    // 30-second History timeout is therefore classified like
                    // any other YARPC DeadlineExceeded status: terminal.
                    if !history_service_retryable(&error) {
                        return Err(previous_error.unwrap_or(error));
                    }
                    let Some(mut delay) = retrier.next_backoff(&mut rand::thread_rng()) else {
                        return Err(previous_error.unwrap_or(error));
                    };
                    if service_busy(&error) {
                        if let Some(throttle_delay) =
                            throttle_retrier.next_backoff(&mut rand::thread_rng())
                        {
                            delay = delay.max(throttle_delay);
                        }
                    }
                    if let Some(deadline) = overall_deadline {
                        if tokio::time::Instant::now() >= deadline {
                            return Err(previous_error.unwrap_or(error));
                        }
                        tokio::select! {
                            _ = tokio::time::sleep_until(deadline) => return Err(previous_error.unwrap_or(error)),
                            _ = tokio::time::sleep(delay) => {}
                        }
                    } else {
                        tokio::time::sleep(delay).await;
                    }
                    previous_retry_error = Some(error);
                }
            }
        }
    }
    /// Go matching's outer Record*TaskStarted retry loop. Each attempt gets a
    /// fresh domain timeout; inside it the resource History retry wrapper may
    /// retry transient errors until this attempt context expires.
    async fn retry_record_started<T: Clone + Send, R, F, Fut>(
        &self,
        payload: T,
        timeout: Option<Duration>,
        caller_deadline: Option<tokio::time::Instant>,
        workflow_id: &str,
        mut call: F,
    ) -> Result<R, Status>
    where
        F: FnMut(h::history_api_client::HistoryApiClient<Channel>, Request<T>) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let policy = ExponentialRetryPolicy::new(Duration::from_millis(250))
            .with_maximum_interval(Duration::from_secs(1))
            .with_maximum_attempts(5)
            .with_expiration_interval(NO_INTERVAL);
        let mut retrier = Retrier::new(policy);
        let throttle_policy = ExponentialRetryPolicy::new(Duration::from_secs(1))
            .with_maximum_interval(Duration::from_secs(10))
            .with_expiration_interval(NO_INTERVAL);
        let mut throttle_retrier = Retrier::new(throttle_policy);
        let mut previous_retry_error = None;
        loop {
            let previous_error = previous_retry_error.clone();
            match self
                .retry(
                    payload.clone(),
                    timeout,
                    caller_deadline,
                    workflow_id,
                    &mut call,
                )
                .await
            {
                Ok(value) => return Ok(value),
                Err(error) => {
                    // Go isMatchingRetryableError retries every error except
                    // the four stable application outcomes mapped above.
                    if start_error_kind(&error) != StartErrorKind::Retry {
                        return Err(previous_error.unwrap_or(error));
                    }
                    let Some(mut delay) = retrier.next_backoff(&mut rand::thread_rng()) else {
                        return Err(previous_error.unwrap_or(error));
                    };
                    if service_busy(&error) {
                        if let Some(throttle_delay) =
                            throttle_retrier.next_backoff(&mut rand::thread_rng())
                        {
                            delay = delay.max(throttle_delay);
                        }
                    }
                    if let Some(deadline) = caller_deadline {
                        if tokio::time::Instant::now() >= deadline {
                            return Err(previous_error.unwrap_or(error));
                        }
                        tokio::select! {
                            _ = tokio::time::sleep_until(deadline) => {
                                return Err(previous_error.unwrap_or(error));
                            }
                            _ = tokio::time::sleep(delay) => {}
                        }
                    } else {
                        tokio::time::sleep(delay).await;
                    }
                    previous_retry_error = Some(error);
                }
            }
        }
    }
    /// Match Go `executeWithRedirect`: validate the operation deadline before
    /// every RPC, and follow only typed ownership-loss statuses. Redirects
    /// share the original per-operation deadline.
    async fn execute_with_redirect<T: Clone + Send, R, F, Fut>(
        &self,
        payload: T,
        timeout: Option<Duration>,
        caller_deadline: Option<tokio::time::Instant>,
        workflow_id: &str,
        call: &mut F,
    ) -> Result<R, Status>
    where
        F: FnMut(h::history_api_client::HistoryApiClient<Channel>, Request<T>) -> Fut + Send,
        Fut: Future<Output = Result<R, Status>> + Send,
    {
        let initial_peer = self.initial_peer(workflow_id)?;
        // The factory timeout wrapper adds a fresh per-RPC timeout. A caller
        // deadline and the Record*Started attempt budget can shorten it.
        let operation_deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
        let deadline = match (caller_deadline, operation_deadline) {
            (Some(caller), Some(operation)) => Some(caller.min(operation)),
            (Some(caller), None) => Some(caller),
            (None, Some(operation)) => Some(operation),
            (None, None) => None,
        };
        let mut client = match initial_peer.as_deref() {
            Some(peer) => self.client_for_peer(peer)?,
            None => self.history.clone(),
        };
        loop {
            let remaining =
                deadline.map(|d| d.saturating_duration_since(tokio::time::Instant::now()));
            if remaining.is_some_and(|remaining| remaining.is_zero()) {
                return Err(Status::deadline_exceeded(
                    "History caller or operation deadline exceeded.",
                ));
            }
            let result = call(
                client.clone(),
                self.request_with_timeout(payload.clone(), remaining),
            );
            let result = if let Some(remaining) = remaining {
                tokio::time::timeout(remaining, result).await.map_err(|_| {
                    Status::deadline_exceeded("History caller or operation deadline exceeded.")
                })?
            } else {
                result.await
            };
            match result {
                Ok(result) => return Ok(result),
                Err(error) => {
                    let Some(owner) = ownership_lost_owner(&error) else {
                        return Err(error);
                    };
                    let Some(resolver) = &self.peer_resolver else {
                        return Err(error);
                    };
                    let peer = resolver.host_address(&owner)?;
                    client = self.client_for_peer(&peer)?;
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
        self.refresh_workflow_tasks_with_deadline(domain_id, task, None)
            .await
    }

    pub async fn refresh_workflow_tasks_with_deadline(
        &self,
        domain_id: &str,
        task: &AllocatedTask,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), Status> {
        let payload = h::RefreshWorkflowTasksRequest {
            domain: String::new(),
            domain_id: domain_id.into(),
            workflow_execution: Some(Self::execution(task)?),
        };
        let result = self
            .retry(
                payload,
                None,
                caller_deadline,
                &task.data.workflow_id,
                &mut |mut client: h::history_api_client::HistoryApiClient<tonic::transport::Channel>,
                      request: Request<h::RefreshWorkflowTasksRequest>| async move {
                    client.refresh_workflow_tasks(request).await.map(|_| ())
                },
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if missing_execution(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub async fn describe_execution(
        &self,
        domain_id: &str,
        domain_name: &str,
        task: &AllocatedTask,
    ) -> Result<h::DescribeWorkflowExecutionResponse, Status> {
        self.describe_execution_with_deadline(domain_id, domain_name, task, None)
            .await
    }

    pub async fn describe_execution_with_deadline(
        &self,
        domain_id: &str,
        domain_name: &str,
        task: &AllocatedTask,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<h::DescribeWorkflowExecutionResponse, Status> {
        let payload = h::DescribeWorkflowExecutionRequest {
            domain_id: domain_id.into(),
            request: Some(p::DescribeWorkflowExecutionRequest {
                domain: domain_name.into(),
                workflow_execution: Some(Self::execution(task)?),
                ..Default::default()
            }),
        };
        self.retry(
            payload,
            None,
            caller_deadline,
            &task.data.workflow_id,
            &mut |mut client: h::history_api_client::HistoryApiClient<tonic::transport::Channel>,
                  request: Request<h::DescribeWorkflowExecutionRequest>| async move {
                client
                    .describe_workflow_execution(request)
                    .await
                    .map(tonic::Response::into_inner)
            },
        )
        .await
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
        self.mutable_state_with_deadline(domain_id, execution, None)
            .await
    }

    pub async fn mutable_state_with_deadline(
        &self,
        domain_id: &str,
        execution: &p::WorkflowExecution,
        caller_deadline: Option<tokio::time::Instant>,
    ) -> Result<h::GetMutableStateResponse, Status> {
        let payload = h::GetMutableStateRequest {
            domain_id: domain_id.into(),
            workflow_execution: Some(execution.clone()),
            ..Default::default()
        };
        self.retry(
            payload,
            None,
            caller_deadline,
            &execution.workflow_id,
            move |mut client, request| async move {
                client
                    .get_mutable_state(request)
                    .await
                    .map(tonic::Response::into_inner)
            },
        )
        .await
    }

    pub async fn start_decision(
        &self,
        domain_id: &str,
        task: &AllocatedTask,
        poll: p::PollForDecisionTaskRequest,
    ) -> Result<m::PollForDecisionTaskResponse, Status> {
        self.start_decision_with_deadline(domain_id, task, poll, None)
            .await
    }

    pub async fn start_decision_with_deadline(
        &self,
        domain_id: &str,
        task: &AllocatedTask,
        poll: p::PollForDecisionTaskRequest,
        caller_deadline: Option<tokio::time::Instant>,
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
        let started = self
            .retry_record_started(
                payload,
                self.record_started_operation_timeout(domain_id),
                caller_deadline,
                &execution.workflow_id,
                move |mut client, request| async move {
                    client
                        .record_decision_task_started(request)
                        .await
                        .map(tonic::Response::into_inner)
                },
            )
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
        self.start_activity_with_deadline(domain_id, task, poll, None)
            .await
    }

    pub async fn start_activity_with_deadline(
        &self,
        domain_id: &str,
        task: &AllocatedTask,
        poll: p::PollForActivityTaskRequest,
        caller_deadline: Option<tokio::time::Instant>,
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
        let started = self
            .retry_record_started(
                payload,
                self.record_started_operation_timeout(domain_id),
                caller_deadline,
                &execution.workflow_id,
                move |mut client, request| async move {
                    client
                        .record_activity_task_started(request)
                        .await
                        .map(tonic::Response::into_inner)
                },
            )
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
    use crate::matching_history_peer_resolver::{
        HistoryMembership, MatchingHistoryPeerResolver, NamedPort,
    };
    use cadence_common::membership::Host;
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
    fn ownership_loss_decoder_obeys_go_status_and_first_detail_mapping() {
        let status = RpcStatus {
            code: Code::Aborted as i32,
            message: "ownership lost".into(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/uber.cadence.shared.v1.ShardOwnershipLostError"
                    .into(),
                value: shared::ShardOwnershipLostError {
                    owner: "history-ring-b:7935".into(),
                }
                .encode_to_vec(),
            }],
        }
        .encode_to_vec();
        assert_eq!(
            ownership_lost_owner(&Status::with_details(
                Code::Aborted,
                "ownership lost",
                status.into()
            )),
            Some("history-ring-b:7935".into())
        );
        assert_eq!(
            ownership_lost_owner(&typed(
                Code::Unavailable,
                "uber.cadence.shared.v1.ShardOwnershipLostError"
            )),
            None
        );
        assert_eq!(
            ownership_lost_owner(&typed(
                Code::Aborted,
                "another.package.ShardOwnershipLostError"
            )),
            None
        );
        let first_detail_wins = RpcStatus {
            code: Code::Aborted as i32,
            message: "ownership lost".into(),
            details: vec![
                prost_types::Any {
                    type_url: "type.googleapis.com/uber.cadence.api.v1.EntityNotExistsError".into(),
                    value: vec![],
                },
                prost_types::Any {
                    type_url: "type.googleapis.com/uber.cadence.shared.v1.ShardOwnershipLostError"
                        .into(),
                    value: shared::ShardOwnershipLostError {
                        owner: "ignored".into(),
                    }
                    .encode_to_vec(),
                },
            ],
        }
        .encode_to_vec();
        assert_eq!(
            ownership_lost_owner(&Status::with_details(
                Code::Aborted,
                "ownership lost",
                first_detail_wins.into()
            )),
            None
        );
    }

    #[derive(Clone)]
    struct TestMembership(Vec<Host>);
    impl HistoryMembership for TestMembership {
        fn hosts(&self) -> Result<Vec<Host>, Status> {
            Ok(self.0.clone())
        }
    }
    fn redirect_resolver() -> Arc<MatchingHistoryPeerResolver> {
        Arc::new(
            MatchingHistoryPeerResolver::new(
                1024,
                NamedPort::Grpc,
                Arc::new(TestMembership(vec![
                    Host {
                        address: "history-ring-a:7935".into(),
                        identity: "a".into(),
                        grpc_address: "http://127.0.0.1:1".into(),
                    },
                    Host {
                        address: "history-ring-b:7935".into(),
                        identity: "b".into(),
                        grpc_address: "http://127.0.0.1:2".into(),
                    },
                ])),
            )
            .unwrap(),
        )
    }
    fn ownership_status(owner: &str) -> Status {
        Status::with_details(
            Code::Aborted,
            "ownership lost",
            RpcStatus {
                code: Code::Aborted as i32,
                message: "ownership lost".into(),
                details: vec![prost_types::Any {
                    type_url: "type.googleapis.com/uber.cadence.shared.v1.ShardOwnershipLostError"
                        .into(),
                    value: shared::ShardOwnershipLostError {
                        owner: owner.into(),
                    }
                    .encode_to_vec(),
                }],
            }
            .encode_to_vec()
            .into(),
        )
    }
    struct DropMark(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for DropMark {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct RedirectHistory {
        redirect_owner: Option<String>,
    }
    #[tonic::async_trait]
    impl h::history_api_server::HistoryApi for RedirectHistory {
        async fn get_mutable_state(
            &self,
            _request: Request<h::GetMutableStateRequest>,
        ) -> Result<tonic::Response<h::GetMutableStateResponse>, Status> {
            if let Some(owner) = &self.redirect_owner {
                let status = RpcStatus {
                    code: Code::Aborted as i32,
                    message: "ownership lost".into(),
                    details: vec![prost_types::Any {
                        type_url:
                            "type.googleapis.com/uber.cadence.shared.v1.ShardOwnershipLostError"
                                .into(),
                        value: shared::ShardOwnershipLostError {
                            owner: owner.clone(),
                        }
                        .encode_to_vec(),
                    }],
                };
                Err(Status::with_details(
                    Code::Aborted,
                    "ownership lost",
                    status.encode_to_vec().into(),
                ))
            } else {
                Ok(tonic::Response::new(h::GetMutableStateResponse {
                    next_event_id: 77,
                    ..Default::default()
                }))
            }
        }
    }
    async fn start_redirect_history(redirect_owner: Option<String>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(h::history_api_server::HistoryApiServer::new(
                    RedirectHistory { redirect_owner },
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{address}")
    }
    #[tokio::test]
    async fn matching_history_redirects_between_two_tonic_peers_on_typed_owner_loss() {
        let peer_a = start_redirect_history(Some("history-ring-b:7935".into())).await;
        let peer_b = start_redirect_history(None).await;
        let hosts = vec![
            Host {
                address: "history-ring-a:7935".into(),
                identity: "a".into(),
                grpc_address: peer_a,
            },
            Host {
                address: "history-ring-b:7935".into(),
                identity: "b".into(),
                grpc_address: peer_b,
            },
        ];
        let resolver = Arc::new(
            MatchingHistoryPeerResolver::new(
                1024,
                NamedPort::Grpc,
                Arc::new(TestMembership(hosts)),
            )
            .unwrap(),
        );
        let workflow_id = (0..1000)
            .map(|n| format!("redirect-workflow-{n}"))
            .find(|id| {
                resolver.workflow(id).unwrap()
                    == resolver.host_address("history-ring-a:7935").unwrap()
            })
            .expect("a key should map to peer A");
        let history = client().with_peer_resolver(resolver);
        let response = history
            .mutable_state(
                "domain-id",
                &p::WorkflowExecution {
                    workflow_id,
                    run_id: "run".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response.next_event_id, 77,
            "the response must come from peer B after A redirects"
        );
    }
    #[tokio::test]
    async fn record_started_redirects_share_one_attempt_budget() {
        let history = client().with_peer_resolver(redirect_resolver());
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let start = tokio::time::Instant::now();
        let result = history
            .execute_with_redirect(
                (),
                Some(Duration::from_millis(35)),
                None,
                "workflow-budget",
                &mut move |_, _| {
                    let attempt = count.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            Err(ownership_status("history-ring-b:7935"))
                        } else {
                            std::future::pending::<Result<(), Status>>().await
                        }
                    }
                },
            )
            .await;
        assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(start.elapsed() < Duration::from_millis(200));
    }
    #[tokio::test]
    async fn caller_deadline_drops_the_redirect_rpc_future() {
        let history = client().with_peer_resolver(redirect_resolver());
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let drop_mark = dropped.clone();
        let result = history
            .execute_with_redirect(
                (),
                None,
                Some(tokio::time::Instant::now() + Duration::from_millis(25)),
                "workflow-caller-deadline",
                &mut move |_, _| {
                    let attempt = count.fetch_add(1, Ordering::SeqCst);
                    let drop_mark = drop_mark.clone();
                    async move {
                        if attempt == 0 {
                            Err(ownership_status("history-ring-b:7935"))
                        } else {
                            let _guard = DropMark(drop_mark);
                            std::future::pending::<Result<(), Status>>().await
                        }
                    }
                },
            )
            .await;
        assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(dropped.load(Ordering::SeqCst));
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
        let history_client = client();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let copy = seen.clone();
        let request = h::RecordDecisionTaskStartedRequest {
            request_id: Uuid::new_v4().to_string(),
            ..Default::default()
        };
        let expected = request.request_id.clone();
        let result = history_client
            .retry(
                request,
                Some(Duration::from_secs(1)),
                None,
                "workflow-test",
                move |_, request| {
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
                },
            )
            .await
            .unwrap();
        assert_eq!(result, 42);
        assert_eq!(*seen.lock().unwrap(), vec![expected; 3]);
    }
    #[tokio::test]
    async fn matching_history_terminal_errors_and_cancellation_stop_retries() {
        let history_client = client();
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let result: Result<(), _> = history_client
            .retry((), None, None, "workflow-test", move |_, _| {
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
            history_client.retry((), None, None, "workflow-test", move |_, _| {
                copy.fetch_add(1, Ordering::Relaxed);
                std::future::ready(Err::<(), _>(Status::unavailable("temporary")))
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
    #[tokio::test]
    async fn matching_history_record_started_timeout_reads_live_per_domain() {
        let history_client = client();
        let default = Duration::from_secs(1);
        // Without a live domain provider, Record*Started uses Go's 1s default.
        assert_eq!(
            history_client.record_started_timeout("domain-a"),
            Some(default)
        );
        assert_eq!(
            client().request(()).metadata().get("grpc-timeout"),
            Some(&grpc_timeout(default))
        );
        // Go reads `matching.recordTaskStartedTimeout(domain)` per attempt.
        let configured_client =
            history_client
                .clone()
                .with_record_started_timeout(Arc::new(|domain_id: &str| {
                    if domain_id == "domain-a" {
                        Duration::from_millis(250)
                    } else {
                        Duration::from_secs(9)
                    }
                }));
        assert_eq!(
            configured_client.record_started_timeout("domain-a"),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            configured_client.record_started_timeout("domain-b"),
            Some(Duration::from_secs(9))
        );
        let unbounded = configured_client
            .clone()
            .with_record_started_timeout(Arc::new(|_: &str| Duration::ZERO));
        assert_eq!(
            unbounded.record_started_operation_timeout("domain-a"),
            None,
            "Go WithOperationTimeout(0) leaves the caller deadline unchanged"
        );
        let request = configured_client.request_with_timeout(
            (),
            configured_client.record_started_timeout("domain-a"),
        );
        assert_eq!(
            request.metadata().get("grpc-timeout"),
            Some(&grpc_timeout(Duration::from_millis(250)))
        );
        // The factory applies its separate 30s timeout to each History RPC.
        assert_eq!(
            history_client.request(()).metadata().get("grpc-timeout"),
            Some(&grpc_timeout(DEFAULT_HISTORY_RPC_TIMEOUT))
        );
    }
    #[test]
    fn history_service_retry_classifier_matches_go_transient_and_terminal_cases() {
        assert!(history_service_retryable(&Status::unavailable("down")));
        assert!(history_service_retryable(&typed(
            Code::Internal,
            "uber.cadence.api.v1.InternalServiceError"
        )));
        assert!(history_service_retryable(&typed(
            Code::ResourceExhausted,
            "uber.cadence.api.v1.ServiceBusyError"
        )));
        assert!(!history_service_retryable(&typed(
            Code::NotFound,
            "uber.cadence.api.v1.EntityNotExistsError"
        )));
        assert!(!history_service_retryable(&Status::invalid_argument(
            "bad request"
        )));
    }
    #[tokio::test]
    async fn history_rpc_timeout_resets_for_each_resource_retry() {
        let history_client = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_millis(25),
            32 * 1024 * 1024,
        )
        .with_history_rpc_timeout(Duration::from_millis(25));
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let started = tokio::time::Instant::now();
        let response = history_client
            .retry(
                (),
                None,
                None,
                "workflow-timeout-reset",
                move |_, request| {
                    assert!(request.metadata().get("grpc-timeout").is_some());
                    let call = copy.fetch_add(1, Ordering::SeqCst);
                    async move {
                        tokio::time::sleep(Duration::from_millis(15)).await;
                        if call == 0 {
                            Err(Status::unavailable("retry me"))
                        } else {
                            Ok(29)
                        }
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(response, 29);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() >= Duration::from_millis(50));
    }
    #[tokio::test]
    async fn history_caller_deadline_bounds_all_resource_retries() {
        let history_client = client();
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(70);
        let error = history_client
            .retry(
                (),
                None,
                Some(deadline),
                "workflow-retry-budget",
                move |_, _| {
                    let attempt = copy.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err::<(), _>(Status::unavailable(format!(
                        "attempt-{attempt}"
                    ))))
                },
            )
            .await
            .unwrap_err();
        // Go records `prevErr` before each attempt; cancellation during the
        // second backoff returns the first operation error.
        assert_eq!(error.code(), Code::Unavailable);
        assert_eq!(error.message(), "attempt-0");
        assert!(calls.load(Ordering::SeqCst) <= 2);
    }
    #[tokio::test]
    async fn history_factory_deadline_error_is_not_retryable_by_resource_wrapper() {
        let history_client = client();
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let error = history_client
            .retry((), None, None, "workflow-factory-deadline", move |_, _| {
                copy.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(Status::deadline_exceeded("factory timeout")))
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::DeadlineExceeded);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn record_started_retries_stop_at_caller_deadline() {
        let history_client = client();
        let calls = Arc::new(AtomicUsize::new(0));
        let copy = calls.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(25);
        let error = history_client
            .retry_record_started(
                (),
                Some(Duration::from_secs(1)),
                Some(deadline),
                "workflow-record-caller-deadline",
                move |_, _| {
                    copy.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err::<(), _>(Status::unavailable("retry me")))
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Unavailable);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    fn grpc_timeout(duration: Duration) -> tonic::metadata::MetadataValue<tonic::metadata::Ascii> {
        let mut request = Request::new(());
        request.set_timeout(duration);
        request.metadata().get("grpc-timeout").unwrap().clone()
    }

    #[tokio::test]
    async fn history_rpc_boundary_uses_caller_deadline_for_query_and_task_start_calls() {
        // The RPC closure is the boundary where Matching hands a generated
        // History request to tonic. A longer factory/operation budget must not
        // replace the caller's shorter deadline on any of these call paths.
        let history = client().with_history_rpc_timeout(Duration::from_secs(30));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let unbounded_factory_timeout = grpc_timeout(Duration::from_secs(30));

        let query_timeout = Arc::new(Mutex::new(None));
        let observed = query_timeout.clone();
        let result = history
            .execute_with_redirect(
                h::GetMutableStateRequest::default(),
                None,
                Some(deadline),
                "deadline-query",
                &mut move |_, request| {
                    *observed.lock().unwrap() = request.metadata().get("grpc-timeout").cloned();
                    async { Ok::<_, Status>(()) }
                },
            )
            .await;
        result.unwrap();
        let value = query_timeout.lock().unwrap().clone().unwrap();
        assert_ne!(value, unbounded_factory_timeout, "query RPC must use the caller deadline");

        for (operation, payload) in [
            ("decision-start", h::RecordDecisionTaskStartedRequest::default().encode_to_vec()),
            ("activity-start", h::RecordActivityTaskStartedRequest::default().encode_to_vec()),
        ] {
            let observed = Arc::new(Mutex::new(None));
            let capture = observed.clone();
            let result = history
                .execute_with_redirect(
                    payload,
                    Some(Duration::from_secs(30)),
                    Some(deadline),
                    operation,
                    &mut move |_, request| {
                        *capture.lock().unwrap() = request.metadata().get("grpc-timeout").cloned();
                        async { Ok::<_, Status>(()) }
                    },
                )
                .await;
            result.unwrap();
            let value = observed.lock().unwrap().clone().unwrap();
            assert_ne!(value, unbounded_factory_timeout,
                "{operation} RPC must use the caller deadline");
        }
    }
}
