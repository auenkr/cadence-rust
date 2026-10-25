//! Durable matching query-task delivery, from Go `matcher.OfferQuery` /
//! `queryTaskC` and the handler's `lockableQueryTaskMap`. A query is an
//! ephemeral task offered to a decision poller: it never touches task storage
//! and is not visible to the backlog.
use cadence_proto::{api as p, history as h, matching as m};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{oneshot, Notify};
use tonic::Status;

/// Go `EmptyEventID`: `previous_started_event_id` is omitted while equal.
const EMPTY_EVENT_ID: i64 = -23;
/// Go `GoWorkerStickyQueryVersion` / `StickyQueryUnknownImplConstraints` are
/// both 1.0.0.
const STICKY_QUERY_MIN_MAJOR: u64 = 1;

/// A query offered by `QueryWorkflow` and awaiting a decision poller.
#[derive(Clone)]
pub struct PendingQuery {
    pub task_id: String,
    pub domain_id: String,
    pub task_list: String,
    pub request: p::QueryWorkflowRequest,
}

/// What a waiting `QueryWorkflow` call receives.
pub enum QueryOutcome {
    /// The worker's `RespondQueryTaskCompleted` result plus its version info,
    /// which the strong-consistency gate checks.
    Worker(WorkerAnswer),
    /// Go `queryResult{internalError}`: polling failed before delivery.
    Internal(Status),
}

/// One worker's query answer. Go's `queryResult.workerResponse` carries the
/// full completed request; only the result and worker version are needed here.
pub struct WorkerAnswer {
    pub result: p::WorkflowQueryResult,
    pub worker_version_info: Option<p::WorkerVersionInfo>,
}

impl WorkerAnswer {
    /// A result without version info, as older callers produce.
    pub fn new(result: p::WorkflowQueryResult) -> Self {
        Self {
            result,
            worker_version_info: None,
        }
    }
}

/// Per-owner query rendezvous: Go `matcher.queryTaskC` (the pending offers)
/// plus the matching engine's `lockableQueryTaskMap` (the result waiters).
#[derive(Default)]
pub struct QueryChannel {
    pending: Mutex<VecDeque<PendingQuery>>,
    waiting: Mutex<HashMap<String, (u64, oneshot::Sender<QueryOutcome>)>>,
    /// Engine-wide task-ID index (Go `lockableQueryTaskMap`), shared by every
    /// owner of one registry. `None` for standalone channels.
    index: Option<Arc<QueryIndex>>,
    notify: Notify,
    /// Decision polls parked in `next`, used by `try_offer` to mirror Go's
    /// non-blocking `queryTaskC` send (succeeds only with a waiting poller).
    waiters: std::sync::atomic::AtomicUsize,
}
/// Go `lockableQueryTaskMap`: task ID -> the channel holding its waiter. The
/// index owns an `Arc` of the channel, so a result can still be delivered after
/// the owning task list was unloaded or replaced. A later `put` of the same ID
/// wins; each entry carries a generation so an older registration's cleanup
/// cannot remove the newer one.
#[derive(Default)]
pub struct QueryIndex {
    entries: Mutex<HashMap<String, (u64, Arc<QueryChannel>)>>,
    next_generation: AtomicU64,
}

impl QueryIndex {
    /// Go `deliverQueryResult`: O(1) lookup by task ID alone.
    pub fn deliver(&self, task_id: &str, outcome: QueryOutcome) -> Result<(), Status> {
        let not_found = || Status::not_found("query task not found, or already expired");
        let entry = self
            .entries
            .lock()
            .expect("matching query index lock")
            .get(task_id)
            .cloned();
        let Some((generation, channel)) = entry else {
            return Err(not_found());
        };
        match channel.take_waiter(task_id, Some(generation)) {
            Some(sender) => {
                // The waiter may have timed out; dropping the send is Go's
                // buffered-channel behaviour after the deferred delete.
                let _ = sender.send(outcome);
                Ok(())
            }
            None => {
                // Stale entry (waiter already gone): drop it and report missing.
                self.remove(task_id, generation);
                Err(not_found())
            }
        }
    }

    fn remove(&self, task_id: &str, generation: u64) {
        let mut entries = self.entries.lock().expect("matching query index lock");
        if entries.get(task_id).is_some_and(|(g, _)| *g == generation) {
            entries.remove(task_id);
        }
    }
}

/// Mirror Go's deferred query-map deletion on completion, timeout, and RPC
/// cancellation (not only on the explicit timeout branch).
pub struct Registration {
    channel: std::sync::Arc<QueryChannel>,
    task_id: String,
    generation: u64,
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.channel
            .withdraw_generation(&self.task_id, self.generation);
    }
}

impl QueryChannel {
    pub fn new() -> Self {
        Self::default()
    }

    /// A channel whose waiters are also indexed engine-wide.
    pub fn with_index(index: Arc<QueryIndex>) -> Self {
        Self {
            index: Some(index),
            ..Self::default()
        }
    }

    /// Install the result waiter and return a guard that removes exactly this
    /// registration (waiter, index entry, pending offer) when dropped.
    pub fn register_guarded(
        self: &Arc<Self>,
        task_id: &str,
    ) -> (oneshot::Receiver<QueryOutcome>, Registration) {
        let (generation, receiver) = self.install_waiter(task_id);
        let registration = Registration {
            channel: self.clone(),
            task_id: task_id.into(),
            generation,
        };
        (receiver, registration)
    }

    fn install_waiter(self: &Arc<Self>, task_id: &str) -> (u64, oneshot::Receiver<QueryOutcome>) {
        let (sender, receiver) = oneshot::channel();
        let generation = match &self.index {
            Some(index) => index.next_generation.fetch_add(1, Ordering::SeqCst) + 1,
            None => 0,
        };
        self.waiting
            .lock()
            .expect("matching query lock")
            .insert(task_id.into(), (generation, sender));
        if let Some(index) = &self.index {
            index
                .entries
                .lock()
                .expect("matching query index lock")
                .insert(task_id.into(), (generation, self.clone()));
        }
        (generation, receiver)
    }

    /// Remove the waiter for `task_id`; with `generation` only if it matches.
    fn take_waiter(
        &self,
        task_id: &str,
        generation: Option<u64>,
    ) -> Option<oneshot::Sender<QueryOutcome>> {
        let mut waiting = self.waiting.lock().expect("matching query lock");
        if let Some(wanted) = generation {
            if waiting.get(task_id).is_none_or(|(g, _)| *g != wanted) {
                return None;
            }
        }
        let (taken, sender) = waiting.remove(task_id)?;
        drop(waiting);
        if let Some(index) = &self.index {
            index.remove(task_id, taken);
        }
        Some(sender)
    }

    fn withdraw_generation(&self, task_id: &str, generation: u64) {
        let replaced = {
            let mut waiting = self.waiting.lock().expect("matching query lock");
            match waiting.get(task_id) {
                Some((g, _)) if *g != generation => true,
                Some(_) => {
                    waiting.remove(task_id);
                    false
                }
                None => false,
            }
        };
        if replaced {
            // A newer registration owns this ID; leave its waiter and offer.
            return;
        }
        if let Some(index) = &self.index {
            index.remove(task_id, generation);
        }
        self.pending
            .lock()
            .expect("matching query lock")
            .retain(|query| query.task_id != task_id);
    }

    /// Go `QueryWorkflow`: install the result waiter, then offer the task. The
    /// waiter is registered first so a poller cannot pick up the offer before
    /// the result channel exists.
    pub fn register(self: &Arc<Self>, query: PendingQuery) -> oneshot::Receiver<QueryOutcome> {
        let receiver = self.register_waiter(&query.task_id);
        self.offer(query);
        receiver
    }

    pub fn register_waiter(self: &Arc<Self>, task_id: &str) -> oneshot::Receiver<QueryOutcome> {
        self.install_waiter(task_id).1
    }

    pub fn offer(&self, query: PendingQuery) {
        self.pending
            .lock()
            .expect("matching query lock")
            .push_back(query);
        self.notify.notify_waiters();
    }

    /// Admit at most one offer per parked poller, mirroring the unbuffered
    /// query channel. Installing a result waiter does not itself offer a task.
    pub fn try_offer(&self, query: PendingQuery) -> bool {
        let mut pending = self.pending.lock().expect("matching query lock");
        if pending.len() >= self.waiters.load(std::sync::atomic::Ordering::SeqCst) {
            return false;
        }
        pending.push_back(query);
        self.notify.notify_waiters();
        true
    }

    pub fn has_waiters(&self) -> bool {
        self.waiters.load(std::sync::atomic::Ordering::SeqCst) > 0
    }

    /// Go `matcher.Poll` on `queryTaskC`: wait for a query offer.
    pub async fn next(&self) -> PendingQuery {
        struct Waiter<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for Waiter<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        self.waiters
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _waiter = Waiter(&self.waiters);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(query) = self.take() {
                return query;
            }
            notified.await;
        }
    }

    fn take(&self) -> Option<PendingQuery> {
        self.pending
            .lock()
            .expect("matching query lock")
            .pop_front()
    }

    /// Go `deliverQueryResult`. A missing waiter means the caller already gave
    /// up or the task expired.
    pub fn complete(&self, task_id: &str, outcome: QueryOutcome) -> Result<(), Status> {
        match self.take_waiter(task_id, None) {
            Some(sender) => {
                let _ = sender.send(outcome);
                Ok(())
            }
            None => Err(Status::not_found(
                "query task not found, or already expired",
            )),
        }
    }

    /// Withdraw an offer whose caller gave up (Go's deferred
    /// `lockableQueryTaskMap.delete` plus matcher cleanup).
    pub fn withdraw(&self, task_id: &str) {
        let _ = self.take_waiter(task_id, None);
        self.pending
            .lock()
            .expect("matching query lock")
            .retain(|query| query.task_id != task_id);
    }
}

/// Go `versionChecker.SupportsStickyQuery` for the query poll response: only
/// the known SDK implementations, or an unknown-impl client, at feature
/// version >= 1.0.0.
fn supports_sticky_query(client_impl: &str, feature_version: &str) -> bool {
    if !matches!(client_impl, "uber-go" | "uber-java" | "") {
        return false;
    }
    released_feature_at_least(feature_version, STICKY_QUERY_MIN_MAJOR, 0)
}

/// Go `createPollForDecisionTaskResponse` for `task.IsQuery()`: the token is a
/// query token, the response carries the query, and History's mutable state
/// supplies the metadata the frontend needs to load history.
pub fn query_response(
    query: PendingQuery,
    state: &h::GetMutableStateResponse,
) -> m::PollForDecisionTaskResponse {
    let execution = query.request.workflow_execution.clone().unwrap_or_default();
    let token = cadence_api::tasktoken::QueryTaskToken {
        domain_id: query.domain_id.clone(),
        workflow_id: execution.workflow_id.clone(),
        run_id: execution.run_id.clone(),
        task_list: query.task_list.clone(),
        task_id: query.task_id.clone(),
    };
    let task_token = cadence_api::tasktoken::JsonTaskTokenSerializer::serialize_query_token(&token)
        .unwrap_or_default();
    let sticky_task_list_configured = state
        .sticky_task_list
        .as_ref()
        .is_some_and(|list| !list.name.is_empty());
    let sticky_execution_enabled = sticky_task_list_configured
        && supports_sticky_query(&state.client_impl, &state.client_feature_version);
    m::PollForDecisionTaskResponse {
        task_token,
        workflow_execution: Some(execution),
        workflow_type: state.workflow_type.clone(),
        previous_started_event_id: state
            .previous_started_event_id
            .filter(|event| *event != EMPTY_EVENT_ID),
        next_event_id: state.next_event_id,
        sticky_execution_enabled,
        query: query.request.query.clone(),
        workflow_execution_task_list: state.task_list.clone(),
        branch_token: state.current_branch_token.clone(),
        total_history_bytes: state.history_size,
        ..Default::default()
    }
}

/// Go `versionChecker.SupportsConsistentQuery`: only the known SDK
/// implementations at feature version >= 1.5.0. An older client that omits
/// `clientImpl` is treated as supported, matching Go's empty `case
/// consistentQuery:` fallthrough; a known impl with no feature version, an
/// unknown impl, or the CLI is not.
fn supports_consistent_query(client_impl: &str, feature_version: &str) -> bool {
    // Go `featureSupported`: the empty-impl `consistentQuery` case is an empty
    // switch arm, so it falls through to the impl/version lookup and fails.
    if !matches!(client_impl, "uber-go" | "uber-java") {
        return false;
    }
    released_feature_at_least(feature_version, 1, 5)
}

/// Hashicorp go-version release constraints reject every prerelease, accept
/// a leading v, pad omitted numeric components, and validate the entire value.
/// Both query constraints have patch zero, so major/minor determine the result
/// once all numeric components and build metadata have been validated.
fn released_feature_at_least(value: &str, major: u64, minor: u64) -> bool {
    let value = value.strip_prefix('v').unwrap_or(value);
    let (numbers, metadata) = value
        .split_once('+')
        .map_or((value, None), |(numbers, metadata)| {
            (numbers, Some(metadata))
        });
    if metadata.is_some_and(|metadata| {
        metadata.split('.').any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'~'))
        })
    }) {
        return false;
    }
    let segments = numbers
        .split('.')
        .map(|part| {
            if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            part.parse::<i64>().ok()
        })
        .collect::<Option<Vec<_>>>();
    segments.is_some_and(|segments| {
        (
            segments[0] as u64,
            segments.get(1).copied().unwrap_or_default() as u64,
        ) >= (major, minor)
    })
}

/// Go `waitForQueryResult`: an answered query returns the payload, a failed
/// query is reported with the worker's message, and any other completed type
/// is an internal error. A strong-consistency query additionally requires the
/// worker to support consistent query, failing with
/// `ClientVersionNotSupportedError` otherwise.
pub fn query_outcome(
    outcome: QueryOutcome,
    strong: bool,
) -> Result<m::QueryWorkflowResponse, Status> {
    let answer = match outcome {
        QueryOutcome::Internal(status) => return Err(status),
        QueryOutcome::Worker(answer) => answer,
    };
    if strong {
        let version = answer.worker_version_info.as_ref();
        let impl_name = version.map(|v| v.r#impl.as_str()).unwrap_or_default();
        let feature = version
            .map(|v| v.feature_version.as_str())
            .unwrap_or_default();
        if !supports_consistent_query(impl_name, feature) {
            return Err(crate::grpc::detailed_status(
                tonic::Code::FailedPrecondition,
                "Client version not supported",
                "ClientVersionNotSupportedError",
                p::ClientVersionNotSupportedError {
                    feature_version: feature.to_string(),
                    client_impl: impl_name.to_string(),
                    supported_versions: ">=1.5.0".to_string(),
                },
            ));
        }
    }
    let result = answer.result;
    if result.result_type == p::QueryResultType::Answered as i32 {
        return Ok(m::QueryWorkflowResponse {
            query_result: result.answer,
            query_rejected: None,
            partition_config: None,
        });
    }
    if result.result_type == p::QueryResultType::Failed as i32 {
        // Go `QueryFailedError{Message: worker error}`: typed detail, message verbatim.
        return Err(crate::grpc::detailed_status(
            tonic::Code::InvalidArgument,
            result.error_message,
            "QueryFailedError",
            p::QueryFailedError {},
        ));
    }
    // Go `waitForQueryResult` default case: an unknown completed type.
    Err(Status::internal("unknown query completed type"))
}

/// Go `forwarder.handleErr`: a parent `ServiceBusyError` becomes
/// `ErrForwarderSlowDown`, so `OfferQuery` keeps waiting for a local poller.
pub fn query_forward_slow_down(status: &Status) -> bool {
    status.code() == tonic::Code::ResourceExhausted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_query_forward_slow_down_only_for_service_busy() {
        let busy = crate::grpc::detailed_status(
            tonic::Code::ResourceExhausted,
            "busy",
            "ServiceBusyError",
            p::ServiceBusyError {
                reason: String::new(),
            },
        );
        assert!(query_forward_slow_down(&busy));
        assert!(!query_forward_slow_down(&Status::internal("boom")));
        assert!(!query_forward_slow_down(&Status::deadline_exceeded("late")));
    }

    #[test]
    fn matching_query_registration_drop_withdraws_cancelled_offer() {
        let channel = std::sync::Arc::new(QueryChannel::new());
        let (receiver, registration) = channel.register_guarded("cancelled");
        channel.offer(query("cancelled"));
        drop(registration);
        assert!(channel.take().is_none());
        assert!(channel
            .complete(
                "cancelled",
                QueryOutcome::Internal(Status::cancelled("cancelled"))
            )
            .is_err());
        assert!(receiver.blocking_recv().is_err());
    }

    fn query(task_id: &str) -> PendingQuery {
        PendingQuery {
            task_id: task_id.into(),
            domain_id: "domain-uuid".into(),
            task_list: "queue".into(),
            request: p::QueryWorkflowRequest {
                domain: "domain".into(),
                workflow_execution: Some(p::WorkflowExecution {
                    workflow_id: "workflow".into(),
                    run_id: "run".into(),
                }),
                query: Some(p::WorkflowQuery {
                    query_type: "ping".into(),
                    query_args: None,
                }),
                ..Default::default()
            },
        }
    }

    fn mutable_state() -> h::GetMutableStateResponse {
        h::GetMutableStateResponse {
            workflow_execution: Some(p::WorkflowExecution {
                workflow_id: "workflow".into(),
                run_id: "run".into(),
            }),
            workflow_type: Some(p::WorkflowType {
                name: "service-compat".into(),
            }),
            next_event_id: 12,
            previous_started_event_id: Some(5),
            task_list: Some(p::TaskList {
                name: "queue".into(),
                ..Default::default()
            }),
            client_impl: "uber-go".into(),
            client_feature_version: "1.0.0".into(),
            current_branch_token: vec![7, 8],
            history_size: 1234,
            ..Default::default()
        }
    }

    #[test]
    fn query_feature_versions_validate_full_release_syntax() {
        for value in ["1", "v1.5", "01.05.0", "1.5.0+build.42", "2.0.0.1"] {
            assert!(supports_sticky_query("uber-go", value), "{value}");
        }
        for value in [
            "1.5.0-alpha",
            "2.0.0-rc1",
            "1.invalid",
            "1.5.bad",
            "1.5.0+",
            "1.5.0+bad..build",
            "1.5.0+build!",
            "1.5.9223372036854775808",
        ] {
            assert!(!supports_sticky_query("uber-go", value), "{value}");
            assert!(!supports_consistent_query("uber-go", value), "{value}");
        }
        assert!(supports_consistent_query("uber-go", "v1.5"));
        assert!(!supports_consistent_query("uber-go", "1"));
    }

    #[tokio::test]
    async fn cancelled_query_poll_removes_waiter_and_cannot_accept_an_offer() {
        let channel = std::sync::Arc::new(QueryChannel::new());
        let mut next = Box::pin(channel.next());
        assert!(matches!(
            futures_poll(next.as_mut()),
            std::task::Poll::Pending
        ));
        assert!(channel.has_waiters());
        drop(next);
        assert!(!channel.has_waiters());
        let _receiver = channel.register_waiter("task");
        assert!(!channel.try_offer(query("task")));
        assert!(channel.take().is_none());
    }

    fn futures_poll<F: std::future::Future>(
        future: std::pin::Pin<&mut F>,
    ) -> std::task::Poll<F::Output> {
        let waker = std::task::Waker::noop();
        future.poll(&mut std::task::Context::from_waker(waker))
    }

    #[tokio::test]
    async fn registered_query_is_offered_exactly_once_to_a_parked_poller() {
        let channel = std::sync::Arc::new(QueryChannel::new());
        let _receiver = channel.register_waiter("task");
        assert!(channel.take().is_none());
        let mut next = Box::pin(channel.next());
        assert!(matches!(
            futures_poll(next.as_mut()),
            std::task::Poll::Pending
        ));
        assert!(channel.try_offer(query("task")));
        assert!(
            !channel.try_offer(query("second")),
            "one parked poll cannot accept two queries"
        );
        assert_eq!(next.await.task_id, "task");
        assert!(channel.take().is_none());
        assert!(!channel.has_waiters());
    }

    #[tokio::test]
    async fn offer_is_delivered_once_and_completed_by_task_id() {
        let channel = std::sync::Arc::new(QueryChannel::new());
        let receiver = channel.register(query("task-1"));
        let offered = channel.next().await;
        assert_eq!(offered.task_id, "task-1");
        // The query response carries a decodable query token, the query, and
        // the mutable state the frontend uses to load history.
        let response = query_response(offered, &mutable_state());
        let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
            &response.task_token,
        )
        .unwrap();
        assert_eq!(token.task_id, "task-1");
        assert_eq!(token.workflow_id, "workflow");
        assert_eq!(token.task_list, "queue");
        assert_eq!(response.next_event_id, 12);
        assert_eq!(response.previous_started_event_id, Some(5));
        assert_eq!(response.total_history_bytes, 1234);
        assert_eq!(
            response.query.as_ref().map(|q| q.query_type.clone()),
            Some("ping".to_string())
        );
        channel
            .complete(
                "task-1",
                QueryOutcome::Worker(WorkerAnswer::new(p::WorkflowQueryResult {
                    result_type: p::QueryResultType::Answered as i32,
                    answer: Some(p::Payload {
                        data: b"answer".to_vec(),
                    }),
                    error_message: String::new(),
                })),
            )
            .unwrap();
        let response = query_outcome(receiver.await.unwrap(), false).unwrap();
        assert_eq!(
            response.query_result.map(|payload| payload.data),
            Some(b"answer".to_vec())
        );
    }

    #[test]
    fn sticky_query_requires_a_configured_list_and_a_supported_client() {
        assert!(supports_sticky_query("uber-go", "1.0.0"));
        assert!(supports_sticky_query("", "1.0.0"));
        assert!(!supports_sticky_query("", ""));
        assert!(!supports_sticky_query("other-sdk", "9.9.9"));
    }

    #[tokio::test]
    async fn internal_error_fails_the_waiting_query() {
        let channel = std::sync::Arc::new(QueryChannel::new());
        let receiver = channel.register(query("task-3"));
        let _ = channel.next().await;
        channel
            .complete(
                "task-3",
                QueryOutcome::Internal(Status::internal("history down")),
            )
            .unwrap();
        let error = query_outcome(receiver.await.unwrap(), false).unwrap_err();
        assert!(error.message().contains("history down"));
    }

    #[tokio::test]
    async fn completing_an_unknown_task_reports_not_found() {
        let channel = std::sync::Arc::new(QueryChannel::new());
        assert!(channel
            .complete(
                "missing",
                QueryOutcome::Worker(WorkerAnswer::new(p::WorkflowQueryResult::default()))
            )
            .is_err());
    }

    #[tokio::test]
    async fn withdraw_removes_an_abandoned_offer() {
        let channel = std::sync::Arc::new(QueryChannel::new());
        let _receiver = channel.register(query("task-2"));
        channel.withdraw("task-2");
        // No offer remains, and a late completion is rejected.
        assert!(channel
            .complete(
                "task-2",
                QueryOutcome::Worker(WorkerAnswer::new(p::WorkflowQueryResult::default()))
            )
            .is_err());
    }

    #[test]
    fn failed_query_reports_the_worker_message() {
        let error = query_outcome(
            QueryOutcome::Worker(WorkerAnswer::new(p::WorkflowQueryResult {
                result_type: p::QueryResultType::Failed as i32,
                answer: None,
                error_message: "boom".into(),
            })),
            false,
        )
        .unwrap_err();
        assert!(error.message().contains("boom"));
    }

    #[test]
    fn strong_consistency_requires_a_supported_worker() {
        assert!(supports_consistent_query("uber-go", "1.5.0"));
        assert!(supports_consistent_query("uber-java", "1.9.3"));
        // Go rejects a client that omits clientImpl (empty switch arm falls through).
        assert!(!supports_consistent_query("", ""));
        assert!(!supports_consistent_query("uber-go", "1.4.9"));
        assert!(!supports_consistent_query("uber-java", "1.0.0"));
        assert!(!supports_consistent_query("cli", "9.9.9"));
        assert!(!supports_consistent_query("uber-go", ""));
    }

    #[test]
    fn strong_query_rejects_a_worker_without_consistent_query() {
        let answer = WorkerAnswer {
            result: p::WorkflowQueryResult {
                result_type: p::QueryResultType::Answered as i32,
                answer: Some(p::Payload {
                    data: b"answer".to_vec(),
                }),
                error_message: String::new(),
            },
            worker_version_info: Some(p::WorkerVersionInfo {
                r#impl: "uber-go".into(),
                feature_version: "1.4.0".into(),
            }),
        };
        let error = query_outcome(QueryOutcome::Worker(answer), true).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        // The same answer is accepted for an eventual query.
        let answer = WorkerAnswer {
            result: p::WorkflowQueryResult {
                result_type: p::QueryResultType::Answered as i32,
                answer: Some(p::Payload {
                    data: b"answer".to_vec(),
                }),
                error_message: String::new(),
            },
            worker_version_info: Some(p::WorkerVersionInfo {
                r#impl: "uber-go".into(),
                feature_version: "1.4.0".into(),
            }),
        };
        assert!(query_outcome(QueryOutcome::Worker(answer), false).is_ok());
    }
    #[tokio::test]
    async fn sync_match_queued_query_is_returned_to_next() {
        // Go Test_poll_QueryTask: a query already on the query channel is the poll result.
        let channel = std::sync::Arc::new(QueryChannel::new());
        channel.offer(query("query-1"));
        let received = tokio::time::timeout(std::time::Duration::from_millis(100), channel.next())
            .await
            .expect("queued query is returned without waiting");
        assert_eq!(received.task_id, "query-1");
    }
    #[tokio::test]
    async fn sync_match_query_hands_off_to_parked_poller() {
        // Go TestQueryLocalSyncMatch: a parked decision poller takes the query, the
        // answer reaches the waiting QueryWorkflow, and no forward is involved.
        let channel = std::sync::Arc::new(QueryChannel::new());
        let poller = channel.clone();
        let poll = tokio::spawn(async move { poller.next().await });
        tokio::task::yield_now().await;
        let receiver = channel.register_waiter("query-1");
        assert!(channel.try_offer(query("query-1")));
        let received = poll.await.unwrap();
        assert_eq!(received.task_id, "query-1");
        assert!(channel
            .complete(
                "query-1",
                QueryOutcome::Worker(WorkerAnswer::new(p::WorkflowQueryResult::default()))
            )
            .is_ok());
        assert!(receiver.await.is_ok());
    }
    #[tokio::test]
    async fn sync_match_non_blocking_query_poll_returns_queued_query_once() {
        // Go Test_pollNonBlocking_QueryTask: a queued query is returned with no
        // wait, and once taken the query channel is empty again.
        let channel = std::sync::Arc::new(QueryChannel::new());
        channel.offer(query("query-1"));
        let received = tokio::time::timeout(std::time::Duration::from_millis(100), channel.next())
            .await
            .expect("queued query is returned without waiting");
        assert_eq!(received.task_id, "query-1");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), channel.next())
                .await
                .is_err()
        );
    }

    fn empty_answer() -> QueryOutcome {
        QueryOutcome::Worker(WorkerAnswer::new(p::WorkflowQueryResult::default()))
    }

    #[test]
    fn matching_query_index_delivers_after_owner_channel_detached() {
        let index = std::sync::Arc::new(QueryIndex::default());
        let channel = std::sync::Arc::new(QueryChannel::with_index(index.clone()));
        let (mut receiver, registration) = channel.register_guarded("task-a");
        drop(channel); // the owner lost its handle; the index keeps the channel
        index.deliver("task-a", empty_answer()).unwrap();
        assert!(receiver.try_recv().is_ok());
        drop(registration);
        let error = index.deliver("task-a", empty_answer()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[test]
    fn matching_query_older_registration_drop_keeps_newer_duplicate() {
        let index = std::sync::Arc::new(QueryIndex::default());
        let channel = std::sync::Arc::new(QueryChannel::with_index(index.clone()));
        let (_old_receiver, old) = channel.register_guarded("dup");
        let (mut new_receiver, _new) = channel.register_guarded("dup");
        drop(old);
        index.deliver("dup", empty_answer()).unwrap();
        assert!(new_receiver.try_recv().is_ok());
    }
}
