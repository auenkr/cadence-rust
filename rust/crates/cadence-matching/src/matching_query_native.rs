//! Byte-preserving native twin of `matching_query.rs`: the same query-task
//! rendezvous (Go `matcher.queryTaskC` + `lockableQueryTaskMap`) over native
//! protobuf types and `GoString` identifiers. Domain IDs, task-list names,
//! task IDs, workflow IDs and run IDs are arbitrary Go bytes and are never
//! converted to `String` or the UTF-8 public protobuf model.
//!
//! Integration boundary (not wired, no production claim): Go's
//! `QueryFailedError.Message` and `ClientVersionNotSupportedError` fields are
//! arbitrary bytes that `tonic::Status` cannot retain. `query_outcome`
//! therefore returns [`NativeQueryError`], keeping them raw so the native
//! error transport can encode them; it must never be flattened lossily.
use cadence_api::tasktoken::{JsonTaskTokenSerializer, QueryTaskTokenBytes};
use cadence_common::go_string::GoString;
use cadence_proto::{
    api as p,
    native::{api as na, history as nh, matching as nm},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{oneshot, Notify};
use tonic::Status;

/// Go `EmptyEventID`: `previous_started_event_id` is omitted while equal.
const EMPTY_EVENT_ID: i64 = -23;
/// Go `GoWorkerStickyQueryVersion` / `StickyQueryUnknownImplConstraints`.
const STICKY_QUERY_MIN_MAJOR: u64 = 1;

pub use crate::matching_query::QueryShape;

/// A query offered by `QueryWorkflow` and awaiting a decision poller.
#[derive(Clone)]
pub struct PendingQuery {
    pub task_id: GoString,
    pub domain_id: GoString,
    pub task_list: GoString,
    pub request: na::QueryWorkflowRequest,
    pub shape: QueryShape,
}

/// What a waiting `QueryWorkflow` call receives.
pub enum QueryOutcome {
    Worker(WorkerAnswer),
    /// Go `queryResult{internalError}`: polling failed before delivery.
    Internal(Status),
}

/// One worker's query answer.
pub struct WorkerAnswer {
    pub result: na::WorkflowQueryResult,
    pub worker_version_info: Option<na::WorkerVersionInfo>,
}

impl WorkerAnswer {
    pub fn new(result: na::WorkflowQueryResult) -> Self {
        Self {
            result,
            worker_version_info: None,
        }
    }
}

/// Query failures whose payload is arbitrary Go bytes. `Status` carries only
/// errors that already existed as statuses (internal polling failures and the
/// unknown-completed-type internal error).
#[derive(Debug)]
pub enum NativeQueryError {
    /// Go `QueryFailedError{Message}` with the worker's raw error message.
    QueryFailed {
        message: GoString,
    },
    /// Go `ClientVersionNotSupportedError` with raw worker-supplied strings.
    ClientVersionNotSupported {
        feature_version: GoString,
        client_impl: GoString,
        supported_versions: GoString,
    },
    Status(Status),
}

impl NativeQueryError {
    /// Requires RawStatusService around the native unary service. The outer
    /// grpc-message uses Go's rune replacement; typed detail fields retain bytes.
    pub fn into_status(self) -> Status {
        use crate::matching_native_status::raw_detailed_status;
        match self {
            Self::Status(status) => status,
            Self::QueryFailed { message } => raw_detailed_status(
                tonic::Code::InvalidArgument, &message, "QueryFailedError", Vec::new(),
            ),
            Self::ClientVersionNotSupported {
                feature_version, client_impl, supported_versions,
            } => {
                let mut detail = Vec::new();
                for (tag, value) in [
                    (1, feature_version), (2, client_impl), (3, supported_versions),
                ] {
                    if !value.is_empty() {
                        cadence_proto::native::wire::encode_string(tag, &value, &mut detail);
                    }
                }
                raw_detailed_status(
                    tonic::Code::FailedPrecondition,
                    &GoString::from("Client version not supported"),
                    "ClientVersionNotSupportedError", detail,
                )
            }
        }
    }
}

type Waiting = HashMap<GoString, (u64, oneshot::Sender<QueryOutcome>)>;

/// Per-owner query rendezvous.
#[derive(Default)]
pub struct QueryChannel {
    pending: Mutex<VecDeque<PendingQuery>>,
    waiting: Mutex<Waiting>,
    index: Option<Arc<QueryIndex>>,
    notify: Notify,
    waiters: AtomicUsize,
}

/// Go `lockableQueryTaskMap`: task ID -> the channel holding its waiter, with
/// generations so an older registration's cleanup cannot remove a newer one.
#[derive(Default)]
pub struct QueryIndex {
    entries: Mutex<HashMap<GoString, (u64, Arc<QueryChannel>)>>,
    next_generation: AtomicU64,
}

fn not_found() -> Status {
    Status::not_found("query task not found, or already expired")
}

impl QueryIndex {
    /// Go `deliverQueryResult`: O(1) lookup by task ID alone.
    pub fn deliver(&self, task_id: &GoString, outcome: QueryOutcome) -> Result<(), Status> {
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
                let _ = sender.send(outcome);
                Ok(())
            }
            None => {
                self.remove(task_id, generation);
                Err(not_found())
            }
        }
    }

    fn remove(&self, task_id: &GoString, generation: u64) {
        let mut entries = self.entries.lock().expect("matching query index lock");
        if entries.get(task_id).is_some_and(|(g, _)| *g == generation) {
            entries.remove(task_id);
        }
    }
}

/// Removes exactly this registration (waiter, index entry, pending offer)
/// on completion, timeout or RPC cancellation.
pub struct Registration {
    channel: Arc<QueryChannel>,
    task_id: GoString,
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

    pub fn with_index(index: Arc<QueryIndex>) -> Self {
        Self {
            index: Some(index),
            ..Self::default()
        }
    }

    pub fn register_guarded(
        self: &Arc<Self>,
        task_id: &GoString,
    ) -> (oneshot::Receiver<QueryOutcome>, Registration) {
        let (generation, receiver) = self.install_waiter(task_id);
        let registration = Registration {
            channel: self.clone(),
            task_id: task_id.clone(),
            generation,
        };
        (receiver, registration)
    }

    fn install_waiter(
        self: &Arc<Self>,
        task_id: &GoString,
    ) -> (u64, oneshot::Receiver<QueryOutcome>) {
        let (sender, receiver) = oneshot::channel();
        let generation = match &self.index {
            Some(index) => index.next_generation.fetch_add(1, Ordering::SeqCst) + 1,
            None => 0,
        };
        self.waiting
            .lock()
            .expect("matching query lock")
            .insert(task_id.clone(), (generation, sender));
        if let Some(index) = &self.index {
            index
                .entries
                .lock()
                .expect("matching query index lock")
                .insert(task_id.clone(), (generation, self.clone()));
        }
        (generation, receiver)
    }

    fn take_waiter(
        &self,
        task_id: &GoString,
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

    fn withdraw_generation(&self, task_id: &GoString, generation: u64) {
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
            return;
        }
        if let Some(index) = &self.index {
            index.remove(task_id, generation);
        }
        self.pending
            .lock()
            .expect("matching query lock")
            .retain(|query| &query.task_id != task_id);
    }

    /// Go `QueryWorkflow`: install the result waiter, then offer the task.
    pub fn register(self: &Arc<Self>, query: PendingQuery) -> oneshot::Receiver<QueryOutcome> {
        let receiver = self.register_waiter(&query.task_id);
        self.offer(query);
        receiver
    }

    pub fn register_waiter(
        self: &Arc<Self>,
        task_id: &GoString,
    ) -> oneshot::Receiver<QueryOutcome> {
        self.install_waiter(task_id).1
    }

    pub fn offer(&self, query: PendingQuery) {
        self.pending
            .lock()
            .expect("matching query lock")
            .push_back(query);
        self.notify.notify_waiters();
    }

    /// Admit at most one offer per parked poller (unbuffered `queryTaskC`).
    pub fn try_offer(&self, query: PendingQuery) -> bool {
        let mut pending = self.pending.lock().expect("matching query lock");
        if pending.len() >= self.waiters.load(Ordering::SeqCst) {
            return false;
        }
        pending.push_back(query);
        self.notify.notify_waiters();
        true
    }

    pub fn has_waiters(&self) -> bool {
        self.waiters.load(Ordering::SeqCst) > 0
    }

    /// Go `matcher.Poll` on `queryTaskC`: wait for a query offer.
    pub async fn next(&self) -> PendingQuery {
        struct Waiter<'a>(&'a AtomicUsize);
        impl Drop for Waiter<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.waiters.fetch_add(1, Ordering::SeqCst);
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

    /// Go `deliverQueryResult`.
    pub fn complete(&self, task_id: &GoString, outcome: QueryOutcome) -> Result<(), Status> {
        match self.take_waiter(task_id, None) {
            Some(sender) => {
                let _ = sender.send(outcome);
                Ok(())
            }
            None => Err(not_found()),
        }
    }

    /// Withdraw an offer whose caller gave up.
    pub fn withdraw(&self, task_id: &GoString) {
        let _ = self.take_waiter(task_id, None);
        self.pending
            .lock()
            .expect("matching query lock")
            .retain(|query| &query.task_id != task_id);
    }
}

fn supports_sticky_query(client_impl: &[u8], feature_version: &[u8]) -> bool {
    if !matches!(client_impl, b"uber-go" | b"uber-java" | b"") {
        return false;
    }
    released_feature_at_least(feature_version, STICKY_QUERY_MIN_MAJOR, 0)
}

/// Go `versionChecker.SupportsConsistentQuery`: known SDKs at >= 1.5.0; an
/// empty impl falls through the empty switch arm and is unsupported.
fn supports_consistent_query(client_impl: &[u8], feature_version: &[u8]) -> bool {
    if !matches!(client_impl, b"uber-go" | b"uber-java") {
        return false;
    }
    released_feature_at_least(feature_version, 1, 5)
}

/// Same hashicorp go-version rules as the legacy checker, directly on bytes:
/// invalid or non-ASCII input is rejected, never aliased through U+FFFD.
fn released_feature_at_least(value: &[u8], major: u64, minor: u64) -> bool {
    let value = value.strip_prefix(b"v").unwrap_or(value);
    let (numbers, metadata) = match value.iter().position(|byte| *byte == b'+') {
        Some(at) => (&value[..at], Some(&value[at + 1..])),
        None => (value, None),
    };
    if metadata.is_some_and(|metadata| {
        metadata.split(|byte| *byte == b'.').any(|part| {
            part.is_empty()
                || !part
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'~'))
        })
    }) {
        return false;
    }
    let segments = numbers
        .split(|byte| *byte == b'.')
        .map(|part| {
            if part.is_empty() || !part.iter().all(u8::is_ascii_digit) {
                return None;
            }
            std::str::from_utf8(part).ok()?.parse::<i64>().ok()
        })
        .collect::<Option<Vec<_>>>();
    segments.is_some_and(|segments| {
        (
            segments[0] as u64,
            segments.get(1).copied().unwrap_or_default() as u64,
        ) >= (major, minor)
    })
}

/// Go `createPollForDecisionTaskResponse` for `task.IsQuery()`.
pub fn query_response(
    query: PendingQuery,
    state: &nh::GetMutableStateResponse,
) -> nm::PollForDecisionTaskResponse {
    let execution = query.request.workflow_execution.clone().unwrap_or_default();
    let token = QueryTaskTokenBytes {
        domain_id: query.domain_id.clone(),
        workflow_id: execution.workflow_id.clone(),
        run_id: execution.run_id.clone(),
        task_list: query.task_list.clone(),
        task_id: query.task_id.clone(),
    };
    let task_token = JsonTaskTokenSerializer::serialize_query_token_bytes(&token);
    let sticky_configured = state
        .sticky_task_list
        .as_ref()
        .is_some_and(|list| !list.name.is_empty());
    let sticky_execution_enabled = sticky_configured
        && supports_sticky_query(
            state.client_impl.as_bytes(),
            state.client_feature_version.as_bytes(),
        );
    nm::PollForDecisionTaskResponse {
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

/// Go `waitForQueryResult`; see `matching_query::query_outcome`. Raw worker
/// failure text and unsupported-client strings stay in [`NativeQueryError`].
pub fn query_outcome(
    outcome: QueryOutcome,
    strong: bool,
) -> Result<nm::QueryWorkflowResponse, NativeQueryError> {
    let answer = match outcome {
        QueryOutcome::Internal(status) => return Err(NativeQueryError::Status(status)),
        QueryOutcome::Worker(answer) => answer,
    };
    if strong {
        let (impl_name, feature) = answer
            .worker_version_info
            .as_ref()
            .map(|v| (v.r#impl.clone(), v.feature_version.clone()))
            .unwrap_or_default();
        if !supports_consistent_query(impl_name.as_bytes(), feature.as_bytes()) {
            return Err(NativeQueryError::ClientVersionNotSupported {
                feature_version: feature,
                client_impl: impl_name,
                supported_versions: ">=1.5.0".into(),
            });
        }
    }
    let result = answer.result;
    if result.result_type == p::QueryResultType::Answered as i32 {
        return Ok(nm::QueryWorkflowResponse {
            query_result: result.answer,
            query_rejected: None,
            partition_config: None,
        });
    }
    if result.result_type == p::QueryResultType::Failed as i32 {
        return Err(NativeQueryError::QueryFailed {
            message: result.error_message,
        });
    }
    Err(NativeQueryError::Status(Status::internal(
        "unknown query completed type",
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(tag: u8) -> GoString {
        GoString::from(vec![0xff, tag, 0x80, 0x00, b'x'])
    }

    fn query(task_id: GoString) -> PendingQuery {
        PendingQuery {
            task_id,
            domain_id: raw(1),
            task_list: raw(2),
            request: na::QueryWorkflowRequest {
                workflow_execution: Some(na::WorkflowExecution {
                    workflow_id: raw(3),
                    run_id: raw(4),
                }),
                query: Some(na::WorkflowQuery {
                    query_type: raw(5),
                    query_args: Some(na::Payload {
                        data: vec![0, 255, 1],
                    }),
                }),
                ..Default::default()
            },
            shape: QueryShape::default(),
        }
    }

    fn state() -> nh::GetMutableStateResponse {
        nh::GetMutableStateResponse {
            next_event_id: 12,
            previous_started_event_id: Some(5),
            client_impl: "uber-go".into(),
            client_feature_version: "1.0.0".into(),
            current_branch_token: vec![7, 8],
            history_size: 1234,
            ..Default::default()
        }
    }

    fn answered(data: &[u8]) -> na::WorkflowQueryResult {
        na::WorkflowQueryResult {
            result_type: p::QueryResultType::Answered as i32,
            answer: Some(na::Payload {
                data: data.to_vec(),
            }),
            error_message: GoString::default(),
        }
    }

    fn version(impl_name: &[u8], feature: &[u8]) -> na::WorkerVersionInfo {
        na::WorkerVersionInfo {
            r#impl: impl_name.to_vec().into(),
            feature_version: feature.to_vec().into(),
        }
    }

    fn poll<F: std::future::Future>(f: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        f.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    #[test]
    fn registration_drop_withdraws_cancelled_offer() {
        let channel = Arc::new(QueryChannel::new());
        let id = raw(9);
        let (receiver, registration) = channel.register_guarded(&id);
        channel.offer(query(id.clone()));
        drop(registration);
        assert!(channel.take().is_none());
        assert!(channel
            .complete(&id, QueryOutcome::Internal(Status::cancelled("x")))
            .is_err());
        assert!(receiver.blocking_recv().is_err());
    }

    #[test]
    fn older_registration_cleanup_keeps_newer_waiter_and_offer() {
        let index = Arc::new(QueryIndex::default());
        let channel = Arc::new(QueryChannel::with_index(index.clone()));
        let id = raw(9);
        let (_old, old_guard) = channel.register_guarded(&id);
        let (new_receiver, _new_guard) = channel.register_guarded(&id);
        channel.offer(query(id.clone()));
        drop(old_guard);
        assert!(channel.take().is_some(), "newer offer survives");
        index
            .deliver(&id, QueryOutcome::Internal(Status::internal("n")))
            .unwrap();
        assert!(new_receiver.blocking_recv().is_ok());
        assert!(index
            .deliver(&id, QueryOutcome::Internal(Status::internal("n")))
            .is_err());
    }

    #[test]
    fn raw_distinct_ids_do_not_alias_in_channel_or_index() {
        let index = Arc::new(QueryIndex::default());
        let a = Arc::new(QueryChannel::with_index(index.clone()));
        let b = Arc::new(QueryChannel::with_index(index.clone()));
        let (ida, idb) = (
            GoString::from(vec![0xff]),
            GoString::from(vec![0xfe]), // both would be U+FFFD if lossy
        );
        let ra = a.register_waiter(&ida);
        let rb = b.register_waiter(&idb);
        index
            .deliver(&idb, QueryOutcome::Internal(Status::internal("b")))
            .unwrap();
        assert!(rb.blocking_recv().is_ok());
        assert!(a
            .complete(&idb, QueryOutcome::Internal(Status::internal("")))
            .is_err());
        index
            .deliver(&ida, QueryOutcome::Internal(Status::internal("a")))
            .unwrap();
        assert!(ra.blocking_recv().is_ok());
    }

    #[tokio::test]
    async fn cancelled_poll_removes_waiter_and_cannot_accept_offer() {
        let channel = Arc::new(QueryChannel::new());
        let mut next = Box::pin(channel.next());
        assert!(matches!(poll(next.as_mut()), std::task::Poll::Pending));
        assert!(channel.has_waiters());
        drop(next);
        assert!(!channel.has_waiters());
        let id = raw(1);
        let _receiver = channel.register_waiter(&id);
        assert!(!channel.try_offer(query(id)));
    }

    #[tokio::test]
    async fn one_parked_poller_accepts_exactly_one_offer() {
        let channel = Arc::new(QueryChannel::new());
        let id = raw(1);
        let _receiver = channel.register_waiter(&id);
        let mut next = Box::pin(channel.next());
        assert!(matches!(poll(next.as_mut()), std::task::Poll::Pending));
        assert!(channel.try_offer(query(id.clone())));
        assert!(!channel.try_offer(query(raw(2))));
        assert_eq!(next.await.task_id, id);
        assert!(!channel.has_waiters());
    }

    #[tokio::test]
    async fn offer_delivers_raw_token_and_answer_bytes() {
        let channel = Arc::new(QueryChannel::new());
        let id = raw(7);
        let receiver = channel.register(query(id.clone()));
        let offered = channel.next().await;
        let response = query_response(offered, &state());
        let expected = JsonTaskTokenSerializer::serialize_query_token_bytes(&QueryTaskTokenBytes {
            domain_id: raw(1),
            workflow_id: raw(3),
            run_id: raw(4),
            task_list: raw(2),
            task_id: id.clone(),
        });
        assert_eq!(response.task_token, expected);
        assert_eq!(response.next_event_id, 12);
        assert_eq!(response.previous_started_event_id, Some(5));
        assert_eq!(response.total_history_bytes, 1234);
        assert_eq!(response.branch_token, vec![7, 8]);
        let q = response.query.unwrap();
        assert_eq!(q.query_type, raw(5));
        assert_eq!(q.query_args.unwrap().data, vec![0, 255, 1]);
        let exec = response.workflow_execution.unwrap();
        assert_eq!((exec.workflow_id, exec.run_id), (raw(3), raw(4)));
        channel
            .complete(
                &id,
                QueryOutcome::Worker(WorkerAnswer::new(answered(b"a\xff"))),
            )
            .unwrap();
        let response = query_outcome(receiver.await.unwrap(), false).unwrap();
        assert_eq!(response.query_result.unwrap().data, b"a\xff".to_vec());
    }

    #[test]
    fn empty_previous_started_event_id_is_omitted() {
        let mut s = state();
        s.previous_started_event_id = Some(EMPTY_EVENT_ID);
        let r = query_response(query(raw(1)), &s);
        assert_eq!(r.previous_started_event_id, None);
    }

    #[test]
    fn sticky_requires_list_and_supported_client() {
        let mut s = state();
        assert!(!query_response(query(raw(1)), &s).sticky_execution_enabled);
        s.sticky_task_list = Some(na::TaskList {
            name: raw(6),
            ..Default::default()
        });
        assert!(query_response(query(raw(1)), &s).sticky_execution_enabled);
        s.client_impl = "other".into();
        assert!(!query_response(query(raw(1)), &s).sticky_execution_enabled);
        assert!(supports_sticky_query(b"", b"1.0.0"));
        assert!(!supports_sticky_query(b"", b""));
    }

    #[test]
    fn feature_versions_validate_on_bytes_without_replacement_aliasing() {
        for ok in [
            &b"1"[..],
            b"v1.5",
            b"01.05.0",
            b"1.5.0+build.42",
            b"2.0.0.1",
        ] {
            assert!(supports_sticky_query(b"uber-go", ok), "{ok:?}");
        }
        for bad in [
            &b"1.5.0-alpha"[..],
            b"1.5.0+",
            b"1.5.0+bad..build",
            b"1.5.0+build!",
            b"1.5.9223372036854775808",
            b"\xff.5.0",
            b"1.5.0+\xef\xbf\xbd",
            b"1.\xff",
        ] {
            assert!(!supports_sticky_query(b"uber-go", bad), "{bad:?}");
            assert!(!supports_consistent_query(b"uber-go", bad), "{bad:?}");
        }
        assert!(supports_consistent_query(b"uber-java", b"v1.5"));
        assert!(!supports_consistent_query(b"uber-go", b"1"));
        assert!(!supports_consistent_query(b"", b"9.9.9"));
        assert!(!supports_consistent_query(b"uber-g\xff", b"9.9.9"));
    }

    #[test]
    fn strong_query_rejects_unsupported_client_with_raw_detail() {
        let mut answer = WorkerAnswer::new(answered(b"x"));
        answer.worker_version_info = Some(version(b"sdk\xff", b"1.\x80"));
        match query_outcome(QueryOutcome::Worker(answer), true) {
            Err(NativeQueryError::ClientVersionNotSupported {
                feature_version,
                client_impl,
                supported_versions,
            }) => {
                assert_eq!(feature_version.as_bytes(), b"1.\x80");
                assert_eq!(client_impl.as_bytes(), b"sdk\xff");
                assert_eq!(supported_versions.as_bytes(), b">=1.5.0");
            }
            other => panic!("unexpected {other:?}"),
        }
        let mut ok = WorkerAnswer::new(answered(b"x"));
        ok.worker_version_info = Some(version(b"uber-go", b"1.5.0"));
        assert!(query_outcome(QueryOutcome::Worker(ok), true).is_ok());
        let missing = WorkerAnswer::new(answered(b"x"));
        assert!(matches!(
            query_outcome(QueryOutcome::Worker(missing), true),
            Err(NativeQueryError::ClientVersionNotSupported { .. })
        ));
        // Eventual queries never check the version.
        assert!(query_outcome(
            QueryOutcome::Worker(WorkerAnswer::new(answered(b"x"))),
            false
        )
        .is_ok());
    }

    #[test]
    fn failed_worker_result_retains_raw_message_bytes() {
        let message = GoString::from(vec![b'b', 0xff, 0xfe, 0x00]);
        let result = na::WorkflowQueryResult {
            result_type: p::QueryResultType::Failed as i32,
            answer: None,
            error_message: message.clone(),
        };
        match query_outcome(QueryOutcome::Worker(WorkerAnswer::new(result)), false) {
            Err(NativeQueryError::QueryFailed { message: got }) => assert_eq!(got, message),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn unknown_type_and_internal_outcomes_keep_statuses() {
        let unknown = na::WorkflowQueryResult::default();
        match query_outcome(QueryOutcome::Worker(WorkerAnswer::new(unknown)), false) {
            Err(NativeQueryError::Status(s)) => assert_eq!(s.code(), tonic::Code::Internal),
            other => panic!("unexpected {other:?}"),
        }
        match query_outcome(
            QueryOutcome::Internal(Status::internal("history down")),
            false,
        ) {
            Err(NativeQueryError::Status(s)) => assert!(s.message().contains("history down")),
            other => panic!("unexpected {other:?}"),
        }
    }
}
