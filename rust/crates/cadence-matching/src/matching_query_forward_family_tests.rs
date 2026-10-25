//! Go `tasklist` query-forward family as RPC-level tests: `forwarder_test.go`
//! (`TestForwardQueryTaskError`, `TestForwardQueryTask`,
//! `TestForwardQueryTaskRateNotEnforced`) and `matcher_test.go`
//! (`TestQueryRemoteSyncMatchError`, `TestOfferQuery_ForwardError`,
//! `TestOfferQuery_ContextExpired`). The forwarded leg is observed through a
//! test-only `MatchingApi` parent with recording, busy and failing modes.
use super::durable_tests::{
    execution, fixture_with_history, membership_with_parent_remote, mutable_state,
    start_fake_history,
};
use super::*;
use m::matching_api_server::MatchingApi;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const CHILD: &str = "/__cadence_sys/queue/1";

#[derive(Clone, Copy)]
enum FamilyParentMode {
    /// Answers "answer" and records every forwarded request.
    Recording,
    /// Answers ServiceBusy, so the child keeps the query for a local poller.
    Busy,
    /// Answers Unavailable, which the child returns to its caller unchanged.
    Failing,
}

/// Test-only parent. Records each `QueryWorkflow` it receives, then answers
/// according to its mode.
#[derive(Clone)]
struct FamilyQueryParent {
    mode: FamilyParentMode,
    forwarded: Arc<Mutex<Vec<m::QueryWorkflowRequest>>>,
    received: Arc<tokio::sync::Notify>,
}

impl FamilyQueryParent {
    fn new(mode: FamilyParentMode) -> Self {
        Self {
            mode,
            forwarded: Default::default(),
            received: Default::default(),
        }
    }

    fn calls(&self) -> Vec<m::QueryWorkflowRequest> {
        self.forwarded.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl MatchingApi for FamilyQueryParent {
    async fn query_workflow(
        &self,
        request: tonic::Request<m::QueryWorkflowRequest>,
    ) -> Result<tonic::Response<m::QueryWorkflowResponse>, tonic::Status> {
        self.forwarded.lock().unwrap().push(request.into_inner());
        self.received.notify_one();
        match self.mode {
            FamilyParentMode::Recording => Ok(tonic::Response::new(m::QueryWorkflowResponse {
                query_result: Some(p::Payload {
                    data: b"answer".to_vec(),
                }),
                ..Default::default()
            })),
            FamilyParentMode::Busy => Err(crate::grpc::detailed_status(
                tonic::Code::ResourceExhausted,
                "parent busy",
                "ServiceBusyError",
                p::ServiceBusyError {
                    reason: String::new(),
                },
            )),
            FamilyParentMode::Failing => Err(tonic::Status::unavailable("parent unavailable")),
        }
    }
}

async fn serve_family_parent(
    parent: FamilyQueryParent,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(m::matching_api_server::MatchingApiServer::new(parent))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (address, server)
}

/// Local child partition `CHILD` whose parent `queue` is a remote host.
async fn child_with_parent(
    child: Matching,
    parent: FamilyQueryParent,
) -> (Matching, tokio::task::JoinHandle<()>) {
    let (address, server) = serve_family_parent(parent).await;
    (
        child.with_membership(membership_with_parent_remote(CHILD, "queue", &address)),
        server,
    )
}

fn child_list(kind: i32) -> p::TaskList {
    p::TaskList {
        name: CHILD.into(),
        kind,
        ..Default::default()
    }
}

fn family_query(
    domain_id: String,
    task_list: p::TaskList,
    timeout: Duration,
) -> Request<m::QueryWorkflowRequest> {
    let mut request = Request::new(m::QueryWorkflowRequest {
        domain_id,
        task_list: Some(task_list),
        request: Some(p::QueryWorkflowRequest {
            domain: "domain".into(),
            workflow_execution: execution(),
            query: Some(p::WorkflowQuery {
                query_type: "ping".into(),
                query_args: None,
            }),
            ..Default::default()
        }),
        forwarded_from: String::new(),
    });
    request.set_timeout(timeout);
    request
}

fn decision_poll(
    domain_id: String,
    identity: &str,
    task_list: p::TaskList,
    timeout: Duration,
) -> Request<m::PollForDecisionTaskRequest> {
    let mut request = Request::new(m::PollForDecisionTaskRequest {
        domain_id,
        poller_id: identity.into(),
        request: Some(p::PollForDecisionTaskRequest {
            task_list: Some(task_list),
            identity: identity.into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    request.set_timeout(timeout);
    request
}

async fn shut_down(matching: &Matching, server: tokio::task::JoinHandle<()>) {
    matching
        .queues
        .as_ref()
        .unwrap()
        .shutdown(matching_now())
        .await
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn query_forward_family_sticky_list_never_forwards_and_waits_for_local_poller() {
    // Go TestForwardQueryTaskError (sticky branch). Go's isFowardingAllowed is
    // false for sticky lists, so the matcher has no forwarder and OfferQuery
    // blocks on the local queue until the context ends. Go's ErrTaskListKind is
    // therefore unreachable from OfferQuery. A recent sticky poll is required,
    // or the engine returns StickyWorkerUnavailable before OfferQuery.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent = FamilyQueryParent::new(FamilyParentMode::Recording);
    let (child, server) = child_with_parent(matching, parent.clone()).await;
    // An empty sticky poll still leaves the sticky list recently polled.
    let _ = child
        .poll_for_decision_task(decision_poll(
            domain.clone(),
            "sticky-worker",
            child_list(2),
            Duration::from_secs(1),
        ))
        .await;
    let started = tokio::time::Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        child.query_workflow(family_query(
            domain,
            child_list(2),
            Duration::from_millis(500),
        )),
    )
    .await
    .expect("sticky query must return at its deadline")
    .expect_err("sticky query without a local poller must fail at the deadline");
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "sticky query returned before its deadline"
    );
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "context deadline exceeded");
    assert!(
        parent.calls().is_empty(),
        "sticky lists must never forward a query"
    );
    shut_down(&child, server).await;
}

#[tokio::test]
async fn query_forward_family_forwards_go_forwarder_fields_to_parent() {
    // Go TestForwardQueryTask: the parent receives TaskList.Name = parent
    // partition, TaskList.Kind = the forwarder's kind, ForwardedFrom = child,
    // and the QueryRequest unchanged.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent = FamilyQueryParent::new(FamilyParentMode::Recording);
    let (child, server) = child_with_parent(matching, parent.clone()).await;
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        child.query_workflow(family_query(
            domain.clone(),
            child_list(1),
            Duration::from_secs(3),
        )),
    )
    .await
    .expect("forwarded query should complete")
    .expect("forwarded query should succeed")
    .into_inner();
    assert_eq!(
        response.query_result.map(|payload| payload.data),
        Some(b"answer".to_vec())
    );
    let calls = parent.calls();
    assert_eq!(calls.len(), 1, "exactly one forward to the parent");
    let forwarded = &calls[0];
    let task_list = forwarded.task_list.as_ref().expect("forwarded task list");
    assert_eq!(task_list.name, "queue");
    assert_eq!(task_list.kind, 1);
    assert_eq!(forwarded.forwarded_from, CHILD);
    assert_eq!(forwarded.domain_id, domain);
    assert_eq!(
        forwarded.request,
        Some(p::QueryWorkflowRequest {
            domain: "domain".into(),
            workflow_execution: execution(),
            query: Some(p::WorkflowQuery {
                query_type: "ping".into(),
                query_args: None,
            }),
            ..Default::default()
        })
    );
    shut_down(&child, server).await;
}

#[tokio::test]
async fn query_forward_family_repeated_forwards_are_not_rate_limited() {
    // Go TestForwardQueryTaskRateNotEnforced: ForwardQueryTask applies no rate
    // limit (ForwardTask does), so three forwards in a row all reach the parent.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent = FamilyQueryParent::new(FamilyParentMode::Recording);
    let (child, server) = child_with_parent(matching, parent.clone()).await;
    for _ in 0..3 {
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            child.query_workflow(family_query(
                domain.clone(),
                child_list(1),
                Duration::from_secs(3),
            )),
        )
        .await
        .expect("forwarded query should complete")
        .expect("forwarded query must not be rate limited")
        .into_inner();
        assert_eq!(
            response.query_result.map(|payload| payload.data),
            Some(b"answer".to_vec())
        );
    }
    assert_eq!(parent.calls().len(), 3);
    shut_down(&child, server).await;
}

#[tokio::test]
async fn query_forward_family_busy_parent_keeps_query_for_local_poller() {
    // Go TestQueryRemoteSyncMatchError: the parent answers ServiceBusy, so the
    // child keeps the query for a local poller instead of failing the caller.
    // Go starts the local poller only after the forward was attempted; the
    // poller here waits for the parent's receipt in the same way.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent = FamilyQueryParent::new(FamilyParentMode::Busy);
    let (child, server) = child_with_parent(matching, parent.clone()).await;
    let received = parent.received.clone();
    let worker = child.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        received.notified().await;
        worker
            .poll_for_decision_task(decision_poll(
                poll_domain,
                "child-worker",
                child_list(1),
                Duration::from_secs(8),
            ))
            .await
    });
    let querier = child.clone();
    let query_domain = domain.clone();
    let query = tokio::spawn(async move {
        querier
            .query_workflow(family_query(
                query_domain,
                child_list(1),
                Duration::from_secs(5),
            ))
            .await
    });
    let delivered = tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("local poller should receive the query after ServiceBusy")
        .unwrap()
        .expect("poll should succeed")
        .into_inner();
    assert_eq!(
        delivered.query.as_ref().map(|q| q.query_type.clone()),
        Some("ping".to_string())
    );
    let token = cadence_api::tasktoken::JsonTaskTokenSerializer::deserialize_query_token(
        &delivered.task_token,
    )
    .expect("poll should carry a query task token");
    child
        .respond_query_task_completed(Request::new(m::RespondQueryTaskCompletedRequest {
            request: Some(p::RespondQueryTaskCompletedRequest {
                task_token: Vec::new(),
                result: Some(p::WorkflowQueryResult {
                    result_type: p::QueryResultType::Answered as i32,
                    answer: Some(p::Payload {
                        data: b"local".to_vec(),
                    }),
                    error_message: String::new(),
                }),
                worker_version_info: None,
            }),
            domain_id: domain,
            task_list: Some(p::TaskList {
                name: token.task_list,
                kind: 1,
                ..Default::default()
            }),
            task_id: token.task_id,
        }))
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("query should complete after the local answer")
        .unwrap()
        .expect("local answer should be returned")
        .into_inner();
    assert_eq!(
        response.query_result.map(|payload| payload.data),
        Some(b"local".to_vec())
    );
    assert_eq!(
        parent.calls().len(),
        1,
        "the busy parent is asked exactly once"
    );
    shut_down(&child, server).await;
}

#[tokio::test]
async fn query_forward_family_parent_error_is_returned_without_local_wait() {
    // Go TestOfferQuery_ForwardError: an error from ForwardQueryTask is returned
    // to the caller, and OfferQuery does not keep waiting for a local poller.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent = FamilyQueryParent::new(FamilyParentMode::Failing);
    let (child, server) = child_with_parent(matching, parent.clone()).await;
    let started = tokio::time::Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        child.query_workflow(family_query(
            domain,
            child_list(1),
            Duration::from_secs(3),
        )),
    )
    .await
    .expect("parent error should return before the RPC deadline")
    .expect_err("parent error must be returned to the caller");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "caller must not wait for the 3s RPC deadline"
    );
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "parent unavailable");
    assert_eq!(parent.calls().len(), 1, "exactly one forward to the parent");
    shut_down(&child, server).await;
}

#[tokio::test]
async fn query_forward_family_expired_deadline_never_forwards() {
    // Go TestOfferQuery_ContextExpired: a context that is already done ends
    // OfferQuery before any forward. A zero grpc-timeout is a deadline that has
    // already passed when the forward decision is made.
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent = FamilyQueryParent::new(FamilyParentMode::Recording);
    let (child, server) = child_with_parent(matching, parent.clone()).await;
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        child.query_workflow(family_query(domain, child_list(1), Duration::ZERO)),
    )
    .await
    .expect("expired query must return immediately")
    .expect_err("expired query must fail");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "context deadline exceeded");
    assert!(
        parent.calls().is_empty(),
        "an expired query must not reach the parent"
    );
    shut_down(&child, server).await;
}

/// Go nil-safe getters: an absent TaskList / QueryRequest / Query dispatches the
/// query, and the poller's response builder then fails with a plain nil
/// dereference (Unknown). The query is consumed but never completed, so the
/// caller keeps waiting until its own deadline.
async fn absent_query_case(
    shaped: impl FnOnce(&mut m::QueryWorkflowRequest),
    poll_list: p::TaskList,
) {
    let peer = start_fake_history(mutable_state()).await;
    let (_dir, _store, matching, key) = fixture_with_history(&peer);
    let domain = key.domain_id.to_string();
    let parent = FamilyQueryParent::new(FamilyParentMode::Busy);
    let (child, server) = child_with_parent(matching, parent.clone()).await;
    let mut query = family_query(
        domain.clone(),
        poll_list.clone(),
        Duration::from_millis(1500),
    );
    shaped(query.get_mut());
    let started = tokio::time::Instant::now();
    let caller = async {
        let result = child.query_workflow(query).await;
        (result, started.elapsed())
    };
    let poller = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let poll_error = child
            .poll_for_decision_task(decision_poll(
                domain,
                "poller",
                poll_list,
                Duration::from_secs(1),
            ))
            .await
            .expect_err("poll of an absent-field query must fail");
        assert_eq!(poll_error.code(), tonic::Code::Unknown);
        assert_eq!(
            poll_error.message(),
            "runtime error: invalid memory address or nil pointer dereference"
        );
    };
    let ((result, elapsed), ()) = tokio::join!(caller, poller);
    let error = result.expect_err("query ends only at its deadline");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "context deadline exceeded");
    assert!(
        elapsed >= Duration::from_millis(1400),
        "query must not complete when the poller fails: {elapsed:?}"
    );
    shut_down(&child, server).await;
}

#[tokio::test]
async fn absent_query_fields_absent_task_list_poller_fails_and_query_waits() {
    absent_query_case(|r| r.task_list = None, p::TaskList::default()).await;
}

#[tokio::test]
async fn absent_query_fields_absent_query_poller_fails_and_query_waits() {
    absent_query_case(
        |r| r.request.as_mut().unwrap().query = None,
        child_list(0),
    )
    .await;
}

#[tokio::test]
async fn absent_query_fields_absent_query_request_poller_fails_and_query_waits() {
    absent_query_case(|r| r.request = None, child_list(0)).await;
}

#[tokio::test]
async fn absent_query_fields_both_absent_poller_fails_and_query_waits() {
    absent_query_case(
        |r| {
            r.task_list = None;
            r.request = None;
        },
        p::TaskList::default(),
    )
    .await;
}
