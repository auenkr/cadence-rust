//! Parity gap: long-poll expiry versus an in-flight decision start.
//!
//! - Go `handler/engine.go:808` starts the decision with `recordDecisionTaskStarted(hCtx.Context, ...)`.
//!   That context is the RPC context, not the long-poll window. Go's `ThrottleRetry` caps each
//!   attempt at `recordTaskStartedTimeout` (1s) and stops only when the RPC context ends
//!   (`common/backoff/retry.go:268`). `tasklist/task_list_manager.go:708` bounds only the match.
//! - Rust `grpc_matching.rs:1247` `poll_timeout` gives the poll a 2s window. `matching_dispatch.rs:892`
//!   sets the start `caller_deadline` to that window. `matching_history.rs` `retry` caps the start at
//!   `min(caller_deadline, now + 1s)` (`overall_deadline`, line 327). `matching_dispatch.rs:1075` ends the
//!   whole poll at the window, so a task delivered near the end is cut and rewritten (`:1346`).
//!
//! Scenario: the poller is parked, a decision task is matched at ~1.3s, and the history start answers at
//! ~2.2s. That is inside Go's 1s per-attempt cap, so Go returns the started task. Rust cuts the start at
//! the 2.0s window, returns an empty poll, and rewrites the task. A rewritten task would then return
//! `EventAlreadyStarted` against real history, so the started decision has no poller.
//!
//! The test is ignored until the parity gap is fixed. Production code is not changed here.
use cadence_common::membership::Host;
use cadence_matching::{
    grpc_matching::Matching, matching_history::MatchingHistoryClient,
    matching_membership::Membership, matching_registry::QueueRegistry,
};
use cadence_persistence::{matching_blob::BlobEncoding, matching_sqlite::SqliteTaskStore};
use cadence_proto::{api as p, history as h, matching as m};
use m::matching_api_server::MatchingApi;
use std::{num::NonZeroU32, sync::Arc, time::Duration};
use tonic::{Request, Response, Status};
use uuid::Uuid;

const PARENT: &str = "queue";
const GO_REMOTE: &str = "go-matching";

/// Go `historyservice` stand-in whose RecordDecisionTaskStarted answers after `delay`.
struct SlowStartingHistory {
    delay: Duration,
}

#[tonic::async_trait]
impl h::history_api_server::HistoryApi for SlowStartingHistory {
    async fn record_decision_task_started(
        &self,
        request: Request<h::RecordDecisionTaskStartedRequest>,
    ) -> Result<Response<h::RecordDecisionTaskStartedResponse>, Status> {
        let request = request.into_inner();
        tokio::time::sleep(self.delay).await;
        Ok(Response::new(h::RecordDecisionTaskStartedResponse {
            scheduled_event_id: request.schedule_id,
            started_event_id: request.schedule_id + 1,
            ..Default::default()
        }))
    }
}

async fn start_slow_history(delay: Duration) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(h::history_api_server::HistoryApiServer::new(
                SlowStartingHistory { delay },
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{address}")
}

/// Ring where `local` is owned by this node.
fn membership(local: &str, remote_address: &str) -> Membership {
    for candidate in 0..100_000 {
        let identity = format!("rust-matching-{candidate}");
        let membership = Membership::new(
            identity.clone(),
            vec![
                Host {
                    address: format!("rust-{candidate}:7935"),
                    identity: identity.clone(),
                    grpc_address: format!("rust-{candidate}:7835"),
                },
                Host {
                    address: remote_address.into(),
                    identity: GO_REMOTE.into(),
                    grpc_address: remote_address.into(),
                },
            ],
        );
        if membership
            .owner(local)
            .is_ok_and(|host| host.identity() == identity)
        {
            return membership;
        }
    }
    panic!("could not construct a matching membership ring for {local}");
}

/// `poll_expiration` is the long-poll window used when dynamic config is absent. The
/// harness seeds the same 2s value through `matching.longPollExpirationInterval`.
fn fixture(
    history: &str,
    membership: Membership,
    poll_expiration: Duration,
) -> (tempfile::TempDir, Matching, String) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteTaskStore::create(dir.path().join("queue.db"), BlobEncoding::ThriftRW).unwrap(),
    );
    let state = cadence_service_core::handlers::AppState::default();
    let domain = Uuid::from_u128(1).to_string();
    state
        .domains
        .create(
            cadence_api::types::DomainInfo {
                name: "domain".into(),
                uuid: domain.clone(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            false,
        )
        .unwrap();
    let history = MatchingHistoryClient::new(
        tonic::transport::Endpoint::from_shared(history.to_string())
            .unwrap()
            .connect_lazy(),
        Duration::from_secs(1),
        4 * 1024 * 1024,
    );
    let queues = QueueRegistry::new(store, history, NonZeroU32::new(100).unwrap());
    let matching = Matching::with_durable_queues(state, queues, poll_expiration)
        .with_membership(membership);
    (dir, matching, domain)
}

fn execution() -> Option<p::WorkflowExecution> {
    Some(p::WorkflowExecution {
        workflow_id: "longpoll-expiry-workflow".into(),
        run_id: Uuid::from_u128(2).to_string(),
    })
}

fn task_list(name: &str, kind: i32) -> Option<p::TaskList> {
    Some(p::TaskList {
        name: name.into(),
        kind,
        ..Default::default()
    })
}

fn decision_poll(
    domain: &str,
    poller: &str,
    list: Option<p::TaskList>,
    deadline: Duration,
) -> Request<m::PollForDecisionTaskRequest> {
    let mut request = Request::new(m::PollForDecisionTaskRequest {
        domain_id: domain.into(),
        poller_id: poller.into(),
        request: Some(p::PollForDecisionTaskRequest {
            task_list: list,
            identity: poller.into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    request.set_timeout(deadline);
    request
}

#[tokio::test]
#[ignore = "parity gap: Rust cuts an in-flight RecordDecisionTaskStarted at the 2s long-poll window; Go returns the started task"]
async fn decision_started_after_long_poll_window_still_returns_task() {
    // Poll window 2s (matching.longPollExpirationInterval). Start answers 900ms after the
    // match, so the start ends at ~2.2s. Go's 1s per-attempt cap is not reached.
    let history = start_slow_history(Duration::from_millis(900)).await;
    let (_dir, matching, domain) = fixture(
        &history,
        membership(PARENT, "127.0.0.1:9"),
        Duration::from_secs(2),
    );

    let worker = matching.clone();
    let poll_domain = domain.clone();
    let poll = tokio::spawn(async move {
        worker
            .poll_for_decision_task(decision_poll(
                &poll_domain,
                "longpoll-worker",
                task_list(PARENT, 1),
                Duration::from_secs(10),
            ))
            .await
    });

    // The poller is parked at ~1.3s, so the match lands with 0.7s of the window left.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    matching
        .add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: domain.clone(),
            workflow_execution: execution(),
            task_list: task_list(PARENT, 1),
            schedule_id: 91,
            ..Default::default()
        }))
        .await
        .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(10), poll)
        .await
        .expect("poll returns before the request deadline")
        .unwrap()
        .unwrap()
        .into_inner();
    assert_eq!(
        response.started_event_id, 92,
        "Go returns the started decision; the start is not bounded by the 2s long-poll window"
    );
}
