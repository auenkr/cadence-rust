//! Go forwarded-poll failure fallback, reproduced over the public Matching RPC surface.
//!
//! - Go `tasklist/matcher_test.go:Test_pollOrForward_ForwardTaskThenPoll`: the forward
//!   returns an error, then a task added locally is delivered by the same parked poll.
//! - Go `tasklist/forwarder_test.go:TestForwardPollError`: a root list has no parent and a
//!   sticky list never forwards, so neither may call the parent.
//!
//! The parent is a test-only fake that fails every forwarded poll and counts the attempts.
//! Production code is not changed.
use cadence_common::membership::Host;
use cadence_matching::{
    grpc_matching::Matching, matching_history::MatchingHistoryClient,
    matching_membership::Membership, matching_registry::QueueRegistry,
};
use cadence_persistence::{matching_blob::BlobEncoding, matching_sqlite::SqliteTaskStore};
use cadence_proto::{api as p, history as h, matching as m};
use m::matching_api_server::MatchingApi;
use std::{
    num::NonZeroU32,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

const CHILD: &str = "/__cadence_sys/queue/1";
const PARENT: &str = "queue";
const GO_REMOTE: &str = "go-matching";

/// Parent partition owner that fails every forwarded poll with `code` and counts the attempts.
#[derive(Clone)]
struct FailingParent {
    code: tonic::Code,
    polls: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl MatchingApi for FailingParent {
    async fn add_decision_task(
        &self,
        _: Request<m::AddDecisionTaskRequest>,
    ) -> Result<Response<m::AddDecisionTaskResponse>, Status> {
        Err(Status::new(self.code, "parent failed"))
    }
    async fn poll_for_decision_task(
        &self,
        _: Request<m::PollForDecisionTaskRequest>,
    ) -> Result<Response<m::PollForDecisionTaskResponse>, Status> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Err(Status::new(self.code, "parent failed"))
    }
}

/// Go `historyservice` stand-in: starts a decision task at `schedule_id + 1`.
struct StartingHistory;

#[tonic::async_trait]
impl h::history_api_server::HistoryApi for StartingHistory {
    async fn record_decision_task_started(
        &self,
        request: Request<h::RecordDecisionTaskStartedRequest>,
    ) -> Result<Response<h::RecordDecisionTaskStartedResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(h::RecordDecisionTaskStartedResponse {
            scheduled_event_id: request.schedule_id,
            started_event_id: request.schedule_id + 1,
            ..Default::default()
        }))
    }
}

async fn start_parent(parent: FailingParent) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(m::matching_api_server::MatchingApiServer::new(parent))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{address}")
}

async fn start_starting_history() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(h::history_api_server::HistoryApiServer::new(StartingHistory))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{address}")
}

/// Ring where `local` is owned by this node and, when given, `remote_owned` by the fake parent.
fn membership(local: &str, remote_owned: Option<&str>, remote_address: &str) -> Membership {
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
        let local_owned = membership
            .owner(local)
            .is_ok_and(|host| host.identity() == identity);
        let remote_ok = remote_owned.is_none_or(|name| {
            membership
                .owner(name)
                .is_ok_and(|host| host.identity() == GO_REMOTE)
        });
        if local_owned && remote_ok {
            return membership;
        }
    }
    panic!("could not construct a matching membership ring for {local}");
}

fn fixture(history: &str, membership: Membership) -> (tempfile::TempDir, Matching, String) {
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
    let matching = Matching::with_durable_queues(state, queues, Duration::from_secs(10))
        .with_membership(membership);
    (dir, matching, domain)
}

fn execution() -> Option<p::WorkflowExecution> {
    Some(p::WorkflowExecution {
        workflow_id: "forward-poll-workflow".into(),
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

async fn add_child_decision(matching: &Matching, domain: &str, schedule_id: i64) {
    matching
        .add_decision_task(Request::new(m::AddDecisionTaskRequest {
            domain_id: domain.into(),
            workflow_execution: execution(),
            task_list: task_list(CHILD, 1),
            schedule_id,
            ..Default::default()
        }))
        .await
        .unwrap();
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition reached before the timeout");
}

#[tokio::test]
async fn forwarded_decision_poll_failure_delivers_later_local_add() {
    // Go Test_pollOrForward_ForwardTaskThenPoll: ForwardPoll fails once with ErrNoParent, the
    // poll stays registered locally, and the task added afterwards is returned to that poll.
    for code in [tonic::Code::Unavailable, tonic::Code::ResourceExhausted] {
        let polls = Arc::new(AtomicUsize::new(0));
        let address = start_parent(FailingParent {
            code,
            polls: polls.clone(),
        })
        .await;
        let history = start_starting_history().await;
        let (_dir, matching, domain) =
            fixture(&history, membership(CHILD, Some(PARENT), &address));

        let worker = matching.clone();
        let poll_domain = domain.clone();
        let poll = tokio::spawn(async move {
            worker
                .poll_for_decision_task(decision_poll(
                    &poll_domain,
                    "child-worker",
                    task_list(CHILD, 1),
                    Duration::from_secs(8),
                ))
                .await
        });
        wait_until(|| polls.load(Ordering::SeqCst) == 1).await;

        add_child_decision(&matching, &domain, 91).await;
        let response = tokio::time::timeout(Duration::from_secs(10), poll)
            .await
            .expect("local add wakes the parked poll")
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(response.started_event_id, 92, "{code:?}: local task delivered");
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "{code:?}: a failed forward is not retried for the same poll"
        );
    }
}

#[tokio::test]
async fn root_and_sticky_decision_polls_never_call_parent() {
    // Go TestForwardPollError: a root list returns ErrNoParent and a sticky list returns
    // ErrTaskListKind, both before any RPC. Each poll must wait out its deadline locally.
    let polls = Arc::new(AtomicUsize::new(0));
    let address = start_parent(FailingParent {
        code: tonic::Code::Unavailable,
        polls: polls.clone(),
    })
    .await;
    let history = start_starting_history().await;

    // Root: owned locally so the poll is not routed to the fake parent.
    let (_root_dir, root, domain) = fixture(&history, membership(PARENT, None, &address));
    let response = root
        .poll_for_decision_task(decision_poll(
            &domain,
            "root-worker",
            task_list(PARENT, 1),
            Duration::from_secs(2),
        ))
        .await
        .expect("root poll returns at its deadline")
        .into_inner();
    assert!(response.task_token.is_empty(), "root poll returns empty");
    assert_eq!(polls.load(Ordering::SeqCst), 0, "root never forwards");

    // Sticky: the child partition is local and its parent is remote, yet sticky never forwards.
    let (_sticky_dir, sticky, domain) = fixture(&history, membership(CHILD, Some(PARENT), &address));
    let response = sticky
        .poll_for_decision_task(decision_poll(
            &domain,
            "sticky-worker",
            task_list(CHILD, 2),
            Duration::from_secs(2),
        ))
        .await
        .expect("sticky poll returns at its deadline")
        .into_inner();
    assert!(response.task_token.is_empty(), "sticky poll returns empty");
    assert_eq!(polls.load(Ordering::SeqCst), 0, "sticky never forwards");
}
