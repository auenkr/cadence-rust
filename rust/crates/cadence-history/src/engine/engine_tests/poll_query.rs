//! Ports of poll_mutable_state / query_workflow cases of history_engine_test.go.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cadence_api::enums::{QueryRejectCondition, WorkflowExecutionCloseStatus};
use cadence_api::tasks::{QueryWorkflowRequest, QueryWorkflowResponse};
use cadence_api::types::{TaskList, VersionHistories, VersionHistory, VersionHistoryItem, WorkflowExecution};

use crate::engine::deps::*;
use crate::engine::runtime::*;
use crate::engine::test_support::*;
use crate::engine::types::*;

const DOMAIN: &str = "00000000-0000-0000-0000-000000000001";

fn histories() -> VersionHistories {
    VersionHistories {
        current_version_history_index: 0,
        histories: vec![VersionHistory { branch_token: b"bt".to_vec(), items: vec![VersionHistoryItem { event_id: 9, version: 1 }] }],
    }
}

fn wf() -> WorkflowExecution {
    WorkflowExecution { workflow_id: "wf".into(), run_id: "run".into() }
}

fn base_ms() -> FakeMs {
    let info = WorkflowExecutionInfo { task_list: "tl".into(), ..Default::default() };
    FakeMs { info: Mutex::new(info), version_histories: Some(histories()), next_event_id: 10, branch_token: b"bt".to_vec(), previous_started_event_id: 3, ..Default::default() }
}

fn engine_with(ms: FakeMs) -> (HistoryEngine, Arc<FakeCache>) {
    let ms: MutableStateRef = Arc::new(ms);
    let ctx: ExecutionContextRef = Arc::new(FakeCtx::new(ms));
    let cache = Arc::new(FakeCache::new(ctx));
    let mut e = nop_engine();
    e.execution_cache = cache.clone();
    let mut dc = FakeDomainCache::default();
    dc.names.insert(DOMAIN.into(), "dom".into());
    dc.by_id.insert(DOMAIN.into(), Arc::new(domain_entry(DOMAIN, "dom")));
    e.domain_cache = Arc::new(dc);
    (e, cache)
}

fn get_req(expected: i64) -> GetMutableStateRequest {
    GetMutableStateRequest { domain_uuid: DOMAIN.into(), execution: Some(wf()), expected_next_event_id: expected, ..Default::default() }
}

#[tokio::test]
async fn get_mutable_state_rejects_bad_domain_uuid() {
    let (e, _) = engine_with(base_ms());
    let mut r = get_req(0);
    r.domain_uuid = String::new();
    assert!(e.get_mutable_state(&r, None).await.unwrap_err().to_string().contains("Missing domain UUID"));
    r.domain_uuid = "not-a-uuid".into();
    assert!(e.get_mutable_state(&r, None).await.unwrap_err().to_string().contains("Invalid domain UUID"));
}

#[tokio::test]
async fn get_mutable_state_returns_immediately_when_event_already_exists() {
    let (e, cache) = engine_with(base_ms());
    let resp = e.get_mutable_state(&get_req(5), None).await.unwrap();
    assert_eq!(resp.next_event_id, 10);
    assert!(resp.is_workflow_running);
    assert_eq!(resp.task_list.unwrap().name, "tl");
    assert_eq!(resp.current_branch_token, b"bt".to_vec());
    assert!(cache.releases.lock().unwrap().iter().all(|errored| !errored));
}

#[tokio::test]
async fn get_mutable_state_default_expected_id_returns_immediately() {
    let (e, _) = engine_with(base_ms());
    assert_eq!(e.get_mutable_state(&get_req(0), None).await.unwrap().next_event_id, 10);
}

#[tokio::test]
async fn get_mutable_state_closed_workflow_does_not_wait() {
    let ms = base_ms();
    *ms.running.lock().unwrap() = false;
    let (e, _) = engine_with(ms);
    let started = Instant::now();
    let resp = e.get_mutable_state(&get_req(50), None).await.unwrap();
    assert!(!resp.is_workflow_running);
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[tokio::test]
async fn get_mutable_state_branch_changed() {
    let (e, _) = engine_with(base_ms());
    let mut r = get_req(5);
    r.version_history_item = Some(VersionHistoryItem { event_id: 100, version: 9 });
    let err = e.get_mutable_state(&r, None).await.unwrap_err();
    assert!(matches!(err, HistoryError::CurrentBranchChanged { .. }));
}

#[tokio::test]
async fn get_mutable_state_missing_version_histories() {
    let mut ms = base_ms();
    ms.version_histories = None;
    let (e, _) = engine_with(ms);
    assert!(matches!(e.get_mutable_state(&get_req(5), None).await.unwrap_err(), HistoryError::MissingVersionHistories));
}

#[tokio::test]
async fn long_poll_times_out_with_current_state() {
    struct Silent(Mutex<Option<tokio::sync::mpsc::UnboundedSender<HistoryNotification>>>);
    impl HistoryEventNotifier for Silent {
        fn watch_history_event(&self, _: &str, _: &WorkflowExecution) -> HResult<(String, tokio::sync::mpsc::UnboundedReceiver<HistoryNotification>)> {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            *self.0.lock().unwrap() = Some(tx);
            Ok(("sub".into(), rx))
        }
        fn unwatch_history_event(&self, _: &str, _: &WorkflowExecution, _: &str) {}
        fn notify_new_history_event(&self, _: HistoryNotification) {}
    }
    let (mut e, _) = engine_with(base_ms());
    e.history_event_notifier = Arc::new(Silent(Mutex::new(None)));
    let resp = e.get_mutable_state(&get_req(10), None).await.unwrap();
    assert_eq!(resp.next_event_id, 10);
}

#[tokio::test]
async fn long_poll_returns_on_notification() {
    struct One(Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<HistoryNotification>>>);
    impl HistoryEventNotifier for One {
        fn watch_history_event(&self, _: &str, _: &WorkflowExecution) -> HResult<(String, tokio::sync::mpsc::UnboundedReceiver<HistoryNotification>)> {
            Ok(("sub".into(), self.0.lock().unwrap().take().unwrap()))
        }
        fn unwatch_history_event(&self, _: &str, _: &WorkflowExecution, _: &str) {}
        fn notify_new_history_event(&self, _: HistoryNotification) {}
    }
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(HistoryNotification {
        domain_id: DOMAIN.into(),
        workflow_id: "wf".into(),
        run_id: "run".into(),
        last_first_event_id: 10,
        next_event_id: 12,
        previous_started_event_id: 11,
        workflow_state: 1,
        workflow_close_state: WORKFLOW_CLOSE_STATUS_NONE,
        version_histories: histories(),
    })
    .unwrap();
    let (mut e, _) = engine_with(base_ms());
    e.history_event_notifier = Arc::new(One(Mutex::new(Some(rx))));
    let started = Instant::now();
    let resp = e.get_mutable_state(&get_req(11), None).await.unwrap();
    assert_eq!(resp.next_event_id, 12);
    assert_eq!(resp.previous_started_event_id, Some(11));
    assert!(started.elapsed() < Duration::from_millis(900));
}

#[tokio::test]
async fn long_poll_with_short_deadline_fails() {
    struct Silent;
    impl HistoryEventNotifier for Silent {
        fn watch_history_event(&self, _: &str, _: &WorkflowExecution) -> HResult<(String, tokio::sync::mpsc::UnboundedReceiver<HistoryNotification>)> {
            Ok(("sub".into(), tokio::sync::mpsc::unbounded_channel().1))
        }
        fn unwatch_history_event(&self, _: &str, _: &WorkflowExecution, _: &str) {}
        fn notify_new_history_event(&self, _: HistoryNotification) {}
    }
    let (mut e, _) = engine_with(base_ms());
    e.history_event_notifier = Arc::new(Silent);
    let err = e.get_mutable_state(&get_req(10), Some(Instant::now() + Duration::from_millis(5))).await.unwrap_err();
    assert!(matches!(err, HistoryError::DeadlineExceeded));
}

struct MissingCache;
#[async_trait]
impl ExecutionCache for MissingCache {
    async fn get_or_create_workflow_execution(&self, _: &str, _: &WorkflowExecution) -> HResult<(ExecutionContextRef, ReleaseFunc)> {
        Err(HistoryError::entity_not_exists("nf"))
    }
    async fn get_and_create_workflow_execution(&self, _: &str, _: &WorkflowExecution) -> HResult<(ExecutionContextRef, ExecutionContextRef, ReleaseFunc, bool)> {
        unimplemented!()
    }
    async fn get_or_create_current_workflow_execution(&self, _: &str, _: &str) -> HResult<ReleaseFunc> {
        unimplemented!()
    }
}

#[tokio::test]
async fn poll_mutable_state_not_found_on_passive_cluster_reports_active_cluster() {
    let preq = PollMutableStateRequest { domain_uuid: DOMAIN.into(), execution: Some(wf()), ..Default::default() };
    let mut e = nop_engine();
    e.execution_cache = Arc::new(MissingCache);
    e.cluster_metadata = Arc::new(FixedClusterMetadata("cluster-a".into()));
    e.active_cluster_manager = Arc::new(FixedActiveCluster("cluster-a".into()));
    let same = e.poll_mutable_state(&preq, None).await.unwrap_err();
    assert!(same.is_entity_not_exists());
    e.active_cluster_manager = Arc::new(FixedActiveCluster("cluster-b".into()));
    let other = e.poll_mutable_state(&preq, None).await.unwrap_err();
    assert!(other.is_entity_not_exists());
    assert_ne!(same.to_string(), other.to_string());
}

#[tokio::test]
async fn poll_mutable_state_maps_fields() {
    let (e, _) = engine_with(base_ms());
    let preq = PollMutableStateRequest { domain_uuid: DOMAIN.into(), execution: Some(wf()), expected_next_event_id: 3, ..Default::default() };
    let resp = e.poll_mutable_state(&preq, None).await.unwrap();
    assert_eq!(resp.next_event_id, 10);
    assert_eq!(resp.previous_started_event_id, Some(3));
    assert_eq!(resp.current_branch_token, b"bt".to_vec());
}

// ---- query_workflow ----

struct RecMatching {
    calls: Mutex<Vec<String>>,
    fail: bool,
}
#[async_trait]
impl QueryMatchingClient for RecMatching {
    async fn query_workflow(&self, domain: &str, _: &QueryWorkflowRequest, tl: Option<&TaskList>) -> HResult<QueryWorkflowResponse> {
        self.calls.lock().unwrap().push(format!("{domain}:{}", tl.map(|t| t.name.clone()).unwrap_or_default()));
        if self.fail {
            return Err(internal("matching boom"));
        }
        Ok(QueryWorkflowResponse::default())
    }
}

fn qreq(cond: Option<QueryRejectCondition>) -> HistoryQueryWorkflowRequest {
    HistoryQueryWorkflowRequest {
        domain_uuid: DOMAIN.into(),
        request: Some(QueryWorkflowRequest { domain: "dom".into(), execution: Some(wf()), query_reject_condition: cond, ..Default::default() }),
    }
}

#[tokio::test]
async fn query_strong_consistency_disabled_is_bad_request() {
    let (mut e, _) = engine_with(base_ms());
    e.config = Arc::new(TestConfig { no_consistent_query: true, ..Default::default() });
    let mut r = qreq(None);
    r.request.as_mut().unwrap().query_consistency_level = Some(cadence_api::enums::QueryConsistencyLevel::Strong);
    let err = e.query_workflow(&r, None).await.unwrap_err();
    assert!(err.to_string().contains("strongly consistent query"), "{err}");
}

fn closed_ms(close: i32) -> FakeMs {
    let ms = base_ms();
    *ms.running.lock().unwrap() = false;
    ms.info.lock().unwrap().close_status = close;
    ms
}

#[tokio::test]
async fn query_closed_workflow_rejected_by_not_open() {
    let (e, _) = engine_with(closed_ms(WORKFLOW_CLOSE_STATUS_COMPLETED));
    let resp = e.query_workflow(&qreq(Some(QueryRejectCondition::NotOpen)), None).await.unwrap();
    let rejected = resp.response.unwrap().query_rejected.unwrap();
    assert_eq!(rejected.close_status, Some(WorkflowExecutionCloseStatus::Completed));
}

#[tokio::test]
async fn query_closed_workflow_rejected_by_not_completed_cleanly() {
    let (e, _) = engine_with(closed_ms(2));
    let resp = e.query_workflow(&qreq(Some(QueryRejectCondition::NotCompletedCleanly)), None).await.unwrap();
    assert_eq!(resp.response.unwrap().query_rejected.unwrap().close_status, Some(WorkflowExecutionCloseStatus::Failed));
}

#[tokio::test]
async fn query_without_decision_started_fails() {
    let mut ms = base_ms();
    ms.previous_started_event_id = 0;
    let (e, _) = engine_with(ms);
    let started = Instant::now();
    let err = e.query_workflow(&qreq(None), None).await.unwrap_err();
    assert!(err.to_string().contains("at least one decision task"), "{err}");
    assert!(started.elapsed() >= Duration::from_millis(900));
}

#[tokio::test]
async fn query_eventual_goes_directly_to_matching_with_task_list() {
    let (mut e, _) = engine_with(base_ms());
    let matching = Arc::new(RecMatching { calls: Mutex::new(vec![]), fail: false });
    e.matching_client = matching.clone();
    let resp = e.query_workflow(&qreq(None), None).await.unwrap();
    assert!(resp.response.is_some());
    assert_eq!(*matching.calls.lock().unwrap(), vec![format!("{DOMAIN}:tl")]);
}

#[tokio::test]
async fn query_matching_error_is_propagated() {
    let (mut e, _) = engine_with(base_ms());
    e.matching_client = Arc::new(RecMatching { calls: Mutex::new(vec![]), fail: true });
    let err = e.query_workflow(&qreq(None), None).await.unwrap_err();
    assert!(err.to_string().contains("matching boom"), "{err}");
}

#[tokio::test]
async fn query_expired_deadline_fails_before_matching() {
    let (mut e, _) = engine_with(base_ms());
    let matching = Arc::new(RecMatching { calls: Mutex::new(vec![]), fail: false });
    e.matching_client = matching.clone();
    let err = e.query_workflow(&qreq(None), Some(Instant::now() - Duration::from_millis(1))).await.unwrap_err();
    assert!(matches!(err, HistoryError::DeadlineExceeded));
    assert!(matching.calls.lock().unwrap().is_empty());
}
