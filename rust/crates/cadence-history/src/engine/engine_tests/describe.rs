//! Ports of engineimpl/describe_workflow_execution_test.go.
use std::sync::Arc;

use cadence_api::enums::*;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::*;

use crate::engine::deps::*;
use crate::engine::deps_ms::PendingChildInfo;
use crate::engine::describe_workflow_execution::*;
use crate::engine::runtime::*;
use crate::engine::test_support::*;
use crate::engine::types::*;

fn request(domain_uuid: &str) -> HistoryDescribeWorkflowExecutionRequest {
    HistoryDescribeWorkflowExecutionRequest {
        domain_uuid: domain_uuid.to_string(),
        request: Some(DescribeWorkflowExecutionRequest {
            execution: Some(WorkflowExecution { workflow_id: "wf".into(), run_id: "run".into() }),
            ..Default::default()
        }),
    }
}

fn engine_with(ms: FakeMs, corruption_check: bool) -> (HistoryEngine, Arc<FakeCache>) {
    let ctx = Arc::new(FakeCtx::new(Arc::new(ms)));
    let cache = Arc::new(FakeCache::new(ctx));
    let mut engine = nop_engine();
    engine.execution_cache = cache.clone();
    engine.config = Arc::new(TestConfig { corruption_check, ..Default::default() });
    (engine, cache)
}

fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[tokio::test]
async fn describe_workflow_execution_invalid_request() {
    let (engine, _) = engine_with(FakeMs::default(), false);
    let err = engine.describe_workflow_execution(&request("")).await.unwrap_err();
    assert_eq!(err.to_string(), "Missing domain UUID.");
}

#[tokio::test]
async fn describe_workflow_execution_cache_error() {
    let mut engine = nop_engine();
    engine.execution_cache = Arc::new(FakeCache::failing("cache error"));
    let err = engine.describe_workflow_execution(&request(&id())).await.unwrap_err();
    assert!(err.to_string().contains("cache error"));
}

#[tokio::test]
async fn describe_workflow_execution_load_error() {
    let ms = Arc::new(FakeMs::default());
    let mut ctx = FakeCtx::new(ms);
    ctx.load_err = Some("load error".into());
    let cache = Arc::new(FakeCache::new(Arc::new(ctx)));
    let mut engine = nop_engine();
    engine.execution_cache = cache.clone();
    let err = engine.describe_workflow_execution(&request(&id())).await.unwrap_err();
    assert!(err.to_string().contains("load error"));
    assert_eq!(*cache.releases.lock().unwrap(), vec![true]);
}

#[tokio::test]
async fn describe_workflow_execution_corruption_detected() {
    let ms = FakeMs { start_event: err_event_missing_start(), ..Default::default() };
    let (engine, cache) = engine_with(ms, true);
    let err = engine.describe_workflow_execution(&request(&id())).await.unwrap_err();
    assert!(err.to_string().contains("Workflow execution corrupted."), "{err}");
    assert_eq!(*cache.releases.lock().unwrap(), vec![true]);
}

fn err_event_missing_start() -> EventFn {
    Arc::new(|| Err(HistoryError::MissingWorkflowStartEvent))
}

#[tokio::test]
async fn describe_workflow_execution_corruption_check_other_error() {
    let ms = FakeMs { start_event: err_event("start boom"), ..Default::default() };
    let (engine, _) = engine_with(ms, true);
    let err = engine.describe_workflow_execution(&request(&id())).await.unwrap_err();
    assert!(err.to_string().contains("start boom"));
}

#[tokio::test]
async fn describe_workflow_execution_success() {
    let info = WorkflowExecutionInfo {
        workflow_id: "wf".into(),
        run_id: "run".into(),
        task_list: "tl".into(),
        workflow_type_name: "type".into(),
        start_timestamp: 1000,
        workflow_timeout: 60,
        decision_start_to_close_timeout: 10,
        ..Default::default()
    };
    let (engine, cache) = engine_with(FakeMs::with_info(info), false);
    let resp = engine.describe_workflow_execution(&request(&id())).await.unwrap();
    let wi = resp.workflow_execution_info.unwrap();
    assert_eq!(wi.execution.unwrap().workflow_id, "wf");
    assert_eq!(wi.history_length, 9); // next_event_id (10) - FIRST_EVENT_ID (1)
    assert_eq!(wi.start_time, Some(1000));
    assert!(!wi.is_cron);
    let cfg = resp.execution_configuration.unwrap();
    assert_eq!(cfg.execution_start_to_close_timeout_seconds, Some(60));
    assert_eq!(cfg.task_start_to_close_timeout_seconds, Some(10));
    assert_eq!(*cache.releases.lock().unwrap(), vec![false]);
}

// ---- TestCreateDescribeWorkflowExecutionResponse / Map* ----

#[tokio::test]
async fn create_response_completed_workflow_has_close_status_and_time() {
    let info = WorkflowExecutionInfo { state: WORKFLOW_STATE_COMPLETED, close_status: WORKFLOW_CLOSE_STATUS_FAILED, start_timestamp: 5, ..Default::default() };
    let ms = FakeMs {
        info: std::sync::Mutex::new(info),
        completion_event: ok_event(HistoryEvent { timestamp: Some(99), ..Default::default() }),
        ..Default::default()
    };
    let resp = create_describe_workflow_execution_response(&ms, &Nop).await.unwrap();
    let wi = resp.workflow_execution_info.unwrap();
    assert_eq!(wi.close_status, Some(WorkflowExecutionCloseStatus::Failed));
    assert_eq!(wi.close_time, Some(99));
}

#[tokio::test]
async fn create_response_start_event_error() {
    let ms = FakeMs { start_event: err_event("no start"), ..Default::default() };
    assert!(create_describe_workflow_execution_response(&ms, &Nop).await.is_err());
}

#[tokio::test]
async fn create_response_completion_event_error() {
    let info = WorkflowExecutionInfo { state: WORKFLOW_STATE_COMPLETED, ..Default::default() };
    let ms = FakeMs { info: std::sync::Mutex::new(info), completion_event: err_event("no completion"), ..Default::default() };
    assert!(create_describe_workflow_execution_response(&ms, &Nop).await.is_err());
}

#[tokio::test]
async fn create_response_pending_activity_scheduled_event_error() {
    let ms = FakeMs {
        pending_activities: vec![ActivityInfo { schedule_id: 3, ..Default::default() }],
        activity_scheduled_event: Arc::new(|_| Err(HistoryError::internal("no scheduled event"))),
        ..Default::default()
    };
    assert!(create_describe_workflow_execution_response(&ms, &Nop).await.is_err());
}

#[tokio::test]
async fn create_response_pending_activities_and_decision() {
    let ms = FakeMs {
        pending_activities: vec![ActivityInfo { schedule_id: 3, activity_id: "a".into(), started_id: EMPTY_EVENT_ID, ..Default::default() }],
        pending_decision: Some(crate::mutable_state::DecisionInfo { schedule_id: 7, started_id: EMPTY_EVENT_ID, ..Default::default() }),
        ..Default::default()
    };
    let resp = create_describe_workflow_execution_response(&ms, &Nop).await.unwrap();
    assert_eq!(resp.pending_activities.len(), 1);
    assert_eq!(resp.pending_activities[0].state, Some(PendingActivityState::Scheduled));
    assert_eq!(resp.pending_decision.unwrap().state, Some(PendingDecisionState::Scheduled));
}

#[test]
fn map_workflow_execution_configuration_maps_fields() {
    let info = WorkflowExecutionInfo { task_list: "tl".into(), workflow_timeout: 5, decision_start_to_close_timeout: 2, ..Default::default() };
    let c = map_workflow_execution_configuration(&info);
    assert_eq!(c.task_list.unwrap().name, "tl");
    assert_eq!(c.execution_start_to_close_timeout_seconds, Some(5));
    assert_eq!(c.task_start_to_close_timeout_seconds, Some(2));
}

#[test]
fn map_pending_activity_info_states() {
    let ev = HistoryEvent::default();
    let scheduled = map_pending_activity_info(&ActivityInfo { started_id: EMPTY_EVENT_ID, scheduled_time: 4, ..Default::default() }, &ev);
    assert_eq!(scheduled.state, Some(PendingActivityState::Scheduled));
    assert_eq!(scheduled.scheduled_timestamp, Some(4));
    let started = map_pending_activity_info(&ActivityInfo { started_id: 5, started_time: 6, ..Default::default() }, &ev);
    assert_eq!(started.state, Some(PendingActivityState::Started));
    assert_eq!(started.last_started_timestamp, Some(6));
    let cancel = map_pending_activity_info(&ActivityInfo { cancel_requested: true, started_id: 5, ..Default::default() }, &ev);
    assert_eq!(cancel.state, Some(PendingActivityState::CancelRequested));
}

#[test]
fn map_pending_activity_info_heartbeat_and_retry() {
    let ev = HistoryEvent::default();
    let ai = ActivityInfo {
        started_id: EMPTY_EVENT_ID,
        last_heart_beat_updated_time: 8,
        details: b"d".to_vec(),
        has_retry_policy: true,
        attempt: 2,
        expiration_time: 50,
        maximum_attempts: 4,
        last_failure_reason: "boom".into(),
        last_worker_identity: "w".into(),
        started_identity: "s".into(),
        ..Default::default()
    };
    let p = map_pending_activity_info(&ai, &ev);
    assert_eq!(p.last_heartbeat_timestamp, Some(8));
    assert_eq!(p.heartbeat_details, b"d".to_vec());
    assert_eq!(p.attempt, 2);
    assert_eq!(p.expiration_timestamp, Some(50));
    assert_eq!(p.maximum_attempts, 4);
    assert_eq!(p.last_failure_reason.as_deref(), Some("boom"));
    assert_eq!(p.last_worker_identity, "w");
    assert_eq!(p.started_worker_identity, "s");
}

#[test]
fn map_pending_child_uses_domain_name_or_falls_back_to_id() {
    let entry = domain_entry("dom-id", "dom-name");
    let mut dc = FakeDomainCache::default();
    dc.by_id.insert("child-id".into(), Arc::new(domain_entry("child-id", "child-name")));
    // child in a known domain
    let known = PendingChildInfo { domain_id: "child-id".into(), initiated_id: 3, ..Default::default() };
    assert_eq!(map_pending_child_execution_info(&known, &entry, &dc).unwrap().domain, "child-name");
    // empty domain id uses the parent's domain
    let same = PendingChildInfo::default();
    assert_eq!(map_pending_child_execution_info(&same, &entry, &dc).unwrap().domain, "dom-name");
    // deleted domain falls back to the domain id
    let gone = PendingChildInfo { domain_id: "gone".into(), ..Default::default() };
    assert_eq!(map_pending_child_execution_info(&gone, &entry, &dc).unwrap().domain, "gone");
}

#[test]
fn map_pending_decision_info_states() {
    use crate::mutable_state::DecisionInfo;
    let s = map_pending_decision_info(&DecisionInfo { started_id: EMPTY_EVENT_ID, schedule_timestamp: 3, ..Default::default() });
    assert_eq!(s.state, Some(PendingDecisionState::Scheduled));
    let st = map_pending_decision_info(&DecisionInfo { started_id: 4, started_timestamp: 9, ..Default::default() });
    assert_eq!(st.state, Some(PendingDecisionState::Started));
    assert_eq!(st.started_timestamp, Some(9));
}

#[tokio::test]
async fn map_workflow_execution_info_parent_and_cron() {
    let mut dc = FakeDomainCache::default();
    dc.by_id.insert("pd".into(), Arc::new(domain_entry("pd", "parent-domain")));
    let info = WorkflowExecutionInfo {
        parent_run_id: "pr".into(),
        parent_workflow_id: "pw".into(),
        parent_domain_id: "pd".into(),
        initiated_id: 12,
        cron_schedule: "* * * * *".into(),
        start_timestamp: 100,
        ..Default::default()
    };
    let r = map_workflow_execution_info(&info, &HistoryEvent::default(), &dc, 5, None).unwrap();
    assert!(r.is_cron);
    assert_eq!(r.parent_domain.as_deref(), Some("parent-domain"));
    assert_eq!(r.parent_initiated_id, Some(12));
    assert_eq!(r.execution_time, Some(100));
}
