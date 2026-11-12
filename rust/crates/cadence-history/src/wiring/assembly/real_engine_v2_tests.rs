//! Ports of history_engine2_test.go (start / duplicate-request / signal-with-start / request-cancel flows) and
//! history_engine3_eventsv2_test.go (deprecated-domain checks, brand-new start, signal-with-start) over the
//! in-memory stores of `real_engine_harness`. Go's gomock `.Return(nil, err)` persistence errors are injected via
//! `Inner::create_errors` / `Inner::update_errors`.
use cadence_api::enums::{DecisionType, WorkflowIDReusePolicy};
use cadence_api::tasks::*;
use cadence_api::tasktoken::{JsonTaskTokenSerializer, TaskToken};
use cadence_api::errors::CadenceError;
use cadence_api::service_types::SignalWithStartWorkflowExecutionRequest;
use cadence_api::service_types::RequestCancelWorkflowExecutionRequest;
use cadence_api::types::*;
use cadence_persistence::history_contract::{CurrentExecution, PersistenceError};

use crate::engine::deps::*;
use crate::engine::types::*;
use crate::wiring::assembly::real_engine_harness::*;

const WF: &str = "workflowID";
const TL: &str = "testTaskList";
const IDENTITY: &str = "testIdentity";

fn start_req(request_id: &str, policy: Option<WorkflowIDReusePolicy>) -> HistoryStartWorkflowExecutionRequest {
    HistoryStartWorkflowExecutionRequest {
        domain_uuid: DOMAIN_ID.into(),
        start_request: Some(StartWorkflowExecutionRequest {
            domain: DOMAIN_NAME.into(),
            workflow_id: WF.into(),
            workflow_type: Some(WorkflowType { name: "workflowType".into() }),
            task_list: Some(TaskList { name: TL.into(), ..Default::default() }),
            execution_start_to_close_timeout_seconds: Some(1),
            task_start_to_close_timeout_seconds: Some(2),
            identity: IDENTITY.into(),
            request_id: request_id.into(),
            workflow_id_reuse_policy: policy,
            ..Default::default()
        }),
        partition_config: [("zone".to_string(), "phx".to_string())].into_iter().collect(),
        ..Default::default()
    }
}

fn sws_req(request_id: &str) -> HistorySignalWithStartWorkflowExecutionRequest {
    HistorySignalWithStartWorkflowExecutionRequest {
        domain_uuid: DOMAIN_ID.into(),
        signal_with_start_request: Some(SignalWithStartWorkflowExecutionRequest {
            domain: DOMAIN_NAME.into(),
            workflow_id: "wId".into(),
            workflow_type: Some(WorkflowType { name: "workflowType".into() }),
            task_list: Some(TaskList { name: TL.into(), ..Default::default() }),
            execution_start_to_close_timeout_seconds: Some(1),
            task_start_to_close_timeout_seconds: Some(2),
            identity: IDENTITY.into(),
            request_id: request_id.into(),
            signal_name: "my signal name".into(),
            signal_input: b"test input".to_vec(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn already_started(start_request_id: &str, run_id: &str, state: i32, close_status: i32) -> PersistenceError {
    PersistenceError::WorkflowAlreadyStarted {
        message: "random message".into(),
        start_request_id: start_request_id.into(),
        run_id: run_id.into(),
        state,
        close_status,
        last_write_version: -24,
    }
}

fn is_already_started(e: &HistoryError) -> bool {
    matches!(e, HistoryError::Cadence(b) if matches!(&**b, CadenceError::WorkflowAlreadyStarted(_)))
}
fn is_bad_request(e: &HistoryError) -> bool {
    matches!(e, HistoryError::Cadence(b) if matches!(&**b, CadenceError::BadRequest(_)))
}

fn push_create_error(h: &Harness, e: PersistenceError) {
    h.execs.inner.lock().unwrap().create_errors.push_back(e);
}

/// Seeds the current-execution record that Go's mocked persistence would have reported.
fn seed_current(h: &Harness, run_id: &str, state: i32, close_status: i32) {
    h.execs.inner.lock().unwrap().current.insert(
        (DOMAIN_ID.into(), WF.into()),
        CurrentExecution { run_id: run_id.into(), create_request_id: "oldRequestID".into(), state, close_status, start_version: -24, last_write_version: -24 },
    );
}

// ---- TestStartWorkflowExecution_BrandNew (engine2 + engine3)

#[tokio::test]
async fn start_brand_new() {
    let h = Harness::new();
    let resp = h.engine.start_workflow_execution(&start_req("req-1", None)).await.expect("start");
    assert!(!resp.run_id.is_empty());
    let snap = h.execs.snapshot(WF, &resp.run_id).expect("persisted");
    assert_eq!(snap.execution_info.partition_config.get("zone").map(String::as_str), Some("phx"));
    assert_eq!(h.execs.inner.lock().unwrap().create_calls[0].0, "BrandNew");
}

#[tokio::test]
async fn start_brand_new_duplicate_request_error() {
    let h = Harness::new();
    push_create_error(&h, PersistenceError::DuplicateRequest { request_type: 0, run_id: "test-run-id".into() });
    let resp = h.engine.start_workflow_execution(&start_req("req-1", None)).await.expect("dup start returns existing run");
    assert_eq!(resp.run_id, "test-run-id");
}

#[tokio::test]
async fn start_brand_new_duplicate_request_error_type_mismatch() {
    let h = Harness::new();
    push_create_error(&h, PersistenceError::DuplicateRequest { request_type: 1, run_id: "test-run-id".into() });
    let err = h.engine.start_workflow_execution(&start_req("req-1", None)).await.unwrap_err();
    assert!(matches!(err, HistoryError::DuplicateRequest { .. }), "{err}");
}

// ---- StillRunning dedup / non-dedup

#[tokio::test]
async fn start_still_running_dedup() {
    let h = Harness::new();
    push_create_error(&h, already_started("requestID", "runID", 1, 0));
    let resp = h.engine.start_workflow_execution(&start_req("requestID", None)).await.expect("same request id returns running run");
    assert_eq!(resp.run_id, "runID");
}

#[tokio::test]
async fn start_still_running_non_dedup() {
    let h = Harness::new();
    push_create_error(&h, already_started("oldRequestID", "runID", 1, 0));
    let err = h.engine.start_workflow_execution(&start_req("newRequestID", None)).await.unwrap_err();
    assert!(is_already_started(&err), "{err}");
}

// ---- NotRunning, previous closed

#[tokio::test]
async fn start_not_running_prev_success_duplicate_request_error() {
    let h = Harness::new();
    push_create_error(&h, already_started("oldRequestID", "runID", 2, 0));
    push_create_error(&h, PersistenceError::DuplicateRequest { request_type: 0, run_id: "test-run-id".into() });
    let resp = h.engine.start_workflow_execution(&start_req("newRequestID", Some(WorkflowIDReusePolicy::AllowDuplicate))).await.expect("start");
    assert_eq!(resp.run_id, "test-run-id");
    let calls = h.execs.inner.lock().unwrap().create_calls.clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, "BrandNew");
    assert_eq!((calls[1].0, calls[1].1.as_str(), calls[1].2), ("WorkflowIdReuse", "runID", -24));
}

#[tokio::test]
async fn start_not_running_prev_success_duplicate_request_error_type_mismatch() {
    let h = Harness::new();
    push_create_error(&h, already_started("oldRequestID", "runID", 2, 0));
    push_create_error(&h, PersistenceError::DuplicateRequest { request_type: 1, run_id: "test-run-id".into() });
    let err = h.engine.start_workflow_execution(&start_req("newRequestID", Some(WorkflowIDReusePolicy::AllowDuplicate))).await.unwrap_err();
    assert!(matches!(err, HistoryError::DuplicateRequest { .. }), "{err}");
}

/// close status values: 0 none, 1 completed, 2 failed, 3 canceled, 4 terminated, 5 continued-as-new, 6 timed out.
async fn reuse_policy_matrix(close_status: i32, run_id: &str, expect_err: [bool; 3]) {
    let policies = [WorkflowIDReusePolicy::AllowDuplicateFailedOnly, WorkflowIDReusePolicy::AllowDuplicate, WorkflowIDReusePolicy::RejectDuplicate];
    let h = Harness::new();
    for (i, policy) in policies.into_iter().enumerate() {
        push_create_error(&h, already_started("oldRequestID", run_id, 2, close_status));
        if !expect_err[i] {
            seed_current(&h, run_id, 2, close_status);
        }
        let res = h.engine.start_workflow_execution(&start_req("newRequestID", Some(policy))).await;
        if expect_err[i] {
            assert!(is_already_started(res.as_ref().unwrap_err()), "policy {i} close status {close_status}: {res:?}");
        } else {
            let resp = res.unwrap_or_else(|e| panic!("policy {i} close status {close_status}: {e}"));
            assert!(!resp.run_id.is_empty());
            let calls = h.execs.inner.lock().unwrap().create_calls.clone();
            let last = calls.last().unwrap();
            assert_eq!((last.0, last.1.as_str()), ("WorkflowIdReuse", run_id));
            // the next iteration starts from a fresh store like a fresh gomock expectation
            h.execs.inner.lock().unwrap().current.clear();
        }
    }
}

#[tokio::test]
async fn start_not_running_prev_success() {
    // AllowDuplicateFailedOnly rejects, AllowDuplicate accepts, RejectDuplicate rejects.
    reuse_policy_matrix(1, "runID", [true, false, true]).await;
}

#[tokio::test]
async fn start_not_running_prev_fail() {
    // failed / canceled / terminated / timed out: only RejectDuplicate rejects.
    for (i, cs) in [2, 3, 4, 6].into_iter().enumerate() {
        reuse_policy_matrix(cs, &format!("{}", i + 1), [false, false, true]).await;
    }
}

// ---- engine3: deprecated domain checks

#[tokio::test]
async fn start_deprecated_domain_is_bad_request() {
    let h = Harness::new();
    h.domain_status.store(1, std::sync::atomic::Ordering::SeqCst);
    let err = h.engine.start_workflow_execution(&start_req("req-1", None)).await.unwrap_err();
    assert!(is_bad_request(&err), "{err}");
    assert!(h.execs.inner.lock().unwrap().runs.is_empty());
}

#[tokio::test]
async fn signal_with_start_deprecated_domain_is_bad_request() {
    let h = Harness::new();
    h.domain_status.store(1, std::sync::atomic::Ordering::SeqCst);
    let err = h.engine.signal_with_start_workflow_execution(&sws_req("req-1")).await.unwrap_err();
    assert!(is_bad_request(&err), "{err}");
}

#[tokio::test]
async fn signal_deprecated_domain_is_bad_request() {
    let h = Harness::new();
    h.domain_status.store(1, std::sync::atomic::Ordering::SeqCst);
    let err = h
        .engine
        .signal_workflow_execution(&HistorySignalWorkflowExecutionRequest {
            domain_uuid: DOMAIN_ID.into(),
            signal_request: Some(SignalWorkflowExecutionRequest {
                domain: DOMAIN_ID.into(),
                workflow_execution: Some(WorkflowExecution { workflow_id: "wId".into(), run_id: "run".into() }),
                identity: IDENTITY.into(),
                signal_name: "my signal name".into(),
                input: b"test input".to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(is_bad_request(&err), "{err}");
}

// ---- signal with start

#[tokio::test]
async fn signal_with_start_workflow_not_exist() {
    let h = Harness::new();
    let resp = h.engine.signal_with_start_workflow_execution(&sws_req("req-1")).await.expect("sws");
    assert!(!resp.run_id.is_empty());
    assert_eq!(h.execs.inner.lock().unwrap().create_calls[0].0, "BrandNew");
}

#[tokio::test]
async fn signal_with_start_workflow_not_exist_duplicate_request_error() {
    let h = Harness::new();
    push_create_error(&h, PersistenceError::DuplicateRequest { request_type: 1, run_id: "test-run-id".into() });
    let resp = h.engine.signal_with_start_workflow_execution(&sws_req("req-1")).await.expect("dup signal-with-start");
    assert_eq!(resp.run_id, "test-run-id");
}

#[tokio::test]
async fn signal_with_start_workflow_not_exist_duplicate_request_error_type_mismatch() {
    let h = Harness::new();
    push_create_error(&h, PersistenceError::DuplicateRequest { request_type: 2, run_id: "test-run-id".into() });
    let err = h.engine.signal_with_start_workflow_execution(&sws_req("req-1")).await.unwrap_err();
    assert!(matches!(err, HistoryError::DuplicateRequest { .. }), "{err}");
}

#[tokio::test]
async fn signal_with_start_create_timeout() {
    let h = Harness::new();
    push_create_error(&h, PersistenceError::Timeout("timeout".into()));
    let err = h.engine.signal_with_start_workflow_execution(&sws_req("req-1")).await.unwrap_err();
    assert!(err.is_timeout() || err.to_string().to_lowercase().contains("timeout"), "{err}");
}

#[tokio::test]
async fn signal_with_start_just_signal() {
    let h = Harness::new();
    let first = h.engine.start_workflow_execution(&{
        let mut r = start_req("req-1", None);
        r.start_request.as_mut().unwrap().workflow_id = "wId".into();
        r
    })
    .await
    .expect("start");
    let resp = h.engine.signal_with_start_workflow_execution(&sws_req("sws-1")).await.expect("sws");
    assert_eq!(resp.run_id, first.run_id);
    assert_eq!(h.execs.snapshot("wId", &first.run_id).unwrap().execution_info.signal_count, 1);
}

#[tokio::test]
async fn signal_with_start_just_signal_duplicate_request_error() {
    let h = Harness::new();
    let first = h
        .engine
        .start_workflow_execution(&{
            let mut r = start_req("req-1", None);
            r.start_request.as_mut().unwrap().workflow_id = "wId".into();
            r
        })
        .await
        .expect("start");
    h.execs.inner.lock().unwrap().update_errors.push_back(PersistenceError::DuplicateRequest { request_type: 1, run_id: "test-run-id".into() });
    let resp = h.engine.signal_with_start_workflow_execution(&sws_req("sws-1")).await.expect("dup");
    assert!(resp.run_id == "test-run-id" || resp.run_id == first.run_id, "{}", resp.run_id);
}

// ---- request cancel

fn cancel_req(run: &str, request_id: &str) -> HistoryRequestCancelWorkflowExecutionRequest {
    HistoryRequestCancelWorkflowExecutionRequest {
        domain_uuid: DOMAIN_ID.into(),
        cancel_request: Some(RequestCancelWorkflowExecutionRequest {
            workflow_execution: Some(WorkflowExecution { workflow_id: WF.into(), run_id: run.into() }),
            identity: "identity".into(),
            request_id: request_id.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn started(h: &Harness) -> String {
    h.engine.start_workflow_execution(&start_req("req-1", None)).await.expect("start").run_id
}

#[tokio::test]
async fn request_cancel_success() {
    let h = Harness::new();
    let run = started(&h).await;
    h.engine.request_cancel_workflow_execution(&cancel_req(&run, "")).await.expect("cancel");
    assert_eq!(h.execs.snapshot(WF, &run).unwrap().execution_info.next_event_id, 4);
}

#[tokio::test]
async fn request_cancel_duplicate_request_error_is_success() {
    let h = Harness::new();
    let run = started(&h).await;
    h.execs.inner.lock().unwrap().update_errors.push_back(PersistenceError::DuplicateRequest { request_type: 2, run_id: "test-run-id".into() });
    h.engine.request_cancel_workflow_execution(&cancel_req(&run, "")).await.expect("duplicate request is success");
}

fn close_run(h: &Harness, run: &str, close_status: i32, cancel_request_id: &str) {
    let mut g = h.execs.inner.lock().unwrap();
    let snap = g.runs.get_mut(&(DOMAIN_ID.to_string(), WF.to_string(), run.to_string())).unwrap();
    snap.execution_info.state = WORKFLOW_STATE_COMPLETED;
    snap.execution_info.close_status = close_status;
    if !cancel_request_id.is_empty() {
        snap.execution_info.cancel_requested = true;
        snap.execution_info.cancel_request_id = cancel_request_id.into();
    }
    drop(g);
    h.cache.clear_all_for_test();
}

#[tokio::test]
async fn request_cancel_already_cancelled_same_request_id_is_success() {
    let h = Harness::new();
    let run = started(&h).await;
    close_run(&h, &run, WORKFLOW_CLOSE_STATUS_CANCELED, "cancelrequestid");
    h.engine.request_cancel_workflow_execution(&cancel_req(&run, "cancelrequestid")).await.expect("already cancelled with same request id");
}

#[tokio::test]
async fn request_cancel_already_cancelled_different_request_id_fails() {
    let h = Harness::new();
    let run = started(&h).await;
    close_run(&h, &run, 0, "cancelrequestid");
    let err = h.engine.request_cancel_workflow_execution(&cancel_req(&run, "cancelrequestidxxx")).await.unwrap_err();
    assert!(err.is_workflow_already_completed(), "{err}");
}

#[tokio::test]
async fn request_cancel_completed_workflow_fails() {
    let h = Harness::new();
    let run = started(&h).await;
    close_run(&h, &run, 0, "");
    let err = h.engine.request_cancel_workflow_execution(&cancel_req(&run, "")).await.unwrap_err();
    assert!(err.is_workflow_already_completed(), "{err}");
}

// ---- direct conflict_resolve store test (Go checks every condition in one tx before mutating)

#[tokio::test]
async fn conflict_resolve_checks_all_conditions_before_mutating() {
    use cadence_persistence::history_contract::{ConflictResolveWorkflowExecutionRequest, ConflictResolveWorkflowMode, GoExecutionStore, WorkflowMutation, WorkflowRequestMode};
    let h = Harness::new();
    let run_a = started(&h).await;
    // Second (zombie-like) run B of the same workflow, not current.
    let mut b = h.execs.snapshot(WF, &run_a).unwrap();
    b.execution_info.run_id = "run-b".into();
    h.execs.inner.lock().unwrap().runs.insert((DOMAIN_ID.into(), WF.into(), "run-b".into()), b.clone());
    let a_snapshot = h.execs.snapshot(WF, &run_a).unwrap();
    let mut reset = b.clone();
    reset.condition = b.execution_info.next_event_id;
    reset.execution_info.next_event_id += 10;
    // Current-run mutation with a wrong condition must fail the whole request.
    let cur = WorkflowMutation {
        execution_info: a_snapshot.execution_info.clone(),
        condition: a_snapshot.execution_info.next_event_id + 99,
        ..Default::default()
    };
    let err = h
        .execs
        .conflict_resolve_workflow_execution(ConflictResolveWorkflowExecutionRequest {
            shard_id: 0,
            range_id: 0,
            mode: ConflictResolveWorkflowMode::UpdateCurrent,
            reset_workflow_snapshot: reset,
            new_workflow_snapshot: None,
            current_workflow_mutation: Some(cur),
            workflow_request_mode: WorkflowRequestMode::New,
            current_timestamp_unix_nanos: 0,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, PersistenceError::ConditionFailed(_)), "{err:?}");
    // Nothing was mutated: current run still A, B untouched.
    assert_eq!(h.execs.current_run(WF).unwrap().run_id, run_a);
    assert_eq!(h.execs.snapshot(WF, "run-b").unwrap().execution_info.next_event_id, b.execution_info.next_event_id);
}

// ---- RecordDecisionTaskStarted* variants (history_engine2_test.go)

fn exec_of(run: &str) -> Option<WorkflowExecution> {
    Some(WorkflowExecution { workflow_id: WF.into(), run_id: run.into() })
}

async fn rec_decision(h: &Harness, run: &str, schedule_id: i64, request_id: &str) -> HResult<RecordDecisionTaskStartedResponse> {
    h.engine
        .record_decision_task_started(&RecordDecisionTaskStartedRequest {
            domain_uuid: DOMAIN_ID.into(),
            workflow_execution: exec_of(run),
            schedule_id,
            task_id: 100,
            request_id: request_id.into(),
            poll_request: Some(PollForDecisionTaskRequest { task_list: Some(TaskList { name: TL.into(), ..Default::default() }), identity: IDENTITY.into(), ..Default::default() }),
        })
        .await
}

async fn rec_activity(h: &Harness, run: &str, schedule_id: i64, request_id: &str) -> HResult<RecordActivityTaskStartedResponse> {
    h.engine
        .record_activity_task_started(&RecordActivityTaskStartedRequest {
            domain_uuid: DOMAIN_ID.into(),
            workflow_execution: exec_of(run),
            schedule_id,
            task_id: 100,
            request_id: request_id.into(),
            poll_request: Some(PollForActivityTaskRequest { task_list: Some(TaskList { name: TL.into(), ..Default::default() }), identity: IDENTITY.into(), ..Default::default() }),
        })
        .await
}

fn task_token(run: &str, schedule_id: i64) -> Vec<u8> {
    JsonTaskTokenSerializer::serialize(&TaskToken { domain_id: DOMAIN_ID.into(), workflow_id: WF.into(), run_id: run.into(), schedule_id, ..Default::default() }).unwrap()
}

async fn respond_decision(h: &Harness, run: &str, decisions: Vec<Decision>) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
    h.engine
        .respond_decision_task_completed(&HistoryRespondDecisionTaskCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            complete_request: Some(RespondDecisionTaskCompletedRequest { task_token: task_token(run, 2), decisions, identity: IDENTITY.into(), ..Default::default() }),
        })
        .await
}

fn schedule_activity(id: &str) -> Decision {
    Decision {
        decision_type: Some(DecisionType::ScheduleActivityTask),
        schedule_activity_task_decision_attributes: Some(ScheduleActivityTaskDecisionAttributes {
            activity_id: id.into(),
            activity_type: Some(ActivityType { name: "activity_type1".into() }),
            domain: DOMAIN_NAME.into(),
            task_list: Some(TaskList { name: TL.into(), ..Default::default() }),
            input: b"input1".to_vec(),
            schedule_to_close_timeout_seconds: Some(100),
            schedule_to_start_timeout_seconds: Some(10),
            start_to_close_timeout_seconds: Some(1),
            heartbeat_timeout_seconds: Some(5),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Go `createExecutionStartedState(..., startDecision=false)`: workflow started + decision scheduled (id 2).
async fn started_with_decision(h: &Harness) -> String {
    started(h).await
}

/// Go `createExecutionStartedState(..., startDecision=true)` + DecisionTaskCompleted + ActivityTaskScheduled (id 5).
async fn started_with_activity(h: &Harness) -> String {
    let run = started(h).await;
    rec_decision(h, &run, 2, "dec-req").await.expect("decision started");
    respond_decision(h, &run, vec![schedule_activity("activity1_id")]).await.expect("decision completed");
    run
}

#[tokio::test]
async fn record_decision_task_started_success() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    let resp = rec_decision(&h, &run, 2, "reqId").await.expect("record");
    assert_eq!(resp.workflow_type.as_ref().unwrap().name, "workflowType");
    assert!(resp.previous_started_event_id.is_none());
    assert_eq!(resp.scheduled_event_id, 2);
    assert_eq!(resp.started_event_id, 3);
    assert!(resp.queries.is_empty());
}

#[tokio::test]
async fn record_decision_task_started_if_no_execution() {
    let h = Harness::new();
    let err = rec_decision(&h, "00000000-0000-0000-0000-000000000009", 2, "reqId").await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err:?}");
}

#[tokio::test]
async fn record_decision_task_started_if_get_execution_failed() {
    let h = Harness::new();
    h.execs.inner.lock().unwrap().get_errors.push_back(PersistenceError::Internal("FAILED".into()));
    let err = rec_decision(&h, "00000000-0000-0000-0000-000000000009", 2, "reqId").await.unwrap_err();
    assert!(err.to_string().contains("FAILED"), "{err:?}");
}

#[tokio::test]
async fn record_decision_task_started_if_task_already_started() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    rec_decision(&h, &run, 2, "first").await.expect("first");
    let err = rec_decision(&h, &run, 2, "reqId").await.unwrap_err();
    assert!(matches!(err, HistoryError::EventAlreadyStarted(_)), "{err:?}");
}

#[tokio::test]
async fn record_decision_task_started_if_task_already_completed() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    rec_decision(&h, &run, 2, "first").await.expect("first");
    respond_decision(&h, &run, vec![]).await.expect("completed");
    let err = rec_decision(&h, &run, 2, "reqId").await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err:?}");
}

#[tokio::test]
async fn record_decision_task_started_conflict_on_update() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    h.execs.inner.lock().unwrap().update_errors.push_back(PersistenceError::ConditionFailed("conflict".into()));
    let resp = rec_decision(&h, &run, 2, "reqId").await.expect("retry after conflict");
    assert_eq!(resp.workflow_type.as_ref().unwrap().name, "workflowType");
    assert!(resp.previous_started_event_id.is_none());
    assert_eq!(resp.started_event_id, 3);
}

/// Competing writer started the decision (with `request_id`) between the first load and the conditional update.
fn competing_decision_start(h: &Harness, run: &str, request_id: &'static str) {
    let key = (DOMAIN_ID.to_string(), WF.to_string(), run.to_string());
    h.execs.inner.lock().unwrap().conflict_hooks.push_back(Box::new(move |runs| {
        let info = &mut runs.get_mut(&key).unwrap().execution_info;
        info.decision_started_id = 3;
        info.decision_request_id = request_id.into();
        info.next_event_id = 4;
    }));
}

#[tokio::test]
async fn record_decision_task_retry_same_request() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    competing_decision_start(&h, &run, "testRecordDecisionTaskRetrySameRequestID");
    let resp = rec_decision(&h, &run, 2, "testRecordDecisionTaskRetrySameRequestID").await.expect("same request returns started decision");
    assert_eq!(resp.workflow_type.as_ref().unwrap().name, "workflowType");
    assert!(resp.previous_started_event_id.is_none());
    assert_eq!(resp.started_event_id, 3);
}

#[tokio::test]
async fn record_decision_task_retry_different_request() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    competing_decision_start(&h, &run, "some_other_req");
    let err = rec_decision(&h, &run, 2, "testRecordDecisionTaskRetrySameRequestID").await.unwrap_err();
    assert!(matches!(err, HistoryError::EventAlreadyStarted(_)), "{err:?}");
}

#[tokio::test]
async fn record_decision_task_started_max_attempts_exceeded() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    for _ in 0..10 {
        h.execs.inner.lock().unwrap().update_errors.push_back(PersistenceError::ConditionFailed("conflict".into()));
    }
    let err = rec_decision(&h, &run, 2, "reqId").await.unwrap_err();
    assert!(matches!(err, HistoryError::MaxAttemptsExceeded), "{err:?}");
}

// ---- RecordActivityTaskStarted* variants

#[tokio::test]
async fn record_activity_task_started_if_no_execution() {
    let h = Harness::new();
    let err = rec_activity(&h, "00000000-0000-0000-0000-000000000009", 5, "reqId").await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err:?}");
}

#[tokio::test]
async fn record_activity_task_started_success() {
    let h = Harness::new();
    let run = started_with_activity(&h).await;
    let resp = rec_activity(&h, &run, 5, "reqId").await.expect("record");
    let ev = resp.scheduled_event.expect("scheduled event");
    assert_eq!(ev.event_id, 5);
    assert_eq!(ev.activity_task_scheduled_event_attributes.as_ref().unwrap().activity_id, "activity1_id");
}

#[tokio::test]
async fn record_activity_task_started_stale_state() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    rec_decision(&h, &run, 2, "dec-req").await.expect("decision started");
    // schedule id 5 is beyond next_event_id (4): the cached state is stale and reloading never fixes it.
    let err = rec_activity(&h, &run, 5, "reqId").await.unwrap_err();
    assert!(matches!(err, HistoryError::MaxAttemptsExceeded), "{err:?}");
}

#[tokio::test]
async fn record_activity_task_started_activity_not_pending() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    rec_decision(&h, &run, 2, "dec-req").await.expect("decision started");
    let err = rec_activity(&h, &run, 3, "reqId").await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err:?}");
}

#[tokio::test]
async fn record_activity_task_started_activity_already_started() {
    let h = Harness::new();
    let run = started_with_activity(&h).await;
    let first = rec_activity(&h, &run, 5, "reqId").await.expect("first");
    // same schedule id and same request id returns the same scheduled event
    let again = rec_activity(&h, &run, 5, "reqId").await.expect("same request id");
    assert_eq!(first.scheduled_event, again.scheduled_event);
    // same schedule id and a different request id is rejected
    let err = rec_activity(&h, &run, 5, "otherReqId").await.unwrap_err();
    assert!(matches!(err, HistoryError::EventAlreadyStarted(_)), "{err:?}");
}

#[tokio::test]
async fn record_activity_task_started_resurrected() {
    let h = Harness::new();
    let run = started_with_activity(&h).await;
    let stale = h.execs.snapshot(WF, &run).unwrap();
    rec_activity(&h, &run, 5, "reqId").await.expect("started");
    h.engine
        .respond_activity_task_completed(&HistoryRespondActivityTaskCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            complete_request: Some(RespondActivityTaskCompletedRequest {
                task_token: JsonTaskTokenSerializer::serialize(&TaskToken { domain_id: DOMAIN_ID.into(), workflow_id: WF.into(), run_id: run.clone(), schedule_id: 5, ..Default::default() }).unwrap(),
                identity: IDENTITY.into(),
                ..Default::default()
            }),
        })
        .await
        .expect("activity completed");
    // The activity info is resurrected in persisted mutable state (next_event_id already past the completion event).
    let next_id = h.execs.snapshot(WF, &run).unwrap().execution_info.next_event_id;
    {
        let mut g = h.execs.inner.lock().unwrap();
        let snap = g.runs.get_mut(&(DOMAIN_ID.into(), WF.into(), run.clone())).unwrap();
        snap.activity_infos = stale.activity_infos.clone();
    }
    h.cache.clear_all_for_test();
    h.clock.0.fetch_add(3_600_000_000_000, std::sync::atomic::Ordering::SeqCst);
    let err = rec_activity(&h, &run, 5, "reqId2").await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err:?} (history next event {next_id})");
    assert!(h.execs.snapshot(WF, &run).unwrap().activity_infos.is_empty(), "resurrected activity deleted from mutable state");
}

// ---- RespondDecisionTaskCompleted RecordMarker, Terminate

#[tokio::test]
async fn respond_decision_task_completed_record_marker_decision() {
    let h = Harness::new();
    let run = started_with_decision(&h).await;
    rec_decision(&h, &run, 2, "dec-req").await.expect("decision started");
    let marker = Decision {
        decision_type: Some(DecisionType::RecordMarker),
        record_marker_decision_attributes: Some(RecordMarkerDecisionAttributes { marker_name: "marker name".into(), details: b"marker details".to_vec(), ..Default::default() }),
        ..Default::default()
    };
    respond_decision(&h, &run, vec![marker]).await.expect("completed");
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_eq!(info.next_event_id, 6);
    assert_eq!(info.last_processed_event, 3);
    assert_eq!(info.state, 1, "Running");
    assert_eq!(info.decision_schedule_id, -23, "no pending decision");
}

#[tokio::test]
async fn terminate_workflow_execution_success() {
    // Go's TestTerminateWorkflowExecution_Success is an empty body; assert the documented behavior.
    let h = Harness::new();
    let run = started(&h).await;
    h.engine
        .terminate_workflow_execution(&HistoryTerminateWorkflowExecutionRequest {
            domain_uuid: DOMAIN_ID.into(),
            terminate_request: Some(TerminateWorkflowExecutionRequest {
                domain: DOMAIN_NAME.into(),
                workflow_execution: exec_of(&run),
                reason: "reason".into(),
                identity: IDENTITY.into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("terminate");
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_eq!(info.state, WORKFLOW_STATE_COMPLETED);
    assert_eq!(info.close_status, WORKFLOW_CLOSE_STATUS_TERMINATED);
    assert_eq!(info.next_event_id, 4, "WorkflowExecutionTerminated appended");
}

// ---- SignalWithStart NotRunning / Start_DuplicateRequests

fn sws_allow_dup(request_id: &str) -> HistorySignalWithStartWorkflowExecutionRequest {
    let mut r = sws_req(request_id);
    r.signal_with_start_request.as_mut().unwrap().workflow_id_reuse_policy = Some(WorkflowIDReusePolicy::AllowDuplicate);
    r.partition_config = [("zone".to_string(), "phx".to_string())].into_iter().collect();
    r
}

#[tokio::test]
async fn signal_with_start_workflow_not_running() {
    let h = Harness::new();
    let mut s = start_req("first-req", None);
    s.start_request.as_mut().unwrap().workflow_id = "wId".into();
    let first = h.engine.start_workflow_execution(&s).await.expect("start");
    {
        let mut g = h.execs.inner.lock().unwrap();
        let snap = g.runs.get_mut(&(DOMAIN_ID.to_string(), "wId".to_string(), first.run_id.clone())).unwrap();
        snap.execution_info.state = WORKFLOW_STATE_COMPLETED;
    }
    h.cache.clear_all_for_test();
    let resp = h.engine.signal_with_start_workflow_execution(&sws_allow_dup("sws-req")).await.expect("sws");
    assert!(!resp.run_id.is_empty());
    assert_ne!(resp.run_id, first.run_id);
    let snap = h.execs.snapshot("wId", &resp.run_id).unwrap();
    assert_eq!(snap.execution_info.partition_config.get("zone").map(String::as_str), Some("phx"));
}

#[tokio::test]
async fn signal_with_start_start_duplicate_requests() {
    let h = Harness::new();
    push_create_error(&h, already_started("testRequestID", "existing-run", 1, 0));
    let resp = h.engine.signal_with_start_workflow_execution(&sws_allow_dup("testRequestID")).await.expect("same request id returns the running run");
    assert_eq!(resp.run_id, "existing-run");
}

#[tokio::test]
async fn signal_with_start_start_duplicate_request_error() {
    let h = Harness::new();
    push_create_error(&h, PersistenceError::DuplicateRequest { request_type: 1, run_id: "test-run-id".into() });
    let resp = h.engine.signal_with_start_workflow_execution(&sws_allow_dup("testRequestID")).await.expect("duplicate request error");
    assert_eq!(resp.run_id, "test-run-id");
}
