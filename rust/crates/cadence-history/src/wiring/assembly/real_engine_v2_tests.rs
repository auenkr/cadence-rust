//! Ports of history_engine2_test.go (start / duplicate-request / signal-with-start / request-cancel flows) and
//! history_engine3_eventsv2_test.go (deprecated-domain checks, brand-new start, signal-with-start) over the
//! in-memory stores of `real_engine_harness`. Go's gomock `.Return(nil, err)` persistence errors are injected via
//! `Inner::create_errors` / `Inner::update_errors`.
use cadence_api::enums::WorkflowIDReusePolicy;
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
            assert!(is_already_started(&res.as_ref().unwrap_err()), "policy {i} close status {close_status}: {res:?}");
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
    push_create_error(&h, PersistenceError::DuplicateRequest { request_type: 0, run_id: "test-run-id".into() });
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
