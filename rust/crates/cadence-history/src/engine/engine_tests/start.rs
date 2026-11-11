//! Ports of engineimpl/start_workflow_execution_test.go (pure helpers and early-exit paths).
use std::collections::HashMap;

use cadence_api::enums::WorkflowIDReusePolicy;
use cadence_api::service_types::SignalWithStartWorkflowExecutionRequest;
use cadence_api::types::*;

use crate::engine::deps::*;
use crate::engine::start_workflow_execution::*;
use crate::engine::test_support::*;
use crate::engine::types::*;

fn exec() -> WorkflowExecution {
    WorkflowExecution { workflow_id: "wf".into(), run_id: "".into() }
}

fn apply(state: i32, close: i32, policy: WorkflowIDReusePolicy) -> HResult<()> {
    apply_workflow_id_reuse_policy_helper("req", "prev-run", state, close, &exec(), policy)
}

#[test]
fn reuse_policy_running_workflow_is_always_rejected() {
    for state in [WORKFLOW_STATE_CREATED, WORKFLOW_STATE_RUNNING] {
        for p in [WorkflowIDReusePolicy::AllowDuplicate, WorkflowIDReusePolicy::AllowDuplicateFailedOnly, WorkflowIDReusePolicy::RejectDuplicate, WorkflowIDReusePolicy::TerminateIfRunning] {
            let err = apply(state, WORKFLOW_CLOSE_STATUS_NONE, p).unwrap_err();
            assert!(err.to_string().contains("Workflow execution is already running. WorkflowId: wf, RunId: prev-run."), "{err}");
        }
    }
}

#[test]
fn reuse_policy_corrupted_workflow_is_ignored() {
    assert!(apply(WORKFLOW_STATE_CORRUPTED, WORKFLOW_CLOSE_STATUS_NONE, WorkflowIDReusePolicy::RejectDuplicate).is_ok());
}

#[test]
fn reuse_policy_invalid_state() {
    let err = apply(99, 0, WorkflowIDReusePolicy::AllowDuplicate).unwrap_err();
    assert!(err.to_string().contains("workflow has invalid state: 99."), "{err}");
}

#[test]
fn reuse_policy_allow_duplicate_failed_only() {
    for close in [WORKFLOW_CLOSE_STATUS_FAILED, WORKFLOW_CLOSE_STATUS_CANCELED, WORKFLOW_CLOSE_STATUS_TERMINATED, WORKFLOW_CLOSE_STATUS_TIMED_OUT] {
        assert!(apply(WORKFLOW_STATE_COMPLETED, close, WorkflowIDReusePolicy::AllowDuplicateFailedOnly).is_ok(), "close {close}");
    }
    for close in [WORKFLOW_CLOSE_STATUS_COMPLETED, WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW] {
        let err = apply(WORKFLOW_STATE_COMPLETED, close, WorkflowIDReusePolicy::AllowDuplicateFailedOnly).unwrap_err();
        assert!(err.to_string().contains("allow duplicate workflow ID if last run failed"), "{err}");
    }
}

#[test]
fn reuse_policy_allow_duplicate_and_terminate_if_running_allow_completed() {
    for p in [WorkflowIDReusePolicy::AllowDuplicate, WorkflowIDReusePolicy::TerminateIfRunning] {
        assert!(apply(WORKFLOW_STATE_COMPLETED, WORKFLOW_CLOSE_STATUS_COMPLETED, p).is_ok());
    }
}

#[test]
fn reuse_policy_reject_duplicate() {
    let err = apply(WORKFLOW_STATE_COMPLETED, WORKFLOW_CLOSE_STATUS_FAILED, WorkflowIDReusePolicy::RejectDuplicate).unwrap_err();
    assert!(err.to_string().contains("reject duplicate workflow ID"), "{err}");
}

#[test]
fn reuse_policy_for_signal_with_start_uses_previous_info() {
    let prev = WorkflowExecutionInfo { state: WORKFLOW_STATE_RUNNING, run_id: "r9".into(), create_request_id: "c".into(), ..Default::default() };
    let err = apply_workflow_id_reuse_policy_for_sig_with_start(&prev, &exec(), WorkflowIDReusePolicy::AllowDuplicate).unwrap_err();
    assert!(err.to_string().contains("RunId: r9"), "{err}");
}

#[test]
fn terminate_if_running_details_format() {
    assert_eq!(terminate_if_running_details("abc"), b"New runID: abc".to_vec());
}

#[test]
fn well_known_errors() {
    assert!(err_concurrent_start_request().to_string().contains("an outstanding start workflow request is in-progress"));
    assert!(err_cluster_attribute_not_found().to_string().contains("cluster attribute that is not found"));
}

#[test]
fn failed_close_states_and_terminate_and_start() {
    assert!(is_failed_close_state(WORKFLOW_CLOSE_STATUS_FAILED));
    assert!(!is_failed_close_state(WORKFLOW_CLOSE_STATUS_COMPLETED));
    let mut req = HistoryStartWorkflowExecutionRequest {
        start_request: Some(StartWorkflowExecutionRequest { workflow_id_reuse_policy: Some(WorkflowIDReusePolicy::TerminateIfRunning), ..Default::default() }),
        ..Default::default()
    };
    assert!(should_terminate_and_start(&req, WORKFLOW_STATE_RUNNING));
    assert!(should_terminate_and_start(&req, WORKFLOW_STATE_CREATED));
    assert!(!should_terminate_and_start(&req, WORKFLOW_STATE_COMPLETED));
    req.start_request.as_mut().unwrap().workflow_id_reuse_policy = Some(WorkflowIDReusePolicy::AllowDuplicate);
    assert!(!should_terminate_and_start(&req, WORKFLOW_STATE_RUNNING));
}

#[test]
fn create_history_start_request_copies_domain_and_partition_config() {
    let pc = HashMap::from([("k".to_string(), "v".to_string())]);
    let sr = StartWorkflowExecutionRequest { workflow_id: "wf".into(), execution_start_to_close_timeout_seconds: Some(100), ..Default::default() };
    let r = create_history_start_request("dom", sr, 1_000, &pc).unwrap();
    assert_eq!(r.domain_uuid, "dom");
    assert_eq!(r.partition_config, pc);
    assert_eq!(r.first_decision_task_backoff_seconds, Some(0));
    assert_eq!(r.start_request.unwrap().workflow_id, "wf");
}

#[test]
fn get_start_request_copies_signal_with_start_fields() {
    let sws = SignalWithStartWorkflowExecutionRequest {
        workflow_id: "wf".into(),
        identity: "id".into(),
        request_id: "rid".into(),
        cron_schedule: "* * * * *".into(),
        execution_start_to_close_timeout_seconds: Some(10),
        ..Default::default()
    };
    let r = get_start_request("dom", &sws, &HashMap::new()).unwrap();
    let s = r.start_request.unwrap();
    assert_eq!((s.workflow_id.as_str(), s.identity.as_str(), s.request_id.as_str()), ("wf", "id", "rid"));
    assert_eq!(s.cron_schedule, "* * * * *");
}

#[tokio::test]
async fn start_workflow_execution_unknown_domain() {
    let engine = nop_engine();
    let req = HistoryStartWorkflowExecutionRequest { domain_uuid: "missing".into(), ..Default::default() };
    let err = engine.start_workflow_execution(&req).await.unwrap_err();
    assert!(err.is_entity_not_exists(), "{err}");
}
