//! Ports of engineimpl reset_workflow_execution tests (validation + reset-point event type).
use std::sync::Arc;

use cadence_api::enums::EventType;
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::*;

use crate::engine::reset_workflow_execution::check_reset_event_type;
use crate::engine::runtime::*;
use crate::engine::test_support::*;
use crate::engine::types::*;

fn ev(id: i64, t: EventType) -> HistoryEvent {
    HistoryEvent { event_id: id, event_type: Some(t), ..Default::default() }
}

#[test]
fn check_reset_event_type_accepts_decision_events() {
    for t in [EventType::DecisionTaskStarted, EventType::DecisionTaskTimedOut, EventType::DecisionTaskFailed, EventType::DecisionTaskCompleted] {
        assert!(check_reset_event_type(&[ev(1, EventType::WorkflowExecutionStarted), ev(4, t)], 4).is_ok(), "{t:?}");
    }
}

#[test]
fn check_reset_event_type_rejects_other_events() {
    let err = check_reset_event_type(&[ev(4, EventType::ActivityTaskScheduled)], 4).unwrap_err();
    assert!(err.to_string().contains("reset event must be of type DecisionTaskStarted"), "{err}");
}

#[test]
fn check_reset_event_type_missing_event() {
    let err = check_reset_event_type(&[ev(1, EventType::WorkflowExecutionStarted)], 9).unwrap_err();
    assert!(err.to_string().contains("reset event ID 9 not found"), "{err}");
}

fn reset_req(finish: i64) -> HistoryResetWorkflowExecutionRequest {
    HistoryResetWorkflowExecutionRequest {
        domain_uuid: uuid::Uuid::new_v4().to_string(),
        reset_request: Some(ResetWorkflowExecutionRequest {
            workflow_execution: Some(WorkflowExecution { workflow_id: "wf".into(), run_id: "run".into() }),
            decision_finish_event_id: finish,
            ..Default::default()
        }),
    }
}

fn engine(ms: FakeMs) -> HistoryEngine {
    let mut e = nop_engine();
    e.execution_cache = Arc::new(FakeCache::new(Arc::new(FakeCtx::new(Arc::new(ms)))));
    e
}

#[tokio::test]
async fn reset_workflow_execution_without_decision_schedule() {
    let ms = FakeMs { processed_or_pending_decision: false, ..Default::default() };
    let err = engine(ms).reset_workflow_execution(&reset_req(5)).await.unwrap_err();
    assert!(err.to_string().contains("Cannot reset workflow without a decision task schedule."), "{err}");
}

#[tokio::test]
async fn reset_workflow_execution_finish_id_out_of_range() {
    for finish in [1, 11] {
        let err = engine(FakeMs::default()).reset_workflow_execution(&reset_req(finish)).await.unwrap_err();
        assert!(err.to_string().contains("Decision finish ID must be > 1 && <= workflow next event ID."), "{finish}: {err}");
    }
}
