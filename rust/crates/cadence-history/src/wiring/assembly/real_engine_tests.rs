//! Ports of history_engine_test.go / history_engine2_test.go / history_engine3_eventsv2_test.go flows that need
//! real mutable state + persistence. They drive the real `HistoryEngine` over the in-memory stores of
//! `real_engine_harness` (instead of gomock persistence): each test starts a workflow through the engine
//! and then performs the Go test's record/respond steps.
use cadence_api::enums::DecisionType;
use cadence_api::service_types::{RequestCancelWorkflowExecutionRequest, SignalWithStartWorkflowExecutionRequest};
use cadence_api::tasks::*;
use cadence_api::tasktoken::{JsonTaskTokenSerializer, TaskToken};
use cadence_api::types::*;

use crate::engine::deps::*;
use crate::engine::types::*;
use crate::wiring::assembly::real_engine_harness::*;

const WF: &str = "wf-1";
const TL: &str = "tl-1";
const IDENTITY: &str = "identity-1";

fn tl() -> TaskList {
    TaskList { name: TL.into(), ..Default::default() }
}

fn start_req(request_id: &str) -> HistoryStartWorkflowExecutionRequest {
    HistoryStartWorkflowExecutionRequest {
        domain_uuid: DOMAIN_ID.into(),
        start_request: Some(StartWorkflowExecutionRequest {
            domain: DOMAIN_NAME.into(),
            workflow_id: WF.into(),
            workflow_type: Some(WorkflowType { name: "wf-type".into() }),
            task_list: Some(tl()),
            execution_start_to_close_timeout_seconds: Some(100),
            task_start_to_close_timeout_seconds: Some(10),
            identity: IDENTITY.into(),
            request_id: request_id.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn start(h: &Harness) -> String {
    h.engine.start_workflow_execution(&start_req("start-req")).await.expect("start").run_id
}

fn exec(run: &str) -> Option<WorkflowExecution> {
    Some(WorkflowExecution { workflow_id: WF.into(), run_id: run.into() })
}

async fn decision_started(h: &Harness, run: &str, schedule_id: i64, request_id: &str) -> HResult<RecordDecisionTaskStartedResponse> {
    h.engine
        .record_decision_task_started(&RecordDecisionTaskStartedRequest {
            domain_uuid: DOMAIN_ID.into(),
            workflow_execution: exec(run),
            schedule_id,
            task_id: 100,
            request_id: request_id.into(),
            poll_request: Some(PollForDecisionTaskRequest { domain: DOMAIN_NAME.into(), task_list: Some(tl()), identity: IDENTITY.into(), ..Default::default() }),
        })
        .await
}

fn token(run: &str, schedule_id: i64) -> Vec<u8> {
    JsonTaskTokenSerializer::serialize(&TaskToken {
        domain_id: DOMAIN_ID.into(),
        workflow_id: WF.into(),
        run_id: run.into(),
        schedule_id,
        ..Default::default()
    })
    .unwrap()
}

async fn decision_completed(h: &Harness, run: &str, schedule_id: i64, decisions: Vec<Decision>) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
    h.engine
        .respond_decision_task_completed(&HistoryRespondDecisionTaskCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            complete_request: Some(RespondDecisionTaskCompletedRequest {
                task_token: token(run, schedule_id),
                decisions,
                identity: IDENTITY.into(),
                ..Default::default()
            }),
        })
        .await
}

fn complete_decision() -> Decision {
    Decision {
        decision_type: Some(DecisionType::CompleteWorkflowExecution),
        complete_workflow_execution_decision_attributes: Some(CompleteWorkflowExecutionDecisionAttributes { result: b"done".to_vec() }),
        ..Default::default()
    }
}

fn schedule_activity_decision(id: &str) -> Decision {
    Decision {
        decision_type: Some(DecisionType::ScheduleActivityTask),
        schedule_activity_task_decision_attributes: Some(ScheduleActivityTaskDecisionAttributes {
            activity_id: id.into(),
            activity_type: Some(ActivityType { name: "act-type".into() }),
            domain: DOMAIN_NAME.into(),
            task_list: Some(tl()),
            input: b"in".to_vec(),
            schedule_to_close_timeout_seconds: Some(100),
            schedule_to_start_timeout_seconds: Some(10),
            start_to_close_timeout_seconds: Some(50),
            heartbeat_timeout_seconds: Some(5),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn activity_started(h: &Harness, run: &str, schedule_id: i64, request_id: &str) -> HResult<RecordActivityTaskStartedResponse> {
    h.engine
        .record_activity_task_started(&RecordActivityTaskStartedRequest {
            domain_uuid: DOMAIN_ID.into(),
            workflow_execution: exec(run),
            schedule_id,
            task_id: 101,
            request_id: request_id.into(),
            poll_request: Some(PollForActivityTaskRequest { domain: DOMAIN_NAME.into(), task_list: Some(tl()), identity: IDENTITY.into(), ..Default::default() }),
        })
        .await
}

/// Starts a workflow and runs the first decision to a ScheduleActivityTask; returns (run id, activity schedule id).
async fn with_scheduled_activity(h: &Harness) -> (String, i64) {
    let run = start(h).await;
    decision_started(h, &run, 2, "dec-req").await.expect("decision started");
    decision_completed(h, &run, 2, vec![schedule_activity_decision("act-1")]).await.expect("decision completed");
    (run, 5)
}

// ---- start (TestStartWorkflowExecution / history_engine2 start tests)

#[tokio::test]
async fn start_creates_run_current_execution_and_first_decision() {
    let h = Harness::new();
    let run = start(&h).await;
    let snap = h.execs.snapshot(WF, &run).expect("persisted");
    assert_eq!(snap.execution_info.next_event_id, 3, "WorkflowExecutionStarted + DecisionTaskScheduled");
    assert_eq!(snap.execution_info.decision_schedule_id, 2);
    assert_eq!(snap.execution_info.state, WORKFLOW_STATE_RUNNING);
    assert_eq!(h.execs.current_run(WF).unwrap().run_id, run);
    assert!(!h.history.nodes.lock().unwrap().is_empty(), "start events appended to history branch");
    assert!(h.execs.transfer_types().contains(&0), "decision transfer task");
    assert!(!h.execs.timer_types().is_empty(), "workflow timeout timer task");
}

#[tokio::test]
async fn start_same_request_id_is_idempotent() {
    let h = Harness::new();
    let run = start(&h).await;
    let again = h.engine.start_workflow_execution(&start_req("start-req")).await;
    match again {
        Ok(r) => assert_eq!(r.run_id, run),
        Err(e) => panic!("duplicate start with same request id must return the same run: {e}"),
    }
    assert_eq!(h.execs.inner.lock().unwrap().runs.len(), 1);
}

#[tokio::test]
async fn start_with_different_request_id_while_running_is_rejected() {
    let h = Harness::new();
    start(&h).await;
    let err = h.engine.start_workflow_execution(&start_req("other-req")).await.unwrap_err();
    assert!(err.to_string().contains("already"), "{err}");
    assert_eq!(h.execs.inner.lock().unwrap().runs.len(), 1);
}

// ---- RecordDecisionTaskStarted (TestRecordDecisionTaskStarted*)

#[tokio::test]
async fn record_decision_started_marks_decision_started() {
    let h = Harness::new();
    let run = start(&h).await;
    let resp = decision_started(&h, &run, 2, "dec-req").await.expect("started");
    assert_eq!(resp.workflow_type.as_ref().map(|t| t.name.as_str()), Some("wf-type"));
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_eq!(info.decision_started_id, 3);
    assert_eq!(info.decision_request_id, "dec-req");
    assert_eq!(info.next_event_id, 4);
    assert_ne!(info.state, WORKFLOW_STATE_COMPLETED);
}

#[tokio::test]
async fn record_decision_started_same_request_id_is_idempotent() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.expect("first");
    decision_started(&h, &run, 2, "dec-req").await.expect("duplicate request id returns the same started decision");
    assert_eq!(h.execs.snapshot(WF, &run).unwrap().execution_info.next_event_id, 4);
}

#[tokio::test]
async fn record_decision_started_different_request_id_is_already_started() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.expect("first");
    let err = decision_started(&h, &run, 2, "dec-req-2").await.unwrap_err();
    assert!(matches!(err, HistoryError::EventAlreadyStarted(_)), "{err:?}");
}

#[tokio::test]
async fn record_decision_started_unknown_schedule_id_is_entity_not_exists() {
    let h = Harness::new();
    let run = start(&h).await;
    let err = decision_started(&h, &run, 1, "dec-req").await.unwrap_err();
    assert!(err.to_string().to_lowercase().contains("not exist") || err.to_string().to_lowercase().contains("not found"), "{err}");
}

#[tokio::test]
async fn record_decision_started_unknown_workflow_is_not_found() {
    let h = Harness::new();
    let err = decision_started(&h, "00000000-0000-0000-0000-000000000009", 2, "dec-req").await.unwrap_err();
    assert!(err.to_string().to_lowercase().contains("not"), "{err}");
}

// ---- RespondDecisionTaskCompleted (TestRespondDecisionTaskCompleted*)

#[tokio::test]
async fn respond_decision_completed_invalid_token() {
    let h = Harness::new();
    let err = h
        .engine
        .respond_decision_task_completed(&HistoryRespondDecisionTaskCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            complete_request: Some(RespondDecisionTaskCompletedRequest { task_token: b"{not json".to_vec(), ..Default::default() }),
        })
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[tokio::test]
async fn respond_decision_completed_no_execution() {
    let h = Harness::new();
    let err = decision_completed(&h, "00000000-0000-0000-0000-000000000009", 2, vec![]).await.unwrap_err();
    assert!(err.to_string().to_lowercase().contains("not"), "{err}");
}

#[tokio::test]
async fn respond_decision_completed_task_not_started() {
    let h = Harness::new();
    let run = start(&h).await;
    let err = decision_completed(&h, &run, 2, vec![]).await.unwrap_err();
    assert!(err.to_string().to_lowercase().contains("not"), "{err}");
}

#[tokio::test]
async fn respond_decision_completed_task_already_completed() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.unwrap();
    decision_completed(&h, &run, 2, vec![]).await.expect("first completion");
    let err = decision_completed(&h, &run, 2, vec![]).await.unwrap_err();
    assert!(err.to_string().to_lowercase().contains("not"), "{err}");
}

#[tokio::test]
async fn respond_decision_completed_complete_workflow_success() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.unwrap();
    decision_completed(&h, &run, 2, vec![complete_decision()]).await.expect("completed");
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_eq!(info.state, WORKFLOW_STATE_COMPLETED);
    assert_eq!(info.close_status, WORKFLOW_CLOSE_STATUS_COMPLETED);
    // Started, Scheduled, Started, DecisionCompleted, WorkflowCompleted
    assert_eq!(info.next_event_id, 6);
    assert!(h.execs.transfer_types().contains(&2), "close execution transfer task");
}

#[tokio::test]
async fn respond_decision_completed_fail_workflow_success() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.unwrap();
    let fail = Decision {
        decision_type: Some(DecisionType::FailWorkflowExecution),
        fail_workflow_execution_decision_attributes: Some(FailWorkflowExecutionDecisionAttributes { reason: Some("reason".into()), details: b"details".to_vec() }),
        ..Default::default()
    };
    decision_completed(&h, &run, 2, vec![fail]).await.expect("completed");
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_eq!(info.state, WORKFLOW_STATE_COMPLETED);
    assert_eq!(info.close_status, WORKFLOW_CLOSE_STATUS_FAILED);
}

#[tokio::test]
async fn respond_decision_completed_single_activity_scheduled() {
    let h = Harness::new();
    let (run, _) = with_scheduled_activity(&h).await;
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert_eq!(snap.activity_infos.len(), 1);
    assert_eq!(snap.activity_infos[0].key, 5);
    // Started, Scheduled, Started, DecisionCompleted, ActivityScheduled
    assert_eq!(snap.execution_info.next_event_id, 6);
    assert_eq!(snap.execution_info.decision_schedule_id, -23, "no new decision pending (EmptyEventID)");
    assert!(h.execs.transfer_types().contains(&1), "activity transfer task");
}

#[tokio::test]
async fn respond_decision_completed_bad_decision_attributes_fails_decision_task() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.unwrap();
    // CompleteWorkflowExecution without attributes is a bad decision.
    let bad = Decision { decision_type: Some(DecisionType::CompleteWorkflowExecution), ..Default::default() };
    decision_completed(&h, &run, 2, vec![bad]).await.expect("Go records DecisionTaskFailed and schedules a new decision");
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_ne!(info.state, WORKFLOW_STATE_COMPLETED, "workflow stays open after a bad decision");
}

// ---- RespondActivityTask* / RecordActivityTaskStarted

#[tokio::test]
async fn record_activity_started_success() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    let resp = activity_started(&h, &run, sched, "act-req").await.expect("started");
    assert_eq!(resp.scheduled_event.as_ref().map(|e| e.event_id), Some(sched));
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert_eq!(snap.execution_info.next_event_id, 7, "ActivityTaskStarted appended");
}

#[tokio::test]
async fn record_activity_started_unknown_schedule_id() {
    let h = Harness::new();
    let (run, _) = with_scheduled_activity(&h).await;
    let err = activity_started(&h, &run, 3, "act-req").await.unwrap_err();
    assert!(err.to_string().to_lowercase().contains("not"), "{err}");
}

#[tokio::test]
async fn record_activity_started_different_request_id_is_already_started() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    activity_started(&h, &run, sched, "act-req").await.unwrap();
    let err = activity_started(&h, &run, sched, "act-req-2").await.unwrap_err();
    assert!(matches!(err, HistoryError::EventAlreadyStarted(_)), "{err:?}");
}

fn activity_token(run: &str, sched: i64) -> Vec<u8> {
    JsonTaskTokenSerializer::serialize(&TaskToken {
        domain_id: DOMAIN_ID.into(),
        workflow_id: WF.into(),
        run_id: run.into(),
        schedule_id: sched,
        ..Default::default()
    })
    .unwrap()
}

#[tokio::test]
async fn respond_activity_completed_success_schedules_decision() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    activity_started(&h, &run, sched, "act-req").await.unwrap();
    h.engine
        .respond_activity_task_completed(&HistoryRespondActivityTaskCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            complete_request: Some(RespondActivityTaskCompletedRequest { task_token: activity_token(&run, sched), result: b"r".to_vec(), identity: IDENTITY.into() }),
        })
        .await
        .expect("completed");
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert!(snap.activity_infos.is_empty());
    // + ActivityTaskCompleted + DecisionTaskScheduled
    assert_eq!(snap.execution_info.next_event_id, 9);
    assert_ne!(snap.execution_info.decision_schedule_id, 0);
}

#[tokio::test]
async fn respond_activity_completed_task_not_started() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    let err = h
        .engine
        .respond_activity_task_completed(&HistoryRespondActivityTaskCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            complete_request: Some(RespondActivityTaskCompletedRequest { task_token: activity_token(&run, sched), ..Default::default() }),
        })
        .await
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("not"), "{err}");
}

#[tokio::test]
async fn respond_activity_failed_success_schedules_decision() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    activity_started(&h, &run, sched, "act-req").await.unwrap();
    h.engine
        .respond_activity_task_failed(&HistoryRespondActivityTaskFailedRequest {
            domain_uuid: DOMAIN_ID.into(),
            failed_request: Some(RespondActivityTaskFailedRequest {
                task_token: activity_token(&run, sched),
                reason: Some("reason".into()),
                details: b"d".to_vec(),
                identity: IDENTITY.into(),
                ..Default::default()
            }),
        })
        .await
        .expect("failed");
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert!(snap.activity_infos.is_empty());
    assert_eq!(snap.execution_info.next_event_id, 9);
}

// ---- Signal / SignalWithStart / Terminate

#[tokio::test]
async fn signal_workflow_execution_appends_event() {
    let h = Harness::new();
    let run = start(&h).await;
    h.engine
        .signal_workflow_execution(&HistorySignalWorkflowExecutionRequest {
            domain_uuid: DOMAIN_ID.into(),
            signal_request: Some(SignalWorkflowExecutionRequest {
                domain: DOMAIN_NAME.into(),
                workflow_execution: exec(&run),
                signal_name: "sig".into(),
                input: b"in".to_vec(),
                identity: IDENTITY.into(),
                request_id: "sig-req".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("signal");
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_eq!(info.signal_count, 1);
    assert_eq!(info.next_event_id, 4);
}

#[tokio::test]
async fn signal_workflow_execution_unknown_workflow() {
    let h = Harness::new();
    let err = h
        .engine
        .signal_workflow_execution(&HistorySignalWorkflowExecutionRequest {
            domain_uuid: DOMAIN_ID.into(),
            signal_request: Some(SignalWorkflowExecutionRequest {
                domain: DOMAIN_NAME.into(),
                workflow_execution: exec("00000000-0000-0000-0000-000000000009"),
                signal_name: "sig".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("not"), "{err}");
}

fn sws_req(request_id: &str) -> HistorySignalWithStartWorkflowExecutionRequest {
    HistorySignalWithStartWorkflowExecutionRequest {
        domain_uuid: DOMAIN_ID.into(),
        signal_with_start_request: Some(SignalWithStartWorkflowExecutionRequest {
            domain: DOMAIN_NAME.into(),
            workflow_id: WF.into(),
            workflow_type: Some(WorkflowType { name: "wf-type".into() }),
            task_list: Some(tl()),
            execution_start_to_close_timeout_seconds: Some(100),
            task_start_to_close_timeout_seconds: Some(10),
            identity: IDENTITY.into(),
            request_id: request_id.into(),
            signal_name: "sig".into(),
            signal_input: b"in".to_vec(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn signal_with_start_starts_new_workflow_with_signal() {
    let h = Harness::new();
    let resp = h.engine.signal_with_start_workflow_execution(&sws_req("sws-1")).await.expect("sws");
    let snap = h.execs.snapshot(WF, &resp.run_id).expect("persisted");
    assert_eq!(snap.execution_info.signal_count, 1);
    // Started, Signaled, DecisionTaskScheduled
    assert_eq!(snap.execution_info.next_event_id, 4);
    assert_eq!(h.execs.current_run(WF).unwrap().run_id, resp.run_id);
}

#[tokio::test]
async fn signal_with_start_signals_running_workflow() {
    let h = Harness::new();
    let run = start(&h).await;
    let resp = h.engine.signal_with_start_workflow_execution(&sws_req("sws-2")).await.expect("sws");
    assert_eq!(resp.run_id, run);
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_eq!(info.signal_count, 1);
    assert_eq!(info.next_event_id, 4);
}

#[tokio::test]
async fn terminate_workflow_execution_closes_run() {
    let h = Harness::new();
    let run = start(&h).await;
    h.engine
        .terminate_workflow_execution(&HistoryTerminateWorkflowExecutionRequest {
            domain_uuid: DOMAIN_ID.into(),
            terminate_request: Some(TerminateWorkflowExecutionRequest {
                domain: DOMAIN_NAME.into(),
                workflow_execution: exec(&run),
                reason: "r".into(),
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
}

// ---- batch 2: heartbeat, timers, markers, request cancel

#[tokio::test]
async fn record_activity_heartbeat_success() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    activity_started(&h, &run, sched, "act-req").await.unwrap();
    let resp = h
        .engine
        .record_activity_task_heartbeat(&HistoryRecordActivityTaskHeartbeatRequest {
            domain_uuid: DOMAIN_ID.into(),
            heartbeat_request: Some(RecordActivityTaskHeartbeatRequest { task_token: activity_token(&run, sched), details: b"hb".to_vec(), identity: IDENTITY.into() }),
        })
        .await
        .expect("heartbeat");
    assert!(!resp.cancel_requested);
    // Heartbeat writes no history event.
    assert_eq!(h.execs.snapshot(WF, &run).unwrap().execution_info.next_event_id, 7);
}

#[tokio::test]
async fn respond_decision_completed_start_timer_creates_timer() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.unwrap();
    let timer = Decision {
        decision_type: Some(DecisionType::StartTimer),
        start_timer_decision_attributes: Some(StartTimerDecisionAttributes { timer_id: "t1".into(), start_to_fire_timeout_seconds: Some(5) }),
        ..Default::default()
    };
    decision_completed(&h, &run, 2, vec![timer]).await.expect("completed");
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert_eq!(snap.timer_infos.len(), 1);
    // + DecisionCompleted + TimerStarted
    assert_eq!(snap.execution_info.next_event_id, 6);
}

#[tokio::test]
async fn respond_decision_completed_record_marker_appends_event() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.unwrap();
    let marker = Decision {
        decision_type: Some(DecisionType::RecordMarker),
        record_marker_decision_attributes: Some(RecordMarkerDecisionAttributes { marker_name: "m".into(), ..Default::default() }),
        ..Default::default()
    };
    decision_completed(&h, &run, 2, vec![marker]).await.expect("completed");
    assert_eq!(h.execs.snapshot(WF, &run).unwrap().execution_info.next_event_id, 6);
}

#[tokio::test]
async fn request_cancel_workflow_execution_appends_event() {
    let h = Harness::new();
    let run = start(&h).await;
    h.engine
        .request_cancel_workflow_execution(&HistoryRequestCancelWorkflowExecutionRequest {
            domain_uuid: DOMAIN_ID.into(),
            cancel_request: Some(RequestCancelWorkflowExecutionRequest {
                domain: DOMAIN_NAME.into(),
                workflow_execution: exec(&run),
                identity: IDENTITY.into(),
                request_id: "cancel-req".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("cancel");
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert!(info.cancel_requested);
    assert_eq!(info.next_event_id, 4);
}

// ---- batch 3: activity canceled, continue-as-new, child, external signal/cancel, retry policy

/// Runs the first decision with one decision and returns the run id (decision completed at event 4).
async fn run_first_decision(h: &Harness, d: Decision) -> String {
    let run = start(h).await;
    decision_started(h, &run, 2, "dec-req").await.expect("decision started");
    decision_completed(h, &run, 2, vec![d]).await.expect("decision completed");
    run
}

async fn signal(h: &Harness, run: &str) {
    h.engine
        .signal_workflow_execution(&HistorySignalWorkflowExecutionRequest {
            domain_uuid: DOMAIN_ID.into(),
            signal_request: Some(SignalWorkflowExecutionRequest {
                domain: DOMAIN_NAME.into(),
                workflow_execution: exec(run),
                signal_name: "sig".into(),
                identity: IDENTITY.into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("signal");
}

fn cancel_activity_decision() -> Decision {
    Decision {
        decision_type: Some(DecisionType::RequestCancelActivityTask),
        request_cancel_activity_task_decision_attributes: Some(RequestCancelActivityTaskDecisionAttributes { activity_id: "act-1".into() }),
        ..Default::default()
    }
}

fn canceled_req(run: &str, sched: i64) -> HistoryRespondActivityTaskCanceledRequest {
    HistoryRespondActivityTaskCanceledRequest {
        domain_uuid: DOMAIN_ID.into(),
        cancel_request: Some(RespondActivityTaskCanceledRequest { task_token: activity_token(run, sched), details: b"d".to_vec(), identity: IDENTITY.into() }),
    }
}

#[tokio::test]
async fn respond_activity_canceled_after_cancel_requested_success() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    activity_started(&h, &run, sched, "act-req").await.unwrap();
    signal(&h, &run).await;
    // events: 6 started, 7 signaled, 8 decision scheduled
    decision_started(&h, &run, 8, "dec-req-2").await.expect("started");
    decision_completed(&h, &run, 8, vec![cancel_activity_decision()]).await.expect("completed");
    let ai = h.execs.snapshot(WF, &run).unwrap();
    assert_eq!(ai.activity_infos.len(), 1, "started activity stays until the worker responds");
    h.engine.respond_activity_task_canceled(&canceled_req(&run, sched)).await.expect("canceled");
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert!(snap.activity_infos.is_empty());
}

#[tokio::test]
async fn respond_activity_canceled_without_cancel_request_is_internal_error() {
    // Go AddActivityTaskCanceledEvent rejects an activity whose cancel was never requested.
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    activity_started(&h, &run, sched, "act-req").await.unwrap();
    let res = h.engine.respond_activity_task_canceled(&canceled_req(&run, sched)).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn respond_activity_canceled_not_started_is_error() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    assert!(h.engine.respond_activity_task_canceled(&canceled_req(&run, sched)).await.is_err());
}

#[tokio::test]
async fn request_cancel_activity_decision_before_start_cancels_activity() {
    let h = Harness::new();
    let (run, _) = with_scheduled_activity(&h).await;
    signal(&h, &run).await; // 6 signaled, 7 decision scheduled
    decision_started(&h, &run, 7, "dec-req-2").await.expect("started");
    decision_completed(&h, &run, 7, vec![cancel_activity_decision()]).await.expect("completed");
    let snap = h.execs.snapshot(WF, &run).unwrap();
    // Not-started activity is canceled immediately: no activity left.
    assert!(snap.activity_infos.is_empty());
}

#[tokio::test]
async fn respond_decision_continue_as_new_closes_run_and_starts_new() {
    let h = Harness::new();
    let d = Decision {
        decision_type: Some(DecisionType::ContinueAsNewWorkflowExecution),
        continue_as_new_workflow_execution_decision_attributes: Some(ContinueAsNewWorkflowExecutionDecisionAttributes {
            workflow_type: Some(WorkflowType { name: "wf-type".into() }),
            task_list: Some(tl()),
            input: b"next".to_vec(),
            execution_start_to_close_timeout_seconds: Some(100),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }),
        ..Default::default()
    };
    let run = run_first_decision(&h, d).await;
    let old = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert_eq!(old.state, WORKFLOW_STATE_COMPLETED);
    assert_eq!(old.close_status, WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW);
    let cur = h.execs.current_run(WF).unwrap();
    assert_ne!(cur.run_id, run);
    let new = h.execs.snapshot(WF, &cur.run_id).unwrap().execution_info;
    assert_ne!(new.state, WORKFLOW_STATE_COMPLETED);
    // WorkflowExecutionStarted + DecisionTaskScheduled
    assert_eq!(new.next_event_id, 3);
}

#[tokio::test]
async fn respond_decision_start_child_workflow_records_initiated() {
    let h = Harness::new();
    let d = Decision {
        decision_type: Some(DecisionType::StartChildWorkflowExecution),
        start_child_workflow_execution_decision_attributes: Some(StartChildWorkflowExecutionDecisionAttributes {
            domain: DOMAIN_NAME.into(),
            workflow_id: "child-1".into(),
            workflow_type: Some(WorkflowType { name: "child-type".into() }),
            task_list: Some(tl()),
            input: b"in".to_vec(),
            execution_start_to_close_timeout_seconds: Some(100),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }),
        ..Default::default()
    };
    let run = run_first_decision(&h, d).await;
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert_eq!(snap.child_execution_infos.len(), 1);
    // + DecisionCompleted + StartChildWorkflowExecutionInitiated
    assert_eq!(snap.execution_info.next_event_id, 6);
}

async fn start_child_with_policy(policy: Option<ParentClosePolicy>, expect: i32) {
    // TestRespondDecisionTaskCompletedStartChildWorkflowWithAbandonPolicy / WithTerminatePolicy.
    let h = Harness::new();
    let d = Decision {
        decision_type: Some(DecisionType::StartChildWorkflowExecution),
        start_child_workflow_execution_decision_attributes: Some(StartChildWorkflowExecutionDecisionAttributes {
            domain: DOMAIN_NAME.into(),
            workflow_id: "child-1".into(),
            workflow_type: Some(WorkflowType { name: "child-type".into() }),
            task_list: Some(tl()),
            execution_start_to_close_timeout_seconds: Some(100),
            task_start_to_close_timeout_seconds: Some(10),
            parent_close_policy: policy,
            ..Default::default()
        }),
        ..Default::default()
    };
    let run = run_first_decision(&h, d).await;
    let infos = h.execs.child_infos(WF, &run);
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].started_workflow_id, "child-1");
    assert_eq!(infos[0].parent_close_policy, expect);
}

#[tokio::test]
async fn respond_decision_start_child_abandon_policy() {
    start_child_with_policy(Some(ParentClosePolicy::Abandon), 0).await;
}

#[tokio::test]
async fn respond_decision_start_child_terminate_policy() {
    start_child_with_policy(Some(ParentClosePolicy::Terminate), 2).await;
}

#[tokio::test]
async fn respond_decision_signal_external_records_initiated() {
    let h = Harness::new();
    let d = Decision {
        decision_type: Some(DecisionType::SignalExternalWorkflowExecution),
        signal_external_workflow_execution_decision_attributes: Some(SignalExternalWorkflowExecutionDecisionAttributes {
            domain: DOMAIN_NAME.into(),
            execution: Some(WorkflowExecution { workflow_id: "other".into(), run_id: String::new() }),
            signal_name: "sig".into(),
            input: b"in".to_vec(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let run = run_first_decision(&h, d).await;
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert_eq!(snap.signal_infos.len(), 1);
    assert_eq!(snap.execution_info.next_event_id, 6);
}

#[tokio::test]
async fn respond_decision_request_cancel_external_records_initiated() {
    let h = Harness::new();
    let d = Decision {
        decision_type: Some(DecisionType::RequestCancelExternalWorkflowExecution),
        request_cancel_external_workflow_execution_decision_attributes: Some(RequestCancelExternalWorkflowExecutionDecisionAttributes {
            domain: DOMAIN_NAME.into(),
            workflow_id: "other".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let run = run_first_decision(&h, d).await;
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert_eq!(snap.request_cancel_infos.len(), 1);
    assert_eq!(snap.execution_info.next_event_id, 6);
}

#[tokio::test]
async fn start_workflow_with_retry_policy_persists_policy() {
    let h = Harness::new();
    let mut req = start_req("retry-req");
    req.start_request.as_mut().unwrap().retry_policy = Some(RetryPolicy {
        initial_interval_in_seconds: 1,
        backoff_coefficient: 2.0,
        maximum_interval_in_seconds: 10,
        maximum_attempts: 3,
        expiration_interval_in_seconds: 100,
        ..Default::default()
    });
    let run = h.engine.start_workflow_execution(&req).await.expect("start").run_id;
    let info = h.execs.snapshot(WF, &run).unwrap().execution_info;
    assert!(info.has_retry_policy);
    assert_eq!(info.maximum_attempts, 3);
    assert_eq!(info.initial_interval_nanos, 1_000_000_000);
}

#[tokio::test]
async fn respond_activity_failed_with_retry_policy_retries_without_history_event() {
    let h = Harness::new();
    let run = start(&h).await;
    decision_started(&h, &run, 2, "dec-req").await.unwrap();
    let mut d = schedule_activity_decision("act-1");
    d.schedule_activity_task_decision_attributes.as_mut().unwrap().retry_policy = Some(RetryPolicy {
        initial_interval_in_seconds: 1,
        backoff_coefficient: 2.0,
        maximum_interval_in_seconds: 10,
        maximum_attempts: 3,
        expiration_interval_in_seconds: 100,
        ..Default::default()
    });
    decision_completed(&h, &run, 2, vec![d]).await.unwrap();
    activity_started(&h, &run, 5, "act-req").await.unwrap();
    h.engine
        .respond_activity_task_failed(&HistoryRespondActivityTaskFailedRequest {
            domain_uuid: DOMAIN_ID.into(),
            failed_request: Some(RespondActivityTaskFailedRequest {
                task_token: activity_token(&run, 5),
                reason: Some("retryable".into()),
                identity: IDENTITY.into(),
                ..Default::default()
            }),
        })
        .await
        .expect("failed");
    let snap = h.execs.snapshot(WF, &run).unwrap();
    // Activity is retried: still pending, no ActivityTaskFailed event written.
    assert_eq!(snap.activity_infos.len(), 1);
    // The ActivityTaskStarted event is not written while the activity may still retry.
    assert_eq!(snap.execution_info.next_event_id, 6);
}

// ---- batch 4: reload after cache clear, child completed errors

#[tokio::test]
async fn reload_after_cache_clear_continues_workflow() {
    let h = Harness::new();
    let (run, sched) = with_scheduled_activity(&h).await;
    h.cache.clear_all_for_test();
    activity_started(&h, &run, sched, "act-req").await.expect("started after reload");
    h.cache.clear_all_for_test();
    h.engine
        .respond_activity_task_completed(&HistoryRespondActivityTaskCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            complete_request: Some(RespondActivityTaskCompletedRequest { task_token: activity_token(&run, sched), result: b"r".to_vec(), identity: IDENTITY.into() }),
        })
        .await
        .expect("completed after reload");
    let snap = h.execs.snapshot(WF, &run).unwrap();
    assert!(snap.activity_infos.is_empty());
    assert_eq!(snap.execution_info.next_event_id, 9);
}

#[tokio::test]
async fn reload_after_cache_clear_preserves_decision_state() {
    let h = Harness::new();
    let run = start(&h).await;
    h.cache.clear_all_for_test();
    decision_started(&h, &run, 2, "dec-req").await.expect("started");
    h.cache.clear_all_for_test();
    decision_completed(&h, &run, 2, vec![complete_decision()]).await.expect("completed");
    assert_eq!(h.execs.snapshot(WF, &run).unwrap().execution_info.state, WORKFLOW_STATE_COMPLETED);
}

#[tokio::test]
async fn record_child_execution_completed_unknown_child_entity_not_exists() {
    let h = Harness::new();
    let run = start(&h).await;
    let err = h
        .engine
        .record_child_execution_completed(&RecordChildExecutionCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            workflow_execution: exec(&run),
            initiated_id: 1,
            completed_execution: Some(WorkflowExecution { workflow_id: "child".into(), run_id: String::new() }),
            completion_event: Some(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionCompleted), ..Default::default() }),
            started_id: 3,
        })
        .await
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("child"), "{err}");
}

#[tokio::test]
async fn record_child_execution_completed_child_not_started_is_stale() {
    let h = Harness::new();
    let d = Decision {
        decision_type: Some(DecisionType::StartChildWorkflowExecution),
        start_child_workflow_execution_decision_attributes: Some(StartChildWorkflowExecutionDecisionAttributes {
            domain: DOMAIN_NAME.into(),
            workflow_id: "child-1".into(),
            workflow_type: Some(WorkflowType { name: "child-type".into() }),
            task_list: Some(tl()),
            execution_start_to_close_timeout_seconds: Some(100),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }),
        ..Default::default()
    };
    let run = run_first_decision(&h, d).await;
    let res = h
        .engine
        .record_child_execution_completed(&RecordChildExecutionCompletedRequest {
            domain_uuid: DOMAIN_ID.into(),
            workflow_execution: exec(&run),
            initiated_id: 5,
            completed_execution: Some(WorkflowExecution { workflow_id: "child-1".into(), run_id: String::new() }),
            completion_event: Some(HistoryEvent { event_type: Some(cadence_api::enums::EventType::WorkflowExecutionCompleted), ..Default::default() }),
            started_id: 7,
        })
        .await;
    assert!(res.is_err());
}

// ---- batch 5: query workflow (history_engine_test.go TestQueryWorkflow_*), sticky query, cancel heartbeat

mod query_ports {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use cadence_api::enums::{QueryConsistencyLevel, QueryRejectCondition, QueryResultType, WorkflowExecutionCloseStatus};

    use super::*;
    use crate::engine::runtime::{ClientChecker, QueryMatchingClient};
    use crate::engine::test_support::TestConfig;
    use crate::query::{Registry, TerminationState, TerminationType, WorkflowQueryResult as RegResult};

    /// Minimal fake Matching: records the task list of every QueryWorkflow call.
    #[derive(Default)]
    struct FakeMatching {
        answer: Vec<u8>,
        sticky_unavailable: bool,
        calls: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl QueryMatchingClient for FakeMatching {
        async fn query_workflow(&self, _: &str, _: &QueryWorkflowRequest, task_list: Option<&TaskList>) -> HResult<QueryWorkflowResponse> {
            self.calls.lock().unwrap().push(task_list.map(|t| t.name.clone()).unwrap_or_default());
            if self.sticky_unavailable {
                return Err(HistoryError::StickyWorkerUnavailable);
            }
            Ok(QueryWorkflowResponse { query_result: self.answer.clone(), ..Default::default() })
        }
    }

    struct SupportsSticky;
    impl ClientChecker for SupportsSticky {
        fn supports_sticky_query(&self, _: &str, _: &str) -> bool {
            true
        }
    }

    fn harness_with(matching: Arc<FakeMatching>, raw: Arc<FakeMatching>) -> Harness {
        Harness::build(move |e| {
            e.matching_client = matching;
            e.raw_matching_client = raw;
            e.client_checker = Arc::new(SupportsSticky);
        })
    }

    fn qreq(run: &str, level: Option<QueryConsistencyLevel>, reject: Option<QueryRejectCondition>) -> HistoryQueryWorkflowRequest {
        HistoryQueryWorkflowRequest {
            domain_uuid: DOMAIN_ID.into(),
            request: Some(QueryWorkflowRequest {
                domain: DOMAIN_NAME.into(),
                execution: exec(run),
                query: Some(WorkflowQuery::default()),
                query_reject_condition: reject,
                query_consistency_level: level,
            }),
        }
    }

    /// First decision completed (activity scheduled), then a signal schedules and starts a second decision.
    async fn with_in_flight_decision(h: &Harness) -> String {
        let (run, _) = with_scheduled_activity(h).await;
        signal(h, &run).await; // 6 signaled, 7 decision scheduled
        decision_started(h, &run, 7, "dec-req-2").await.expect("decision started");
        run
    }

    async fn raw_registry(h: &Harness, run: &str) -> Arc<Registry> {
        let (ctx, release) = h.engine.execution_cache.get_or_create_workflow_execution(DOMAIN_ID, &exec(run).unwrap()).await.expect("ctx");
        let state = ctx.load_workflow_execution().await.expect("load");
        let reg = state.get_query_registry().raw_registry_for_test().expect("raw registry");
        release(None);
        reg
    }

    fn assert_registry_empty(r: &Registry) {
        assert!(!r.has_buffered_query());
        assert!(!r.has_completed_query());
        assert!(!r.has_unblocked_query());
        assert!(!r.has_failed_query());
    }

    #[tokio::test]
    async fn query_strong_reject_when_consistent_query_not_enabled() {
        // TestQueryWorkflow_RejectBasedOnNotEnabled (cluster-level switch half).
        let h = Harness::build(|e| e.config = Arc::new(TestConfig { no_consistent_query: true, ..Default::default() }));
        let err = h.engine.query_workflow(&qreq("any", Some(QueryConsistencyLevel::Strong), None), None).await.unwrap_err();
        assert!(err.to_string().contains("does not enable strongly consistent query"), "{err}");
    }

    #[tokio::test]
    async fn query_strong_reject_when_consistent_query_not_enabled_by_domain() {
        // TestQueryWorkflow_RejectBasedOnNotEnabled (per-domain half).
        let h = Harness::build(|e| e.config = Arc::new(TestConfig { no_consistent_query_by_domain: true, ..Default::default() }));
        let err = h.engine.query_workflow(&qreq("any", Some(QueryConsistencyLevel::Strong), None), None).await.unwrap_err();
        assert!(err.to_string().contains("does not enable strongly consistent query"), "{err}");
    }

    #[tokio::test]
    async fn query_reject_based_on_completed() {
        let h = Harness::new();
        let run = run_first_decision(&h, complete_decision()).await;
        let resp = h.engine.query_workflow(&qreq(&run, None, Some(QueryRejectCondition::NotOpen)), None).await.expect("query").response.unwrap();
        assert!(resp.query_result.is_empty());
        assert_eq!(resp.query_rejected.unwrap().close_status, Some(WorkflowExecutionCloseStatus::Completed));
    }

    #[tokio::test]
    async fn query_reject_based_on_failed() {
        let h = Harness::new();
        let fail = Decision {
            decision_type: Some(DecisionType::FailWorkflowExecution),
            fail_workflow_execution_decision_attributes: Some(FailWorkflowExecutionDecisionAttributes { reason: Some("failure reason".into()), details: vec![1, 2, 3] }),
            ..Default::default()
        };
        let run = run_first_decision(&h, fail).await;
        for cond in [QueryRejectCondition::NotOpen, QueryRejectCondition::NotCompletedCleanly] {
            let resp = h.engine.query_workflow(&qreq(&run, None, Some(cond)), None).await.expect("query").response.unwrap();
            assert!(resp.query_result.is_empty());
            assert_eq!(resp.query_rejected.unwrap().close_status, Some(WorkflowExecutionCloseStatus::Failed));
        }
    }

    #[tokio::test]
    async fn query_first_decision_not_completed() {
        // TestQueryWorkflow_FirstDecisionNotCompleted: the load loop waits ~1s, then ErrQueryWorkflowBeforeFirstDecision.
        let h = Harness::new();
        let run = start(&h).await;
        decision_started(&h, &run, 2, "dec-req").await.unwrap();
        let err = h.engine.query_workflow(&qreq(&run, None, None), None).await.unwrap_err();
        assert!(err.to_string().contains("at least one decision task"), "{err:?}");
    }

    #[tokio::test]
    async fn query_directly_through_matching() {
        let matching = Arc::new(FakeMatching { answer: vec![1, 2, 3], ..Default::default() });
        let h = harness_with(matching.clone(), Arc::new(FakeMatching::default()));
        let run = with_in_flight_decision(&h).await;
        let resp = h
            .engine
            .query_workflow(&qreq(&run, Some(QueryConsistencyLevel::Eventual), Some(QueryRejectCondition::NotOpen)), None)
            .await
            .expect("query")
            .response
            .unwrap();
        assert!(resp.query_rejected.is_none());
        assert_eq!(resp.query_result, vec![1, 2, 3]);
        assert_eq!(matching.calls.lock().unwrap().as_slice(), [TL.to_string()]);
    }

    #[tokio::test]
    async fn query_consistent_buffer_full() {
        let h = Harness::new();
        let run = with_in_flight_decision(&h).await;
        let reg = raw_registry(&h, &run).await;
        reg.buffer_query(WorkflowQuery::default()); // max buffered query count is 1
        let err = h.engine.query_workflow(&qreq(&run, Some(QueryConsistencyLevel::Strong), None), None).await.unwrap_err();
        assert!(err.to_string().contains("consistent query buffer is full"), "{err}");
    }

    #[tokio::test]
    async fn query_decision_task_dispatch_timeout() {
        let h = Harness::new();
        let run = with_in_flight_decision(&h).await;
        let reg = raw_registry(&h, &run).await;
        let deadline = Instant::now() + Duration::from_millis(800);
        let res = h.engine.query_workflow(&qreq(&run, Some(QueryConsistencyLevel::Strong), None), Some(deadline)).await;
        assert!(res.is_err());
        assert_registry_empty(&reg);
    }

    #[tokio::test]
    async fn query_decision_task_dispatch_complete() {
        let h = Harness::new();
        let run = with_in_flight_decision(&h).await;
        let reg = raw_registry(&h, &run).await;
        let updater = {
            let reg = reg.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let ids = reg.get_buffered_ids();
                assert_eq!(ids.len(), 1);
                for id in ids {
                    let state = TerminationState {
                        termination_type: TerminationType::Completed,
                        query_result: Some(RegResult { result_type: Some(QueryResultType::Answered), answer: Some(vec![1, 2, 3]), error_message: String::new() }),
                        failure: None,
                    };
                    reg.set_termination_state(&id, state).unwrap();
                }
            }
        };
        let started = Instant::now();
        let req = qreq(&run, Some(QueryConsistencyLevel::Strong), None);
        let (res, _) = tokio::join!(h.engine.query_workflow(&req, None), updater);
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert_eq!(res.expect("query").response.unwrap().query_result, vec![1, 2, 3]);
        assert!(!reg.has_buffered_query());
        assert!(!reg.has_completed_query());
    }

    #[tokio::test]
    async fn query_decision_task_dispatch_unblocked() {
        let matching = Arc::new(FakeMatching { answer: vec![1, 2, 3], ..Default::default() });
        let h = harness_with(matching.clone(), Arc::new(FakeMatching::default()));
        let run = with_in_flight_decision(&h).await;
        let reg = raw_registry(&h, &run).await;
        let updater = {
            let reg = reg.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                for id in reg.get_buffered_ids() {
                    reg.set_termination_state(&id, TerminationState { termination_type: TerminationType::Unblocked, query_result: None, failure: None }).unwrap();
                }
            }
        };
        let req = qreq(&run, Some(QueryConsistencyLevel::Strong), None);
        let (res, _) = tokio::join!(h.engine.query_workflow(&req, None), updater);
        assert_eq!(res.expect("query").response.unwrap().query_result, vec![1, 2, 3]);
        assert_registry_empty(&reg);
        assert_eq!(matching.calls.lock().unwrap().len(), 1);
    }

    /// Completes the first decision with sticky attributes, leaving the run sticky-enabled with an activity pending.
    async fn sticky_run(h: &Harness) -> String {
        let run = start(h).await;
        decision_started(h, &run, 2, "dec-req").await.expect("started");
        h.engine
            .respond_decision_task_completed(&HistoryRespondDecisionTaskCompletedRequest {
                domain_uuid: DOMAIN_ID.into(),
                complete_request: Some(RespondDecisionTaskCompletedRequest {
                    task_token: token(&run, 2),
                    decisions: vec![schedule_activity_decision("act-1")],
                    identity: IDENTITY.into(),
                    sticky_attributes: Some(StickyExecutionAttributes {
                        worker_task_list: Some(TaskList { name: "sticky-tl".into(), ..Default::default() }),
                        schedule_to_start_timeout_seconds: Some(5),
                    }),
                    ..Default::default()
                }),
            })
            .await
            .expect("completed");
        run
    }

    #[tokio::test]
    async fn query_sticky_goes_to_sticky_task_list() {
        let matching = Arc::new(FakeMatching::default());
        let raw = Arc::new(FakeMatching { answer: vec![9], ..Default::default() });
        let h = harness_with(matching.clone(), raw.clone());
        let run = sticky_run(&h).await;
        let resp = h.engine.query_workflow(&qreq(&run, Some(QueryConsistencyLevel::Eventual), None), None).await.expect("query").response.unwrap();
        assert_eq!(resp.query_result, vec![9]);
        assert_eq!(raw.calls.lock().unwrap().as_slice(), ["sticky-tl".to_string()]);
        assert!(matching.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn query_sticky_worker_unavailable_falls_back_to_normal_task_list() {
        let matching = Arc::new(FakeMatching { answer: vec![7], ..Default::default() });
        let raw = Arc::new(FakeMatching { sticky_unavailable: true, ..Default::default() });
        let h = harness_with(matching.clone(), raw.clone());
        let run = sticky_run(&h).await;
        let resp = h.engine.query_workflow(&qreq(&run, Some(QueryConsistencyLevel::Eventual), None), None).await.expect("query").response.unwrap();
        assert_eq!(resp.query_result, vec![7]);
        assert_eq!(raw.calls.lock().unwrap().len(), 1);
        assert_eq!(matching.calls.lock().unwrap().as_slice(), [TL.to_string()]);
    }

    // ---- activity heartbeat after a cancel request (RecordActivityTaskHeartbeat returns CancelRequested)

    #[tokio::test]
    async fn record_activity_heartbeat_after_cancel_request_reports_cancel_requested() {
        let h = Harness::new();
        let (run, sched) = with_scheduled_activity(&h).await;
        activity_started(&h, &run, sched, "act-req").await.unwrap();
        signal(&h, &run).await; // 6 started? decision scheduled at 8 (see respond_activity_canceled_after_cancel_requested_success)
        decision_started(&h, &run, 8, "dec-req-2").await.expect("started");
        decision_completed(&h, &run, 8, vec![cancel_activity_decision()]).await.expect("completed");
        let resp = h
            .engine
            .record_activity_task_heartbeat(&HistoryRecordActivityTaskHeartbeatRequest {
                domain_uuid: DOMAIN_ID.into(),
                heartbeat_request: Some(RecordActivityTaskHeartbeatRequest { task_token: activity_token(&run, sched), details: b"hb".to_vec(), identity: IDENTITY.into() }),
            })
            .await
            .expect("heartbeat");
        assert!(resp.cancel_requested);
    }
}
