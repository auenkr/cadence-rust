//! Replay tests over hand-built histories (shapes taken from the Go client's
//! decision-task handler tests).

use std::time::Duration;

use cadence_proto::api as pb;
use cadence_proto::api::decision::Attributes as DA;
use cadence_proto::api::history_event::Attributes as A;

use super::converter;
use super::core::{ActivityOptions, WorkflowContext, WorkflowError, WorkflowResult};
use super::registry::Registry;
use super::replay::{process_decision_task, Query, ReplayError, TaskInput};
use crate::parentclosepolicy as pcp;

fn ev(id: i64, a: A) -> pb::HistoryEvent {
    pb::HistoryEvent {
        event_id: id,
        attributes: Some(a),
        ..Default::default()
    }
}

fn started(task_list: &str) -> A {
    A::WorkflowExecutionStartedEventAttributes(pb::WorkflowExecutionStartedEventAttributes {
        task_list: Some(pb::TaskList {
            name: task_list.to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn signal(name: &str, data: Vec<u8>) -> A {
    A::WorkflowExecutionSignaledEventAttributes(pb::WorkflowExecutionSignaledEventAttributes {
        signal_name: name.to_owned(),
        input: Some(pb::Payload { data }),
        ..Default::default()
    })
}

fn dt_sched() -> A {
    A::DecisionTaskScheduledEventAttributes(Default::default())
}
fn dt_started() -> A {
    A::DecisionTaskStartedEventAttributes(Default::default())
}
fn dt_completed() -> A {
    A::DecisionTaskCompletedEventAttributes(Default::default())
}

fn activity_scheduled(id: &str) -> A {
    A::ActivityTaskScheduledEventAttributes(pb::ActivityTaskScheduledEventAttributes {
        activity_id: id.to_owned(),
        ..Default::default()
    })
}

fn input(wf_type: &str, events: Vec<pb::HistoryEvent>) -> TaskInput {
    TaskInput {
        workflow_id: "wf".into(),
        run_id: "run".into(),
        workflow_type: wf_type.into(),
        domain: "cadence-system".into(),
        events,
        queries: Vec::new(),
    }
}

fn request() -> pcp::Request {
    pcp::Request {
        domain_name: "d".into(),
        ..Default::default()
    }
}

fn pcp_registry() -> Registry {
    let mut r = Registry::default();
    pcp::register_sdk(&mut r);
    r
}

#[test]
fn converter_roundtrip() {
    let v = converter::encode(&"x").unwrap();
    assert_eq!(v, b"\"x\"\n");
    let s: String = converter::decode(&v).unwrap();
    assert_eq!(s, "x");
}

#[test]
fn parent_close_policy_schedules_activity_for_signal() {
    let events = vec![
        ev(1, started(pcp::TASK_LIST)),
        ev(2, signal(pcp::SIGNAL_NAME, converter::encode(&request()).unwrap())),
        ev(3, dt_sched()),
        ev(4, dt_started()),
    ];
    let out = process_decision_task(&pcp_registry(), input(pcp::WORKFLOW_TYPE, events)).unwrap();
    assert_eq!(out.decisions.len(), 1);
    let Some(DA::ScheduleActivityTaskDecisionAttributes(a)) = &out.decisions[0].attributes else {
        panic!("expected schedule activity, got {:?}", out.decisions[0]);
    };
    assert_eq!(a.activity_id, "0");
    assert_eq!(a.activity_type.as_ref().unwrap().name, pcp::ACTIVITY_TYPE);
    assert_eq!(a.task_list.as_ref().unwrap().name, pcp::TASK_LIST);
    assert_eq!(a.schedule_to_start_timeout.as_ref().unwrap().seconds, 60);
    assert_eq!(a.start_to_close_timeout.as_ref().unwrap().seconds, 300);
    assert_eq!(a.schedule_to_close_timeout.as_ref().unwrap().seconds, 360);
    let retry = a.retry_policy.as_ref().unwrap();
    assert_eq!(retry.initial_interval.as_ref().unwrap().seconds, 10);
    assert!((retry.backoff_coefficient - 1.7).abs() < 1e-9);
    assert_eq!(retry.maximum_interval.as_ref().unwrap().seconds, 300);
    assert_eq!(retry.expiration_interval.as_ref().unwrap().seconds, 630_720_000);
    let sent: pcp::Request = converter::decode(&a.input.as_ref().unwrap().data).unwrap();
    assert_eq!(sent, request());
}

fn pcp_after_activity(result: A) -> Vec<pb::HistoryEvent> {
    vec![
        ev(1, started(pcp::TASK_LIST)),
        ev(2, signal(pcp::SIGNAL_NAME, converter::encode(&request()).unwrap())),
        ev(3, dt_sched()),
        ev(4, dt_started()),
        ev(5, dt_completed()),
        ev(6, activity_scheduled("0")),
        ev(
            7,
            A::ActivityTaskStartedEventAttributes(pb::ActivityTaskStartedEventAttributes {
                scheduled_event_id: 6,
                ..Default::default()
            }),
        ),
        ev(8, result),
        ev(9, dt_sched()),
        ev(10, dt_started()),
    ]
}

#[test]
fn parent_close_policy_completes_after_activity_success() {
    let done = A::ActivityTaskCompletedEventAttributes(pb::ActivityTaskCompletedEventAttributes {
        scheduled_event_id: 6,
        ..Default::default()
    });
    let out = process_decision_task(
        &pcp_registry(),
        input(pcp::WORKFLOW_TYPE, pcp_after_activity(done)),
    )
    .unwrap();
    assert_eq!(out.decisions.len(), 1);
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::CompleteWorkflowExecutionDecisionAttributes(_))
    ));
}

#[test]
fn parent_close_policy_ignores_activity_failure() {
    let failed = A::ActivityTaskFailedEventAttributes(pb::ActivityTaskFailedEventAttributes {
        scheduled_event_id: 6,
        failure: Some(pb::Failure {
            reason: "boom".into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    let out = process_decision_task(
        &pcp_registry(),
        input(pcp::WORKFLOW_TYPE, pcp_after_activity(failed)),
    )
    .unwrap();
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::CompleteWorkflowExecutionDecisionAttributes(_))
    ));
}

#[test]
fn parent_close_policy_without_signal_completes_immediately() {
    let events = vec![ev(1, started(pcp::TASK_LIST)), ev(2, dt_sched()), ev(3, dt_started())];
    let out = process_decision_task(&pcp_registry(), input(pcp::WORKFLOW_TYPE, events)).unwrap();
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::CompleteWorkflowExecutionDecisionAttributes(_))
    ));
}

#[test]
fn replay_detects_nondeterminism() {
    // History recorded a timer where the workflow now schedules an activity.
    let events = vec![
        ev(1, started(pcp::TASK_LIST)),
        ev(2, signal(pcp::SIGNAL_NAME, converter::encode(&request()).unwrap())),
        ev(3, dt_sched()),
        ev(4, dt_started()),
        ev(5, dt_completed()),
        ev(
            6,
            A::TimerStartedEventAttributes(pb::TimerStartedEventAttributes {
                timer_id: "0".into(),
                ..Default::default()
            }),
        ),
        ev(7, dt_sched()),
        ev(8, dt_started()),
    ];
    let err = process_decision_task(&pcp_registry(), input(pcp::WORKFLOW_TYPE, events))
        .err()
        .expect("must fail");
    assert!(matches!(err, ReplayError::NonDeterministic(_)), "{err:?}");
}

#[test]
fn unknown_workflow_type_is_reported() {
    let events = vec![ev(1, started("tl")), ev(2, dt_sched()), ev(3, dt_started())];
    let err = process_decision_task(&Registry::default(), input("nope", events))
        .err()
        .expect("must fail");
    assert!(matches!(err, ReplayError::UnknownWorkflowType(_)));
}

async fn timer_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    let _ = ctx.timer(Duration::from_secs(5)).await;
    Ok(Some(converter::encode(&"done").unwrap()))
}

#[test]
fn timer_start_and_fire() {
    let mut r = Registry::default();
    r.register_workflow("timer", timer_workflow);
    let first = vec![ev(1, started("tl")), ev(2, dt_sched()), ev(3, dt_started())];
    let out = process_decision_task(&r, input("timer", first)).unwrap();
    let Some(DA::StartTimerDecisionAttributes(t)) = &out.decisions[0].attributes else {
        panic!("expected start timer");
    };
    assert_eq!(t.timer_id, "0");
    assert_eq!(t.start_to_fire_timeout.as_ref().unwrap().seconds, 5);

    let second = vec![
        ev(1, started("tl")),
        ev(2, dt_sched()),
        ev(3, dt_started()),
        ev(4, dt_completed()),
        ev(
            5,
            A::TimerStartedEventAttributes(pb::TimerStartedEventAttributes {
                timer_id: "0".into(),
                ..Default::default()
            }),
        ),
        ev(
            6,
            A::TimerFiredEventAttributes(pb::TimerFiredEventAttributes {
                timer_id: "0".into(),
                started_event_id: 5,
            }),
        ),
        ev(7, dt_sched()),
        ev(8, dt_started()),
    ];
    let out = process_decision_task(&r, input("timer", second)).unwrap();
    let Some(DA::CompleteWorkflowExecutionDecisionAttributes(c)) = &out.decisions[0].attributes
    else {
        panic!("expected complete");
    };
    assert_eq!(c.result.as_ref().unwrap().data, b"\"done\"\n");
}

async fn side_effect_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    let v: i32 = ctx.side_effect(|| 42)?;
    Ok(Some(converter::encode(&v).unwrap()))
}

#[test]
fn side_effect_records_marker_then_replays_value() {
    let mut r = Registry::default();
    r.register_workflow("se", side_effect_workflow);
    let first = vec![ev(1, started("tl")), ev(2, dt_sched()), ev(3, dt_started())];
    let out = process_decision_task(&r, input("se", first)).unwrap();
    assert_eq!(out.decisions.len(), 2);
    let Some(DA::RecordMarkerDecisionAttributes(m)) = &out.decisions[0].attributes else {
        panic!("expected marker");
    };
    assert_eq!(m.marker_name, "SideEffect");
    let marker_details = m.details.clone().unwrap();

    // Replay with the recorded marker: the closure result comes from history.
    let second = vec![
        ev(1, started("tl")),
        ev(2, dt_sched()),
        ev(3, dt_started()),
        ev(4, dt_completed()),
        ev(
            5,
            A::MarkerRecordedEventAttributes(pb::MarkerRecordedEventAttributes {
                marker_name: "SideEffect".into(),
                details: Some(marker_details),
                ..Default::default()
            }),
        ),
        ev(
            6,
            A::WorkflowExecutionCompletedEventAttributes(Default::default()),
        ),
    ];
    let out = process_decision_task(&r, input("se", second)).unwrap();
    assert!(out.decisions.is_empty());
}

async fn can_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    Err(ctx.continue_as_new(&7))
}

#[test]
fn continue_as_new_decision() {
    let mut r = Registry::default();
    r.register_workflow("can", can_workflow);
    let events = vec![ev(1, started("tl")), ev(2, dt_sched()), ev(3, dt_started())];
    let out = process_decision_task(&r, input("can", events)).unwrap();
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::ContinueAsNewWorkflowExecutionDecisionAttributes(_))
    ));
}

async fn activity_cancel_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    let options = ActivityOptions {
        schedule_to_start: Duration::from_secs(1),
        start_to_close: Duration::from_secs(1),
        ..Default::default()
    };
    let f = ctx.execute_activity("a", &options, &1);
    f.cancel();
    let _ = f.await;
    Ok(None)
}

#[test]
fn cancelling_unsent_activity_removes_decision() {
    let mut r = Registry::default();
    r.register_workflow("c", activity_cancel_workflow);
    let events = vec![ev(1, started("tl")), ev(2, dt_sched()), ev(3, dt_started())];
    let out = process_decision_task(&r, input("c", events)).unwrap();
    assert_eq!(out.decisions.len(), 1);
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::CompleteWorkflowExecutionDecisionAttributes(_))
    ));
}

async fn query_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    ctx.set_query_handler("state", |_args: &[u8]| Ok(converter::encode(&"ok").unwrap()));
    std::future::pending::<()>().await;
    Err(WorkflowError::generic("unreachable"))
}

#[test]
fn query_is_answered_and_unknown_query_fails() {
    let mut r = Registry::default();
    r.register_workflow("q", query_workflow);
    let events = vec![ev(1, started("tl")), ev(2, dt_sched()), ev(3, dt_started())];
    let mut task = input("q", events);
    task.queries = vec![
        Query {
            id: "1".into(),
            query_type: "state".into(),
            args: Vec::new(),
        },
        Query {
            id: "2".into(),
            query_type: "missing".into(),
            args: Vec::new(),
        },
    ];
    let out = process_decision_task(&r, task).unwrap();
    assert_eq!(out.query_results[0].1, Ok(b"\"ok\"\n".to_vec()));
    let err = out.query_results[1].1.clone().unwrap_err();
    assert!(err.starts_with("unknown queryType missing"), "{err}");
}
