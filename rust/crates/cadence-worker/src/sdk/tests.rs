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

// ------------------------------------------------- step 3: SDK features

use super::features::{
    ChildError, ChildWorkflowOptions, ExternalError, LocalActivityOptions, LocalFailure,
};
use std::sync::atomic::{AtomicU32, Ordering};

fn first_task() -> Vec<pb::HistoryEvent> {
    vec![ev(1, started("tl")), ev(2, dt_sched()), ev(3, dt_started())]
}

fn execution(id: &str, run: &str) -> Option<pb::WorkflowExecution> {
    Some(pb::WorkflowExecution {
        workflow_id: id.into(),
        run_id: run.into(),
    })
}

fn child_options() -> ChildWorkflowOptions {
    ChildWorkflowOptions {
        workflow_id: Some("child-1".into()),
        execution_start_to_close: Duration::from_secs(60),
        ..Default::default()
    }
}

fn child_initiated() -> A {
    A::StartChildWorkflowExecutionInitiatedEventAttributes(
        pb::StartChildWorkflowExecutionInitiatedEventAttributes {
            workflow_id: "child-1".into(),
            ..Default::default()
        },
    )
}

fn child_started() -> A {
    A::ChildWorkflowExecutionStartedEventAttributes(pb::ChildWorkflowExecutionStartedEventAttributes {
        workflow_execution: execution("child-1", "r1"),
        initiated_event_id: 5,
        ..Default::default()
    })
}

async fn child_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    let out: String = ctx
        .execute_child_workflow("child", &child_options(), &"in")
        .get()
        .await?;
    Ok(Some(converter::encode(&out).unwrap()))
}

fn child_registry() -> Registry {
    let mut r = Registry::default();
    r.register_workflow("p", child_workflow);
    r
}

#[test]
fn child_workflow_start_decision_uses_go_defaults() {
    let out = process_decision_task(&child_registry(), input("p", first_task())).unwrap();
    assert_eq!(out.decisions.len(), 1);
    let Some(DA::StartChildWorkflowExecutionDecisionAttributes(a)) = &out.decisions[0].attributes
    else {
        panic!("expected start child");
    };
    assert_eq!(a.domain, "cadence-system");
    assert_eq!(a.workflow_id, "child-1");
    assert_eq!(a.workflow_type.as_ref().unwrap().name, "child");
    assert_eq!(a.task_list.as_ref().unwrap().name, "tl");
    assert_eq!(a.input.as_ref().unwrap().data, b"\"in\"\n");
    assert_eq!(a.execution_start_to_close_timeout.as_ref().unwrap().seconds, 60);
    assert_eq!(a.task_start_to_close_timeout.as_ref().unwrap().seconds, 10);
    assert_eq!(a.parent_close_policy, pb::ParentClosePolicy::Abandon as i32);
    assert_eq!(
        a.workflow_id_reuse_policy,
        pb::WorkflowIdReusePolicy::AllowDuplicateFailedOnly as i32
    );
}

fn child_history(close: A) -> Vec<pb::HistoryEvent> {
    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(5, child_initiated()),
        ev(6, child_started()),
        ev(7, close),
        ev(8, dt_sched()),
        ev(9, dt_started()),
    ]);
    e
}

#[test]
fn child_workflow_result_completes_parent() {
    let close = A::ChildWorkflowExecutionCompletedEventAttributes(
        pb::ChildWorkflowExecutionCompletedEventAttributes {
            workflow_execution: execution("child-1", "r1"),
            initiated_event_id: 5,
            started_event_id: 6,
            result: Some(pb::Payload {
                data: b"\"out\"\n".to_vec(),
            }),
            ..Default::default()
        },
    );
    let out = process_decision_task(&child_registry(), input("p", child_history(close))).unwrap();
    let Some(DA::CompleteWorkflowExecutionDecisionAttributes(c)) = &out.decisions[0].attributes
    else {
        panic!("expected complete, got {:?}", out.decisions);
    };
    assert_eq!(c.result.as_ref().unwrap().data, b"\"out\"\n");
}

#[test]
fn child_workflow_failure_fails_parent_with_child_reason() {
    let close = A::ChildWorkflowExecutionFailedEventAttributes(
        pb::ChildWorkflowExecutionFailedEventAttributes {
            workflow_execution: execution("child-1", "r1"),
            failure: Some(pb::Failure {
                reason: "custom".into(),
                details: b"d".to_vec(),
                options: None,
            }),
            ..Default::default()
        },
    );
    let out = process_decision_task(&child_registry(), input("p", child_history(close))).unwrap();
    let Some(DA::FailWorkflowExecutionDecisionAttributes(f)) = &out.decisions[0].attributes else {
        panic!("expected fail");
    };
    assert_eq!(f.failure.as_ref().unwrap().reason, "custom");
}

#[test]
fn child_workflow_terminal_events_map_to_errors() {
    for (close, want) in [
        (
            A::ChildWorkflowExecutionTimedOutEventAttributes(
                pb::ChildWorkflowExecutionTimedOutEventAttributes {
                    workflow_execution: execution("child-1", "r1"),
                    timeout_type: 2,
                    ..Default::default()
                },
            ),
            "timed out",
        ),
        (
            A::ChildWorkflowExecutionTerminatedEventAttributes(
                pb::ChildWorkflowExecutionTerminatedEventAttributes {
                    workflow_execution: execution("child-1", "r1"),
                    ..Default::default()
                },
            ),
            "Terminated",
        ),
    ] {
        let out =
            process_decision_task(&child_registry(), input("p", child_history(close))).unwrap();
        let Some(DA::FailWorkflowExecutionDecisionAttributes(f)) = &out.decisions[0].attributes
        else {
            panic!("expected fail");
        };
        let msg: String = converter::decode(&f.failure.as_ref().unwrap().details).unwrap();
        assert!(msg.contains(want), "{msg}");
    }
}

#[test]
fn start_child_failed_reports_already_started() {
    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(5, child_initiated()),
        ev(
            6,
            A::StartChildWorkflowExecutionFailedEventAttributes(
                pb::StartChildWorkflowExecutionFailedEventAttributes {
                    workflow_id: "child-1".into(),
                    initiated_event_id: 5,
                    ..Default::default()
                },
            ),
        ),
        ev(7, dt_sched()),
        ev(8, dt_started()),
    ]);
    let out = process_decision_task(&child_registry(), input("p", e)).unwrap();
    let Some(DA::FailWorkflowExecutionDecisionAttributes(f)) = &out.decisions[0].attributes else {
        panic!("expected fail");
    };
    let msg: String = converter::decode(&f.failure.as_ref().unwrap().details).unwrap();
    assert!(msg.contains("already started"), "{msg}");
}

async fn child_default_id_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    let options = ChildWorkflowOptions {
        execution_start_to_close: Duration::from_secs(5),
        task_list: Some("other".into()),
        ..Default::default()
    };
    let f = ctx.execute_child_workflow("child", &options, &());
    let _ = f.workflow_id();
    std::future::pending::<()>().await;
    Ok(None)
}

#[test]
fn child_workflow_default_id_uses_run_id_and_sequence_and_validates() {
    let mut r = Registry::default();
    r.register_workflow("d", child_default_id_workflow);
    let out = process_decision_task(&r, input("d", first_task())).unwrap();
    let Some(DA::StartChildWorkflowExecutionDecisionAttributes(a)) = &out.decisions[0].attributes
    else {
        panic!("expected start child");
    };
    assert_eq!(a.workflow_id, "run_0");
    assert_eq!(a.task_list.as_ref().unwrap().name, "other");

    async fn invalid(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
        let r = ctx
            .execute_child_workflow("c", &ChildWorkflowOptions::default(), &())
            .await;
        assert!(matches!(r, Err(ChildError::Invalid(_))));
        Ok(None)
    }
    let mut r = Registry::default();
    r.register_workflow("i", invalid);
    let out = process_decision_task(&r, input("i", first_task())).unwrap();
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::CompleteWorkflowExecutionDecisionAttributes(_))
    ));
}

async fn child_cancel_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    let f = ctx.execute_child_workflow("child", &child_options(), &());
    ctx.cancelled().await;
    f.cancel();
    match f.await {
        Err(ChildError::Canceled) => Err(WorkflowError::Canceled),
        other => Err(WorkflowError::generic(format!("unexpected {other:?}"))),
    }
}

fn cancel_requested() -> A {
    A::WorkflowExecutionCancelRequestedEventAttributes(Default::default())
}

fn child_cancel_registry() -> Registry {
    let mut r = Registry::default();
    r.register_workflow("cc", child_cancel_workflow);
    r
}

#[test]
fn started_child_cancel_emits_child_only_request_and_completes_canceled() {
    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(5, child_initiated()),
        ev(6, child_started()),
        ev(7, cancel_requested()),
        ev(8, dt_sched()),
        ev(9, dt_started()),
    ]);
    let out = process_decision_task(&child_cancel_registry(), input("cc", e.clone())).unwrap();
    let Some(DA::RequestCancelExternalWorkflowExecutionDecisionAttributes(a)) =
        &out.decisions[0].attributes
    else {
        panic!("expected request cancel, got {:?}", out.decisions);
    };
    assert!(a.child_workflow_only);
    assert!(a.control.is_empty());
    assert_eq!(a.workflow_execution.as_ref().unwrap().workflow_id, "child-1");
    assert_eq!(a.domain, "cadence-system");

    e.extend([
        ev(10, dt_completed()),
        ev(
            11,
            A::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(
                pb::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                    workflow_execution: execution("child-1", ""),
                    child_workflow_only: true,
                    ..Default::default()
                },
            ),
        ),
        ev(
            12,
            A::ExternalWorkflowExecutionCancelRequestedEventAttributes(
                pb::ExternalWorkflowExecutionCancelRequestedEventAttributes {
                    initiated_event_id: 11,
                    workflow_execution: execution("child-1", "r1"),
                    ..Default::default()
                },
            ),
        ),
        ev(
            13,
            A::ChildWorkflowExecutionCanceledEventAttributes(
                pb::ChildWorkflowExecutionCanceledEventAttributes {
                    workflow_execution: execution("child-1", "r1"),
                    ..Default::default()
                },
            ),
        ),
        ev(14, dt_sched()),
        ev(15, dt_started()),
    ]);
    let out = process_decision_task(&child_cancel_registry(), input("cc", e)).unwrap();
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::CancelWorkflowExecutionDecisionAttributes(_))
    ));
}

#[test]
fn child_cancel_before_started_is_forwarded_once_started() {
    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(5, child_initiated()),
        ev(6, cancel_requested()),
        ev(7, dt_sched()),
        ev(8, dt_started()),
    ]);
    // Cancel requested before the child started: no cancel decision yet.
    let out = process_decision_task(&child_cancel_registry(), input("cc", e.clone())).unwrap();
    assert!(out.decisions.is_empty());

    e.extend([
        ev(9, dt_completed()),
        ev(10, child_started()),
        ev(11, dt_sched()),
        ev(12, dt_started()),
    ]);
    let out = process_decision_task(&child_cancel_registry(), input("cc", e)).unwrap();
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::RequestCancelExternalWorkflowExecutionDecisionAttributes(_))
    ));
}

async fn signal_external_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    ctx.signal_external_workflow("d", "other", "", "sig", &5).await?;
    Ok(None)
}

fn signal_external_history(reply: A) -> Vec<pb::HistoryEvent> {
    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(
            5,
            A::SignalExternalWorkflowExecutionInitiatedEventAttributes(
                pb::SignalExternalWorkflowExecutionInitiatedEventAttributes {
                    domain: "d".into(),
                    workflow_execution: execution("other", ""),
                    signal_name: "sig".into(),
                    control: b"0".to_vec(),
                    ..Default::default()
                },
            ),
        ),
        ev(6, reply),
        ev(7, dt_sched()),
        ev(8, dt_started()),
    ]);
    e
}

#[test]
fn signal_external_workflow_decision_and_results() {
    let mut r = Registry::default();
    r.register_workflow("s", signal_external_workflow);
    let out = process_decision_task(&r, input("s", first_task())).unwrap();
    let Some(DA::SignalExternalWorkflowExecutionDecisionAttributes(a)) =
        &out.decisions[0].attributes
    else {
        panic!("expected signal external");
    };
    assert_eq!(a.control, b"0");
    assert_eq!(a.signal_name, "sig");
    assert_eq!(a.input.as_ref().unwrap().data, b"5\n");
    assert!(!a.child_workflow_only);
    assert_eq!(a.workflow_execution.as_ref().unwrap().workflow_id, "other");

    let ok = A::ExternalWorkflowExecutionSignaledEventAttributes(
        pb::ExternalWorkflowExecutionSignaledEventAttributes {
            initiated_event_id: 5,
            ..Default::default()
        },
    );
    let out = process_decision_task(&r, input("s", signal_external_history(ok))).unwrap();
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::CompleteWorkflowExecutionDecisionAttributes(_))
    ));

    let failed = A::SignalExternalWorkflowExecutionFailedEventAttributes(
        pb::SignalExternalWorkflowExecutionFailedEventAttributes {
            cause: pb::SignalExternalWorkflowExecutionFailedCause::UnknownExternalWorkflowExecution
                as i32,
            initiated_event_id: 5,
            ..Default::default()
        },
    );
    let out = process_decision_task(&r, input("s", signal_external_history(failed))).unwrap();
    let Some(DA::FailWorkflowExecutionDecisionAttributes(f)) = &out.decisions[0].attributes else {
        panic!("expected fail");
    };
    let msg: String = converter::decode(&f.failure.as_ref().unwrap().details).unwrap();
    assert_eq!(msg, ExternalError::UnknownExternalWorkflowExecution.to_string());
}

async fn cancel_external_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    ctx.request_cancel_external_workflow("d", "other", "run").await?;
    Ok(None)
}

#[test]
fn cancel_external_workflow_decision_and_result() {
    let mut r = Registry::default();
    r.register_workflow("ce", cancel_external_workflow);
    let out = process_decision_task(&r, input("ce", first_task())).unwrap();
    let Some(DA::RequestCancelExternalWorkflowExecutionDecisionAttributes(a)) =
        &out.decisions[0].attributes
    else {
        panic!("expected cancel external");
    };
    assert_eq!(a.control, b"0");
    assert!(!a.child_workflow_only);
    assert_eq!(a.workflow_execution.as_ref().unwrap().run_id, "run");

    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(
            5,
            A::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(
                pb::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                    domain: "d".into(),
                    workflow_execution: execution("other", "run"),
                    control: b"0".to_vec(),
                    ..Default::default()
                },
            ),
        ),
        ev(
            6,
            A::RequestCancelExternalWorkflowExecutionFailedEventAttributes(
                pb::RequestCancelExternalWorkflowExecutionFailedEventAttributes {
                    cause: 2,
                    initiated_event_id: 5,
                    workflow_execution: execution("other", "run"),
                    ..Default::default()
                },
            ),
        ),
        ev(7, dt_sched()),
        ev(8, dt_started()),
    ]);
    let out = process_decision_task(&r, input("ce", e)).unwrap();
    let Some(DA::FailWorkflowExecutionDecisionAttributes(f)) = &out.decisions[0].attributes else {
        panic!("expected fail");
    };
    let msg: String = converter::decode(&f.failure.as_ref().unwrap().details).unwrap();
    assert!(msg.starts_with("cancel external workflow failed"), "{msg}");
}

async fn upsert_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    assert!(ctx.upsert_search_attributes(&[]).is_err());
    assert!(ctx
        .upsert_search_attributes(&[("CadenceChangeVersion", serde_json::json!("x"))])
        .is_err());
    ctx.upsert_search_attributes(&[("CustomKeywordField", serde_json::json!("v"))])
        .unwrap();
    assert_eq!(ctx.info().search_attributes["CustomKeywordField"], b"\"v\"");
    Ok(None)
}

fn upsert_event() -> A {
    A::UpsertWorkflowSearchAttributesEventAttributes(Default::default())
}

#[test]
fn upsert_search_attributes_decision_and_replay() {
    let mut r = Registry::default();
    r.register_workflow("u", upsert_workflow);
    let out = process_decision_task(&r, input("u", first_task())).unwrap();
    assert_eq!(out.decisions.len(), 2);
    let Some(DA::UpsertWorkflowSearchAttributesDecisionAttributes(a)) =
        &out.decisions[0].attributes
    else {
        panic!("expected upsert");
    };
    assert_eq!(
        a.search_attributes.as_ref().unwrap().indexed_fields["CustomKeywordField"].data,
        b"\"v\""
    );

    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(5, upsert_event()),
        ev(6, A::WorkflowExecutionCompletedEventAttributes(Default::default())),
    ]);
    let out = process_decision_task(&r, input("u", e)).unwrap();
    assert!(out.decisions.is_empty());
}

async fn version_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    let v = ctx.get_version("chg", -1, 2)?;
    Ok(Some(converter::encode(&v).unwrap()))
}

#[test]
fn get_version_records_marker_and_change_version_upsert() {
    let mut r = Registry::default();
    r.register_workflow("v", version_workflow);
    let out = process_decision_task(&r, input("v", first_task())).unwrap();
    assert_eq!(out.decisions.len(), 3);
    let Some(DA::RecordMarkerDecisionAttributes(m)) = &out.decisions[0].attributes else {
        panic!("expected marker");
    };
    assert_eq!(m.marker_name, "Version");
    let details = m.details.clone().unwrap();
    assert_eq!(details.data, b"\"chg\"\n2\n");
    let Some(DA::UpsertWorkflowSearchAttributesDecisionAttributes(u)) =
        &out.decisions[1].attributes
    else {
        panic!("expected upsert");
    };
    let upsert = u.search_attributes.clone().unwrap();
    assert_eq!(upsert.indexed_fields["CadenceChangeVersion"].data, b"[\"chg-2\"]");

    // Replay the recorded history: marker and upsert need no matching decision.
    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(
            5,
            A::MarkerRecordedEventAttributes(pb::MarkerRecordedEventAttributes {
                marker_name: "Version".into(),
                details: Some(details),
                ..Default::default()
            }),
        ),
        ev(
            6,
            A::UpsertWorkflowSearchAttributesEventAttributes(
                pb::UpsertWorkflowSearchAttributesEventAttributes {
                    search_attributes: Some(upsert),
                    ..Default::default()
                },
            ),
        ),
        ev(7, A::WorkflowExecutionCompletedEventAttributes(Default::default())),
    ]);
    let out = process_decision_task(&r, input("v", e)).unwrap();
    assert!(out.decisions.is_empty());

    // History without a marker replays as DefaultVersion and records nothing.
    let mut old = first_task();
    old.extend([
        ev(4, dt_completed()),
        ev(5, A::WorkflowExecutionCompletedEventAttributes(Default::default())),
    ]);
    let out = process_decision_task(&r, input("v", old)).unwrap();
    assert!(out.decisions.is_empty());
}

static LA_RUNS: AtomicU32 = AtomicU32::new(0);

async fn local_activity_workflow(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
    let options = LocalActivityOptions {
        schedule_to_close: Duration::from_secs(10),
        ..Default::default()
    };
    let v: i32 = ctx
        .execute_local_activity("pkg.la", &options, || {
            LA_RUNS.fetch_add(1, Ordering::SeqCst);
            Ok::<_, LocalFailure>(7)
        })
        .get()
        .await?;
    Ok(Some(converter::encode(&(v + 1)).unwrap()))
}

#[test]
fn local_activity_runs_once_records_marker_and_replays_from_history() {
    let mut r = Registry::default();
    r.register_workflow("la", local_activity_workflow);
    let out = process_decision_task(&r, input("la", first_task())).unwrap();
    assert_eq!(LA_RUNS.load(Ordering::SeqCst), 1);
    assert_eq!(out.decisions.len(), 2);
    let Some(DA::RecordMarkerDecisionAttributes(m)) = &out.decisions[0].attributes else {
        panic!("expected marker");
    };
    assert_eq!(m.marker_name, "LocalActivity");
    let details = m.details.clone().unwrap();
    let json: serde_json::Value = converter::decode(&details.data).unwrap();
    assert_eq!(json["activityId"], "0");
    assert_eq!(json["activityType"], "pkg.la");
    assert_eq!(json["resultJson"], "7\n");
    let Some(DA::CompleteWorkflowExecutionDecisionAttributes(c)) = &out.decisions[1].attributes
    else {
        panic!("expected complete");
    };
    assert_eq!(c.result.as_ref().unwrap().data, b"8\n");

    // Replay: the closure must not run again; the marker supplies the result.
    let mut e = first_task();
    e.extend([
        ev(4, dt_completed()),
        ev(
            5,
            A::MarkerRecordedEventAttributes(pb::MarkerRecordedEventAttributes {
                marker_name: "LocalActivity".into(),
                details: Some(details),
                ..Default::default()
            }),
        ),
        ev(6, A::WorkflowExecutionCompletedEventAttributes(Default::default())),
    ]);
    let out = process_decision_task(&r, input("la", e)).unwrap();
    assert!(out.decisions.is_empty());
    assert_eq!(LA_RUNS.load(Ordering::SeqCst), 1);
}

#[test]
fn local_activity_requires_schedule_to_close() {
    async fn bad(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
        let r = ctx
            .execute_local_activity("la", &LocalActivityOptions::default(), || {
                Ok::<_, LocalFailure>(1)
            })
            .await;
        assert!(r.is_err());
        Ok(None)
    }
    let mut r = Registry::default();
    r.register_workflow("bad", bad);
    let out = process_decision_task(&r, input("bad", first_task())).unwrap();
    assert!(matches!(
        out.decisions[0].attributes,
        Some(DA::CompleteWorkflowExecutionDecisionAttributes(_))
    ));
}

#[test]
fn local_activity_failure_is_recorded_and_surfaced() {
    async fn failing(ctx: WorkflowContext, _: Vec<u8>) -> WorkflowResult {
        let options = LocalActivityOptions {
            schedule_to_close: Duration::from_secs(10),
            ..Default::default()
        };
        let r: Result<i32, _> = ctx
            .execute_local_activity("la", &options, || {
                Err::<i32, _>(LocalFailure {
                    reason: "boom".into(),
                    details: b"\"why\"\n".to_vec(),
                })
            })
            .get()
            .await;
        Err(r.unwrap_err().into())
    }
    let mut r = Registry::default();
    r.register_workflow("f", failing);
    let out = process_decision_task(&r, input("f", first_task())).unwrap();
    let Some(DA::FailWorkflowExecutionDecisionAttributes(f)) = &out.decisions[1].attributes else {
        panic!("expected fail");
    };
    assert_eq!(f.failure.as_ref().unwrap().reason, "boom");
}

// ------------------------------------------------------------ batcher

mod batcher_workflows {
    use super::*;
    use crate::batcher as b;
    use crate::batcher::workflow::TuneSignal;

    fn params() -> b::BatchParams {
        b::BatchParams {
            domain_name: "d".into(),
            query: "q".into(),
            reason: "r".into(),
            batch_type: "terminate".into(),
            ..Default::default()
        }
    }

    fn start_with(params: &b::BatchParams) -> A {
        A::WorkflowExecutionStartedEventAttributes(pb::WorkflowExecutionStartedEventAttributes {
            task_list: Some(pb::TaskList {
                name: b::TASK_LIST.into(),
                ..Default::default()
            }),
            input: Some(pb::Payload {
                data: converter::encode(params).unwrap(),
            }),
            ..Default::default()
        })
    }

    fn registry() -> Registry {
        let mut r = Registry::default();
        b::workflow::register_sdk(&mut r);
        r
    }

    fn decoded_params(a: &pb::ScheduleActivityTaskDecisionAttributes) -> b::BatchParams {
        converter::decode(&a.input.as_ref().unwrap().data).unwrap()
    }

    #[test]
    fn v1_schedules_activity_with_go_options_and_returns_its_result() {
        let p = params();
        let mut e = vec![ev(1, start_with(&p)), ev(2, dt_sched()), ev(3, dt_started())];
        let out = process_decision_task(&registry(), input(b::V1_WORKFLOW_TYPE, e.clone())).unwrap();
        let Some(DA::ScheduleActivityTaskDecisionAttributes(a)) = &out.decisions[0].attributes
        else {
            panic!("expected schedule activity");
        };
        assert_eq!(a.activity_type.as_ref().unwrap().name, b::ACTIVITY_TYPE);
        assert_eq!(a.heartbeat_timeout.as_ref().unwrap().seconds, 10);
        assert_eq!(a.schedule_to_start_timeout.as_ref().unwrap().seconds, 300);
        let retry = a.retry_policy.as_ref().unwrap();
        assert_eq!(retry.non_retryable_error_reasons, vec!["non-retriable-error"]);
        assert_eq!(retry.maximum_attempts, 0);
        assert_eq!(decoded_params(a).rps, 50);

        let result = b::HeartBeatDetails {
            success_count: 3,
            ..Default::default()
        };
        e.extend([
            ev(4, dt_completed()),
            ev(5, activity_scheduled("0")),
            ev(
                6,
                A::ActivityTaskCompletedEventAttributes(pb::ActivityTaskCompletedEventAttributes {
                    scheduled_event_id: 5,
                    result: Some(pb::Payload {
                        data: converter::encode(&result).unwrap(),
                    }),
                    ..Default::default()
                }),
            ),
            ev(7, dt_sched()),
            ev(8, dt_started()),
        ]);
        let out = process_decision_task(&registry(), input(b::V1_WORKFLOW_TYPE, e)).unwrap();
        let Some(DA::CompleteWorkflowExecutionDecisionAttributes(c)) = &out.decisions[0].attributes
        else {
            panic!("expected complete");
        };
        let back: b::HeartBeatDetails =
            converter::decode(&c.result.as_ref().unwrap().data).unwrap();
        assert_eq!(back.success_count, 3);
    }

    #[test]
    fn invalid_params_fail_the_workflow() {
        let mut p = params();
        p.batch_type = "bogus".into();
        let e = vec![ev(1, start_with(&p)), ev(2, dt_sched()), ev(3, dt_started())];
        let out = process_decision_task(&registry(), input(b::V1_WORKFLOW_TYPE, e)).unwrap();
        let Some(DA::FailWorkflowExecutionDecisionAttributes(f)) = &out.decisions[0].attributes
        else {
            panic!("expected fail");
        };
        let msg: String = converter::decode(&f.failure.as_ref().unwrap().details).unwrap();
        assert_eq!(msg, "not supported batch type: bogus");
    }

    #[test]
    fn v2_tune_signal_cancels_activity_and_restarts_with_progress() {
        let p = params();
        let tune = TuneSignal {
            rps: 7,
            concurrency: 0,
        };
        let mut e = vec![
            ev(1, start_with(&p)),
            ev(2, dt_sched()),
            ev(3, dt_started()),
            ev(4, dt_completed()),
            ev(5, activity_scheduled("0")),
            ev(6, signal(b::TUNE_SIGNAL, converter::encode(&tune).unwrap())),
            ev(7, dt_sched()),
            ev(8, dt_started()),
        ];
        let out =
            process_decision_task(&registry(), input(b::V2_WORKFLOW_TYPE, e.clone())).unwrap();
        assert!(matches!(
            out.decisions[0].attributes,
            Some(DA::RequestCancelActivityTaskDecisionAttributes(_))
        ));

        let progress = b::HeartBeatDetails {
            current_page: 2,
            page_token: vec![9],
            ..Default::default()
        };
        e.extend([
            ev(9, dt_completed()),
            ev(
                10,
                A::ActivityTaskCancelRequestedEventAttributes(
                    pb::ActivityTaskCancelRequestedEventAttributes {
                        activity_id: "0".into(),
                        ..Default::default()
                    },
                ),
            ),
            ev(
                11,
                A::ActivityTaskCanceledEventAttributes(pb::ActivityTaskCanceledEventAttributes {
                    scheduled_event_id: 5,
                    details: Some(pb::Payload {
                        data: converter::encode(&progress).unwrap(),
                    }),
                    ..Default::default()
                }),
            ),
            ev(12, dt_sched()),
            ev(13, dt_started()),
        ]);
        let out = process_decision_task(&registry(), input(b::V2_WORKFLOW_TYPE, e)).unwrap();
        let Some(DA::ScheduleActivityTaskDecisionAttributes(a)) = &out.decisions[0].attributes
        else {
            panic!("expected reschedule, got {:?}", out.decisions);
        };
        assert_eq!(a.activity_type.as_ref().unwrap().name, b::V2_ACTIVITY_TYPE);
        let restarted = decoded_params(a);
        assert_eq!(restarted.rps, 7);
        assert_eq!(restarted.concurrency, 5);
        assert_eq!(restarted.progress, Some(progress));
    }
}
