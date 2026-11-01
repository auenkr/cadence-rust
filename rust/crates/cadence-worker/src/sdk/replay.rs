//! Deterministic replay of a workflow history (port of the decision-task
//! handling in `internal_task_handlers.go` / `internal_event_handlers.go`).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use cadence_proto::api as pb;
use cadence_proto::api::history_event::Attributes as A;

use super::converter;
use super::core::*;
use super::registry::{Registry, WorkflowFuture};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayError {
    NonDeterministic(String),
    UnknownWorkflowType(String),
    Internal(String),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonDeterministic(m) => write!(f, "nondeterministic workflow: {m}"),
            Self::UnknownWorkflowType(t) => write!(f, "unknown workflow type: {t}"),
            Self::Internal(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for ReplayError {}

pub struct Query {
    pub id: String,
    pub query_type: String,
    pub args: Vec<u8>,
}

pub struct TaskInput {
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub domain: String,
    pub events: Vec<pb::HistoryEvent>,
    pub queries: Vec<Query>,
}

pub struct TaskOutput {
    pub decisions: Vec<pb::Decision>,
    /// Query id to answer (`Ok`) or error message (`Err`).
    pub query_results: Vec<(String, Result<Vec<u8>, String>)>,
}

fn noop_waker() -> Waker {
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: the vtable functions never dereference the data pointer.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

fn attrs(e: &pb::HistoryEvent) -> Option<&A> {
    e.attributes.as_ref()
}

fn event_nanos(e: &pb::HistoryEvent) -> i64 {
    e.event_time
        .as_ref()
        .map(|t| t.seconds * 1_000_000_000 + i64::from(t.nanos))
        .unwrap_or(0)
}

/// Events the SDK's own decisions produce.
fn is_decision_event(e: &pb::HistoryEvent) -> bool {
    matches!(
        attrs(e),
        Some(
            A::ActivityTaskScheduledEventAttributes(_)
                | A::ActivityTaskCancelRequestedEventAttributes(_)
                | A::RequestCancelActivityTaskFailedEventAttributes(_)
                | A::TimerStartedEventAttributes(_)
                | A::TimerCanceledEventAttributes(_)
                | A::CancelTimerFailedEventAttributes(_)
                | A::MarkerRecordedEventAttributes(_)
                | A::WorkflowExecutionCompletedEventAttributes(_)
                | A::WorkflowExecutionFailedEventAttributes(_)
                | A::WorkflowExecutionCanceledEventAttributes(_)
                | A::WorkflowExecutionContinuedAsNewEventAttributes(_)
                | A::StartChildWorkflowExecutionInitiatedEventAttributes(_)
                | A::SignalExternalWorkflowExecutionInitiatedEventAttributes(_)
                | A::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(_)
                | A::UpsertWorkflowSearchAttributesEventAttributes(_)
                | A::SemaphoreAcquireInitiatedEventAttributes(_)
                | A::SemaphoreReleasedEventAttributes(_)
        )
    )
}

fn decision_key(e: &pb::HistoryEvent) -> Option<DKey> {
    Some(match attrs(e)? {
        A::ActivityTaskScheduledEventAttributes(a) => DKey::Activity(a.activity_id.clone()),
        A::ActivityTaskCancelRequestedEventAttributes(a) => {
            DKey::CancelActivity(a.activity_id.clone())
        }
        A::TimerStartedEventAttributes(a) => DKey::Timer(a.timer_id.clone()),
        A::TimerCanceledEventAttributes(a) => DKey::CancelTimer(a.timer_id.clone()),
        A::MarkerRecordedEventAttributes(a) => DKey::Marker(a.marker_name.clone()),
        A::WorkflowExecutionCompletedEventAttributes(_) => DKey::Complete,
        A::WorkflowExecutionFailedEventAttributes(_) => DKey::Fail,
        A::WorkflowExecutionCanceledEventAttributes(_) => DKey::Cancel,
        A::WorkflowExecutionContinuedAsNewEventAttributes(_) => DKey::ContinueAsNew,
        _ => return None,
    })
}

struct Run {
    core: CoreRef,
    fut: Option<WorkflowFuture>,
}

impl Run {
    fn poll_once(&mut self) {
        let Some(fut) = self.fut.as_mut() else {
            return;
        };
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let polled = catch_unwind(AssertUnwindSafe(|| {
            Pin::as_mut(fut).poll(&mut cx)
        }));
        match polled {
            Ok(Poll::Pending) => {}
            Ok(Poll::Ready(result)) => {
                self.fut = None;
                self.core.borrow_mut().complete(result);
            }
            Err(panic) => {
                self.fut = None;
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "panic".to_owned());
                let details = converter::encode(&msg).unwrap_or_default();
                self.core
                    .borrow_mut()
                    .fail(REASON_PANIC.to_owned(), details);
            }
        }
    }
}

fn apply_reply_event(core: &mut Core, e: &pb::HistoryEvent) {
    let Some(a) = attrs(e) else { return };
    match a {
        A::DecisionTaskStartedEventAttributes(_) => core.now_nanos = event_nanos(e),
        A::ActivityTaskCompletedEventAttributes(a) => resolve_activity(
            core,
            a.scheduled_event_id,
            Ok(a.result.as_ref().map(|p| p.data.clone())),
        ),
        A::ActivityTaskFailedEventAttributes(a) => {
            let f = a.failure.clone().unwrap_or_default();
            resolve_activity(
                core,
                a.scheduled_event_id,
                Err(ActivityError::Failed {
                    reason: f.reason,
                    details: f.details,
                }),
            )
        }
        A::ActivityTaskTimedOutEventAttributes(a) => resolve_activity(
            core,
            a.scheduled_event_id,
            Err(ActivityError::TimedOut(a.timeout_type)),
        ),
        A::ActivityTaskCanceledEventAttributes(a) => {
            resolve_activity(core, a.scheduled_event_id, Err(ActivityError::Canceled))
        }
        A::TimerFiredEventAttributes(a) => {
            if let Some(slot) = core.timers.get_mut(&a.timer_id) {
                if slot.is_none() {
                    *slot = Some(TimerOutcome::Fired);
                }
            }
        }
        A::WorkflowExecutionSignaledEventAttributes(a) => {
            core.signals
                .entry(a.signal_name.clone())
                .or_default()
                .push_back(a.input.as_ref().map(|p| p.data.clone()).unwrap_or_default());
        }
        A::WorkflowExecutionCancelRequestedEventAttributes(_) => core.cancel_requested = true,
        _ => {}
    }
}

fn resolve_activity(core: &mut Core, scheduled_event_id: i64, result: ActivityResult) {
    if let Some(id) = core.activity_by_scheduled.get(&scheduled_event_id).cloned() {
        if let Some(slot) = core.activities.get_mut(&id) {
            if slot.is_none() {
                *slot = Some(result);
            }
        }
    }
}

/// Markers are applied before the batch runs so side effect and version
/// results are known to the code that produced them (Go `nextDecisionEvents`).
fn apply_marker(core: &mut Core, e: &pb::HistoryEvent) -> Result<(), ReplayError> {
    let Some(A::MarkerRecordedEventAttributes(m)) = attrs(e) else {
        return Ok(());
    };
    let details = m.details.as_ref().map(|p| p.data.as_slice()).unwrap_or(&[]);
    let args = converter::decode_args(details)
        .map_err(|err| ReplayError::Internal(format!("bad marker details: {err}")))?;
    match m.marker_name.as_str() {
        SIDE_EFFECT_MARKER => {
            use base64::Engine;
            let id = args.first().and_then(|v| v.as_i64());
            let data = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok());
            if let (Some(id), Some(data)) = (id, data) {
                core.side_effects.insert(id as i32, data);
            }
        }
        VERSION_MARKER => {
            let change = args.first().and_then(|v| v.as_str());
            let version = args.get(1).and_then(|v| v.as_i64());
            if let (Some(c), Some(v)) = (change, version) {
                core.versions.insert(c.to_owned(), v as i32);
            }
        }
        _ => {}
    }
    Ok(())
}

fn match_decision_event(core: &mut Core, e: &pb::HistoryEvent) -> Result<(), ReplayError> {
    let Some(key) = decision_key(e) else {
        return Err(ReplayError::NonDeterministic(format!(
            "history contains unsupported decision event {:?}",
            e.event_id
        )));
    };
    // Go state machines match by decision id, not by queue head: a cancelled
    // sibling may have been removed, but the recorded order is the creation order.
    // Go creates the Version marker state machine lazily from the recorded
    // event, so a replayed marker needs no pending decision.
    if key == DKey::Marker(VERSION_MARKER.to_owned())
        && core.decisions.first().map(|d| &d.key) != Some(&key)
    {
        return Ok(());
    }
    match core.decisions.first() {
        Some(d) if d.key == key => {
            core.decisions.remove(0);
        }
        Some(d) => {
            return Err(ReplayError::NonDeterministic(format!(
                "history event {} ({key:?}) does not match next decision {:?}",
                e.event_id, d.key
            )))
        }
        None => {
            return Err(ReplayError::NonDeterministic(format!(
                "history event {} ({key:?}) has no matching decision",
                e.event_id
            )))
        }
    }
    match attrs(e) {
        Some(A::ActivityTaskScheduledEventAttributes(a)) => {
            core.activity_by_scheduled
                .insert(e.event_id, a.activity_id.clone());
        }
        Some(A::TimerStartedEventAttributes(a)) => {
            core.timer_by_started.insert(e.event_id, a.timer_id.clone());
        }
        _ => {}
    }
    Ok(())
}

/// Replay `input.events` and produce the decisions for the final decision task
/// together with answers to any queries.
pub fn process_decision_task(
    registry: &Registry,
    input: TaskInput,
) -> Result<TaskOutput, ReplayError> {
    let events = &input.events;
    let Some(A::WorkflowExecutionStartedEventAttributes(started)) =
        events.first().and_then(attrs)
    else {
        return Err(ReplayError::Internal(
            "history does not start with WorkflowExecutionStarted".into(),
        ));
    };
    let wf = registry
        .workflow(&input.workflow_type)
        .ok_or_else(|| ReplayError::UnknownWorkflowType(input.workflow_type.clone()))?;
    let secs = |d: &Option<prost_types::Duration>| d.as_ref().map(|d| d.seconds as i32).unwrap_or(0);
    let core = Core::new_ref(WorkflowInfo {
        workflow_id: input.workflow_id.clone(),
        run_id: input.run_id.clone(),
        workflow_type: input.workflow_type.clone(),
        domain: input.domain.clone(),
        task_list: started
            .task_list
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default(),
        execution_start_to_close_seconds: secs(&started.execution_start_to_close_timeout),
        task_start_to_close_seconds: secs(&started.task_start_to_close_timeout),
    });
    let workflow_input = started
        .input
        .as_ref()
        .map(|p| p.data.clone())
        .unwrap_or_default();
    core.borrow_mut().now_nanos = event_nanos(&events[0]);
    let mut run = Run {
        core: core.clone(),
        fut: Some(wf(WorkflowContext { core: core.clone() }, workflow_input)),
    };

    let len = events.len();
    let is_started = |e: &pb::HistoryEvent| matches!(attrs(e), Some(A::DecisionTaskStartedEventAttributes(_)));
    let mut i = 0usize;
    let mut ran_any = false;
    while i < len {
        // Find the end of the reply events (through DecisionTaskStarted).
        let mut j = i;
        let reply_end;
        let mut completed_at = None;
        loop {
            let mut k = j;
            while k < len && !is_started(&events[k]) {
                k += 1;
            }
            if k >= len {
                reply_end = len;
                break;
            }
            match events.get(k + 1).and_then(attrs) {
                Some(A::DecisionTaskCompletedEventAttributes(_)) => {
                    reply_end = k + 1;
                    completed_at = Some(k + 1);
                    break;
                }
                Some(
                    A::DecisionTaskTimedOutEventAttributes(_)
                    | A::DecisionTaskFailedEventAttributes(_),
                ) => {
                    j = k + 2;
                    continue;
                }
                _ => {
                    reply_end = k + 1;
                    break;
                }
            }
        }
        let (decision_start, decision_end) = match completed_at {
            Some(c) => {
                let mut m = c + 1;
                while m < len && is_decision_event(&events[m]) {
                    m += 1;
                }
                (c + 1, m)
            }
            None => (len, len),
        };
        let replaying = completed_at.is_some();
        {
            let mut c = core.borrow_mut();
            c.replaying = replaying;
            for e in &events[decision_start..decision_end] {
                apply_marker(&mut c, e)?;
            }
            for e in &events[i..reply_end] {
                apply_reply_event(&mut c, e);
            }
        }
        run.poll_once();
        ran_any = true;
        if replaying {
            let mut c = core.borrow_mut();
            for e in &events[decision_start..decision_end] {
                match_decision_event(&mut c, e)?;
            }
            if let Some(d) = c.decisions.first() {
                return Err(ReplayError::NonDeterministic(format!(
                    "workflow produced decision {:?} that is missing from history",
                    d.key
                )));
            }
            i = decision_end;
        } else {
            i = len;
        }
    }
    if !ran_any {
        run.poll_once();
    }

    let mut query_results = Vec::new();
    for q in &input.queries {
        let c = core.borrow();
        let result = match c.query_handlers.get(&q.query_type) {
            Some(h) => h(&q.args),
            None => {
                let mut known: Vec<_> = c.query_handlers.keys().cloned().collect();
                known.sort();
                Err(format!(
                    "unknown queryType {}. KnownQueryTypes=[{}]",
                    q.query_type,
                    known.join(" ")
                ))
            }
        };
        query_results.push((q.id.clone(), result));
    }
    let decisions = core.borrow_mut().take_decisions();
    Ok(TaskOutput {
        decisions,
        query_results,
    })
}
