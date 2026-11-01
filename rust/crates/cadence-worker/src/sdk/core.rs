//! Per-run workflow state, decision generation and the futures that workflow
//! code awaits. Mirrors `workflowEnvironmentImpl` and the decision state
//! machines of the Go client for the supported decision types.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine;
use cadence_proto::api as pb;
use cadence_proto::api::decision::Attributes as DA;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::converter;

pub const REASON_GENERIC: &str = "cadenceInternal:Generic";
pub const REASON_PANIC: &str = "cadenceInternal:Panic";
pub const SIDE_EFFECT_MARKER: &str = "SideEffect";
pub const VERSION_MARKER: &str = "Version";
/// Go `DefaultVersion` returned by `GetVersion` for code that predates a change.
pub const DEFAULT_VERSION: i32 = -1;

/// Identifies a decision so it can be matched to its history event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DKey {
    Activity(String),
    Timer(String),
    CancelActivity(String),
    CancelTimer(String),
    Marker(String),
    Complete,
    Fail,
    ContinueAsNew,
    Cancel,
    /// Child workflow start, keyed by child workflow id.
    Child(String),
    /// Cancellation request for a child workflow (child workflow id).
    CancelChild(String),
    /// Signal of an external workflow, keyed by the signal id (decision `control`).
    SignalExternal(String),
    /// Cancellation of an external workflow, keyed by the cancellation id.
    CancelExternal(String),
    /// Search attribute upsert. History events carry no id, so any two
    /// upserts compare equal when matching decisions to events.
    Upsert(String),
}

#[derive(Clone, Debug)]
pub struct PendingDecision {
    pub key: DKey,
    pub decision: pb::Decision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivityError {
    Failed { reason: String, details: Vec<u8> },
    /// Cadence `TimeoutType` enum value.
    TimedOut(i32),
    Canceled,
    /// Bad options or encoding problems detected before scheduling.
    Invalid(String),
}

impl std::fmt::Display for ActivityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed { reason, .. } => write!(f, "activity failed: {reason}"),
            Self::TimedOut(t) => write!(f, "activity timed out (type {t})"),
            Self::Canceled => write!(f, "activity canceled"),
            Self::Invalid(m) => write!(f, "invalid activity: {m}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinueAsNewParams {
    pub workflow_type: Option<String>,
    pub task_list: Option<String>,
    pub input: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowError {
    Failed { reason: String, details: Vec<u8> },
    Canceled,
    ContinueAsNew(ContinueAsNewParams),
}

impl WorkflowError {
    /// Go generic error: reason `cadenceInternal:Generic`, details are the JSON message.
    pub fn generic(message: impl AsRef<str>) -> Self {
        Self::Failed {
            reason: REASON_GENERIC.to_owned(),
            details: converter::encode(message.as_ref()).unwrap_or_default(),
        }
    }
}

pub type WorkflowResult = Result<Option<Vec<u8>>, WorkflowError>;
pub type ActivityResult = Result<Option<Vec<u8>>, ActivityError>;

#[derive(Clone, Debug, Default)]
pub struct WorkflowInfo {
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type: String,
    pub domain: String,
    pub task_list: String,
    pub execution_start_to_close_seconds: i32,
    pub task_start_to_close_seconds: i32,
    pub original_run_id: String,
    /// Indexed search attributes (raw JSON payload bytes), updated by upserts.
    pub search_attributes: std::collections::BTreeMap<String, Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
pub struct RetryPolicy {
    pub initial_interval: Duration,
    pub backoff_coefficient: f64,
    pub maximum_interval: Duration,
    pub maximum_attempts: i32,
    pub expiration_interval: Duration,
    pub non_retriable_error_reasons: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ActivityOptions {
    pub activity_id: Option<String>,
    pub task_list: Option<String>,
    pub schedule_to_close: Duration,
    pub schedule_to_start: Duration,
    pub start_to_close: Duration,
    pub heartbeat: Duration,
    pub retry_policy: Option<RetryPolicy>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerOutcome {
    Fired,
    Canceled,
}

pub(crate) type QueryHandler = Box<dyn Fn(&[u8]) -> Result<Vec<u8>, String>>;

#[derive(Default)]
pub struct Core {
    next_id: i32,
    pub now_nanos: i64,
    pub replaying: bool,
    pub info: WorkflowInfo,
    pub decisions: Vec<PendingDecision>,
    pub activities: HashMap<String, Option<ActivityResult>>,
    pub activity_by_scheduled: HashMap<i64, String>,
    pub timers: HashMap<String, Option<TimerOutcome>>,
    pub timer_by_started: HashMap<i64, String>,
    pub signals: HashMap<String, VecDeque<Vec<u8>>>,
    pub side_effects: HashMap<i32, Vec<u8>>,
    pub versions: HashMap<String, i32>,
    pub cancel_requested: bool,
    pub children: HashMap<String, super::features::ChildState>,
    /// Signal id -> outcome of `SignalExternalWorkflow`.
    pub external_signals: HashMap<String, Option<Result<(), super::features::ExternalError>>>,
    pub signal_by_initiated: HashMap<i64, String>,
    /// Cancellation id -> outcome of `RequestCancelExternalWorkflow`.
    pub external_cancels: HashMap<String, Option<Result<(), super::features::ExternalError>>>,
    /// Initiated event id -> cancellation id (`None` for child workflow cancels).
    pub cancel_by_initiated: HashMap<i64, Option<String>>,
    pub(crate) local_pending: std::collections::VecDeque<super::features::LocalTask>,
    pub local_results: HashMap<String, Option<ActivityResult>>,
    pub(crate) query_handlers: HashMap<String, QueryHandler>,
}

pub type CoreRef = Rc<RefCell<Core>>;

pub(crate) fn dur(d: Duration) -> Option<prost_types::Duration> {
    Some(prost_types::Duration {
        seconds: d.as_secs() as i64 + i64::from(d.subsec_nanos() > 0),
        nanos: 0,
    })
}

pub(crate) fn secs_ceil(d: Duration) -> i64 {
    d.as_secs() as i64 + i64::from(d.subsec_nanos() > 0)
}

impl Core {
    pub fn new_ref(info: WorkflowInfo) -> CoreRef {
        Rc::new(RefCell::new(Core {
            info,
            ..Core::default()
        }))
    }

    /// Go `GenerateSequence`: shared by activities, timers and side effects.
    pub fn generate_sequence(&mut self) -> i32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub(crate) fn push(&mut self, key: DKey, attributes: DA) {
        self.decisions.push(PendingDecision {
            key,
            decision: pb::Decision {
                attributes: Some(attributes),
            },
        });
    }

    pub(crate) fn remove_unsent(&mut self, key: &DKey) -> bool {
        if let Some(pos) = self.decisions.iter().position(|d| &d.key == key) {
            self.decisions.remove(pos);
            true
        } else {
            false
        }
    }

    pub fn schedule_activity(
        &mut self,
        activity_type: &str,
        input: Vec<u8>,
        options: &ActivityOptions,
    ) -> Result<String, ActivityError> {
        // getValidatedActivityOptions
        let mut o = options.clone();
        let task_list = match &o.task_list {
            Some(t) if !t.is_empty() => t.clone(),
            _ => self.info.task_list.clone(),
        };
        if o.schedule_to_start.is_zero() {
            return Err(ActivityError::Invalid(
                "missing or negative ScheduleToStartTimeoutSeconds".into(),
            ));
        }
        if o.start_to_close.is_zero() {
            return Err(ActivityError::Invalid(
                "missing or negative StartToCloseTimeoutSeconds".into(),
            ));
        }
        if o.schedule_to_close.is_zero() {
            o.schedule_to_close = Duration::from_secs(
                secs_ceil(o.schedule_to_start) as u64 + secs_ceil(o.start_to_close) as u64,
            );
        }
        let retry = o.retry_policy.as_ref().map(convert_retry_policy).transpose()?;
        let activity_id = match &o.activity_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => self.generate_sequence().to_string(),
        };
        let attrs = pb::ScheduleActivityTaskDecisionAttributes {
            activity_id: activity_id.clone(),
            activity_type: Some(pb::ActivityType {
                name: activity_type.to_owned(),
            }),
            task_list: Some(pb::TaskList {
                name: task_list,
                ..Default::default()
            }),
            input: Some(pb::Payload { data: input }),
            schedule_to_close_timeout: dur(o.schedule_to_close),
            schedule_to_start_timeout: dur(o.schedule_to_start),
            start_to_close_timeout: dur(o.start_to_close),
            heartbeat_timeout: dur(o.heartbeat),
            retry_policy: retry,
            ..Default::default()
        };
        self.activities.insert(activity_id.clone(), None);
        self.push(
            DKey::Activity(activity_id.clone()),
            DA::ScheduleActivityTaskDecisionAttributes(attrs),
        );
        Ok(activity_id)
    }

    /// Go `RequestCancelActivity` with `WaitForCancellation=false`.
    pub fn cancel_activity(&mut self, activity_id: &str) {
        match self.activities.get(activity_id) {
            Some(None) => {}
            _ => return,
        }
        if !self.remove_unsent(&DKey::Activity(activity_id.to_owned())) {
            self.push(
                DKey::CancelActivity(activity_id.to_owned()),
                DA::RequestCancelActivityTaskDecisionAttributes(
                    pb::RequestCancelActivityTaskDecisionAttributes {
                        activity_id: activity_id.to_owned(),
                    },
                ),
            );
        }
        self.activities
            .insert(activity_id.to_owned(), Some(Err(ActivityError::Canceled)));
    }

    pub fn start_timer(&mut self, d: Duration) -> String {
        let id = self.generate_sequence().to_string();
        self.timers.insert(id.clone(), None);
        self.push(
            DKey::Timer(id.clone()),
            DA::StartTimerDecisionAttributes(pb::StartTimerDecisionAttributes {
                timer_id: id.clone(),
                start_to_fire_timeout: dur(d),
            }),
        );
        id
    }

    pub fn cancel_timer(&mut self, timer_id: &str) {
        match self.timers.get(timer_id) {
            Some(None) => {}
            _ => return,
        }
        if !self.remove_unsent(&DKey::Timer(timer_id.to_owned())) {
            self.push(
                DKey::CancelTimer(timer_id.to_owned()),
                DA::CancelTimerDecisionAttributes(pb::CancelTimerDecisionAttributes {
                    timer_id: timer_id.to_owned(),
                }),
            );
        }
        self.timers
            .insert(timer_id.to_owned(), Some(TimerOutcome::Canceled));
    }

    pub fn record_marker(&mut self, name: &str, details: Vec<u8>) {
        self.push(
            DKey::Marker(name.to_owned()),
            DA::RecordMarkerDecisionAttributes(pb::RecordMarkerDecisionAttributes {
                marker_name: name.to_owned(),
                details: Some(pb::Payload { data: details }),
                header: None,
            }),
        );
    }

    /// Append the workflow close decision for the workflow function result.
    pub fn complete(&mut self, result: WorkflowResult) {
        match result {
            Ok(r) => self.push(
                DKey::Complete,
                DA::CompleteWorkflowExecutionDecisionAttributes(
                    pb::CompleteWorkflowExecutionDecisionAttributes {
                        result: r.map(|data| pb::Payload { data }),
                    },
                ),
            ),
            Err(WorkflowError::Failed { reason, details }) => self.fail(reason, details),
            Err(WorkflowError::Canceled) => self.push(
                DKey::Cancel,
                DA::CancelWorkflowExecutionDecisionAttributes(
                    pb::CancelWorkflowExecutionDecisionAttributes { details: None },
                ),
            ),
            Err(WorkflowError::ContinueAsNew(p)) => {
                let info = self.info.clone();
                let attrs = pb::ContinueAsNewWorkflowExecutionDecisionAttributes {
                    workflow_type: Some(pb::WorkflowType {
                        name: p.workflow_type.unwrap_or(info.workflow_type),
                    }),
                    task_list: Some(pb::TaskList {
                        name: p.task_list.unwrap_or(info.task_list),
                        ..Default::default()
                    }),
                    input: Some(pb::Payload { data: p.input }),
                    execution_start_to_close_timeout: Some(prost_types::Duration {
                        seconds: i64::from(info.execution_start_to_close_seconds),
                        nanos: 0,
                    }),
                    task_start_to_close_timeout: Some(prost_types::Duration {
                        seconds: i64::from(info.task_start_to_close_seconds),
                        nanos: 0,
                    }),
                    ..Default::default()
                };
                self.push(
                    DKey::ContinueAsNew,
                    DA::ContinueAsNewWorkflowExecutionDecisionAttributes(attrs),
                );
            }
        }
    }

    pub fn fail(&mut self, reason: String, details: Vec<u8>) {
        self.push(
            DKey::Fail,
            DA::FailWorkflowExecutionDecisionAttributes(pb::FailWorkflowExecutionDecisionAttributes {
                failure: Some(pb::Failure {
                    reason,
                    details,
                    options: None,
                }),
            }),
        );
    }

    pub fn take_decisions(&mut self) -> Vec<pb::Decision> {
        self.decisions.drain(..).map(|d| d.decision).collect()
    }
}

pub(crate) fn convert_retry_policy(p: &RetryPolicy) -> Result<pb::RetryPolicy, ActivityError> {
    // convertRetryPolicy + validateRetryPolicy
    let initial = secs_ceil(p.initial_interval);
    if initial <= 0 {
        return Err(ActivityError::Invalid(
            "missing or negative InitialIntervalInSeconds on retry policy".into(),
        ));
    }
    let mut max_interval = secs_ceil(p.maximum_interval);
    if max_interval == 0 {
        max_interval = 100 * initial;
    }
    let backoff = if p.backoff_coefficient == 0.0 {
        2.0
    } else {
        p.backoff_coefficient
    };
    if p.maximum_attempts < 0 {
        return Err(ActivityError::Invalid(
            "negative MaximumAttempts on retry policy is invalid".into(),
        ));
    }
    let expiration = secs_ceil(p.expiration_interval);
    if backoff < 1.0 {
        return Err(ActivityError::Invalid(
            "BackoffCoefficient on retry policy cannot be less than 1.0".into(),
        ));
    }
    if p.maximum_attempts == 0 && expiration == 0 {
        return Err(ActivityError::Invalid(
            "both MaximumAttempts and ExpirationIntervalInSeconds on retry policy are not set, at least one of them must be set".into(),
        ));
    }
    let d = |s: i64| {
        Some(prost_types::Duration {
            seconds: s,
            nanos: 0,
        })
    };
    Ok(pb::RetryPolicy {
        initial_interval: d(initial),
        backoff_coefficient: backoff,
        maximum_interval: d(max_interval),
        maximum_attempts: p.maximum_attempts,
        non_retryable_error_reasons: p.non_retriable_error_reasons.clone(),
        expiration_interval: d(expiration),
        jitter_coefficient: 0.0,
    })
}

// ---------------------------------------------------------------- context

/// Handle passed to workflow functions; cheap to clone.
#[derive(Clone)]
pub struct WorkflowContext {
    pub(crate) core: CoreRef,
}

impl WorkflowContext {
    pub fn info(&self) -> WorkflowInfo {
        self.core.borrow().info.clone()
    }

    /// Workflow time (the DecisionTaskStarted timestamp), Unix nanoseconds.
    pub fn now_nanos(&self) -> i64 {
        self.core.borrow().now_nanos
    }

    pub fn is_replaying(&self) -> bool {
        self.core.borrow().replaying
    }

    pub fn cancel_requested(&self) -> bool {
        self.core.borrow().cancel_requested
    }

    /// Future that resolves once cancellation is requested.
    pub fn cancelled(&self) -> CancelledFuture {
        CancelledFuture {
            core: self.core.clone(),
        }
    }

    pub fn execute_activity<T: Serialize + ?Sized>(
        &self,
        activity_type: &str,
        options: &ActivityOptions,
        input: &T,
    ) -> ActivityFuture {
        let encoded = match converter::encode(input) {
            Ok(e) => e,
            Err(e) => return ActivityFuture::failed(ActivityError::Invalid(e.to_string())),
        };
        match self
            .core
            .borrow_mut()
            .schedule_activity(activity_type, encoded, options)
        {
            Ok(id) => ActivityFuture {
                core: Some(self.core.clone()),
                id,
                immediate: None,
            },
            Err(e) => ActivityFuture::failed(e),
        }
    }

    /// Go `workflow.NewTimer`/`Sleep`: a zero duration is ready immediately.
    pub fn timer(&self, d: Duration) -> TimerFuture {
        if d.is_zero() {
            return TimerFuture {
                core: None,
                id: String::new(),
            };
        }
        let id = self.core.borrow_mut().start_timer(d);
        TimerFuture {
            core: Some(self.core.clone()),
            id,
        }
    }

    pub fn signal_channel(&self, name: &str) -> SignalChannel {
        SignalChannel {
            core: self.core.clone(),
            name: name.to_owned(),
        }
    }

    pub fn set_query_handler(
        &self,
        query_type: &str,
        handler: impl Fn(&[u8]) -> Result<Vec<u8>, String> + 'static,
    ) {
        self.core
            .borrow_mut()
            .query_handlers
            .insert(query_type.to_owned(), Box::new(handler));
    }

    /// Go `workflow.SideEffect`: the closure runs only when not replaying.
    pub fn side_effect<T, F>(&self, f: F) -> Result<T, WorkflowError>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> T,
    {
        let mut core = self.core.borrow_mut();
        let id = core.generate_sequence();
        // Go always records the marker decision, also when the value comes
        // from history, so replay matches the recorded MarkerRecorded event.
        let (value, encoded) = if let Some(data) = core.side_effects.get(&id).cloned() {
            let v = converter::decode(&data).map_err(|e| WorkflowError::generic(e.to_string()))?;
            (v, data)
        } else if core.replaying {
            return Err(WorkflowError::generic(format!(
                "No cached result found for side effectID={id}. KnownSideEffects="
            )));
        } else {
            let v = f();
            let e = converter::encode(&v).map_err(|e| WorkflowError::generic(e.to_string()))?;
            (v, e)
        };
        let b64 = base64::engine::general_purpose::STANDARD.encode(&encoded);
        let details = converter::encode_args(&[serde_json::json!(id), serde_json::json!(b64)])
            .map_err(|e| WorkflowError::generic(e.to_string()))?;
        core.side_effects.insert(id, encoded);
        core.record_marker(SIDE_EFFECT_MARKER, details);
        Ok(value)
    }

    /// Go `workflow.GetVersion`: the first call outside replay records a
    /// `Version` marker and upserts the `CadenceChangeVersion` search attribute.
    pub fn get_version(
        &self,
        change_id: &str,
        min_supported: i32,
        max_supported: i32,
    ) -> Result<i32, WorkflowError> {
        let mut core = self.core.borrow_mut();
        let validate = |version: i32| {
            if version < min_supported || version > max_supported {
                Err(WorkflowError::generic(format!(
                    "Version {version} of changeID {change_id} is not supported, supported version is between {min_supported} and {max_supported}"
                )))
            } else {
                Ok(version)
            }
        };
        if let Some(v) = core.versions.get(change_id).copied() {
            return validate(v);
        }
        let version = if core.replaying {
            DEFAULT_VERSION
        } else {
            max_supported
        };
        validate(version)?;
        if !core.replaying && version != DEFAULT_VERSION {
            let details = converter::encode_args(&[
                serde_json::json!(change_id),
                serde_json::json!(version),
            ])
            .map_err(|e| WorkflowError::generic(e.to_string()))?;
            core.record_marker(VERSION_MARKER, details);
            core.upsert_change_version(change_id, version)
                .map_err(WorkflowError::generic)?;
        }
        core.versions.insert(change_id.to_owned(), version);
        Ok(version)
    }

    /// Go `workflow.NewContinueAsNewError`; return it from the workflow.
    pub fn continue_as_new<T: Serialize + ?Sized>(&self, input: &T) -> WorkflowError {
        match converter::encode(input) {
            Ok(data) => WorkflowError::ContinueAsNew(ContinueAsNewParams {
                workflow_type: None,
                task_list: None,
                input: data,
            }),
            Err(e) => WorkflowError::generic(e.to_string()),
        }
    }
}

// ---------------------------------------------------------------- futures

pub struct ActivityFuture {
    core: Option<CoreRef>,
    id: String,
    immediate: Option<ActivityResult>,
}

impl ActivityFuture {
    fn failed(e: ActivityError) -> Self {
        Self {
            core: None,
            id: String::new(),
            immediate: Some(Err(e)),
        }
    }

    pub fn activity_id(&self) -> &str {
        &self.id
    }

    /// Request cancellation (Go: cancelling the activity context).
    pub fn cancel(&self) {
        if let Some(core) = &self.core {
            core.borrow_mut().cancel_activity(&self.id);
        }
    }

    /// Await and decode the result.
    pub async fn get<R: DeserializeOwned>(self) -> Result<R, ActivityError> {
        let data = self.await?.unwrap_or_default();
        converter::decode(&data).map_err(|e| ActivityError::Invalid(e.to_string()))
    }
}

impl Future for ActivityFuture {
    type Output = ActivityResult;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(r) = self.immediate.take() {
            return Poll::Ready(r);
        }
        let Some(core) = self.core.clone() else {
            return Poll::Pending;
        };
        let outcome = core.borrow().activities.get(&self.id).cloned().flatten();
        match outcome {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }
}

pub struct TimerFuture {
    core: Option<CoreRef>,
    id: String,
}

impl TimerFuture {
    pub fn cancel(&self) {
        if let Some(core) = &self.core {
            core.borrow_mut().cancel_timer(&self.id);
        }
    }
}

impl Future for TimerFuture {
    /// `Ok(())` when fired, `Err(Canceled)` when canceled.
    type Output = Result<(), ActivityError>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(core) = &self.core else {
            return Poll::Ready(Ok(()));
        };
        let outcome = core.borrow().timers.get(&self.id).copied().flatten();
        match outcome {
            Some(TimerOutcome::Fired) => Poll::Ready(Ok(())),
            Some(TimerOutcome::Canceled) => Poll::Ready(Err(ActivityError::Canceled)),
            None => Poll::Pending,
        }
    }
}

pub struct CancelledFuture {
    core: CoreRef,
}

impl Future for CancelledFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.core.borrow().cancel_requested {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub struct SignalChannel {
    core: CoreRef,
    name: String,
}

impl SignalChannel {
    /// Go `ReceiveAsync`: pop a buffered signal without blocking.
    pub fn receive_async<T: DeserializeOwned>(&self) -> Option<T> {
        let data = self
            .core
            .borrow_mut()
            .signals
            .get_mut(&self.name)
            .and_then(|q| q.pop_front())?;
        converter::decode(&data).ok()
    }

    /// Go `Receive`: wait for the next signal.
    pub fn receive<T: DeserializeOwned>(&self) -> SignalFuture<T> {
        SignalFuture {
            core: self.core.clone(),
            name: self.name.clone(),
            _t: std::marker::PhantomData,
        }
    }
}

pub struct SignalFuture<T> {
    core: CoreRef,
    name: String,
    _t: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned> Future for SignalFuture<T> {
    type Output = Option<T>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let data = self
            .core
            .borrow_mut()
            .signals
            .get_mut(&self.name)
            .and_then(|q| q.pop_front());
        match data {
            Some(d) => Poll::Ready(converter::decode(&d).ok()),
            None => Poll::Pending,
        }
    }
}
