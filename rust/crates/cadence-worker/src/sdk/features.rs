//! Child workflows, signal/cancel of external workflows, search attribute
//! upserts and local activities. Mirrors the corresponding parts of
//! `internal_event_handlers.go`, `internal_decision_state_machine.go` and
//! `workflow.go` of the Go client.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use cadence_proto::api as pb;
use cadence_proto::api::decision::Attributes as DA;
use cadence_proto::api::history_event::Attributes as A;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::converter;
use super::core::*;

pub const LOCAL_ACTIVITY_MARKER: &str = "LocalActivity";
/// Reserved search attribute written by `GetVersion`.
pub const CADENCE_CHANGE_VERSION: &str = "CadenceChangeVersion";
/// Go `defaultDecisionTaskTimeoutInSecs`.
const DEFAULT_TASK_TIMEOUT_SECS: u64 = 10;

// ------------------------------------------------------------------ errors

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChildError {
    Failed { reason: String, details: Vec<u8> },
    Canceled,
    /// Cadence `TimeoutType` enum value.
    TimedOut(i32),
    Terminated,
    /// `StartChildWorkflowExecutionFailed` (workflow already running).
    AlreadyStarted,
    Invalid(String),
}

impl std::fmt::Display for ChildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed { reason, .. } => write!(f, "child workflow failed: {reason}"),
            Self::Canceled => write!(f, "child workflow canceled"),
            Self::TimedOut(t) => write!(f, "child workflow timed out (type {t})"),
            Self::Terminated => write!(f, "Terminated"),
            Self::AlreadyStarted => write!(f, "Workflow execution already started"),
            Self::Invalid(m) => write!(f, "{m}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalError {
    /// Go `UnknownExternalWorkflowExecutionError`.
    UnknownExternalWorkflowExecution,
    Failed(String),
    Invalid(String),
}

impl std::fmt::Display for ExternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownExternalWorkflowExecution => write!(f, "UnknownExternalWorkflowExecution"),
            Self::Failed(m) | Self::Invalid(m) => write!(f, "{m}"),
        }
    }
}

impl From<ChildError> for WorkflowError {
    /// A workflow returning a child error fails with the child's reason and
    /// details, or is canceled when the child was canceled (Go `constructError`).
    fn from(e: ChildError) -> Self {
        match e {
            ChildError::Failed { reason, details } => WorkflowError::Failed { reason, details },
            ChildError::Canceled => WorkflowError::Canceled,
            other => WorkflowError::generic(other.to_string()),
        }
    }
}

impl From<ExternalError> for WorkflowError {
    fn from(e: ExternalError) -> Self {
        WorkflowError::generic(e.to_string())
    }
}

impl From<ActivityError> for WorkflowError {
    fn from(e: ActivityError) -> Self {
        match e {
            ActivityError::Failed { reason, details } => WorkflowError::Failed { reason, details },
            ActivityError::Canceled => WorkflowError::Canceled,
            other => WorkflowError::generic(other.to_string()),
        }
    }
}

pub type ChildResult = Result<Option<Vec<u8>>, ChildError>;

// ------------------------------------------------------------ child state

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildPhase {
    /// Start decision created, not yet recorded in history.
    Created,
    /// `StartChildWorkflowExecutionInitiated` seen.
    Initiated,
    /// `ChildWorkflowExecutionStarted` seen.
    Started,
    /// Cancellation decision created or sent (Go `CanceledAfterStarted` and
    /// `CancellationDecisionSent`).
    CancelRequested,
    Completed,
}

#[derive(Clone, Debug)]
pub struct ChildState {
    pub phase: ChildPhase,
    pub domain: String,
    pub started: Option<Result<String, ChildError>>,
    pub result: Option<ChildResult>,
    /// Go `shouldCancelAsync`: cancel once the child has started.
    pub cancel_async: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ChildWorkflowOptions {
    pub domain: Option<String>,
    pub workflow_id: Option<String>,
    pub task_list: Option<String>,
    pub execution_start_to_close: Duration,
    pub task_start_to_close: Duration,
    /// Proto `ParentClosePolicy` value; default Abandon.
    pub parent_close_policy: Option<i32>,
    /// Proto `WorkflowIdReusePolicy` value; default AllowDuplicateFailedOnly.
    pub workflow_id_reuse_policy: Option<i32>,
    pub retry_policy: Option<RetryPolicy>,
    pub cron_schedule: String,
    pub memo: Vec<(String, serde_json::Value)>,
    pub search_attributes: Vec<(String, serde_json::Value)>,
}

fn payload_map<'a>(
    items: impl Iterator<Item = (&'a String, Vec<u8>)>,
) -> HashMap<String, pb::Payload> {
    items
        .map(|(k, data)| (k.clone(), pb::Payload { data }))
        .collect()
}

fn set_once<T>(slot: Option<&mut Option<T>>, value: T) {
    if let Some(s) = slot {
        if s.is_none() {
            *s = Some(value);
        }
    }
}

fn unknown_decision(kind: &str, id: &str) -> String {
    format!(
        "unknown decision {kind} {id}, possible causes are nondeterministic workflow definition code or incompatible change in the workflow definition"
    )
}

impl Core {
    /// Go `getValidatedWorkflowOptions` + `ExecuteChildWorkflow`.
    pub fn start_child(
        &mut self,
        workflow_type: &str,
        input: Vec<u8>,
        o: &ChildWorkflowOptions,
    ) -> Result<String, ChildError> {
        let domain = match &o.domain {
            Some(d) if !d.is_empty() => d.clone(),
            _ => self.info.domain.clone(),
        };
        let task_list = match &o.task_list {
            Some(t) if !t.is_empty() => t.clone(),
            _ => self.info.task_list.clone(),
        };
        let task_timeout = if o.task_start_to_close.is_zero() {
            Duration::from_secs(DEFAULT_TASK_TIMEOUT_SECS)
        } else {
            o.task_start_to_close
        };
        if o.execution_start_to_close.is_zero() {
            return Err(ChildError::Invalid(
                "missing or invalid ExecutionStartToCloseTimeout".into(),
            ));
        }
        let retry = o
            .retry_policy
            .as_ref()
            .map(convert_retry_policy)
            .transpose()
            .map_err(|e| match e {
                ActivityError::Invalid(m) => ChildError::Invalid(m),
                other => ChildError::Invalid(other.to_string()),
            })?;
        let memo = if o.memo.is_empty() {
            None
        } else {
            let mut fields = HashMap::new();
            for (k, v) in &o.memo {
                let data =
                    converter::encode(v).map_err(|e| ChildError::Invalid(e.to_string()))?;
                fields.insert(k.clone(), pb::Payload { data });
            }
            Some(pb::Memo { fields })
        };
        let search_attributes = if o.search_attributes.is_empty() {
            None
        } else {
            let mut fields = HashMap::new();
            for (k, v) in &o.search_attributes {
                let data = serde_json::to_vec(v).map_err(|e| {
                    ChildError::Invalid(format!("encode search attribute [{k}] error: {e}"))
                })?;
                fields.insert(k.clone(), pb::Payload { data });
            }
            Some(pb::SearchAttributes {
                indexed_fields: fields,
            })
        };
        let workflow_id = match &o.workflow_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => {
                let base = if self.info.original_run_id.is_empty() {
                    self.info.run_id.clone()
                } else {
                    self.info.original_run_id.clone()
                };
                format!("{base}_{}", self.generate_sequence())
            }
        };
        if self.children.contains_key(&workflow_id) {
            return Err(ChildError::Invalid(format!(
                "adding duplicate decision child workflow {workflow_id}"
            )));
        }
        let attrs = pb::StartChildWorkflowExecutionDecisionAttributes {
            domain: domain.clone(),
            workflow_id: workflow_id.clone(),
            workflow_type: Some(pb::WorkflowType {
                name: workflow_type.to_owned(),
            }),
            task_list: Some(pb::TaskList {
                name: task_list,
                ..Default::default()
            }),
            input: Some(pb::Payload { data: input }),
            execution_start_to_close_timeout: dur(o.execution_start_to_close),
            task_start_to_close_timeout: dur(task_timeout),
            parent_close_policy: o
                .parent_close_policy
                .unwrap_or(pb::ParentClosePolicy::Abandon as i32),
            workflow_id_reuse_policy: o
                .workflow_id_reuse_policy
                .unwrap_or(pb::WorkflowIdReusePolicy::AllowDuplicateFailedOnly as i32),
            retry_policy: retry,
            cron_schedule: o.cron_schedule.clone(),
            memo,
            search_attributes,
            ..Default::default()
        };
        self.children.insert(
            workflow_id.clone(),
            ChildState {
                phase: ChildPhase::Created,
                domain,
                started: None,
                result: None,
                cancel_async: false,
            },
        );
        self.push(
            DKey::Child(workflow_id.clone()),
            DA::StartChildWorkflowExecutionDecisionAttributes(attrs),
        );
        Ok(workflow_id)
    }

    /// The cancellation callback of Go `ExecuteChildWorkflow` (context cancel).
    pub fn cancel_child(&mut self, id: &str) {
        let Some(st) = self.children.get_mut(id) else {
            return;
        };
        if st.result.is_some() {
            return;
        }
        let phase = st.phase;
        match phase {
            ChildPhase::Started => self.request_cancel_started_child(id),
            ChildPhase::Created | ChildPhase::Initiated => st.cancel_async = true,
            ChildPhase::CancelRequested | ChildPhase::Completed => {}
        }
    }

    /// State machine `cancel()` in `Started`: emits a child-only
    /// `RequestCancelExternalWorkflowExecution` decision.
    fn request_cancel_started_child(&mut self, id: &str) {
        let Some(st) = self.children.get_mut(id) else {
            return;
        };
        st.phase = ChildPhase::CancelRequested;
        let domain = st.domain.clone();
        self.push(
            DKey::CancelChild(id.to_owned()),
            DA::RequestCancelExternalWorkflowExecutionDecisionAttributes(
                pb::RequestCancelExternalWorkflowExecutionDecisionAttributes {
                    domain,
                    workflow_execution: Some(pb::WorkflowExecution {
                        workflow_id: id.to_owned(),
                        run_id: String::new(),
                    }),
                    control: Vec::new(),
                    child_workflow_only: true,
                },
            ),
        );
    }

    /// Go `SignalExternalWorkflow`.
    pub fn signal_external(
        &mut self,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        signal_name: &str,
        input: Vec<u8>,
        child_only: bool,
    ) -> Result<String, ExternalError> {
        if domain.is_empty() {
            return Err(ExternalError::Invalid("domain is not set".into()));
        }
        if workflow_id.is_empty() {
            return Err(ExternalError::Invalid("workflowId is not set".into()));
        }
        let id = self.generate_sequence().to_string();
        self.external_signals.insert(id.clone(), None);
        self.push(
            DKey::SignalExternal(id.clone()),
            DA::SignalExternalWorkflowExecutionDecisionAttributes(
                pb::SignalExternalWorkflowExecutionDecisionAttributes {
                    domain: domain.to_owned(),
                    workflow_execution: Some(pb::WorkflowExecution {
                        workflow_id: workflow_id.to_owned(),
                        run_id: run_id.to_owned(),
                    }),
                    signal_name: signal_name.to_owned(),
                    input: Some(pb::Payload { data: input }),
                    control: id.clone().into_bytes(),
                    child_workflow_only: child_only,
                },
            ),
        );
        Ok(id)
    }

    /// Go `RequestCancelExternalWorkflow` (not limited to children).
    pub fn request_cancel_external(
        &mut self,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<String, ExternalError> {
        if domain.is_empty() {
            return Err(ExternalError::Invalid("domain is not set".into()));
        }
        if workflow_id.is_empty() {
            return Err(ExternalError::Invalid("workflowId is not set".into()));
        }
        let id = self.generate_sequence().to_string();
        self.external_cancels.insert(id.clone(), None);
        self.push(
            DKey::CancelExternal(id.clone()),
            DA::RequestCancelExternalWorkflowExecutionDecisionAttributes(
                pb::RequestCancelExternalWorkflowExecutionDecisionAttributes {
                    domain: domain.to_owned(),
                    workflow_execution: Some(pb::WorkflowExecution {
                        workflow_id: workflow_id.to_owned(),
                        run_id: run_id.to_owned(),
                    }),
                    control: id.clone().into_bytes(),
                    child_workflow_only: false,
                },
            ),
        );
        Ok(id)
    }

    /// Go `workflowEnvironmentImpl.UpsertSearchAttributes`. A change-version
    /// upsert uses its first value as the id, others take a sequence number.
    pub fn upsert_search_attributes(
        &mut self,
        attributes: BTreeMap<String, Vec<u8>>,
        upsert_id: Option<String>,
    ) -> Result<(), String> {
        if attributes.is_empty() {
            return Err("search attributes is empty".into());
        }
        let id = match upsert_id {
            Some(id) => id,
            None => self.generate_sequence().to_string(),
        };
        let indexed_fields = payload_map(attributes.iter().map(|(k, v)| (k, v.clone())));
        self.push(
            DKey::Upsert(id),
            DA::UpsertWorkflowSearchAttributesDecisionAttributes(
                pb::UpsertWorkflowSearchAttributesDecisionAttributes {
                    search_attributes: Some(pb::SearchAttributes { indexed_fields }),
                },
            ),
        );
        // updateWorkflowInfoWithSearchAttributes
        self.info.search_attributes.extend(attributes);
        Ok(())
    }

    /// Go `createSearchAttributesForChangeVersion` + upsert.
    pub(crate) fn upsert_change_version(
        &mut self,
        change_id: &str,
        version: i32,
    ) -> Result<(), String> {
        let first = format!("{change_id}-{version}");
        let mut others: Vec<String> = self
            .versions
            .iter()
            .map(|(k, v)| format!("{k}-{v}"))
            .collect();
        others.sort();
        let mut values = vec![first.clone()];
        values.extend(others);
        let data = serde_json::to_vec(&values).map_err(|e| e.to_string())?;
        let mut attrs = BTreeMap::new();
        attrs.insert(CADENCE_CHANGE_VERSION.to_owned(), data);
        self.upsert_search_attributes(attrs, Some(first))
    }

    // ---------------------------------------------------- event handling

    /// Effects of decision events (Initiated) once matched to a decision.
    pub(crate) fn on_decision_event_matched(&mut self, a: &A, event_id: i64) {
        match a {
            A::StartChildWorkflowExecutionInitiatedEventAttributes(a) => {
                if let Some(st) = self.children.get_mut(&a.workflow_id) {
                    if st.phase == ChildPhase::Created {
                        st.phase = ChildPhase::Initiated;
                    }
                }
            }
            A::SignalExternalWorkflowExecutionInitiatedEventAttributes(a) => {
                self.signal_by_initiated
                    .insert(event_id, String::from_utf8_lossy(&a.control).into_owned());
            }
            A::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(a) => {
                let cid = if a.control.is_empty() {
                    None
                } else {
                    Some(String::from_utf8_lossy(&a.control).into_owned())
                };
                self.cancel_by_initiated.insert(event_id, cid);
            }
            _ => {}
        }
    }

    fn child_mut(&mut self, id: &str) -> Result<&mut ChildState, String> {
        self.children
            .get_mut(id)
            .ok_or_else(|| unknown_decision("child workflow", id))
    }

    fn close_child(&mut self, id: &str, result: ChildResult) -> Result<(), String> {
        let st = self.child_mut(id)?;
        if st.result.is_none() {
            st.result = Some(result);
        }
        st.phase = ChildPhase::Completed;
        Ok(())
    }

    /// Reply events produced by child workflows and external executions.
    pub(crate) fn apply_feature_reply_event(&mut self, a: &A) -> Result<(), String> {
        let wid = |w: &Option<pb::WorkflowExecution>| {
            w.as_ref().map(|w| w.workflow_id.clone()).unwrap_or_default()
        };
        match a {
            A::StartChildWorkflowExecutionFailedEventAttributes(a) => {
                let st = self.child_mut(&a.workflow_id)?;
                if st.result.is_none() {
                    st.started = Some(Err(ChildError::AlreadyStarted));
                    st.result = Some(Err(ChildError::AlreadyStarted));
                }
                st.phase = ChildPhase::Completed;
            }
            A::ChildWorkflowExecutionStartedEventAttributes(a) => {
                let id = wid(&a.workflow_execution);
                let run_id = a
                    .workflow_execution
                    .as_ref()
                    .map(|w| w.run_id.clone())
                    .unwrap_or_default();
                let st = self.child_mut(&id)?;
                if st.result.is_some() {
                    return Ok(());
                }
                st.started = Some(Ok(run_id));
                let cancel = st.cancel_async;
                if matches!(st.phase, ChildPhase::Created | ChildPhase::Initiated) {
                    st.phase = ChildPhase::Started;
                }
                if cancel {
                    // forward the delayed cancellation
                    self.cancel_child(&id);
                }
            }
            A::ChildWorkflowExecutionCompletedEventAttributes(a) => {
                let r = Ok(a.result.as_ref().map(|p| p.data.clone()));
                self.close_child(&wid(&a.workflow_execution), r)?;
            }
            A::ChildWorkflowExecutionFailedEventAttributes(a) => {
                let f = a.failure.clone().unwrap_or_default();
                self.close_child(
                    &wid(&a.workflow_execution),
                    Err(ChildError::Failed {
                        reason: f.reason,
                        details: f.details,
                    }),
                )?;
            }
            A::ChildWorkflowExecutionCanceledEventAttributes(a) => {
                self.close_child(&wid(&a.workflow_execution), Err(ChildError::Canceled))?;
            }
            A::ChildWorkflowExecutionTimedOutEventAttributes(a) => {
                self.close_child(
                    &wid(&a.workflow_execution),
                    Err(ChildError::TimedOut(a.timeout_type)),
                )?;
            }
            A::ChildWorkflowExecutionTerminatedEventAttributes(a) => {
                self.close_child(&wid(&a.workflow_execution), Err(ChildError::Terminated))?;
            }
            A::ExternalWorkflowExecutionSignaledEventAttributes(a) => {
                let id = self
                    .signal_by_initiated
                    .get(&a.initiated_event_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!("unable to find signal ID: {}", a.initiated_event_id)
                    })?;
                set_once(self.external_signals.get_mut(&id), Ok(()));
            }
            A::SignalExternalWorkflowExecutionFailedEventAttributes(a) => {
                let id = self
                    .signal_by_initiated
                    .get(&a.initiated_event_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!("unable to find signal ID: {}", a.initiated_event_id)
                    })?;
                let err = if a.cause
                    == pb::SignalExternalWorkflowExecutionFailedCause::UnknownExternalWorkflowExecution
                        as i32
                {
                    ExternalError::UnknownExternalWorkflowExecution
                } else {
                    ExternalError::Failed(format!(
                        "signal external workflow failed, {}",
                        a.cause
                    ))
                };
                set_once(self.external_signals.get_mut(&id), Err(err));
            }
            A::ExternalWorkflowExecutionCancelRequestedEventAttributes(a) => {
                match self.cancel_by_initiated.get(&a.initiated_event_id).cloned() {
                    Some(Some(cid)) => {
                        set_once(self.external_cancels.get_mut(&cid), Ok(()));
                    }
                    // child workflow: no state change, still waiting for its close
                    Some(None) => {}
                    None => {
                        let id = wid(&a.workflow_execution);
                        self.child_mut(&id)?;
                    }
                }
            }
            A::RequestCancelExternalWorkflowExecutionFailedEventAttributes(a) => {
                match self.cancel_by_initiated.get(&a.initiated_event_id).cloned() {
                    Some(Some(cid)) => {
                        let err = ExternalError::Failed(format!(
                            "cancel external workflow failed, {}",
                            a.cause
                        ));
                        set_once(self.external_cancels.get_mut(&cid), Err(err));
                    }
                    _ => {
                        // child workflow cancel failed: back to Started
                        let id = wid(&a.workflow_execution);
                        let st = self.child_mut(&id)?;
                        if st.phase == ChildPhase::CancelRequested {
                            st.phase = ChildPhase::Started;
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

// --------------------------------------------------------- local activity

/// Failure reported by a local activity function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFailure {
    pub reason: String,
    pub details: Vec<u8>,
}

impl LocalFailure {
    /// Go generic error: reason `cadenceInternal:Generic`, JSON message details.
    pub fn generic(message: impl AsRef<str>) -> Self {
        Self {
            reason: REASON_GENERIC.to_owned(),
            details: converter::encode(message.as_ref()).unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LocalActivityOptions {
    pub schedule_to_close: Duration,
    pub retry_policy: Option<RetryPolicy>,
}

type LocalFn = Box<dyn FnMut() -> Result<Option<Vec<u8>>, LocalFailure>>;

pub(crate) struct LocalTask {
    pub id: String,
    pub activity_type: String,
    run: LocalFn,
    max_attempts: i32,
    non_retriable: Vec<String>,
}

/// Go `localActivityMarkerData`.
#[derive(Serialize, Deserialize, Default, Debug)]
#[serde(default)]
struct LocalMarkerData {
    #[serde(rename = "activityId", skip_serializing_if = "String::is_empty")]
    activity_id: String,
    #[serde(rename = "activityType", skip_serializing_if = "String::is_empty")]
    activity_type: String,
    #[serde(rename = "errReason", skip_serializing_if = "String::is_empty")]
    err_reason: String,
    #[serde(rename = "errJson", skip_serializing_if = "String::is_empty")]
    err_json: String,
    #[serde(rename = "resultJson", skip_serializing_if = "String::is_empty")]
    result_json: String,
    #[serde(rename = "replayTime")]
    replay_time: String,
    #[serde(skip_serializing_if = "is_zero_i32")]
    attempt: i32,
    #[serde(skip_serializing_if = "is_zero_i64")]
    backoff: i64,
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

/// Go `lastPartOfName`.
fn last_part_of_name(name: &str) -> &str {
    let name = name.strip_suffix("-fm").unwrap_or(name);
    match name.rfind('.') {
        Some(i) if i + 1 != name.len() => &name[i + 1..],
        _ => name,
    }
}

impl Core {
    pub fn schedule_local_activity(
        &mut self,
        activity_type: &str,
        options: &LocalActivityOptions,
        run: LocalFn,
    ) -> Result<String, ActivityError> {
        if options.schedule_to_close.is_zero() {
            return Err(ActivityError::Invalid(
                "missing or negative ScheduleToCloseTimeoutSeconds".into(),
            ));
        }
        let retry = options.retry_policy.as_ref();
        let id = self.generate_sequence().to_string();
        self.local_results.insert(id.clone(), None);
        self.local_pending.push_back(LocalTask {
            id: id.clone(),
            activity_type: activity_type.to_owned(),
            run,
            max_attempts: retry.map(|r| r.maximum_attempts).filter(|m| *m > 0).unwrap_or(1),
            non_retriable: retry
                .map(|r| r.non_retriable_error_reasons.clone())
                .unwrap_or_default(),
        });
        Ok(id)
    }

    fn finish_local(&mut self, marker: LocalMarkerData, details: Vec<u8>) {
        self.record_marker(LOCAL_ACTIVITY_MARKER, details);
        let result = if !marker.err_reason.is_empty() {
            Err(ActivityError::Failed {
                reason: marker.err_reason,
                details: marker.err_json.into_bytes(),
            })
        } else if !marker.result_json.is_empty() {
            Ok(Some(marker.result_json.into_bytes()))
        } else {
            Ok(None)
        };
        if let Some(slot) = self.local_results.get_mut(&marker.activity_id) {
            *slot = Some(result);
        }
    }
}

/// Run the oldest pending local activity (not replaying) and record its
/// marker; returns whether a task ran. The closure runs without any borrow
/// of the core so it may use the workflow context.
pub(crate) fn run_pending_local_activity(core: &CoreRef) -> bool {
    let Some(mut task) = core.borrow_mut().local_pending.pop_front() else {
        return false;
    };
    let mut attempt = 0;
    let outcome = loop {
        match (task.run)() {
            Ok(r) => break Ok(r),
            Err(e) => {
                if attempt + 1 >= task.max_attempts || task.non_retriable.contains(&e.reason) {
                    break Err(e);
                }
                attempt += 1;
            }
        }
    };
    let mut c = core.borrow_mut();
    let mut marker = LocalMarkerData {
        activity_id: task.id.clone(),
        activity_type: task.activity_type.clone(),
        replay_time: format_rfc3339_nanos(c.now_nanos),
        attempt,
        ..Default::default()
    };
    match outcome {
        Ok(r) => {
            marker.result_json = r
                .map(|d| String::from_utf8_lossy(&d).into_owned())
                .unwrap_or_default();
        }
        Err(e) => {
            marker.err_reason = e.reason;
            marker.err_json = String::from_utf8_lossy(&e.details).into_owned();
        }
    }
    match converter::encode(&marker) {
        Ok(details) => c.finish_local(marker, details),
        Err(e) => {
            if let Some(slot) = c.local_results.get_mut(&task.id) {
                *slot = Some(Err(ActivityError::Invalid(e.to_string())));
            }
        }
    }
    true
}

/// Apply a recorded `LocalActivity` marker during replay (Go
/// `handleLocalActivityMarker`). Returns whether a pending task consumed it.
pub(crate) fn apply_local_marker(
    core: &CoreRef,
    m: &pb::MarkerRecordedEventAttributes,
) -> Result<bool, String> {
    let details = m.details.as_ref().map(|p| p.data.as_slice()).unwrap_or(&[]);
    let marker: LocalMarkerData = converter::decode(details).map_err(|e| e.to_string())?;
    let mut c = core.borrow_mut();
    let Some(pos) = c.local_pending.iter().position(|t| t.id == marker.activity_id) else {
        return Ok(false);
    };
    let task_type = c.local_pending[pos].activity_type.clone();
    if !marker.activity_type.is_empty()
        && last_part_of_name(&marker.activity_type) != last_part_of_name(&task_type)
    {
        return Err(format!(
            "code execute local activity {task_type}, but history event found {}, markerData: {}",
            marker.activity_type,
            String::from_utf8_lossy(details)
        ));
    }
    c.local_pending.remove(pos);
    if let Some(ns) = parse_rfc3339_nanos(&marker.replay_time) {
        c.now_nanos = ns;
    }
    c.finish_local(marker, details.to_vec());
    Ok(true)
}

// ------------------------------------------------------------ time helpers

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Go `time.Time` JSON (RFC 3339 with nanoseconds, trailing zeros trimmed), UTC.
pub(crate) fn format_rfc3339_nanos(nanos: i64) -> String {
    let secs = nanos.div_euclid(1_000_000_000);
    let frac = nanos.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let mut s = format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    );
    if frac != 0 {
        let f = format!("{frac:09}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s.push('Z');
    s
}

pub(crate) fn parse_rfc3339_nanos(s: &str) -> Option<i64> {
    let (date, rest) = s.split_once('T')?;
    let mut dp = date.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let m: i64 = dp.next()?.parse().ok()?;
    let d: i64 = dp.next()?.parse().ok()?;
    let split = rest.find(['Z', '+', '-'])?;
    let (clock, zone) = rest.split_at(split);
    let (hms, frac) = match clock.split_once('.') {
        Some((h, f)) => (h, f),
        None => (clock, ""),
    };
    let mut tp = hms.split(':');
    let hh: i64 = tp.next()?.parse().ok()?;
    let mm: i64 = tp.next()?.parse().ok()?;
    let ss: i64 = tp.next()?.parse().ok()?;
    let mut nanos: i64 = 0;
    if !frac.is_empty() {
        let digits = format!("{frac:0<9}");
        nanos = digits.get(..9)?.parse().ok()?;
    }
    let offset = if zone == "Z" {
        0
    } else {
        let sign = if zone.starts_with('-') { -1 } else { 1 };
        let (oh, om) = zone[1..].split_once(':')?;
        sign * (oh.parse::<i64>().ok()? * 3600 + om.parse::<i64>().ok()? * 60)
    };
    let secs = days_from_civil(y, m, d) * 86_400 + hh * 3600 + mm * 60 + ss - offset;
    Some(secs * 1_000_000_000 + nanos)
}

// ---------------------------------------------------------------- context

impl WorkflowContext {
    /// Go `workflow.ExecuteChildWorkflow`.
    pub fn execute_child_workflow<T: Serialize + ?Sized>(
        &self,
        workflow_type: &str,
        options: &ChildWorkflowOptions,
        input: &T,
    ) -> ChildWorkflowFuture {
        let encoded = match converter::encode(input) {
            Ok(e) => e,
            Err(e) => return ChildWorkflowFuture::failed(ChildError::Invalid(e.to_string())),
        };
        match self
            .core
            .borrow_mut()
            .start_child(workflow_type, encoded, options)
        {
            Ok(id) => ChildWorkflowFuture {
                core: Some(self.core.clone()),
                id,
                immediate: None,
            },
            Err(e) => ChildWorkflowFuture::failed(e),
        }
    }

    /// Go `workflow.SignalExternalWorkflow` (not limited to child workflows).
    pub fn signal_external_workflow<T: Serialize + ?Sized>(
        &self,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        signal_name: &str,
        input: &T,
    ) -> ExternalFuture {
        self.signal_external_impl(domain, workflow_id, run_id, signal_name, input, false)
    }

    fn signal_external_impl<T: Serialize + ?Sized>(
        &self,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        signal_name: &str,
        input: &T,
        child_only: bool,
    ) -> ExternalFuture {
        let encoded = match converter::encode(input) {
            Ok(e) => e,
            Err(e) => return ExternalFuture::failed(ExternalError::Invalid(e.to_string())),
        };
        match self.core.borrow_mut().signal_external(
            domain,
            workflow_id,
            run_id,
            signal_name,
            encoded,
            child_only,
        ) {
            Ok(id) => ExternalFuture {
                core: Some(self.core.clone()),
                id,
                kind: ExternalKind::Signal,
                immediate: None,
            },
            Err(e) => ExternalFuture::failed(e),
        }
    }

    /// Go `workflow.RequestCancelExternalWorkflow`.
    pub fn request_cancel_external_workflow(
        &self,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> ExternalFuture {
        match self
            .core
            .borrow_mut()
            .request_cancel_external(domain, workflow_id, run_id)
        {
            Ok(id) => ExternalFuture {
                core: Some(self.core.clone()),
                id,
                kind: ExternalKind::Cancel,
                immediate: None,
            },
            Err(e) => ExternalFuture::failed(e),
        }
    }

    /// Go `workflow.UpsertSearchAttributes`; each value is JSON encoded.
    pub fn upsert_search_attributes(
        &self,
        attributes: &[(&str, serde_json::Value)],
    ) -> Result<(), String> {
        if attributes.iter().any(|(k, _)| *k == CADENCE_CHANGE_VERSION) {
            return Err(
                "CadenceChangeVersion is a reserved key that cannot be set, please use other key"
                    .into(),
            );
        }
        let mut map = BTreeMap::new();
        for (k, v) in attributes {
            let data = serde_json::to_vec(v)
                .map_err(|e| format!("encode search attribute [{k}] error: {e}"))?;
            map.insert((*k).to_owned(), data);
        }
        self.core.borrow_mut().upsert_search_attributes(map, None)
    }

    /// Go `workflow.ExecuteLocalActivity`. `f` runs in-process when the
    /// decision task is not a replay; its outcome is recorded as a
    /// `LocalActivity` marker.
    pub fn execute_local_activity<R, F>(
        &self,
        activity_type: &str,
        options: &LocalActivityOptions,
        mut f: F,
    ) -> LocalActivityFuture
    where
        R: Serialize,
        F: FnMut() -> Result<R, LocalFailure> + 'static,
    {
        let run: LocalFn = Box::new(move || {
            let v = f()?;
            converter::encode(&v)
                .map(Some)
                .map_err(|e| LocalFailure::generic(e.to_string()))
        });
        match self
            .core
            .borrow_mut()
            .schedule_local_activity(activity_type, options, run)
        {
            Ok(id) => LocalActivityFuture {
                core: Some(self.core.clone()),
                id,
                immediate: None,
            },
            Err(e) => LocalActivityFuture {
                core: None,
                id: String::new(),
                immediate: Some(Err(e)),
            },
        }
    }
}

// ---------------------------------------------------------------- futures

pub struct ChildWorkflowFuture {
    core: Option<CoreRef>,
    id: String,
    immediate: Option<ChildError>,
}

impl ChildWorkflowFuture {
    fn failed(e: ChildError) -> Self {
        Self {
            core: None,
            id: String::new(),
            immediate: Some(e),
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.id
    }

    /// Resolves with the child run id once the child started.
    pub fn started(&self) -> ChildStartedFuture {
        ChildStartedFuture {
            core: self.core.clone(),
            id: self.id.clone(),
            immediate: self.immediate.clone(),
        }
    }

    /// Cancel the child (Go: cancelling the child workflow context).
    pub fn cancel(&self) {
        if let Some(core) = &self.core {
            core.borrow_mut().cancel_child(&self.id);
        }
    }

    /// Go `SignalChildWorkflow`: waits for the child to start, then signals it.
    pub async fn signal<T: Serialize + ?Sized>(
        &self,
        ctx: &WorkflowContext,
        signal_name: &str,
        input: &T,
    ) -> Result<(), ExternalError> {
        self.started()
            .await
            .map_err(|e| ExternalError::Failed(e.to_string()))?;
        let domain = self
            .core
            .as_ref()
            .and_then(|c| c.borrow().children.get(&self.id).map(|s| s.domain.clone()))
            .unwrap_or_default();
        ctx.signal_external_impl(&domain, &self.id, "", signal_name, input, true)
            .await
    }

    /// Await and decode the child result.
    pub async fn get<R: DeserializeOwned>(self) -> Result<R, ChildError> {
        let data = self.await?.unwrap_or_default();
        converter::decode(&data).map_err(|e| ChildError::Invalid(e.to_string()))
    }
}

impl Future for ChildWorkflowFuture {
    type Output = ChildResult;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(e) = self.immediate.take() {
            return Poll::Ready(Err(e));
        }
        let Some(core) = self.core.clone() else {
            return Poll::Pending;
        };
        let r = core
            .borrow()
            .children
            .get(&self.id)
            .and_then(|s| s.result.clone());
        match r {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }
}

pub struct ChildStartedFuture {
    core: Option<CoreRef>,
    id: String,
    immediate: Option<ChildError>,
}

impl Future for ChildStartedFuture {
    /// Child run id.
    type Output = Result<String, ChildError>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(e) = self.immediate.take() {
            return Poll::Ready(Err(e));
        }
        let Some(core) = self.core.clone() else {
            return Poll::Pending;
        };
        let r = core
            .borrow()
            .children
            .get(&self.id)
            .and_then(|s| s.started.clone());
        match r {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }
}

#[derive(Clone, Copy)]
enum ExternalKind {
    Signal,
    Cancel,
}

/// Future for a signal or cancellation request sent to another workflow.
pub struct ExternalFuture {
    core: Option<CoreRef>,
    id: String,
    kind: ExternalKind,
    immediate: Option<ExternalError>,
}

impl ExternalFuture {
    fn failed(e: ExternalError) -> Self {
        Self {
            core: None,
            id: String::new(),
            kind: ExternalKind::Signal,
            immediate: Some(e),
        }
    }
}

impl Future for ExternalFuture {
    type Output = Result<(), ExternalError>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(e) = self.immediate.take() {
            return Poll::Ready(Err(e));
        }
        let Some(core) = self.core.clone() else {
            return Poll::Pending;
        };
        let c = core.borrow();
        let slot = match self.kind {
            ExternalKind::Signal => c.external_signals.get(&self.id),
            ExternalKind::Cancel => c.external_cancels.get(&self.id),
        };
        match slot.cloned().flatten() {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }
}

pub struct LocalActivityFuture {
    core: Option<CoreRef>,
    id: String,
    immediate: Option<ActivityResult>,
}

impl LocalActivityFuture {
    pub fn activity_id(&self) -> &str {
        &self.id
    }

    pub async fn get<R: DeserializeOwned>(self) -> Result<R, ActivityError> {
        let data = self.await?.unwrap_or_default();
        converter::decode(&data).map_err(|e| ActivityError::Invalid(e.to_string()))
    }
}

impl Future for LocalActivityFuture {
    type Output = ActivityResult;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(r) = self.immediate.take() {
            return Poll::Ready(r);
        }
        let Some(core) = self.core.clone() else {
            return Poll::Pending;
        };
        let r = core.borrow().local_results.get(&self.id).cloned().flatten();
        match r {
            Some(r) => Poll::Ready(r),
            None => Poll::Pending,
        }
    }
}
