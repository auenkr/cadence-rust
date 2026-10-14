//! In-memory workflow execution store.
//!
//! Mirrors the observable semantics of the Go frontend + history path for
//! the core lifecycle: start (with request-ID idempotency and
//! `WorkflowIDReusePolicy`), describe, signal, request-cancel, terminate,
//! and history append/read. Each mutation appends a history event, so
//! `history_length` and `GetWorkflowExecutionHistory` behave like the Go
//! implementation for these operations.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use cadence_api::enums::{
    CancelExternalWorkflowExecutionFailedCause, ChildWorkflowExecutionFailedCause,
    DecisionTaskFailedCause, DecisionType, EventType, ParentClosePolicy, PendingActivityState,
    PendingDecisionState, SignalExternalWorkflowExecutionFailedCause, WorkflowExecutionCloseStatus,
    WorkflowIDReusePolicy,
};
use cadence_api::errors::{
    BadRequestError, EntityNotExistsError, WorkflowExecutionAlreadyCompletedError,
    WorkflowExecutionAlreadyStartedError,
};
use cadence_api::service_types::{
    DescribeWorkflowExecutionResponse, GetWorkflowExecutionHistoryResponse, History, HistoryEvent,
    RequestCancelWorkflowExecutionRequest, WorkflowExecutionCancelRequestedEventAttributes,
    WorkflowExecutionConfiguration, WorkflowExecutionInfo,
    WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes,
    WorkflowExecutionTerminatedEventAttributes,
};
use cadence_api::tasks::{
    ActivityTaskCompletedEventAttributes, ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes, ActivityTaskStartedEventAttributes,
    ChildWorkflowExecutionCanceledEventAttributes, ChildWorkflowExecutionCompletedEventAttributes,
    ChildWorkflowExecutionFailedEventAttributes, ChildWorkflowExecutionStartedEventAttributes,
    ChildWorkflowExecutionTerminatedEventAttributes, DecisionTaskCompletedEventAttributes,
    DecisionTaskScheduledEventAttributes, DecisionTaskStartedEventAttributes,
    MarkerRecordedEventAttributes, PollForActivityTaskResponse, PollForDecisionTaskResponse,
    RequestCancelExternalWorkflowExecutionFailedEventAttributes,
    RequestCancelExternalWorkflowExecutionInitiatedEventAttributes,
    RespondActivityTaskCompletedRequest, RespondActivityTaskFailedRequest,
    RespondDecisionTaskCompletedRequest, RespondDecisionTaskCompletedResponse,
    SignalExternalWorkflowExecutionFailedEventAttributes,
    SignalExternalWorkflowExecutionInitiatedEventAttributes, TimerCanceledEventAttributes,
    TimerFiredEventAttributes, TimerStartedEventAttributes,
    UpsertWorkflowSearchAttributesEventAttributes, WorkflowExecutionCanceledEventAttributes,
    WorkflowExecutionCompletedEventAttributes, WorkflowExecutionFailedEventAttributes,
    WorkflowQueryResult,
};
use cadence_api::tasktoken::{JsonTaskTokenSerializer, TaskToken};
use cadence_api::types::{
    ActivityType, Header, RetryPolicy, SearchAttributes, SignalWorkflowExecutionRequest,
    StartWorkflowExecutionRequest, StartWorkflowExecutionResponse, TaskList,
    TerminateWorkflowExecutionRequest, WorkflowExecution, WorkflowType,
};

/// Milliseconds since the Unix epoch.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionRecord {
    pub domain: String,
    pub execution: WorkflowExecution,
    pub workflow_type: WorkflowType,
    pub task_list: TaskList,
    pub execution_start_to_close_timeout_seconds: Option<i32>,
    pub task_start_to_close_timeout_seconds: Option<i32>,
    pub memo: Option<cadence_api::types::Memo>,
    pub search_attributes: Option<SearchAttributes>,
    pub close_status: Option<WorkflowExecutionCloseStatus>,
    pub start_time_millis: i64,
    pub close_time_millis: Option<i64>,
    pub history: Vec<HistoryEvent>,
    pub(crate) pending_decision: Option<PendingDecision>,
    pub(crate) pending_activities: HashMap<i64, PendingActivity>,
    pub(crate) pending_timers: HashMap<String, PendingTimer>,
    pub(crate) pending_queries: std::collections::VecDeque<PendingQuery>,
    pub(crate) parent: Option<ParentRef>,
    pub(crate) prev_started_event_id: Option<i64>,
}

/// Parent link for a child workflow, mirroring Go's parent execution info.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ParentRef {
    pub(crate) domain: String,
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) initiated_event_id: i64,
}

/// Child link tracked on the parent, mirroring Go's child execution infos.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChildRef {
    pub(crate) domain: String,
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) initiated_event_id: i64,
    pub(crate) parent_close_policy: ParentClosePolicy,
}

/// A started-but-unfired timer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingTimer {
    pub(crate) started_event_id: i64,
    pub(crate) deadline_millis: i64,
}

/// A buffered direct query waiting for a worker answer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingQuery {
    pub(crate) query_id: String,
    pub(crate) query_type: String,
    pub(crate) query_args: Vec<u8>,
    pub(crate) answered: Option<WorkflowQueryResult>,
}

/// A scheduled-but-unresolved decision task, mirroring Go history's
/// decision-task state machine (scheduled → started → completed/failed).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingDecision {
    pub(crate) scheduled_event_id: i64,
    pub(crate) started_event_id: Option<i64>,
    pub(crate) attempt: i64,
}

/// A scheduled activity, mirroring the pending-activity lifecycle.
/// Timeout/retry fields are populated for the future timeout engine;
/// firing is a follow-up.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) struct PendingActivity {
    pub(crate) activity_id: String,
    pub(crate) activity_type: ActivityType,
    pub(crate) input: Vec<u8>,
    pub(crate) task_list: TaskList,
    pub(crate) scheduled_event_id: i64,
    pub(crate) started_event_id: Option<i64>,
    pub(crate) attempt: i32,
    pub(crate) cancel_requested: bool,
    pub(crate) cancel_requested_event_id: Option<i64>,
    pub(crate) heartbeat_details: Vec<u8>,
    pub(crate) queued: bool,
    pub(crate) retry_ready_millis: Option<i64>,
    pub(crate) schedule_to_close_timeout_seconds: Option<i32>,
    pub(crate) start_to_close_timeout_seconds: Option<i32>,
    pub(crate) heartbeat_timeout_seconds: Option<i32>,
    pub(crate) scheduled_time_millis: i64,
    pub(crate) retry_policy: Option<RetryPolicy>,
    pub(crate) header: Option<Header>,
}

impl ExecutionRecord {
    fn is_open(&self) -> bool {
        self.close_status.is_none()
    }

    fn to_info(&self) -> WorkflowExecutionInfo {
        WorkflowExecutionInfo {
            execution: Some(self.execution.clone()),
            workflow_type: Some(self.workflow_type.clone()),
            start_time: Some(self.start_time_millis),
            close_time: self.close_time_millis,
            close_status: self.close_status,
            history_length: self.history.len() as i64,
            memo: self.memo.clone(),
        }
    }

    fn to_describe_response(
        &self,
        pending_children: Vec<cadence_api::tasks::PendingChildExecutionInfo>,
    ) -> DescribeWorkflowExecutionResponse {
        DescribeWorkflowExecutionResponse {
            execution_configuration: Some(WorkflowExecutionConfiguration {
                task_list: Some(self.task_list.clone()),
                execution_start_to_close_timeout_seconds: self
                    .execution_start_to_close_timeout_seconds,
                task_start_to_close_timeout_seconds: self.task_start_to_close_timeout_seconds,
            }),
            workflow_execution_info: Some(self.to_info()),
            pending_activities: self
                .pending_activities
                .values()
                .map(|p| cadence_api::tasks::PendingActivityInfo {
                    activity_id: p.activity_id.clone(),
                    activity_type: Some(p.activity_type.clone()),
                    state: Some(if p.started_event_id.is_some() {
                        PendingActivityState::Started
                    } else {
                        PendingActivityState::Scheduled
                    }),
                    attempt: p.attempt,
                    scheduled_timestamp: Some(p.scheduled_time_millis),
                    last_started_timestamp: None,
                    heartbeat_details: p.heartbeat_details.clone(),
                    maximum_attempts: p
                        .retry_policy
                        .as_ref()
                        .map(|r| r.maximum_attempts)
                        .unwrap_or(0),
                    schedule_id: p.scheduled_event_id,
                    last_failure_reason: None,
                })
                .collect(),
            pending_children,
            pending_decision: self.pending_decision.as_ref().map(|p| {
                cadence_api::tasks::PendingDecisionInfo {
                    state: Some(if p.started_event_id.is_some() {
                        PendingDecisionState::Started
                    } else {
                        PendingDecisionState::Scheduled
                    }),
                    scheduled_timestamp: None,
                    started_timestamp: None,
                    attempt: p.attempt,
                    schedule_id: p.scheduled_event_id,
                    original_scheduled_timestamp: None,
                }
            }),
        }
    }

    fn next_event_id(&self) -> i64 {
        self.history.len() as i64 + 1
    }

    fn push_event(&mut self, event_type: EventType, mut event: HistoryEvent) {
        event.event_id = self.next_event_id();
        event.timestamp = Some(now_millis());
        event.event_type = Some(event_type);
        self.history.push(event);
    }
}

/// Queue key: (domain, task-list name).
type QueueKey = (String, String);
/// Queued decision task: (workflow-id, run-id).
type QueuedDecision = (String, String);
/// Queued activity task: (workflow-id, run-id, scheduled-event-id).
type QueuedActivity = (String, String, i64);

#[derive(Debug, Default)]
pub(crate) struct Inner {
    /// (domain, workflow_id, run_id) -> record.
    pub(crate) by_run: HashMap<(String, String, String), ExecutionRecord>,
    /// (domain, workflow_id) -> run_ids in start order.
    pub(crate) by_workflow: HashMap<(String, String), Vec<String>>,
    /// (domain, workflow_id, request_id) -> run_id for start idempotency.
    pub(crate) by_request_id: HashMap<(String, String, String), String>,
    /// (domain, task_list) -> queued decision tasks (FIFO).
    pub(crate) decision_queues: HashMap<QueueKey, std::collections::VecDeque<QueuedDecision>>,
    /// (domain, task_list) -> queued activity tasks (FIFO).
    pub(crate) activity_queues: HashMap<QueueKey, std::collections::VecDeque<QueuedActivity>>,
    /// (parent domain, workflow_id, run_id) -> children.
    pub(crate) children: HashMap<(String, String, String), Vec<ChildRef>>,
    /// Domain names registered through the server, for decision validation.
    pub(crate) known_domains: std::collections::HashSet<String>,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryExecutionStore {
    inner: Arc<RwLock<Inner>>,
}

impl InMemoryExecutionStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn open_run_id(&self, inner: &Inner, domain: &str, workflow_id: &str) -> Option<String> {
        inner
            .by_workflow
            .get(&(domain.to_string(), workflow_id.to_string()))
            .and_then(|runs| {
                runs.iter().rev().find_map(|run_id| {
                    inner
                        .by_run
                        .get(&(domain.to_string(), workflow_id.to_string(), run_id.clone()))
                        .filter(|r| r.is_open())
                        .map(|_| run_id.clone())
                })
            })
    }

    fn last_close_status(
        &self,
        inner: &Inner,
        domain: &str,
        workflow_id: &str,
    ) -> Option<WorkflowExecutionCloseStatus> {
        inner
            .by_workflow
            .get(&(domain.to_string(), workflow_id.to_string()))
            .and_then(|runs| {
                runs.iter().rev().find_map(|run_id| {
                    inner
                        .by_run
                        .get(&(domain.to_string(), workflow_id.to_string(), run_id.clone()))
                        .and_then(|r| r.close_status)
                })
            })
    }

    /// Start a workflow, mirroring Go reuse-policy + request-ID semantics.
    pub fn start(
        &self,
        req: &StartWorkflowExecutionRequest,
    ) -> Result<StartWorkflowExecutionResponse, StartError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let key = (
            req.domain.clone(),
            req.workflow_id.clone(),
            req.request_id.clone(),
        );

        // Request-ID idempotency: same (domain, workflow, request) returns the
        // original run, mirroring Go's dedup on request ID.
        if !req.request_id.is_empty() {
            if let Some(run_id) = inner.by_request_id.get(&key).cloned() {
                return Ok(StartWorkflowExecutionResponse { run_id });
            }
        }

        let policy = req.workflow_id_reuse_policy.unwrap_or_default();
        if let Some(open_run) = self.open_run_id(&inner, &req.domain, &req.workflow_id) {
            if policy == WorkflowIDReusePolicy::TerminateIfRunning {
                // Close the running execution as terminated, like Go does.
                Self::close_run_locked(
                    &mut inner,
                    &req.domain,
                    &req.workflow_id,
                    &open_run,
                    WorkflowExecutionCloseStatus::Terminated,
                    None,
                    Vec::new(),
                    HistoryEvent {
                        workflow_execution_terminated_event_attributes: Some(
                            WorkflowExecutionTerminatedEventAttributes {
                                reason: "terminated by terminate-if-running reuse policy"
                                    .to_string(),
                                ..Default::default()
                            },
                        ),
                        ..Default::default()
                    },
                    EventType::WorkflowExecutionTerminated,
                );
            } else {
                let run_id = inner
                    .by_run
                    .get(&(
                        req.domain.clone(),
                        req.workflow_id.clone(),
                        open_run.clone(),
                    ))
                    .map(|r| r.execution.run_id.clone())
                    .unwrap_or_default();
                return Err(StartError::AlreadyStarted(
                    WorkflowExecutionAlreadyStartedError {
                        message: "Workflow execution is already running.".to_string(),
                        start_request_id: req.request_id.clone(),
                        run_id,
                    },
                ));
            }
        } else {
            let last = self.last_close_status(&inner, &req.domain, &req.workflow_id);
            let duplicate_blocked = policy == WorkflowIDReusePolicy::RejectDuplicate
                && last.is_some()
                || policy == WorkflowIDReusePolicy::AllowDuplicateFailedOnly
                    && matches!(
                        last,
                        Some(
                            WorkflowExecutionCloseStatus::Completed
                                | WorkflowExecutionCloseStatus::ContinuedAsNew
                        )
                    );
            if duplicate_blocked {
                return Err(StartError::AlreadyStarted(
                    WorkflowExecutionAlreadyStartedError {
                        message: "Workflow execution already started.".to_string(),
                        start_request_id: req.request_id.clone(),
                        run_id: String::new(),
                    },
                ));
            }
        }

        let run_id = uuid::Uuid::new_v4().to_string();
        let execution = WorkflowExecution::new(req.workflow_id.clone(), run_id.clone());
        let task_list = req.task_list.clone().unwrap_or_default();
        let mut record = ExecutionRecord {
            domain: req.domain.clone(),
            execution: execution.clone(),
            workflow_type: req.workflow_type.clone().unwrap_or_default(),
            task_list: task_list.clone(),
            execution_start_to_close_timeout_seconds: req.execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: req.task_start_to_close_timeout_seconds,
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            close_status: None,
            start_time_millis: now_millis(),
            close_time_millis: None,
            history: Vec::new(),
            pending_decision: None,
            pending_activities: HashMap::new(),
            pending_timers: HashMap::new(),
            pending_queries: std::collections::VecDeque::new(),
            parent: None,
            prev_started_event_id: None,
        };
        let started = HistoryEvent {
            workflow_execution_started_event_attributes: Some(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: req.workflow_type.clone(),
                    task_list: req.task_list.clone(),
                    input: req.input.clone(),
                    execution_start_to_close_timeout_seconds: req
                        .execution_start_to_close_timeout_seconds,
                    task_start_to_close_timeout_seconds: req.task_start_to_close_timeout_seconds,
                    identity: req.identity.clone(),
                    request_id: req.request_id.clone(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        record.push_event(EventType::WorkflowExecutionStarted, started);

        inner
            .by_workflow
            .entry((req.domain.clone(), req.workflow_id.clone()))
            .or_default()
            .push(run_id.clone());
        inner.by_run.insert(
            (req.domain.clone(), req.workflow_id.clone(), run_id.clone()),
            record,
        );
        if !req.request_id.is_empty() {
            inner.by_request_id.insert(key, run_id.clone());
        }
        // Every new run starts with a decision task, like Go history does.
        Self::schedule_decision_locked(
            &mut inner,
            &req.domain,
            &req.workflow_id,
            &run_id,
            &task_list.name,
            0,
        );
        Ok(StartWorkflowExecutionResponse { run_id })
    }

    /// Append a `DecisionTaskScheduled` event and enqueue the task.
    /// Skips closed executions and executions that already have an
    /// outstanding decision, mirroring Go's single-outstanding-decision rule.
    fn schedule_decision_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        task_list: &str,
        attempt: i64,
    ) {
        let record = match inner.by_run.get_mut(&(
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        )) {
            Some(r) if r.is_open() && r.pending_decision.is_none() => r,
            _ => return,
        };
        let event = HistoryEvent {
            decision_task_scheduled_event_attributes: Some(DecisionTaskScheduledEventAttributes {
                task_list: Some(record.task_list.clone()),
                start_to_close_timeout_seconds: record.task_start_to_close_timeout_seconds,
                attempt,
            }),
            ..Default::default()
        };
        record.push_event(EventType::DecisionTaskScheduled, event);
        let scheduled_event_id = record.history.len() as i64;
        record.pending_decision = Some(PendingDecision {
            scheduled_event_id,
            started_event_id: None,
            attempt,
        });
        inner
            .decision_queues
            .entry((domain.to_string(), task_list.to_string()))
            .or_default()
            .push_back((workflow_id.to_string(), run_id.to_string()));
    }

    fn get_open_mut<'a>(
        inner: &'a mut Inner,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<&'a mut ExecutionRecord, WorkflowMutationError> {
        if execution.run_id.is_empty() {
            // No run ID: target the open run, mirroring Go's current-execution lookup.
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| {
                    WorkflowMutationError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
                })?;
            let run_id = runs
                .iter()
                .rev()
                .find(|run_id| {
                    inner
                        .by_run
                        .get(&(
                            domain.to_string(),
                            execution.workflow_id.clone(),
                            (*run_id).clone(),
                        ))
                        .is_some_and(ExecutionRecord::is_open)
                })
                .cloned()
                .ok_or_else(|| {
                    WorkflowMutationError::AlreadyCompleted(
                        WorkflowExecutionAlreadyCompletedError {
                            message: "Workflow execution already completed.".to_string(),
                        },
                    )
                })?;
            return inner
                .by_run
                .get_mut(&(domain.to_string(), execution.workflow_id.clone(), run_id))
                .ok_or_else(|| {
                    WorkflowMutationError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
                });
        }
        let record = inner
            .by_run
            .get_mut(&(
                domain.to_string(),
                execution.workflow_id.clone(),
                execution.run_id.clone(),
            ))
            .ok_or_else(|| {
                WorkflowMutationError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
            })?;
        if !record.is_open() {
            return Err(WorkflowMutationError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError {
                    message: "Workflow execution already completed.".to_string(),
                },
            ));
        }
        Ok(record)
    }

    pub fn signal(
        &self,
        req: &SignalWorkflowExecutionRequest,
    ) -> Result<(), WorkflowMutationError> {
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut inner = self.inner.write().expect("execution store lock");
        let record = Self::get_open_mut(&mut inner, &req.domain, &execution)?;
        let event = HistoryEvent {
            workflow_execution_signaled_event_attributes: Some(
                WorkflowExecutionSignaledEventAttributes {
                    signal_name: req.signal_name.clone(),
                    input: req.input.clone(),
                    identity: req.identity.clone(),
                    request_id: req.request_id.clone(),
                },
            ),
            ..Default::default()
        };
        record.push_event(EventType::WorkflowExecutionSignaled, event);
        Ok(())
    }

    pub fn request_cancel(
        &self,
        req: &RequestCancelWorkflowExecutionRequest,
    ) -> Result<(), WorkflowMutationError> {
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut inner = self.inner.write().expect("execution store lock");
        let record = Self::get_open_mut(&mut inner, &req.domain, &execution)?;
        let event = HistoryEvent {
            workflow_execution_cancel_requested_event_attributes: Some(
                WorkflowExecutionCancelRequestedEventAttributes {
                    cause: req.cause.clone(),
                    external_initiated_event_id: None,
                    external_workflow_execution: None,
                    identity: req.identity.clone(),
                    request_id: req.request_id.clone(),
                },
            ),
            ..Default::default()
        };
        record.push_event(EventType::WorkflowExecutionCancelRequested, event);
        Ok(())
    }

    pub fn terminate(
        &self,
        req: &TerminateWorkflowExecutionRequest,
    ) -> Result<(), WorkflowMutationError> {
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut inner = self.inner.write().expect("execution store lock");
        // Resolve the open run first (get_open_mut validates openness).
        let (workflow_id, run_id) = {
            let record = Self::get_open_mut(&mut inner, &req.domain, &execution)?;
            (
                record.execution.workflow_id.clone(),
                record.execution.run_id.clone(),
            )
        };
        Self::close_run_locked(
            &mut inner,
            &req.domain,
            &workflow_id,
            &run_id,
            WorkflowExecutionCloseStatus::Terminated,
            None,
            req.details.clone(),
            HistoryEvent {
                workflow_execution_terminated_event_attributes: Some(
                    WorkflowExecutionTerminatedEventAttributes {
                        reason: req.reason.clone(),
                        details: req.details.clone(),
                        identity: req.identity.clone(),
                    },
                ),
                ..Default::default()
            },
            EventType::WorkflowExecutionTerminated,
        );
        Ok(())
    }

    pub fn describe(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<DescribeWorkflowExecutionResponse, EntityNotExistsError> {
        let inner = self.inner.read().expect("execution store lock");
        let missing = || EntityNotExistsError::new("Workflow execution does not exist.".to_string());
        let key: (String, String, String) = if execution.run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(missing)?;
            let latest = runs.last().ok_or_else(missing)?;
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                latest.clone(),
            )
        } else {
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                execution.run_id.clone(),
            )
        };
        let record = inner.by_run.get(&key).ok_or_else(missing)?;
        let pending_children = inner
            .children
            .get(&key)
            .map(|kids| {
                kids.iter()
                    .filter_map(|c| {
                        let child = inner.by_run.get(&(
                            c.domain.clone(),
                            c.workflow_id.clone(),
                            c.run_id.clone(),
                        ))?;
                        if !child.is_open() {
                            return None;
                        }
                        Some(cadence_api::tasks::PendingChildExecutionInfo {
                            domain: c.domain.clone(),
                            workflow_id: c.workflow_id.clone(),
                            run_id: c.run_id.clone(),
                            workflow_type_name: child.workflow_type.name.clone(),
                            initiated_id: c.initiated_event_id,
                            parent_close_policy: Some(c.parent_close_policy),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(record.to_describe_response(pending_children))
    }

    pub fn history(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<GetWorkflowExecutionHistoryResponse, EntityNotExistsError> {
        let inner = self.inner.read().expect("execution store lock");
        let key = if execution.run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| EntityNotExistsError::new("Workflow execution does not exist.".to_string()))?;
            let latest = runs.last().ok_or_else(|| EntityNotExistsError::new("Workflow execution does not exist.".to_string()))?;
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                latest.clone(),
            )
        } else {
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                execution.run_id.clone(),
            )
        };
        inner
            .by_run
            .get(&key)
            .map(|r| GetWorkflowExecutionHistoryResponse {
                history: Some(History {
                    events: r.history.clone(),
                }),
                next_page_token: Vec::new(),
                archived: false,
            })
            .ok_or_else(|| EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
    }

    /// List executions for a domain; `open_only=true` lists running workflows.
    #[must_use]
    pub fn list(&self, domain: &str, open_only: bool) -> Vec<WorkflowExecutionInfo> {
        let inner = self.inner.read().expect("execution store lock");
        let mut out: Vec<WorkflowExecutionInfo> = inner
            .by_run
            .values()
            .filter(|r| r.domain == domain && r.is_open() == open_only)
            .map(ExecutionRecord::to_info)
            .collect();
        out.sort_by_key(|i| i.start_time.unwrap_or(0));
        out
    }

    // -----------------------------------------------------------------------
    // Matching: decision tasks
    // -----------------------------------------------------------------------

    fn encode_task_token(token: &TaskToken) -> Vec<u8> {
        JsonTaskTokenSerializer::serialize(token).unwrap_or_default()
    }

    fn decode_task_token(bytes: &[u8]) -> Result<TaskToken, TaskError> {
        JsonTaskTokenSerializer::deserialize(bytes)
            .map_err(|_| TaskError::InvalidToken(BadRequestError::new("Invalid TaskToken.")))
    }

    /// Dequeue the next schedulable decision task for a task list.
    /// Stale queue entries (closed executions, already-started decisions)
    /// are dropped, mirroring matching-service task requeue behavior.
    pub fn poll_decision(
        &self,
        domain: &str,
        task_list: &str,
        identity: &str,
    ) -> Option<PollForDecisionTaskResponse> {
        let mut inner = self.inner.write().expect("execution store lock");
        let key = (domain.to_string(), task_list.to_string());
        loop {
            let (workflow_id, run_id) = inner.decision_queues.get_mut(&key)?.pop_front()?;
            let backlog = inner
                .decision_queues
                .get(&key)
                .map(|q| q.len())
                .unwrap_or(0);
            // Clone the pending decision first so the response can be built
            // without holding a borrow across queue access.
            let (pending, started_event_id) = {
                let record = inner.by_run.get_mut(&(
                    domain.to_string(),
                    workflow_id.clone(),
                    run_id.clone(),
                ))?;
                let pending = record.pending_decision.clone()?;
                if !record.is_open() || pending.started_event_id.is_some() {
                    continue; // stale entry
                }
                let started_event_id = record.next_event_id();
                let event = HistoryEvent {
                    decision_task_started_event_attributes: Some(
                        DecisionTaskStartedEventAttributes {
                            scheduled_event_id: pending.scheduled_event_id,
                            identity: identity.to_string(),
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                };
                record.push_event(EventType::DecisionTaskStarted, event);
                if let Some(p) = record.pending_decision.as_mut() {
                    p.started_event_id = Some(started_event_id);
                }
                (pending, started_event_id)
            };
            // Gather response data without holding the write borrow.
            let response_data = {
                let record =
                    inner
                        .by_run
                        .get(&(domain.to_string(), workflow_id.clone(), run_id.clone()))?;
                (
                    record.workflow_type.clone(),
                    record.task_list.clone(),
                    record.prev_started_event_id,
                    record.history.clone(),
                    Self::head_query(record),
                    Self::head_query_id(record),
                    Self::all_queries(record),
                )
            };
            let (workflow_type, task_list, prev_started, history, head_query, head_query_id, queries) =
                response_data;
            let token = TaskToken {
                domain_id: domain.to_string(),
                workflow_id: workflow_id.clone(),
                workflow_type: workflow_type.name.clone(),
                run_id: run_id.clone(),
                schedule_id: pending.scheduled_event_id,
                schedule_attempt: pending.attempt,
                activity_id: String::new(),
                activity_type: String::new(),
            };
            // Attach the head buffered query like Go's embedded legacy query.
            return Some(PollForDecisionTaskResponse {
                task_token: Self::encode_task_token(&token),
                workflow_execution: Some(WorkflowExecution::new(workflow_id, run_id)),
                workflow_type: Some(workflow_type),
                previous_started_event_id: prev_started,
                started_event_id,
                attempt: pending.attempt,
                backlog_count_hint: backlog as i64,
                history: Some(History { events: history }),
                next_page_token: Vec::new(),
                workflow_execution_task_list: Some(task_list),
                query: head_query,
                query_id: head_query_id,
                queries,
            });
        }
    }

    /// Apply a completed decision task: validate every decision first (like
    /// Go history does), then record completion and apply them in order.
    /// Semantic decision errors are returned without mutating state; the
    /// worker may fix and retry. Closing decisions do not schedule a new
    /// decision task.
    pub fn complete_decision(
        &self,
        req: &RespondDecisionTaskCompletedRequest,
    ) -> Result<RespondDecisionTaskCompletedResponse, TaskError> {
        let token = Self::decode_task_token(&req.task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let record = inner.by_run.get(&record_key).ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
        })?;
        if !record.is_open() {
            return Err(TaskError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError {
                    message: "Workflow execution already completed.".to_string(),
                },
            ));
        }
        let pending = record
            .pending_decision
            .clone()
            .ok_or_else(|| TaskError::InvalidToken(BadRequestError::new("Invalid TaskToken.")))?;
        if pending.started_event_id.is_none() || pending.scheduled_event_id != token.schedule_id {
            return Err(TaskError::InvalidToken(BadRequestError::new(
                "Invalid TaskToken.",
            )));
        }
        Self::validate_decisions(&inner, &record_key, &req.decisions)?;
        // External events can arrive after this decision was polled but before
        // its completion is committed. In that case the worker's response
        // cannot have observed those events, so another replay task is needed.
        let has_new_events = inner
            .by_run
            .get(&record_key)
            .and_then(|record| record.history.last())
            .is_some_and(|event| event.event_id > pending.started_event_id.unwrap_or_default());
        // All valid: record completion.
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        let started_event_id = pending.started_event_id.expect("verified started");
        let completed = HistoryEvent {
            decision_task_completed_event_attributes: Some(DecisionTaskCompletedEventAttributes {
                execution_context: req.execution_context.clone(),
                scheduled_event_id: pending.scheduled_event_id,
                started_event_id,
                identity: req.identity.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        record.push_event(EventType::DecisionTaskCompleted, completed);
        let completed_event_id = record.history.len() as i64;
        record.prev_started_event_id = Some(started_event_id);
        record.pending_decision = None;
        if let Some(results) = req.query_results.clone() {
            for q in record.pending_queries.iter_mut() {
                if q.answered.is_none() {
                    if let Some(result) = results.get(&q.query_id) {
                        q.answered = Some(result.clone());
                    }
                }
            }
        }
        let domain = record.domain.clone();
        let workflow_id = record.execution.workflow_id.clone();
        let run_id = record.execution.run_id.clone();
        let task_list = record.task_list.name.clone();
        let has_decisions = !req.decisions.is_empty();
        Self::apply_decisions_locked(
            &mut inner,
            &domain,
            &workflow_id,
            &run_id,
            &req.decisions,
            completed_event_id,
            &req.identity,
        )?;
        // A non-empty command batch gets one follow-up task so the worker can
        // observe the recorded decision events on replay. An empty replay
        // means the workflow is blocked on a timer/activity/signal; enqueueing
        // another task there would spin indefinitely.
        if (has_decisions || has_new_events)
            && inner
                .by_run
                .get(&record_key)
                .is_some_and(ExecutionRecord::is_open)
        {
            Self::schedule_decision_locked(
                &mut inner,
                &domain,
                &workflow_id,
                &run_id,
                &task_list,
                0,
            );
        }
        Ok(RespondDecisionTaskCompletedResponse {
            decision_task: None,
        })
    }

    /// Validate a decision batch against the execution state.
    /// Mirrors the Go history `validateDecision*` checks for the supported
    /// decision subset.
    fn validate_decisions(
        inner: &Inner,
        record_key: &(String, String, String),
        decisions: &[cadence_api::tasks::Decision],
    ) -> Result<(), TaskError> {
        let record = inner.by_run.get(record_key).expect("record exists");
        let mut seen_activity_ids = std::collections::HashSet::new();
        for d in decisions {
            match d.decision_type {
                None => {
                    return Err(TaskError::BadDecision(BadRequestError::new(
                        "Unhandled decision: decisionType is not set.",
                    )));
                }
                Some(DecisionType::ScheduleActivityTask) => {
                    let a = d
                        .schedule_activity_task_decision_attributes
                        .as_ref()
                        .ok_or_else(|| {
                            TaskError::BadDecision(BadRequestError::new(
                                "Bad schedule activity attributes: attributes not set.",
                            ))
                        })?;
                    if a.activity_id.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad schedule activity attributes: activityId is not set.",
                        )));
                    }
                    if a.activity_type.as_ref().is_none_or(|t| t.name.is_empty()) {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad schedule activity attributes: activityType is not set.",
                        )));
                    }
                    if !seen_activity_ids.insert(a.activity_id.clone()) {
                        return Err(TaskError::BadDecision(BadRequestError::new(format!(
                            "Bad schedule activity attributes: duplicate activityId {:?} in decision batch.",
                            a.activity_id
                        ))));
                    }
                    if record
                        .pending_activities
                        .values()
                        .any(|p| p.activity_id == a.activity_id)
                    {
                        return Err(TaskError::BadDecision(BadRequestError::new(format!(
                            "Bad schedule activity attributes: activityId {:?} already scheduled.",
                            a.activity_id
                        ))));
                    }
                }
                Some(DecisionType::CompleteWorkflowExecution) => {}
                Some(DecisionType::FailWorkflowExecution) => {}
                Some(DecisionType::CancelWorkflowExecution) => {}
                Some(DecisionType::StartTimer) => {
                    let t = d.start_timer_decision_attributes.as_ref().ok_or_else(|| {
                        TaskError::BadDecision(BadRequestError::new(
                            "Bad start timer attributes: attributes not set.",
                        ))
                    })?;
                    if t.timer_id.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad start timer attributes: timerId is not set.",
                        )));
                    }
                    if t.start_to_fire_timeout_seconds.is_none_or(|v| v <= 0) {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad start timer attributes: startToFireTimeoutSeconds must be positive.",
                        )));
                    }
                }
                Some(DecisionType::CancelTimer) => {
                    let t = d.cancel_timer_decision_attributes.as_ref().ok_or_else(|| {
                        TaskError::BadDecision(BadRequestError::new(
                            "Bad cancel timer attributes: attributes not set.",
                        ))
                    })?;
                    if t.timer_id.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad cancel timer attributes: timerId is not set.",
                        )));
                    }
                }
                Some(DecisionType::RecordMarker) => {
                    let m = d
                        .record_marker_decision_attributes
                        .as_ref()
                        .ok_or_else(|| {
                            TaskError::BadDecision(BadRequestError::new(
                                "Bad record marker attributes: attributes not set.",
                            ))
                        })?;
                    if m.marker_name.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad record marker attributes: markerName is not set.",
                        )));
                    }
                }
                Some(DecisionType::RequestCancelActivityTask) => {
                    let c = d
                        .request_cancel_activity_task_decision_attributes
                        .as_ref()
                        .ok_or_else(|| {
                            TaskError::BadDecision(BadRequestError::new(
                                "Bad request cancel activity attributes: attributes not set.",
                            ))
                        })?;
                    if c.activity_id.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad request cancel activity attributes: activityId is not set.",
                        )));
                    }
                    if !record
                        .pending_activities
                        .values()
                        .any(|p| p.activity_id == c.activity_id)
                    {
                        return Err(TaskError::BadDecision(BadRequestError::new(format!(
                            "Bad request cancel activity attributes: activityId {:?} not scheduled.",
                            c.activity_id
                        ))));
                    }
                }
                Some(DecisionType::ContinueAsNewWorkflowExecution) => {
                    if d.continue_as_new_workflow_execution_decision_attributes
                        .is_none()
                    {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad continue-as-new attributes: attributes not set.",
                        )));
                    }
                }
                Some(DecisionType::StartChildWorkflowExecution) => {
                    let c = d
                        .start_child_workflow_execution_decision_attributes
                        .as_ref()
                        .ok_or_else(|| {
                            TaskError::BadDecision(BadRequestError::new(
                                "Bad start child attributes: attributes not set.",
                            ))
                        })?;
                    if c.domain.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad start child attributes: domain is not set.",
                        )));
                    }
                    if !inner.known_domains.contains(&c.domain) {
                        return Err(TaskError::BadDecision(BadRequestError::new(format!(
                            "Bad start child attributes: domain {:?} does not exist.",
                            c.domain
                        ))));
                    }
                    if c.workflow_id.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad start child attributes: workflowId is not set.",
                        )));
                    }
                    if c.workflow_type.as_ref().is_none_or(|t| t.name.is_empty()) {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad start child attributes: workflowType is not set.",
                        )));
                    }
                }
                Some(DecisionType::SignalExternalWorkflowExecution) => {
                    let s = d
                        .signal_external_workflow_execution_decision_attributes
                        .as_ref()
                        .ok_or_else(|| {
                            TaskError::BadDecision(BadRequestError::new(
                                "Bad signal attributes: attributes not set.",
                            ))
                        })?;
                    if s.domain.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad signal attributes: domain is not set.",
                        )));
                    }
                    if !inner.known_domains.contains(&s.domain) {
                        return Err(TaskError::BadDecision(BadRequestError::new(format!(
                            "Bad signal attributes: domain {:?} does not exist.",
                            s.domain
                        ))));
                    }
                    if s.execution
                        .as_ref()
                        .is_none_or(|e| e.workflow_id.is_empty())
                    {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad signal attributes: execution is not set.",
                        )));
                    }
                    if s.signal_name.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad signal attributes: signalName is not set.",
                        )));
                    }
                }
                Some(DecisionType::UpsertWorkflowSearchAttributes) => {
                    let u = d
                        .upsert_workflow_search_attributes_decision_attributes
                        .as_ref()
                        .ok_or_else(|| {
                            TaskError::BadDecision(BadRequestError::new(
                                "Bad search attributes: attributes not set.",
                            ))
                        })?;
                    if u.search_attributes.is_none() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad search attributes: searchAttributes is not set.",
                        )));
                    }
                }
                Some(DecisionType::RequestCancelExternalWorkflowExecution) => {
                    let c = d
                        .request_cancel_external_workflow_execution_decision_attributes
                        .as_ref()
                        .ok_or_else(|| {
                            TaskError::BadDecision(BadRequestError::new(
                                "Bad request-cancel attributes: attributes not set.",
                            ))
                        })?;
                    if c.domain.is_empty() || c.workflow_id.is_empty() {
                        return Err(TaskError::BadDecision(BadRequestError::new(
                            "Bad request-cancel attributes: domain or workflowId is not set.",
                        )));
                    }
                    if !inner.known_domains.contains(&c.domain) {
                        return Err(TaskError::BadDecision(BadRequestError::new(format!(
                            "Bad request-cancel attributes: domain {:?} does not exist.",
                            c.domain
                        ))));
                    }
                }
                Some(other) => {
                    return Err(TaskError::BadDecision(BadRequestError::new(format!(
                        "Unsupported decision type {other} in this server version."
                    ))));
                }
            }
        }
        Ok(())
    }

    /// Apply validated decisions in order. The caller guarantees validity.
    fn apply_decisions_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        decisions: &[cadence_api::tasks::Decision],
        completed_event_id: i64,
        identity: &str,
    ) -> Result<(), TaskError> {
        for d in decisions {
            let record = inner
                .by_run
                .get_mut(&(
                    domain.to_string(),
                    workflow_id.to_string(),
                    run_id.to_string(),
                ))
                .expect("record exists");
            if !record.is_open() {
                break;
            }
            match d.decision_type {
                Some(DecisionType::ScheduleActivityTask) => {
                    let a = d
                        .schedule_activity_task_decision_attributes
                        .as_ref()
                        .expect("validated");
                    let event = HistoryEvent {
                        activity_task_scheduled_event_attributes: Some(
                            ActivityTaskScheduledEventAttributes {
                                activity_id: a.activity_id.clone(),
                                activity_type: a.activity_type.clone(),
                                domain: Some(if a.domain.is_empty() {
                                    domain.to_string()
                                } else {
                                    a.domain.clone()
                                }),
                                task_list: a.task_list.clone(),
                                input: a.input.clone(),
                                schedule_to_close_timeout_seconds: a
                                    .schedule_to_close_timeout_seconds,
                                schedule_to_start_timeout_seconds: a
                                    .schedule_to_start_timeout_seconds,
                                start_to_close_timeout_seconds: a.start_to_close_timeout_seconds,
                                heartbeat_timeout_seconds: a.heartbeat_timeout_seconds,
                                decision_task_completed_event_id: completed_event_id,
                                retry_policy: a.retry_policy.clone(),
                                header: a.header.clone(),
                            },
                        ),
                        ..Default::default()
                    };
                    record.push_event(EventType::ActivityTaskScheduled, event);
                    let scheduled_event_id = record.history.len() as i64;
                    let task_list_name = a
                        .task_list
                        .as_ref()
                        .map(|t| t.name.clone())
                        .filter(|n| !n.is_empty())
                        .unwrap_or_else(|| record.task_list.name.clone());
                    record.pending_activities.insert(
                        scheduled_event_id,
                        PendingActivity {
                            activity_id: a.activity_id.clone(),
                            activity_type: a.activity_type.clone().unwrap_or_default(),
                            input: a.input.clone(),
                            task_list: a
                                .task_list
                                .clone()
                                .unwrap_or_else(|| record.task_list.clone()),
                            scheduled_event_id,
                            started_event_id: None,
                            attempt: 0,
                            cancel_requested: false,
                            cancel_requested_event_id: None,
                            heartbeat_details: Vec::new(),
                            queued: true,
                            retry_ready_millis: None,
                            schedule_to_close_timeout_seconds: a.schedule_to_close_timeout_seconds,
                            start_to_close_timeout_seconds: a.start_to_close_timeout_seconds,
                            heartbeat_timeout_seconds: a.heartbeat_timeout_seconds,
                            scheduled_time_millis: now_millis(),
                            retry_policy: a.retry_policy.clone(),
                            header: a.header.clone(),
                        },
                    );
                    inner
                        .activity_queues
                        .entry((domain.to_string(), task_list_name))
                        .or_default()
                        .push_back((
                            workflow_id.to_string(),
                            run_id.to_string(),
                            scheduled_event_id,
                        ));
                }
                Some(DecisionType::CompleteWorkflowExecution) => {
                    let result = d
                        .complete_workflow_execution_decision_attributes
                        .as_ref()
                        .map(|a| a.result.clone())
                        .unwrap_or_default();
                    Self::close_run_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        WorkflowExecutionCloseStatus::Completed,
                        None,
                        result.clone(),
                        HistoryEvent {
                            workflow_execution_completed_event_attributes: Some(
                                WorkflowExecutionCompletedEventAttributes {
                                    result,
                                    decision_task_completed_event_id: completed_event_id,
                                },
                            ),
                            ..Default::default()
                        },
                        EventType::WorkflowExecutionCompleted,
                    );
                }
                Some(DecisionType::FailWorkflowExecution) => {
                    let (reason, details) = d
                        .fail_workflow_execution_decision_attributes
                        .as_ref()
                        .map(|f| (f.reason.clone(), f.details.clone()))
                        .unwrap_or((None, Vec::new()));
                    Self::close_run_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        WorkflowExecutionCloseStatus::Failed,
                        reason.clone(),
                        details.clone(),
                        HistoryEvent {
                            workflow_execution_failed_event_attributes: Some(
                                WorkflowExecutionFailedEventAttributes {
                                    reason,
                                    details,
                                    decision_task_completed_event_id: completed_event_id,
                                },
                            ),
                            ..Default::default()
                        },
                        EventType::WorkflowExecutionFailed,
                    );
                }
                Some(DecisionType::CancelWorkflowExecution) => {
                    let details = d
                        .cancel_workflow_execution_decision_attributes
                        .as_ref()
                        .map(|c| c.details.clone())
                        .unwrap_or_default();
                    Self::close_run_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        WorkflowExecutionCloseStatus::Canceled,
                        None,
                        details.clone(),
                        HistoryEvent {
                            workflow_execution_canceled_event_attributes: Some(
                                WorkflowExecutionCanceledEventAttributes {
                                    decision_task_completed_event_id: completed_event_id,
                                    details,
                                },
                            ),
                            ..Default::default()
                        },
                        EventType::WorkflowExecutionCanceled,
                    );
                }
                Some(DecisionType::StartTimer) => {
                    let t = d
                        .start_timer_decision_attributes
                        .as_ref()
                        .expect("validated");
                    record.push_event(
                        EventType::TimerStarted,
                        HistoryEvent {
                            timer_started_event_attributes: Some(TimerStartedEventAttributes {
                                timer_id: t.timer_id.clone(),
                                start_to_fire_timeout_seconds: t.start_to_fire_timeout_seconds,
                                decision_task_completed_event_id: completed_event_id,
                            }),
                            ..Default::default()
                        },
                    );
                    let started_event_id = record.history.len() as i64;
                    let timeout_secs = t.start_to_fire_timeout_seconds.unwrap_or(0).max(0);
                    record.pending_timers.insert(
                        t.timer_id.clone(),
                        PendingTimer {
                            started_event_id,
                            deadline_millis: now_millis() + timeout_secs * 1000,
                        },
                    );
                }
                Some(DecisionType::CancelTimer) => {
                    let t = d
                        .cancel_timer_decision_attributes
                        .as_ref()
                        .expect("validated");
                    let started_event_id = record
                        .pending_timers
                        .get(&t.timer_id)
                        .map(|t| t.started_event_id)
                        .unwrap_or(0);
                    record.pending_timers.remove(&t.timer_id);
                    record.push_event(
                        EventType::TimerCanceled,
                        HistoryEvent {
                            timer_canceled_event_attributes: Some(TimerCanceledEventAttributes {
                                timer_id: t.timer_id.clone(),
                                started_event_id,
                                decision_task_completed_event_id: completed_event_id,
                                identity: identity.to_string(),
                            }),
                            ..Default::default()
                        },
                    );
                }
                Some(DecisionType::RecordMarker) => {
                    let m = d
                        .record_marker_decision_attributes
                        .as_ref()
                        .expect("validated");
                    record.push_event(
                        EventType::MarkerRecorded,
                        HistoryEvent {
                            marker_recorded_event_attributes: Some(MarkerRecordedEventAttributes {
                                marker_name: m.marker_name.clone(),
                                details: m.details.clone(),
                                decision_task_completed_event_id: completed_event_id,
                                header: None,
                            }),
                            ..Default::default()
                        },
                    );
                }
                Some(DecisionType::RequestCancelActivityTask) => {
                    let c = d
                        .request_cancel_activity_task_decision_attributes
                        .as_ref()
                        .expect("validated");
                    let sched_id = record
                        .pending_activities
                        .iter()
                        .find(|(_, p)| p.activity_id == c.activity_id)
                        .map(|(id, _)| *id);
                    if let Some(id) = sched_id {
                        if let Some(p) = record.pending_activities.get_mut(&id) {
                            p.cancel_requested = true;
                        }
                        record.push_event(
                            EventType::ActivityTaskCancelRequested,
                            HistoryEvent {
                                activity_task_cancel_requested_event_attributes: Some(
                                    cadence_api::tasks::ActivityTaskCancelRequestedEventAttributes {
                                        activity_id: c.activity_id.clone(),
                                        decision_task_completed_event_id: completed_event_id,
                                    },
                                ),
                                ..Default::default()
                            },
                        );
                        let cancel_event_id = record.history.len() as i64;
                        if let Some(p) = record.pending_activities.get_mut(&id) {
                            p.cancel_requested_event_id = Some(cancel_event_id);
                        }
                    }
                }
                Some(DecisionType::ContinueAsNewWorkflowExecution) => {
                    let attrs = d
                        .continue_as_new_workflow_execution_decision_attributes
                        .clone()
                        .expect("validated");
                    Self::continue_as_new_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        &attrs,
                        completed_event_id,
                    );
                }
                Some(DecisionType::StartChildWorkflowExecution) => {
                    let attrs = d
                        .start_child_workflow_execution_decision_attributes
                        .clone()
                        .expect("validated");
                    Self::start_child_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        &attrs,
                        completed_event_id,
                        identity,
                    );
                }
                Some(DecisionType::SignalExternalWorkflowExecution) => {
                    let attrs = d
                        .signal_external_workflow_execution_decision_attributes
                        .clone()
                        .expect("validated");
                    Self::signal_external_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        &attrs,
                        completed_event_id,
                        identity,
                    );
                }
                Some(DecisionType::UpsertWorkflowSearchAttributes) => {
                    let attrs = d
                        .upsert_workflow_search_attributes_decision_attributes
                        .clone()
                        .expect("validated");
                    let record = inner
                        .by_run
                        .get_mut(&(
                            domain.to_string(),
                            workflow_id.to_string(),
                            run_id.to_string(),
                        ))
                        .expect("record exists");
                    if let Some(sa) = attrs.search_attributes.clone() {
                        let entry = record
                            .search_attributes
                            .get_or_insert_with(SearchAttributes::default);
                        let fields = entry.indexed_fields.get_or_insert_with(Default::default);
                        if let Some(new_fields) = sa.indexed_fields {
                            fields.extend(new_fields);
                        }
                    }
                    record.push_event(
                        EventType::UpsertWorkflowSearchAttributes,
                        HistoryEvent {
                            upsert_workflow_search_attributes_event_attributes: Some(
                                UpsertWorkflowSearchAttributesEventAttributes {
                                    decision_task_completed_event_id: completed_event_id,
                                    search_attributes: attrs.search_attributes,
                                },
                            ),
                            ..Default::default()
                        },
                    );
                }
                Some(DecisionType::RequestCancelExternalWorkflowExecution) => {
                    let attrs = d
                        .request_cancel_external_workflow_execution_decision_attributes
                        .clone()
                        .expect("validated");
                    Self::request_cancel_external_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        &attrs,
                        completed_event_id,
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Apply a continue-as-new decision: close the current run and spawn
    /// its successor, mirroring Go history. Children stay attached by
    /// repointing them at the new run.
    fn continue_as_new_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        attrs: &cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes,
        completed_event_id: i64,
    ) {
        use cadence_api::tasks::WorkflowExecutionContinuedAsNewEventAttributes;
        let new_run_id = uuid::Uuid::new_v4().to_string();
        let (workflow_type, task_list, timeouts, memo, search_attributes, parent) = {
            let old = inner
                .by_run
                .get(&(
                    domain.to_string(),
                    workflow_id.to_string(),
                    run_id.to_string(),
                ))
                .expect("record exists");
            (
                attrs
                    .workflow_type
                    .clone()
                    .unwrap_or_else(|| old.workflow_type.clone()),
                attrs
                    .task_list
                    .clone()
                    .unwrap_or_else(|| old.task_list.clone()),
                (
                    attrs
                        .execution_start_to_close_timeout_seconds
                        .or(old.execution_start_to_close_timeout_seconds),
                    attrs
                        .task_start_to_close_timeout_seconds
                        .or(old.task_start_to_close_timeout_seconds),
                ),
                old.memo.clone(),
                old.search_attributes.clone(),
                old.parent.clone(),
            )
        };
        // Close the old run first.
        Self::close_run_locked(
            inner,
            domain,
            workflow_id,
            run_id,
            WorkflowExecutionCloseStatus::ContinuedAsNew,
            None,
            Vec::new(),
            HistoryEvent {
                workflow_execution_continued_as_new_event_attributes: Some(
                    WorkflowExecutionContinuedAsNewEventAttributes {
                        new_execution_run_id: new_run_id.clone(),
                        workflow_type: Some(workflow_type.clone()),
                        task_list: Some(task_list.clone()),
                        input: attrs.input.clone(),
                        execution_start_to_close_timeout_seconds: timeouts.0,
                        task_start_to_close_timeout_seconds: timeouts.1,
                        decision_task_completed_event_id: completed_event_id,
                        initiator: None,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
            EventType::WorkflowExecutionContinuedAsNew,
        );
        // Repoint children and the parent link at the successor run.
        let old_key = (
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        let new_key = (
            domain.to_string(),
            workflow_id.to_string(),
            new_run_id.clone(),
        );
        if let Some(kids) = inner.children.remove(&old_key) {
            inner.children.insert(new_key.clone(), kids);
        }
        if parent.is_some() {
            for kids in inner.children.values_mut() {
                for child in kids.iter_mut() {
                    if child.domain == domain
                        && child.workflow_id == workflow_id
                        && child.run_id == run_id
                    {
                        child.run_id = new_run_id.clone();
                    }
                }
            }
        }
        let mut record = ExecutionRecord {
            domain: domain.to_string(),
            execution: WorkflowExecution::new(workflow_id.to_string(), new_run_id.clone()),
            workflow_type,
            task_list: task_list.clone(),
            execution_start_to_close_timeout_seconds: timeouts.0,
            task_start_to_close_timeout_seconds: timeouts.1,
            memo,
            search_attributes,
            close_status: None,
            start_time_millis: now_millis(),
            close_time_millis: None,
            history: Vec::new(),
            pending_decision: None,
            pending_activities: HashMap::new(),
            pending_timers: HashMap::new(),
            pending_queries: std::collections::VecDeque::new(),
            parent,
            prev_started_event_id: None,
        };
        record.push_event(
            EventType::WorkflowExecutionStarted,
            HistoryEvent {
                workflow_execution_started_event_attributes: Some(
                    WorkflowExecutionStartedEventAttributes {
                        input: attrs.input.clone(),
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
        );
        inner
            .by_workflow
            .entry((domain.to_string(), workflow_id.to_string()))
            .or_default()
            .push(new_run_id.clone());
        inner.by_run.insert(new_key.clone(), record);
        Self::schedule_decision_locked(inner, domain, workflow_id, &new_run_id, &task_list.name, 0);
    }

    /// Apply a start-child decision: append Initiated to the parent and
    /// spawn the child run, mirroring Go history. A duplicate open child
    /// records `StartChildWorkflowExecutionFailed` instead.
    fn start_child_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        attrs: &cadence_api::tasks::StartChildWorkflowExecutionDecisionAttributes,
        completed_event_id: i64,
        _identity: &str,
    ) {
        use cadence_api::tasks::{
            StartChildWorkflowExecutionFailedEventAttributes,
            StartChildWorkflowExecutionInitiatedEventAttributes,
        };
        let parent_key = (
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        // The Initiated event always lands on the parent first.
        let initiated_event_id = {
            let parent = inner.by_run.get_mut(&parent_key).expect("record exists");
            parent.push_event(
                EventType::StartChildWorkflowExecutionInitiated,
                HistoryEvent {
                    start_child_workflow_execution_initiated_event_attributes: Some(
                        StartChildWorkflowExecutionInitiatedEventAttributes {
                            domain: attrs.domain.clone(),
                            workflow_id: attrs.workflow_id.clone(),
                            workflow_type: attrs.workflow_type.clone(),
                            task_list: attrs.task_list.clone(),
                            input: attrs.input.clone(),
                            execution_start_to_close_timeout_seconds: attrs
                                .execution_start_to_close_timeout_seconds,
                            task_start_to_close_timeout_seconds: attrs
                                .task_start_to_close_timeout_seconds,
                            parent_close_policy: attrs.parent_close_policy,
                            control: attrs.control.clone(),
                            decision_task_completed_event_id: completed_event_id,
                            workflow_id_reuse_policy: attrs.workflow_id_reuse_policy,
                            header: attrs.header.clone(),
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                },
            );
            parent.history.len() as i64
        };
        // Duplicate open child: record the Failed event like Go does.
        let duplicate = inner
            .by_workflow
            .get(&(attrs.domain.clone(), attrs.workflow_id.clone()))
            .is_some_and(|runs| {
                runs.iter().any(|r| {
                    inner
                        .by_run
                        .get(&(attrs.domain.clone(), attrs.workflow_id.clone(), r.clone()))
                        .is_some_and(ExecutionRecord::is_open)
                })
            });
        if duplicate
            && attrs.workflow_id_reuse_policy != Some(WorkflowIDReusePolicy::TerminateIfRunning)
        {
            let parent = inner.by_run.get_mut(&parent_key).expect("record exists");
            parent.push_event(
                EventType::StartChildWorkflowExecutionFailed,
                HistoryEvent {
                    start_child_workflow_execution_failed_event_attributes: Some(
                        StartChildWorkflowExecutionFailedEventAttributes {
                            domain: attrs.domain.clone(),
                            workflow_id: attrs.workflow_id.clone(),
                            workflow_type: attrs.workflow_type.clone(),
                            cause: Some(ChildWorkflowExecutionFailedCause::WorkflowAlreadyRunning),
                            control: attrs.control.clone(),
                            initiated_event_id,
                            decision_task_completed_event_id: completed_event_id,
                        },
                    ),
                    ..Default::default()
                },
            );
            return;
        }
        let child_run_id = uuid::Uuid::new_v4().to_string();
        let child_task_list = attrs
            .task_list
            .clone()
            .filter(|t| !t.name.is_empty())
            .unwrap_or_else(|| {
                inner
                    .by_run
                    .get(&parent_key)
                    .map(|p| p.task_list.clone())
                    .unwrap_or_default()
            });
        let mut child = ExecutionRecord {
            domain: attrs.domain.clone(),
            execution: WorkflowExecution::new(attrs.workflow_id.clone(), child_run_id.clone()),
            workflow_type: attrs.workflow_type.clone().unwrap_or_default(),
            task_list: child_task_list.clone(),
            execution_start_to_close_timeout_seconds: attrs
                .execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: attrs.task_start_to_close_timeout_seconds,
            memo: attrs.memo.clone(),
            search_attributes: attrs.search_attributes.clone(),
            close_status: None,
            start_time_millis: now_millis(),
            close_time_millis: None,
            history: Vec::new(),
            pending_decision: None,
            pending_activities: HashMap::new(),
            pending_timers: HashMap::new(),
            pending_queries: std::collections::VecDeque::new(),
            parent: Some(ParentRef {
                domain: domain.to_string(),
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                initiated_event_id,
            }),
            prev_started_event_id: None,
        };
        child.push_event(
            EventType::WorkflowExecutionStarted,
            HistoryEvent {
                workflow_execution_started_event_attributes: Some(
                    WorkflowExecutionStartedEventAttributes {
                        workflow_type: attrs.workflow_type.clone(),
                        parent_workflow_domain: Some(domain.to_string()),
                        parent_workflow_execution: Some(WorkflowExecution::new(
                            workflow_id.to_string(),
                            run_id.to_string(),
                        )),
                        parent_initiated_event_id: Some(initiated_event_id),
                        task_list: Some(child_task_list.clone()),
                        input: attrs.input.clone(),
                        execution_start_to_close_timeout_seconds: attrs
                            .execution_start_to_close_timeout_seconds,
                        task_start_to_close_timeout_seconds: attrs
                            .task_start_to_close_timeout_seconds,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
        );
        inner
            .by_workflow
            .entry((attrs.domain.clone(), attrs.workflow_id.clone()))
            .or_default()
            .push(child_run_id.clone());
        inner.by_run.insert(
            (
                attrs.domain.clone(),
                attrs.workflow_id.clone(),
                child_run_id.clone(),
            ),
            child,
        );
        inner
            .children
            .entry(parent_key)
            .or_default()
            .push(ChildRef {
                domain: attrs.domain.clone(),
                workflow_id: attrs.workflow_id.clone(),
                run_id: child_run_id.clone(),
                initiated_event_id,
                parent_close_policy: attrs
                    .parent_close_policy
                    .unwrap_or(ParentClosePolicy::Abandon),
            });
        // Parent observes the child start, like Go's ChildWorkflowExecutionStarted.
        let parent = inner
            .by_run
            .get_mut(&(
                domain.to_string(),
                workflow_id.to_string(),
                run_id.to_string(),
            ))
            .expect("record exists");
        parent.push_event(
            EventType::ChildWorkflowExecutionStarted,
            HistoryEvent {
                child_workflow_execution_started_event_attributes: Some(
                    ChildWorkflowExecutionStartedEventAttributes {
                        domain: attrs.domain.clone(),
                        initiated_event_id,
                        workflow_execution: Some(WorkflowExecution::new(
                            attrs.workflow_id.clone(),
                            child_run_id.clone(),
                        )),
                        workflow_type: attrs.workflow_type.clone(),
                        header: None,
                    },
                ),
                ..Default::default()
            },
        );
        Self::schedule_decision_locked(
            inner,
            &attrs.domain,
            &attrs.workflow_id,
            &child_run_id,
            &child_task_list.name,
            0,
        );
    }

    /// Resolve a cross-workflow target (signal / request-cancel).
    /// `child_only` restricts to children of the source run, like Go.
    fn resolve_external_target(
        inner: &Inner,
        source_key: &(String, String, String),
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        child_only: bool,
    ) -> Result<(String, String, String), ExternalTargetError> {
        if child_only {
            let kids = inner.children.get(source_key).cloned().unwrap_or_default();
            let hit = kids.into_iter().find(|c| {
                c.domain == domain
                    && c.workflow_id == workflow_id
                    && (run_id.is_empty() || c.run_id == run_id)
            });
            return match hit {
                Some(c) => {
                    let open = inner
                        .by_run
                        .get(&(c.domain.clone(), c.workflow_id.clone(), c.run_id.clone()))
                        .is_some_and(ExecutionRecord::is_open);
                    if open {
                        Ok((c.domain, c.workflow_id, c.run_id))
                    } else {
                        Err(ExternalTargetError::Completed)
                    }
                }
                None => Err(ExternalTargetError::Unknown),
            };
        }
        if run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), workflow_id.to_string()));
            match runs {
                None => Err(ExternalTargetError::Unknown),
                Some(runs) => {
                    for r in runs.iter().rev() {
                        let open = inner
                            .by_run
                            .get(&(domain.to_string(), workflow_id.to_string(), r.clone()))
                            .is_some_and(ExecutionRecord::is_open);
                        if open {
                            return Ok((domain.to_string(), workflow_id.to_string(), r.clone()));
                        }
                    }
                    Err(ExternalTargetError::Completed)
                }
            }
        } else {
            match inner.by_run.get(&(
                domain.to_string(),
                workflow_id.to_string(),
                run_id.to_string(),
            )) {
                None => Err(ExternalTargetError::Unknown),
                Some(r) if r.is_open() => Ok((
                    domain.to_string(),
                    workflow_id.to_string(),
                    run_id.to_string(),
                )),
                Some(_) => Err(ExternalTargetError::Completed),
            }
        }
    }

    /// Apply a signal-external decision: deliver to the target or record a
    /// Failed event, mirroring Go history.
    fn signal_external_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        attrs: &cadence_api::tasks::SignalExternalWorkflowExecutionDecisionAttributes,
        completed_event_id: i64,
        identity: &str,
    ) {
        let source_key = (
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        let target_exec = attrs.execution.clone().unwrap_or_default();
        // The Initiated event always lands on the source first.
        let initiated_event_id = {
            let source = inner.by_run.get_mut(&source_key).expect("record exists");
            source.push_event(
                EventType::SignalExternalWorkflowExecutionInitiated,
                HistoryEvent {
                    signal_external_workflow_execution_initiated_event_attributes: Some(
                        SignalExternalWorkflowExecutionInitiatedEventAttributes {
                            decision_task_completed_event_id: completed_event_id,
                            domain: attrs.domain.clone(),
                            workflow_execution: Some(target_exec.clone()),
                            signal_name: attrs.signal_name.clone(),
                            input: attrs.input.clone(),
                            control: attrs.control.clone(),
                        },
                    ),
                    ..Default::default()
                },
            );
            source.history.len() as i64
        };
        match Self::resolve_external_target(
            inner,
            &source_key,
            &attrs.domain,
            &target_exec.workflow_id,
            &target_exec.run_id,
            attrs.child_workflow_only,
        ) {
            Ok(target_key) => {
                let is_child = inner
                    .children
                    .get(&source_key)
                    .is_some_and(|kids| {
                        kids.iter().any(|c| {
                            c.domain == target_key.0
                                && c.workflow_id == target_key.1
                                && c.run_id == target_key.2
                        })
                    });
                let target = inner.by_run.get_mut(&target_key).expect("target open");
                target.push_event(
                    EventType::WorkflowExecutionSignaled,
                    HistoryEvent {
                        workflow_execution_signaled_event_attributes: Some(
                            WorkflowExecutionSignaledEventAttributes {
                                signal_name: attrs.signal_name.clone(),
                                input: attrs.input.clone(),
                                identity: identity.to_string(),
                                ..Default::default()
                            },
                        ),
                        ..Default::default()
                    },
                );
                let task_list = target.task_list.name.clone();
                Self::schedule_decision_locked(
                    inner,
                    &target_key.0,
                    &target_key.1,
                    &target_key.2,
                    &task_list,
                    0,
                );
                // Child-targeted signals also record ExternalWorkflowExecutionSignaled
                // on the source, mirroring Go history.
                if is_child {
                    let source = inner.by_run.get_mut(&source_key).expect("record exists");
                    source.push_event(
                        EventType::ExternalWorkflowExecutionSignaled,
                        HistoryEvent {
                            external_workflow_execution_signaled_event_attributes: Some(
                                cadence_api::tasks::ExternalWorkflowExecutionSignaledEventAttributes {
                                    initiated_event_id,
                                    domain: attrs.domain.clone(),
                                    workflow_execution: Some(target_exec.clone()),
                                    control: attrs.control.clone(),
                                },
                            ),
                            ..Default::default()
                        },
                    );
                }
            }
            Err(cause) => {
                let source = inner.by_run.get_mut(&source_key).expect("record exists");
                source.push_event(
                    EventType::SignalExternalWorkflowExecutionFailed,
                    HistoryEvent {
                        signal_external_workflow_execution_failed_event_attributes: Some(
                            SignalExternalWorkflowExecutionFailedEventAttributes {
                                cause: Some(match cause {
                                    ExternalTargetError::Unknown => {
                                        SignalExternalWorkflowExecutionFailedCause::UnknownExternalWorkflowExecution
                                    }
                                    ExternalTargetError::Completed => {
                                        SignalExternalWorkflowExecutionFailedCause::WorkflowAlreadyCompleted
                                    }
                                }),
                                decision_task_completed_event_id: completed_event_id,
                                domain: attrs.domain.clone(),
                                workflow_execution: Some(target_exec),
                                initiated_event_id,
                                control: attrs.control.clone(),
                            },
                        ),
                        ..Default::default()
                    },
                );
            }
        }
    }

    /// Apply a request-cancel-external decision, mirroring Go history.
    fn request_cancel_external_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        attrs: &cadence_api::tasks::RequestCancelExternalWorkflowExecutionDecisionAttributes,
        completed_event_id: i64,
    ) {
        let source_key = (
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        let initiated_event_id = {
            let source = inner.by_run.get_mut(&source_key).expect("record exists");
            source.push_event(
                EventType::RequestCancelExternalWorkflowExecutionInitiated,
                HistoryEvent {
                    request_cancel_external_workflow_execution_initiated_event_attributes: Some(
                        RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                            decision_task_completed_event_id: completed_event_id,
                            domain: attrs.domain.clone(),
                            workflow_execution: Some(WorkflowExecution::new(
                                attrs.workflow_id.clone(),
                                attrs.run_id.clone(),
                            )),
                            control: attrs.control.clone(),
                            child_workflow_only: attrs.child_workflow_only,
                        },
                    ),
                    ..Default::default()
                },
            );
            source.history.len() as i64
        };
        // Go uses its own Initiated event type name; map to the same enum slot.
        match Self::resolve_external_target(
            inner,
            &source_key,
            &attrs.domain,
            &attrs.workflow_id,
            &attrs.run_id,
            attrs.child_workflow_only,
        ) {
            Ok(target_key) => {
                let is_child = inner.children.get(&source_key).is_some_and(|kids| {
                    kids.iter().any(|c| {
                        c.domain == target_key.0
                            && c.workflow_id == target_key.1
                            && c.run_id == target_key.2
                    })
                });
                let target = inner.by_run.get_mut(&target_key).expect("target open");
                target.push_event(
                    EventType::WorkflowExecutionCancelRequested,
                    HistoryEvent {
                        workflow_execution_cancel_requested_event_attributes: Some(
                            WorkflowExecutionCancelRequestedEventAttributes::default(),
                        ),
                        ..Default::default()
                    },
                );
                let task_list = target.task_list.name.clone();
                Self::schedule_decision_locked(
                    inner,
                    &target_key.0,
                    &target_key.1,
                    &target_key.2,
                    &task_list,
                    0,
                );
                if is_child {
                    let source = inner.by_run.get_mut(&source_key).expect("record exists");
                    source.push_event(
                        EventType::ExternalWorkflowExecutionCancelRequested,
                        HistoryEvent {
                            external_workflow_execution_cancel_requested_event_attributes: Some(
                                cadence_api::tasks::ExternalWorkflowExecutionCancelRequestedEventAttributes {
                                    initiated_event_id,
                                    domain: attrs.domain.clone(),
                                    workflow_execution: Some(WorkflowExecution::new(
                                        attrs.workflow_id.clone(),
                                        attrs.run_id.clone(),
                                    )),
                                },
                            ),
                            ..Default::default()
                        },
                    );
                }
            }
            Err(cause) => {
                let source = inner.by_run.get_mut(&source_key).expect("record exists");
                source.push_event(
                    EventType::RequestCancelExternalWorkflowExecutionFailed,
                    HistoryEvent {
                        request_cancel_external_workflow_execution_failed_event_attributes: Some(
                            RequestCancelExternalWorkflowExecutionFailedEventAttributes {
                                cause: Some(match cause {
                                    ExternalTargetError::Unknown => {
                                        CancelExternalWorkflowExecutionFailedCause::UnknownExternalWorkflowExecution
                                    }
                                    ExternalTargetError::Completed => {
                                        CancelExternalWorkflowExecutionFailedCause::WorkflowAlreadyCompleted
                                    }
                                }),
                                decision_task_completed_event_id: completed_event_id,
                                domain: attrs.domain.clone(),
                                workflow_execution: Some(WorkflowExecution::new(
                                    attrs.workflow_id.clone(),
                                    attrs.run_id.clone(),
                                )),
                                initiated_event_id,
                                control: attrs.control.clone(),
                            },
                        ),
                        ..Default::default()
                    },
                );
            }
        }
    }
    /// Close a run and propagate the effects, mirroring Go history:
    /// the parent (if any) is notified with a child-completion event and
    /// gets a new decision task, and this run's own children are handled
    /// per their parent-close policy (terminate/cancel; abandon = nothing).
    /// `ContinuedAsNew` closes notify nobody but still apply policy to
    /// children, matching Go's `RecordChildExecutionCompleted` switch.
    #[allow(clippy::too_many_arguments)]
    fn close_run_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        status: WorkflowExecutionCloseStatus,
        reason: Option<String>,
        payload: Vec<u8>,
        event: HistoryEvent,
        event_type: EventType,
    ) {
        let (parent, workflow_type_name) = {
            let record = inner
                .by_run
                .get_mut(&(
                    domain.to_string(),
                    workflow_id.to_string(),
                    run_id.to_string(),
                ))
                .expect("record exists");
            record.push_event(event_type, event);
            record.close_status = Some(status);
            record.close_time_millis = Some(now_millis());
            (record.parent.clone(), record.workflow_type.name.clone())
        };
        // 1. Notify the parent with the terminal child event.
        if let Some(p) = parent {
            let notified = Self::notify_parent_locked(
                inner,
                &p,
                domain,
                workflow_id,
                run_id,
                &workflow_type_name,
                status,
                reason.clone(),
                payload.clone(),
            );
            let _ = notified;
        }
        // 2. Apply this run's parent-close policy to its children.
        // Continued-as-new keeps children attached (repointed by the caller).
        if status == WorkflowExecutionCloseStatus::ContinuedAsNew {
            return;
        }
        let kids = inner
            .children
            .remove(&(
                domain.to_string(),
                workflow_id.to_string(),
                run_id.to_string(),
            ))
            .unwrap_or_default();
        for child in kids {
            let child_key = (
                child.domain.clone(),
                child.workflow_id.clone(),
                child.run_id.clone(),
            );
            let open = inner
                .by_run
                .get(&child_key)
                .is_some_and(ExecutionRecord::is_open);
            if !open {
                continue;
            }
            match child.parent_close_policy {
                ParentClosePolicy::Terminate => {
                    Self::close_run_locked(
                        inner,
                        &child.domain,
                        &child.workflow_id,
                        &child.run_id,
                        WorkflowExecutionCloseStatus::Terminated,
                        Some("terminated by parent close policy".to_string()),
                        Vec::new(),
                        HistoryEvent {
                            workflow_execution_terminated_event_attributes: Some(
                                WorkflowExecutionTerminatedEventAttributes {
                                    reason: "terminated by parent close policy".to_string(),
                                    ..Default::default()
                                },
                            ),
                            ..Default::default()
                        },
                        EventType::WorkflowExecutionTerminated,
                    );
                }
                ParentClosePolicy::RequestCancel => {
                    let record = inner.by_run.get_mut(&child_key).expect("child open");
                    record.push_event(
                        EventType::WorkflowExecutionCancelRequested,
                        HistoryEvent {
                            workflow_execution_cancel_requested_event_attributes: Some(
                                WorkflowExecutionCancelRequestedEventAttributes {
                                    cause: "cancelled by parent close policy".to_string(),
                                    ..Default::default()
                                },
                            ),
                            ..Default::default()
                        },
                    );
                    let task_list = record.task_list.name.clone();
                    Self::schedule_decision_locked(
                        inner,
                        &child.domain,
                        &child.workflow_id,
                        &child.run_id,
                        &task_list,
                        0,
                    );
                }
                ParentClosePolicy::Abandon => {}
                _ => {}
            }
        }
    }

    /// Append the terminal child event to the parent and schedule a parent
    /// decision. Returns false when the parent is gone or closed.
    #[allow(clippy::too_many_arguments)]
    fn notify_parent_locked(
        inner: &mut Inner,
        parent: &ParentRef,
        child_domain: &str,
        child_workflow_id: &str,
        child_run_id: &str,
        child_type_name: &str,
        status: WorkflowExecutionCloseStatus,
        reason: Option<String>,
        payload: Vec<u8>,
    ) -> bool {
        let parent_key = (
            parent.domain.clone(),
            parent.workflow_id.clone(),
            parent.run_id.clone(),
        );
        let open = inner
            .by_run
            .get(&parent_key)
            .is_some_and(ExecutionRecord::is_open);
        // Drop the child link regardless (terminal states leave no pending child).
        if let Some(kids) = inner.children.get_mut(&parent_key) {
            kids.retain(|c| {
                !(c.domain == child_domain
                    && c.workflow_id == child_workflow_id
                    && c.run_id == child_run_id)
            });
        }
        if !open {
            return false;
        }
        let child_execution =
            WorkflowExecution::new(child_workflow_id.to_string(), child_run_id.to_string());
        let child_type = WorkflowType {
            name: child_type_name.to_string(),
        };
        let (event, event_type) = match status {
            WorkflowExecutionCloseStatus::Completed => (
                HistoryEvent {
                    child_workflow_execution_completed_event_attributes: Some(
                        ChildWorkflowExecutionCompletedEventAttributes {
                            result: payload,
                            domain: child_domain.to_string(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id: 1,
                        },
                    ),
                    ..Default::default()
                },
                EventType::ChildWorkflowExecutionCompleted,
            ),
            WorkflowExecutionCloseStatus::Failed => (
                HistoryEvent {
                    child_workflow_execution_failed_event_attributes: Some(
                        ChildWorkflowExecutionFailedEventAttributes {
                            reason,
                            details: payload,
                            domain: child_domain.to_string(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id: 1,
                        },
                    ),
                    ..Default::default()
                },
                EventType::ChildWorkflowExecutionFailed,
            ),
            WorkflowExecutionCloseStatus::Canceled => (
                HistoryEvent {
                    child_workflow_execution_canceled_event_attributes: Some(
                        ChildWorkflowExecutionCanceledEventAttributes {
                            details: payload,
                            domain: child_domain.to_string(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id: 1,
                        },
                    ),
                    ..Default::default()
                },
                EventType::ChildWorkflowExecutionCanceled,
            ),
            WorkflowExecutionCloseStatus::Terminated => (
                HistoryEvent {
                    child_workflow_execution_terminated_event_attributes: Some(
                        ChildWorkflowExecutionTerminatedEventAttributes {
                            domain: child_domain.to_string(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id: 1,
                        },
                    ),
                    ..Default::default()
                },
                EventType::ChildWorkflowExecutionTerminated,
            ),
            WorkflowExecutionCloseStatus::TimedOut => {
                // No TimedOut child event type is ported yet; report as failed.
                (
                    HistoryEvent {
                        child_workflow_execution_failed_event_attributes: Some(
                            ChildWorkflowExecutionFailedEventAttributes {
                                reason: Some("TIMEOUT".to_string()),
                                details: payload,
                                domain: child_domain.to_string(),
                                workflow_execution: Some(child_execution),
                                workflow_type: Some(child_type),
                                initiated_event_id: parent.initiated_event_id,
                                started_event_id: 1,
                            },
                        ),
                        ..Default::default()
                    },
                    EventType::ChildWorkflowExecutionFailed,
                )
            }
            WorkflowExecutionCloseStatus::ContinuedAsNew => return true,
            _ => return true,
        };
        let record = inner.by_run.get_mut(&parent_key).expect("parent open");
        record.push_event(event_type, event);
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(
            inner,
            &parent.domain,
            &parent.workflow_id,
            &parent.run_id,
            &task_list,
            0,
        );
        true
    }

    /// Record a decision-task failure and schedule a retry with an
    /// incremented attempt, mirroring Go's decision retry behavior.
    pub fn fail_decision(
        &self,
        task_token: &[u8],
        cause: Option<DecisionTaskFailedCause>,
        details: Vec<u8>,
        identity: &str,
        binary_checksum: &str,
    ) -> Result<(), TaskError> {
        let token = Self::decode_task_token(task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let record = inner.by_run.get(&record_key).ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
        })?;
        if !record.is_open() {
            return Err(TaskError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError {
                    message: "Workflow execution already completed.".to_string(),
                },
            ));
        }
        let pending = record
            .pending_decision
            .clone()
            .ok_or_else(|| TaskError::InvalidToken(BadRequestError::new("Invalid TaskToken.")))?;
        let started_event_id = pending
            .started_event_id
            .ok_or_else(|| TaskError::InvalidToken(BadRequestError::new("Invalid TaskToken.")))?;
        if pending.scheduled_event_id != token.schedule_id {
            return Err(TaskError::InvalidToken(BadRequestError::new(
                "Invalid TaskToken.",
            )));
        }
        let attempt = pending.attempt + 1;
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        record.push_event(
            EventType::DecisionTaskFailed,
            HistoryEvent {
                decision_task_failed_event_attributes: Some(
                    cadence_api::tasks::DecisionTaskFailedEventAttributes {
                        scheduled_event_id: pending.scheduled_event_id,
                        started_event_id,
                        cause,
                        details,
                        identity: identity.to_string(),
                        binary_checksum: binary_checksum.to_string(),
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
        );
        record.prev_started_event_id = Some(started_event_id);
        record.pending_decision = None;
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(
            &mut inner,
            &token.domain_id,
            &token.workflow_id,
            &token.run_id,
            &task_list,
            attempt,
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Matching: activity tasks
    // -----------------------------------------------------------------------

    /// Dequeue the next schedulable activity task for a task list.
    pub fn poll_activity(
        &self,
        domain: &str,
        task_list: &str,
        identity: &str,
    ) -> Option<PollForActivityTaskResponse> {
        let mut inner = self.inner.write().expect("execution store lock");
        let key = (domain.to_string(), task_list.to_string());
        loop {
            let (workflow_id, run_id, scheduled_event_id) =
                inner.activity_queues.get_mut(&key)?.pop_front()?;
            let record =
                inner
                    .by_run
                    .get_mut(&(domain.to_string(), workflow_id.clone(), run_id.clone()))?;
            if !record.is_open() {
                continue;
            }
            let pending = record.pending_activities.get(&scheduled_event_id)?.clone();
            if pending.started_event_id.is_some() {
                continue; // already dispatched
            }
            let started_event_id = record.next_event_id();
            let event = HistoryEvent {
                activity_task_started_event_attributes: Some(ActivityTaskStartedEventAttributes {
                    scheduled_event_id,
                    identity: identity.to_string(),
                    attempt: pending.attempt,
                    ..Default::default()
                }),
                ..Default::default()
            };
            record.push_event(EventType::ActivityTaskStarted, event);
            if let Some(p) = record.pending_activities.get_mut(&scheduled_event_id) {
                p.started_event_id = Some(started_event_id);
                p.queued = false;
            }
            let token = TaskToken {
                domain_id: domain.to_string(),
                workflow_id: workflow_id.clone(),
                workflow_type: record.workflow_type.name.clone(),
                run_id: run_id.clone(),
                schedule_id: scheduled_event_id,
                schedule_attempt: i64::from(pending.attempt),
                activity_id: pending.activity_id.clone(),
                activity_type: pending.activity_type.name.clone(),
            };
            return Some(PollForActivityTaskResponse {
                task_token: Self::encode_task_token(&token),
                workflow_execution: Some(WorkflowExecution::new(workflow_id, run_id)),
                activity_id: pending.activity_id,
                activity_type: Some(pending.activity_type),
                input: pending.input,
                scheduled_timestamp: Some(pending.scheduled_time_millis),
                schedule_to_close_timeout_seconds: pending.schedule_to_close_timeout_seconds,
                started_timestamp: Some(now_millis()),
                start_to_close_timeout_seconds: pending.start_to_close_timeout_seconds,
                heartbeat_timeout_seconds: pending.heartbeat_timeout_seconds,
                attempt: pending.attempt,
                scheduled_timestamp_of_this_attempt: Some(now_millis()),
                heartbeat_details: Vec::new(),
                workflow_type: Some(record.workflow_type.clone()),
                workflow_domain: domain.to_string(),
                header: pending.header,
            });
        }
    }

    fn resolve_activity_mut<'a>(
        inner: &'a mut Inner,
        token: &TaskToken,
    ) -> Result<&'a mut PendingActivity, TaskError> {
        if token.activity_id.is_empty() {
            return Err(TaskError::InvalidToken(BadRequestError::new(
                "Invalid TaskToken.",
            )));
        }
        let record = inner
            .by_run
            .get_mut(&(
                token.domain_id.clone(),
                token.workflow_id.clone(),
                token.run_id.clone(),
            ))
            .ok_or_else(|| {
                TaskError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
            })?;
        if !record.is_open() {
            return Err(TaskError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError {
                    message: "Workflow execution already completed.".to_string(),
                },
            ));
        }
        let pending = record
            .pending_activities
            .get_mut(&token.schedule_id)
            .ok_or_else(|| TaskError::InvalidToken(BadRequestError::new("Invalid TaskToken.")))?;
        if pending.activity_id != token.activity_id {
            return Err(TaskError::InvalidToken(BadRequestError::new(
                "Invalid TaskToken.",
            )));
        }
        if pending.started_event_id.is_none() {
            return Err(TaskError::BadDecision(BadRequestError::new(
                "Activity task has not started.",
            )));
        }
        Ok(pending)
    }

    pub fn complete_activity(
        &self,
        req: &RespondActivityTaskCompletedRequest,
    ) -> Result<(), TaskError> {
        let token = Self::decode_task_token(&req.task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let scheduled_event_id = token.schedule_id;
        let started_event_id = {
            let pending = Self::resolve_activity_mut(&mut inner, &token)?;
            pending.started_event_id.expect("verified started")
        };
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        record.push_event(
            EventType::ActivityTaskCompleted,
            HistoryEvent {
                activity_task_completed_event_attributes: Some(
                    ActivityTaskCompletedEventAttributes {
                        result: req.result.clone(),
                        scheduled_event_id,
                        started_event_id,
                        identity: req.identity.clone(),
                    },
                ),
                ..Default::default()
            },
        );
        record.pending_activities.remove(&scheduled_event_id);
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(
            &mut inner,
            &token.domain_id,
            &token.workflow_id,
            &token.run_id,
            &task_list,
            0,
        );
        Ok(())
    }

    pub fn fail_activity(&self, req: &RespondActivityTaskFailedRequest) -> Result<(), TaskError> {
        let token = Self::decode_task_token(&req.task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let scheduled_event_id = token.schedule_id;
        let (started_event_id, attempt, retry_policy, activity_snapshot) = {
            let pending = Self::resolve_activity_mut(&mut inner, &token)?;
            (
                pending.started_event_id.expect("verified started"),
                pending.attempt,
                pending.retry_policy.clone(),
                pending.clone(),
            )
        };
        if Self::is_retryable_activity_failure(req, attempt, retry_policy.as_ref()) {
            // Schedule the next attempt like Go: a fresh Scheduled event,
            // requeued after backoff. No Failed event until exhausted.
            let policy = retry_policy.unwrap_or_default();
            let initial = policy.initial_interval_in_seconds.max(1) as i64;
            let coeff = policy.backoff_coefficient.max(1.0);
            let max_interval = if policy.maximum_interval_in_seconds > 0 {
                policy.maximum_interval_in_seconds as i64
            } else {
                i64::MAX / 2
            };
            let delay_secs =
                ((initial as f64) * coeff.powi(attempt)).min(max_interval as f64) as i64;
            let record = inner.by_run.get_mut(&record_key).expect("record exists");
            record.push_event(
                EventType::ActivityTaskScheduled,
                HistoryEvent {
                    activity_task_scheduled_event_attributes: Some(
                        ActivityTaskScheduledEventAttributes {
                            activity_id: activity_snapshot.activity_id.clone(),
                            activity_type: Some(activity_snapshot.activity_type.clone()),
                            domain: None,
                            task_list: Some(activity_snapshot.task_list.clone()),
                            input: activity_snapshot.input.clone(),
                            schedule_to_close_timeout_seconds: activity_snapshot
                                .schedule_to_close_timeout_seconds,
                            schedule_to_start_timeout_seconds: None,
                            start_to_close_timeout_seconds: activity_snapshot
                                .start_to_close_timeout_seconds,
                            heartbeat_timeout_seconds: activity_snapshot.heartbeat_timeout_seconds,
                            decision_task_completed_event_id: 0,
                            retry_policy: activity_snapshot.retry_policy.clone(),
                            header: activity_snapshot.header.clone(),
                        },
                    ),
                    ..Default::default()
                },
            );
            let new_scheduled_id = record.history.len() as i64;
            record.pending_activities.remove(&scheduled_event_id);
            record.pending_activities.insert(
                new_scheduled_id,
                PendingActivity {
                    scheduled_event_id: new_scheduled_id,
                    started_event_id: None,
                    attempt: attempt + 1,
                    queued: false,
                    retry_ready_millis: Some(now_millis() + delay_secs * 1000),
                    heartbeat_details: req.heartbeat_details.clone(),
                    ..activity_snapshot
                },
            );
            return Ok(());
        }
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        record.push_event(
            EventType::ActivityTaskFailed,
            HistoryEvent {
                activity_task_failed_event_attributes: Some(ActivityTaskFailedEventAttributes {
                    reason: req.reason.clone(),
                    details: req.details.clone(),
                    failure_options: req.failure_options.clone(),
                    scheduled_event_id,
                    started_event_id,
                    identity: req.identity.clone(),
                }),
                ..Default::default()
            },
        );
        record.pending_activities.remove(&scheduled_event_id);
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(
            &mut inner,
            &token.domain_id,
            &token.workflow_id,
            &token.run_id,
            &task_list,
            0,
        );
        Ok(())
    }

    /// Whether a failed activity attempt should retry, mirroring Go's
    /// retry-policy evaluation (fatal category and non-retriable reasons
    /// stop retries; `maximumAttempts` bounds the count).
    fn is_retryable_activity_failure(
        req: &RespondActivityTaskFailedRequest,
        attempt: i32,
        policy: Option<&RetryPolicy>,
    ) -> bool {
        use cadence_api::enums::FailureCategory;
        let category = req
            .failure_options
            .as_ref()
            .map(|o| o.failure_category_or_default())
            .unwrap_or(FailureCategory::Standard);
        if category == FailureCategory::Fatal {
            return false;
        }
        if let Some(policy) = policy {
            if let Some(reason) = req.reason.as_deref() {
                if !policy.is_retriable_reason(reason) {
                    return false;
                }
            }
            if policy.maximum_attempts > 0 && attempt + 1 >= policy.maximum_attempts {
                return false;
            }
        }
        true
    }

    /// Requeue activity retries whose backoff has elapsed. Called by the
    /// server's background loop alongside timer firing.
    pub fn requeue_due_activities(&self, now_millis: i64) -> usize {
        let mut inner = self.inner.write().expect("execution store lock");
        let mut due: Vec<(String, String, String, String, i64)> = Vec::new();
        for (key, record) in inner.by_run.iter() {
            if !record.is_open() {
                continue;
            }
            for (sched_id, pending) in record.pending_activities.iter() {
                if !pending.queued {
                    if let Some(ready) = pending.retry_ready_millis {
                        if ready <= now_millis {
                            due.push((
                                key.0.clone(),
                                key.1.clone(),
                                key.2.clone(),
                                record.task_list.name.clone(),
                                *sched_id,
                            ));
                        }
                    }
                }
            }
        }
        let count = due.len();
        for (domain, wf, run, task_list, sched_id) in due {
            let key = (domain.clone(), wf.clone(), run.clone());
            let record = match inner.by_run.get_mut(&key) {
                Some(r) if r.is_open() => r,
                _ => continue,
            };
            let pending = match record.pending_activities.get_mut(&sched_id) {
                Some(p) if !p.queued => p,
                _ => continue,
            };
            pending.queued = true;
            pending.retry_ready_millis = None;
            let task_list_name = if task_list.is_empty() {
                record.task_list.name.clone()
            } else {
                pending.task_list.name.clone()
            };
            inner
                .activity_queues
                .entry((domain, task_list_name))
                .or_default()
                .push_back((wf, run, sched_id));
        }
        count
    }

    /// Record an activity heartbeat, mirroring Go: heartbeat details are
    /// stored and the worker learns whether cancellation was requested.
    pub fn record_heartbeat(&self, task_token: &[u8], details: Vec<u8>) -> Result<bool, TaskError> {
        let token = Self::decode_task_token(task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let pending = Self::resolve_activity_mut(&mut inner, &token)?;
        pending.heartbeat_details = details;
        Ok(pending.cancel_requested)
    }

    /// Respond to an activity task as canceled, mirroring Go history.
    pub fn cancel_activity(
        &self,
        req: &cadence_api::tasks::RespondActivityTaskCanceledRequest,
    ) -> Result<(), TaskError> {
        let token = Self::decode_task_token(&req.task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let scheduled_event_id = token.schedule_id;
        let (started_event_id, cancel_requested_event_id) = {
            let pending = Self::resolve_activity_mut(&mut inner, &token)?;
            (
                pending.started_event_id.expect("verified started"),
                pending.cancel_requested_event_id.unwrap_or(0),
            )
        };
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        record.push_event(
            EventType::ActivityTaskCanceled,
            HistoryEvent {
                activity_task_canceled_event_attributes: Some(
                    cadence_api::tasks::ActivityTaskCanceledEventAttributes {
                        details: req.details.clone(),
                        latest_cancel_requested_event_id: cancel_requested_event_id,
                        scheduled_event_id,
                        started_event_id,
                        identity: req.identity.clone(),
                    },
                ),
                ..Default::default()
            },
        );
        record.pending_activities.remove(&scheduled_event_id);
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(
            &mut inner,
            &token.domain_id,
            &token.workflow_id,
            &token.run_id,
            &task_list,
            0,
        );
        Ok(())
    }

    /// Read one page of history. `next_page_token` is an opaque offset
    /// cursor (`""` starts at the beginning); `max_page_size <= 0` returns
    /// everything, mirroring Go's default paging behavior. A `CloseEvent`
    /// filter returns only the close event of a closed execution, like Go.
    pub fn history_page(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        max_page_size: i32,
        next_page_token: &[u8],
        filter: Option<cadence_api::enums::HistoryEventFilterType>,
    ) -> Result<GetWorkflowExecutionHistoryResponse, HistoryPageError> {
        let offset: usize = if next_page_token.is_empty() {
            0
        } else {
            std::str::from_utf8(next_page_token)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    HistoryPageError::InvalidToken(BadRequestError::new("Invalid NextPageToken."))
                })?
        };
        let full = self
            .history(domain, execution)
            .map_err(HistoryPageError::NotFound)?;
        let mut events = full.history.unwrap_or_default().events;
        // Close-event filter: only the terminal event of a closed execution.
        if filter == Some(cadence_api::enums::HistoryEventFilterType::CloseEvent) {
            let closed = !events.is_empty()
                && matches!(
                    events.last().and_then(|e| e.event_type),
                    Some(
                        cadence_api::enums::EventType::WorkflowExecutionCompleted
                            | cadence_api::enums::EventType::WorkflowExecutionFailed
                            | cadence_api::enums::EventType::WorkflowExecutionTimedOut
                            | cadence_api::enums::EventType::WorkflowExecutionCanceled
                            | cadence_api::enums::EventType::WorkflowExecutionTerminated
                            | cadence_api::enums::EventType::WorkflowExecutionContinuedAsNew
                    )
                );
            events = if closed {
                vec![events.pop().expect("nonempty")]
            } else {
                Vec::new()
            };
        }
        if offset > events.len() {
            return Err(HistoryPageError::InvalidToken(BadRequestError::new(
                "Invalid NextPageToken.",
            )));
        }
        let limit = if max_page_size <= 0 {
            events.len()
        } else {
            max_page_size as usize
        };
        let end = (offset + limit).min(events.len());
        let next_page_token = if end < events.len() {
            end.to_string().into_bytes()
        } else {
            Vec::new()
        };
        Ok(GetWorkflowExecutionHistoryResponse {
            history: Some(History {
                events: events[offset..end].to_vec(),
            }),
            next_page_token,
            archived: false,
        })
    }

    // -----------------------------------------------------------------------
    // Signal-with-start
    // -----------------------------------------------------------------------

    /// Mirrors Go `SignalWithStartWorkflowExecution`: signal the open run if
    /// one exists, otherwise start a new run and signal it.
    pub fn signal_with_start(
        &self,
        req: &cadence_api::service_types::SignalWithStartWorkflowExecutionRequest,
    ) -> Result<StartWorkflowExecutionResponse, StartError> {
        let open_run = {
            let inner = self.inner.read().expect("execution store lock");
            self.open_run_id(&inner, &req.domain, &req.workflow_id)
        };
        if let Some(run_id) = open_run {
            let mut inner = self.inner.write().expect("execution store lock");
            let record = inner
                .by_run
                .get_mut(&(req.domain.clone(), req.workflow_id.clone(), run_id.clone()))
                .expect("open run exists");
            record.push_event(
                EventType::WorkflowExecutionSignaled,
                HistoryEvent {
                    workflow_execution_signaled_event_attributes: Some(
                        cadence_api::service_types::WorkflowExecutionSignaledEventAttributes {
                            signal_name: req.signal_name.clone(),
                            input: req.signal_input.clone(),
                            identity: req.identity.clone(),
                            request_id: req.request_id.clone(),
                        },
                    ),
                    ..Default::default()
                },
            );
            return Ok(StartWorkflowExecutionResponse { run_id });
        }
        let start_req = StartWorkflowExecutionRequest {
            domain: req.domain.clone(),
            workflow_id: req.workflow_id.clone(),
            workflow_type: req.workflow_type.clone(),
            task_list: req.task_list.clone(),
            input: req.input.clone(),
            execution_start_to_close_timeout_seconds: req.execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: req.task_start_to_close_timeout_seconds,
            identity: req.identity.clone(),
            request_id: req.request_id.clone(),
            workflow_id_reuse_policy: req.workflow_id_reuse_policy,
            retry_policy: req.retry_policy.clone(),
            cron_schedule: req.cron_schedule.clone(),
            memo: req.memo.clone(),
            search_attributes: req.search_attributes.clone(),
            header: req.header.clone(),
            cron_overlap_policy: req.cron_overlap_policy,
            delay_start_seconds: req.delay_start_seconds,
            jitter_start_seconds: req.jitter_start_seconds,
            first_run_at_timestamp: req.first_run_at_timestamp,
        };
        let resp = self.start(&start_req)?;
        {
            let mut inner = self.inner.write().expect("execution store lock");
            let record = inner
                .by_run
                .get_mut(&(
                    req.domain.clone(),
                    req.workflow_id.clone(),
                    resp.run_id.clone(),
                ))
                .expect("new run exists");
            record.push_event(
                EventType::WorkflowExecutionSignaled,
                HistoryEvent {
                    workflow_execution_signaled_event_attributes: Some(
                        cadence_api::service_types::WorkflowExecutionSignaledEventAttributes {
                            signal_name: req.signal_name.clone(),
                            input: req.signal_input.clone(),
                            identity: req.identity.clone(),
                            request_id: req.request_id.clone(),
                        },
                    ),
                    ..Default::default()
                },
            );
        }
        Ok(resp)
    }

    /// Register a domain name so decision validation can resolve
    /// cross-domain references. Called by the server on domain register.
    pub fn register_domain_name(&self, name: &str) {
        self.inner
            .write()
            .expect("execution store lock")
            .known_domains
            .insert(name.to_string());
    }

    // -----------------------------------------------------------------------
    // Direct queries (embedded-query flow)
    // -----------------------------------------------------------------------

    /// Buffer a direct query on an open execution, mirroring Go's query
    /// buffering in history. Returns a query ID the worker echoes back in
    /// `queryResults`. Rejects queries on closed executions per the reject
    /// condition, like Go's `QueryRejectCondition` handling.
    pub fn buffer_query(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        query_type: &str,
        query_args: Vec<u8>,
        reject_condition: Option<cadence_api::enums::QueryRejectCondition>,
    ) -> Result<String, QueryError> {
        use cadence_api::enums::QueryRejectCondition;
        let mut inner = self.inner.write().expect("execution store lock");
        let key = if execution.run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| {
                    QueryError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
                })?;
            let latest = runs.last().ok_or_else(|| {
                QueryError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
            })?;
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                latest.clone(),
            )
        } else {
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                execution.run_id.clone(),
            )
        };
        let record = inner.by_run.get_mut(&key).ok_or_else(|| {
            QueryError::NotFound(EntityNotExistsError::new("Workflow execution does not exist.".to_string()))
        })?;
        if !record.is_open() {
            let status = record.close_status;
            let reject = match reject_condition.unwrap_or(QueryRejectCondition::NotOpen) {
                QueryRejectCondition::NotOpen => true,
                QueryRejectCondition::NotCompletedCleanly => !matches!(
                    status,
                    Some(WorkflowExecutionCloseStatus::Completed)
                        | Some(WorkflowExecutionCloseStatus::ContinuedAsNew)
                ),
                _ => true,
            };
            if reject {
                return Err(QueryError::Rejected(status));
            }
        }
        let query_id = uuid::Uuid::new_v4().to_string();
        let (needs_decision, task_list, wf_id, run_id) = {
            let record = inner.by_run.get_mut(&key).expect("record exists");
            record.pending_queries.push_back(PendingQuery {
                query_id: query_id.clone(),
                query_type: query_type.to_string(),
                query_args,
                answered: None,
            });
            (
                record.pending_decision.is_none() && record.is_open(),
                record.task_list.name.clone(),
                record.execution.workflow_id.clone(),
                record.execution.run_id.clone(),
            )
        };
        // Ensure a decision task reaches a worker promptly.
        if needs_decision {
            Self::schedule_decision_locked(&mut inner, domain, &wf_id, &run_id, &task_list, 0);
        }
        Ok(query_id)
    }

    /// Take an answered query result, if a worker has responded.
    pub fn take_query_result(&self, query_id: &str) -> Option<WorkflowQueryResult> {
        let inner = self.inner.read().expect("execution store lock");
        inner.by_run.values().find_map(|r| {
            r.pending_queries
                .iter()
                .find(|q| q.query_id == query_id)
                .and_then(|q| q.answered.clone())
        })
    }

    /// The head buffered (unanswered) query on an execution, attached to
    /// polled decision tasks like Go's embedded legacy query.
    fn head_query(record: &ExecutionRecord) -> Option<cadence_api::tasks::WorkflowQuery> {
        record
            .pending_queries
            .iter()
            .find(|q| q.answered.is_none())
            .map(|q| cadence_api::tasks::WorkflowQuery {
                query_type: q.query_type.clone(),
                query_args: q.query_args.clone(),
            })
    }

    /// All buffered (unanswered) queries keyed by query ID, mirroring Go's
    /// `queries` map on polled decision tasks.
    fn all_queries(
        record: &ExecutionRecord,
    ) -> Option<std::collections::HashMap<String, cadence_api::tasks::WorkflowQuery>> {
        let map: std::collections::HashMap<String, cadence_api::tasks::WorkflowQuery> = record
            .pending_queries
            .iter()
            .filter(|q| q.answered.is_none())
            .map(|q| {
                (
                    q.query_id.clone(),
                    cadence_api::tasks::WorkflowQuery {
                        query_type: q.query_type.clone(),
                        query_args: q.query_args.clone(),
                    },
                )
            })
            .collect();
        if map.is_empty() { None } else { Some(map) }
    }

    /// Query ID of the head buffered query, echoed to workers so answers
    /// can be matched. `None` when no query is buffered.
    fn head_query_id(record: &ExecutionRecord) -> Option<String> {
        record
            .pending_queries
            .iter()
            .find(|q| q.answered.is_none())
            .map(|q| q.query_id.clone())
    }

    // -----------------------------------------------------------------------
    // Timers
    // -----------------------------------------------------------------------

    /// Fire all timers whose deadline has passed, appending `TimerFired`
    /// events and scheduling decisions. Called by the server's timer loop.
    /// Returns the number of timers fired.
    pub fn fire_due_timers(&self, now_millis: i64) -> usize {
        let mut inner = self.inner.write().expect("execution store lock");
        // Collect due timers first to keep borrows simple.
        let mut due: Vec<(String, String, String, String, i64)> = Vec::new();
        for (key, record) in inner.by_run.iter() {
            if !record.is_open() {
                continue;
            }
            for (timer_id, timer) in record.pending_timers.iter() {
                if timer.deadline_millis <= now_millis {
                    due.push((
                        key.0.clone(),
                        key.1.clone(),
                        key.2.clone(),
                        timer_id.clone(),
                        timer.started_event_id,
                    ));
                }
            }
        }
        let count = due.len();
        for (domain, wf, run, timer_id, started_event_id) in due {
            let record = match inner
                .by_run
                .get_mut(&(domain.clone(), wf.clone(), run.clone()))
            {
                Some(r) if r.is_open() => r,
                _ => continue,
            };
            if !record.pending_timers.contains_key(&timer_id) {
                continue;
            }
            record.pending_timers.remove(&timer_id);
            record.push_event(
                EventType::TimerFired,
                HistoryEvent {
                    timer_fired_event_attributes: Some(TimerFiredEventAttributes {
                        timer_id: timer_id.clone(),
                        started_event_id,
                    }),
                    ..Default::default()
                },
            );
            let task_list = record.task_list.name.clone();
            Self::schedule_decision_locked(&mut inner, &domain, &wf, &run, &task_list, 0);
        }
        count
    }

    /// Backlog sizes per task list for `DescribeTaskList`.
    pub fn task_list_backlog(&self, domain: &str, task_list: &str) -> (usize, usize) {
        let inner = self.inner.read().expect("execution store lock");
        let key = (domain.to_string(), task_list.to_string());
        (
            inner
                .decision_queues
                .get(&key)
                .map(|q| q.len())
                .unwrap_or(0),
            inner
                .activity_queues
                .get(&key)
                .map(|q| q.len())
                .unwrap_or(0),
        )
    }

    /// List executions with optional workflow-ID / type / close-status
    /// filters, mirroring the list-filter behavior of the Go visibility
    /// path for the in-memory store.
    #[must_use]
    pub fn list_filtered(
        &self,
        domain: &str,
        open_only: bool,
        workflow_id: Option<&str>,
        workflow_type: Option<&str>,
        close_status: Option<WorkflowExecutionCloseStatus>,
    ) -> Vec<WorkflowExecutionInfo> {
        let inner = self.inner.read().expect("execution store lock");
        let mut out: Vec<WorkflowExecutionInfo> = inner
            .by_run
            .values()
            .filter(|r| r.domain == domain && r.is_open() == open_only)
            .filter(|r| workflow_id.is_none_or(|w| r.execution.workflow_id == w))
            .filter(|r| workflow_type.is_none_or(|t| r.workflow_type.name == t))
            .filter(|r| close_status.is_none_or(|s| r.close_status == Some(s)))
            .map(ExecutionRecord::to_info)
            .collect();
        out.sort_by_key(|i| i.start_time.unwrap_or(0));
        out
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    AlreadyStarted(#[from] WorkflowExecutionAlreadyStartedError),
}

#[derive(Debug, thiserror::Error)]
pub enum WorkflowMutationError {
    #[error(transparent)]
    NotFound(#[from] EntityNotExistsError),
    #[error(transparent)]
    AlreadyCompleted(#[from] WorkflowExecutionAlreadyCompletedError),
}

/// How an external (signal/cancel) target lookup failed.
/// Mirrors Go's unknown vs already-completed failure causes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExternalTargetError {
    Unknown,
    Completed,
}

/// Errors for task-list operations, mirroring Go matching/frontend mapping:
/// invalid tokens and decision-validation failures are `BadRequest`, unknown
/// tasks are `EntityNotExists`, and completed executions surface
/// `WorkflowExecutionAlreadyCompleted`.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error(transparent)]
    InvalidToken(#[from] BadRequestError),
    #[error("bad decision: {0}")]
    BadDecision(BadRequestError),
    #[error(transparent)]
    NotFound(#[from] EntityNotExistsError),
    #[error(transparent)]
    AlreadyCompleted(#[from] WorkflowExecutionAlreadyCompletedError),
}

/// Errors for direct queries, mirroring Go's query reject/fail mapping.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    NotFound(#[from] EntityNotExistsError),
    #[error(transparent)]
    Invalid(#[from] BadRequestError),
    #[error("query rejected")]
    Rejected(Option<WorkflowExecutionCloseStatus>),
}

/// A serializable snapshot of the execution store for file durability.
/// Tuple map keys are stored as entry vectors since JSON needs strings.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ExecutionSnapshot {
    runs: Vec<((String, String, String), ExecutionRecord)>,
    by_workflow: Vec<((String, String), Vec<String>)>,
    by_request_id: Vec<((String, String, String), String)>,
    decision_queues: Vec<(QueueKey, Vec<QueuedDecision>)>,
    activity_queues: Vec<(QueueKey, Vec<QueuedActivity>)>,
    children: Vec<((String, String, String), Vec<ChildRef>)>,
    known_domains: Vec<String>,
}

impl InMemoryExecutionStore {
    fn snapshot(&self) -> ExecutionSnapshot {
        let inner = self.inner.read().expect("execution store lock");
        ExecutionSnapshot {
            runs: inner.by_run.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            by_workflow: inner.by_workflow.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            by_request_id: inner.by_request_id.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            decision_queues: inner
                .decision_queues
                .iter()
                .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
                .collect(),
            activity_queues: inner
                .activity_queues
                .iter()
                .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
                .collect(),
            children: inner.children.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            known_domains: inner.known_domains.iter().cloned().collect(),
        }
    }

    fn restore(snapshot: ExecutionSnapshot) -> Self {
        let inner = Inner {
            by_run: snapshot.runs.into_iter().collect(),
            by_workflow: snapshot.by_workflow.into_iter().collect(),
            by_request_id: snapshot.by_request_id.into_iter().collect(),
            decision_queues: snapshot
                .decision_queues
                .into_iter()
                .map(|(k, v)| (k, v.into_iter().collect()))
                .collect(),
            activity_queues: snapshot
                .activity_queues
                .into_iter()
                .map(|(k, v)| (k, v.into_iter().collect()))
                .collect(),
            children: snapshot.children.into_iter().collect(),
            known_domains: snapshot.known_domains.into_iter().collect(),
        };
        Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(inner)),
        }
    }

    /// Persist the store to a JSON file (atomic rename). Used by the
    /// server's `--data-dir` durability mode.
    pub fn save_to_file(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        let tmp = path.with_extension("tmp");
        let data = serde_json::to_vec_pretty(&self.snapshot())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(&tmp, data)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }

    /// Load a store previously written by [`Self::save_to_file`].
    pub fn load_from_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let data = std::fs::read(path.as_ref())?;
        let snapshot: ExecutionSnapshot = serde_json::from_slice(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Self::restore(snapshot))
    }
}

/// Errors for paged history reads.
#[derive(Debug, thiserror::Error)]
pub enum HistoryPageError {
    #[error(transparent)]
    NotFound(#[from] EntityNotExistsError),
    #[error(transparent)]
    InvalidToken(#[from] BadRequestError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::types::{TaskList, WorkflowType};

    fn start_req(domain: &str, wf: &str, request_id: &str) -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            domain: domain.to_string(),
            workflow_id: wf.to_string(),
            workflow_type: Some(WorkflowType {
                name: "wt".to_string(),
            }),
            task_list: Some(TaskList {
                name: "tl".to_string(),
                kind: None,
                base_name: String::new(),
            }),
            request_id: request_id.to_string(),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }
    }

    #[test]
    fn start_describe_lifecycle() {
        let store = InMemoryExecutionStore::new();
        let resp = store.start(&start_req("d", "w", "req-1")).unwrap();
        assert!(!resp.run_id.is_empty());
        let desc = store
            .describe("d", &WorkflowExecution::new("w", resp.run_id.clone()))
            .unwrap();
        let info = desc.workflow_execution_info.unwrap();
        assert!(info.close_status.is_none());
        assert_eq!(info.history_length, 2); // started + decision-scheduled
    }

    #[test]
    fn request_id_idempotency() {
        let store = InMemoryExecutionStore::new();
        let a = store.start(&start_req("d", "w", "same-req")).unwrap();
        let b = store.start(&start_req("d", "w", "same-req")).unwrap();
        assert_eq!(a.run_id, b.run_id);
    }

    #[test]
    fn duplicate_start_rejected() {
        let store = InMemoryExecutionStore::new();
        store.start(&start_req("d", "w", "req-1")).unwrap();
        let err = store.start(&start_req("d", "w", "req-2")).unwrap_err();
        assert!(matches!(err, StartError::AlreadyStarted(_)));
    }

    #[test]
    fn terminate_closes_and_appends_history() {
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                reason: "test".to_string(),
                ..Default::default()
            })
            .unwrap();
        let desc = store.describe("d", &exec).unwrap();
        let info = desc.workflow_execution_info.unwrap();
        assert_eq!(
            info.close_status,
            Some(WorkflowExecutionCloseStatus::Terminated)
        );
        assert_eq!(info.history_length, 3); // + terminated
                                            // Second terminate reports already-completed, like Go history.
        let err = store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec),
                ..Default::default()
            })
            .unwrap_err();
        assert!(matches!(err, WorkflowMutationError::AlreadyCompleted(_)));
    }

    #[test]
    fn signal_and_cancel_append_events() {
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id);
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                signal_name: "sig".to_string(),
                ..Default::default()
            })
            .unwrap();
        store
            .request_cancel(&RequestCancelWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                ..Default::default()
            })
            .unwrap();
        let hist = store.history("d", &exec).unwrap();
        assert_eq!(hist.history.unwrap().events.len(), 4); // + signaled + cancel-requested
    }

    #[test]
    fn terminate_if_running_policy() {
        let store = InMemoryExecutionStore::new();
        let mut req = start_req("d", "w", "req-1");
        store.start(&req).unwrap();
        req.request_id = "req-2".to_string();
        req.workflow_id_reuse_policy = Some(WorkflowIDReusePolicy::TerminateIfRunning);
        let second = store.start(&req).unwrap();
        // Old run is now terminated; new run is open.
        let open = store.list("d", true);
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].execution.as_ref().unwrap().run_id, second.run_id);
        let closed = store.list("d", false);
        assert_eq!(closed.len(), 1);
    }

    fn decision_complete_req(
        token: Vec<u8>,
        decisions: Vec<cadence_api::tasks::Decision>,
    ) -> cadence_api::tasks::RespondDecisionTaskCompletedRequest {
        cadence_api::tasks::RespondDecisionTaskCompletedRequest {
            task_token: token,
            decisions,
            identity: "worker".to_string(),
            ..Default::default()
        }
    }

    fn schedule_activity_decision(id: &str) -> cadence_api::tasks::Decision {
        cadence_api::tasks::Decision {
            decision_type: Some(cadence_api::enums::DecisionType::ScheduleActivityTask),
            schedule_activity_task_decision_attributes: Some(
                cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                    activity_id: id.to_string(),
                    activity_type: Some(cadence_api::types::ActivityType {
                        name: "act-type".to_string(),
                    }),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }
    }

    #[test]
    fn decision_poll_complete_close_flow() {
        use cadence_api::enums::{DecisionType, EventType};
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();

        // Poll delivers the initial decision with full history.
        let task = store.poll_decision("d", "tl", "worker-1").unwrap();
        assert_eq!(
            task.workflow_execution,
            Some(WorkflowExecution::new("w", started.run_id.clone()))
        );
        assert_eq!(task.attempt, 0);
        assert_eq!(task.history.as_ref().unwrap().events.len(), 3); // + decision-started
                                                                    // Queue is now empty: second poll finds nothing.
        assert!(store.poll_decision("d", "tl", "worker-1").is_none());

        // Complete with a schedule-activity + complete-workflow batch.
        let complete = cadence_api::tasks::Decision {
            decision_type: Some(DecisionType::CompleteWorkflowExecution),
            complete_workflow_execution_decision_attributes: Some(
                cadence_api::tasks::CompleteWorkflowExecutionDecisionAttributes {
                    result: b"ok".to_vec(),
                },
            ),
            ..Default::default()
        };
        let resp = store
            .complete_decision(&decision_complete_req(
                task.task_token,
                vec![schedule_activity_decision("a1"), complete],
            ))
            .unwrap();
        assert!(resp.decision_task.is_none());

        // Workflow closed completed; activity was scheduled first (event order).
        let desc = store
            .describe("d", &WorkflowExecution::new("w", started.run_id.clone()))
            .unwrap();
        let info = desc.workflow_execution_info.unwrap();
        assert_eq!(
            info.close_status,
            Some(cadence_api::enums::WorkflowExecutionCloseStatus::Completed)
        );
        let hist = store
            .history("d", &WorkflowExecution::new("w", started.run_id))
            .unwrap()
            .history
            .unwrap();
        let kinds: Vec<EventType> = hist.events.iter().filter_map(|e| e.event_type).collect();
        assert_eq!(
            kinds,
            vec![
                EventType::WorkflowExecutionStarted,
                EventType::DecisionTaskScheduled,
                EventType::DecisionTaskStarted,
                EventType::DecisionTaskCompleted,
                EventType::ActivityTaskScheduled,
                EventType::WorkflowExecutionCompleted,
            ]
        );
    }

    #[test]
    fn activity_poll_respond_flow() {
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .complete_decision(&decision_complete_req(
                task.task_token,
                vec![schedule_activity_decision("a1")],
            ))
            .unwrap();

        // A new decision is scheduled; activity is queued.
        let act = store.poll_activity("d", "tl", "worker").unwrap();
        assert_eq!(act.activity_id, "a1");
        assert_eq!(act.attempt, 0);
        assert!(store.poll_activity("d", "tl", "worker").is_none());

        store
            .complete_activity(&RespondActivityTaskCompletedRequest {
                task_token: act.task_token,
                result: b"done".to_vec(),
                identity: "worker".to_string(),
            })
            .unwrap();
        let hist = store
            .history("d", &WorkflowExecution::new("w", started.run_id))
            .unwrap()
            .history
            .unwrap();
        assert!(hist
            .events
            .iter()
            .any(|e| e.event_type == Some(cadence_api::enums::EventType::ActivityTaskCompleted)));
    }

    #[test]
    fn bad_decisions_rejected_without_state_change() {
        let store = InMemoryExecutionStore::new();
        store.start(&start_req("d", "w", "req-1")).unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        let before = store
            .history("d", &task.workflow_execution.clone().unwrap())
            .unwrap()
            .history
            .unwrap()
            .events
            .len();
        // Empty activity ID is rejected like Go's BadScheduleActivityAttributes.
        let err = store
            .complete_decision(&decision_complete_req(
                task.task_token.clone(),
                vec![schedule_activity_decision("")],
            ))
            .unwrap_err();
        assert!(matches!(err, TaskError::BadDecision(_)));
        // Unknown decision types are rejected, not applied.
        let err = store
            .complete_decision(&decision_complete_req(
                task.task_token,
                vec![cadence_api::tasks::Decision {
                    decision_type: Some(
                        cadence_api::enums::DecisionType::StartChildWorkflowExecution,
                    ),
                    ..Default::default()
                }],
            ))
            .unwrap_err();
        assert!(matches!(err, TaskError::BadDecision(_)));
        let after = store
            .history(
                "d",
                &WorkflowExecution::new("w", task.workflow_execution.unwrap().run_id),
            )
            .unwrap()
            .history
            .unwrap()
            .events
            .len();
        assert_eq!(before, after);
    }

    #[test]
    fn fail_decision_retries_with_incremented_attempt() {
        let store = InMemoryExecutionStore::new();
        store.start(&start_req("d", "w", "req-1")).unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .fail_decision(&task.task_token, None, Vec::new(), "worker", "")
            .unwrap();
        let retry = store.poll_decision("d", "tl", "worker").unwrap();
        assert_eq!(retry.attempt, 1);
        assert_eq!(retry.previous_started_event_id, Some(task.started_event_id));
    }

    #[test]
    fn signal_with_start_signals_open_or_starts_new() {
        use cadence_api::service_types::SignalWithStartWorkflowExecutionRequest;
        let store = InMemoryExecutionStore::new();
        let base = SignalWithStartWorkflowExecutionRequest {
            domain: "d".to_string(),
            workflow_id: "w".to_string(),
            workflow_type: Some(WorkflowType {
                name: "wt".to_string(),
            }),
            task_list: Some(TaskList {
                name: "tl".to_string(),
                kind: None,
                base_name: String::new(),
            }),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            request_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            signal_name: "sig".to_string(),
            ..Default::default()
        };
        let first = store.signal_with_start(&base).unwrap();
        let second = store.signal_with_start(&base).unwrap();
        assert_eq!(first.run_id, second.run_id); // signaled the open run
        let hist = store
            .history("d", &WorkflowExecution::new("w", first.run_id))
            .unwrap()
            .history
            .unwrap();
        assert_eq!(hist.events.len(), 4); // started, scheduled, signaled x2
    }
}

#[cfg(test)]
mod advanced_tests {
    use super::*;
    use cadence_api::enums::{DecisionType, EventType, ParentClosePolicy};
    use cadence_api::tasks::{
        ContinueAsNewWorkflowExecutionDecisionAttributes, Decision,
        RequestCancelExternalWorkflowExecutionDecisionAttributes,
        SignalExternalWorkflowExecutionDecisionAttributes,
        StartChildWorkflowExecutionDecisionAttributes, StartTimerDecisionAttributes,
        UpsertWorkflowSearchAttributesDecisionAttributes,
    };
    use cadence_api::types::{SearchAttributes, TaskList, WorkflowType};

    fn start_req(domain: &str, wf: &str, request_id: &str) -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            domain: domain.to_string(),
            workflow_id: wf.to_string(),
            workflow_type: Some(WorkflowType {
                name: "wt".to_string(),
            }),
            task_list: Some(TaskList {
                name: "tl".to_string(),
                kind: None,
                base_name: String::new(),
            }),
            request_id: request_id.to_string(),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }
    }

    fn complete_with(
        store: &InMemoryExecutionStore,
        domain: &str,
        task_list: &str,
        decisions: Vec<Decision>,
    ) {
        let task = store.poll_decision(domain, task_list, "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions,
                identity: "worker".to_string(),
                ..Default::default()
            })
            .unwrap();
    }

    fn start_child_decision(domain: &str, wf: &str, policy: ParentClosePolicy) -> Decision {
        Decision {
            decision_type: Some(DecisionType::StartChildWorkflowExecution),
            start_child_workflow_execution_decision_attributes: Some(
                StartChildWorkflowExecutionDecisionAttributes {
                    domain: domain.to_string(),
                    workflow_id: wf.to_string(),
                    workflow_type: Some(WorkflowType {
                        name: "child-type".to_string(),
                    }),
                    parent_close_policy: Some(policy),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }
    }

    fn event_kinds(
        store: &InMemoryExecutionStore,
        domain: &str,
        exec: &WorkflowExecution,
    ) -> Vec<EventType> {
        store
            .history(domain, exec)
            .unwrap()
            .history
            .unwrap()
            .events
            .iter()
            .filter_map(|e| e.event_type)
            .collect()
    }

    #[test]
    fn child_workflow_started_and_parent_notified_on_close() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let parent = store.start(&start_req("d", "parent", "req-1")).unwrap();
        let parent_exec = WorkflowExecution::new("parent", parent.run_id.clone());
        complete_with(
            &store,
            "d",
            "tl",
            vec![start_child_decision(
                "d",
                "child",
                ParentClosePolicy::Abandon,
            )],
        );
        // Parent history: started, scheduled, started(decision), completed(decision),
        // initiated, child-started.
        let kinds = event_kinds(&store, "d", &parent_exec);
        assert!(kinds.contains(&EventType::StartChildWorkflowExecutionInitiated));
        assert!(kinds.contains(&EventType::ChildWorkflowExecutionStarted));
        // Describe reports the pending child.
        let desc = store.describe("d", &parent_exec).unwrap();
        assert_eq!(desc.pending_children.len(), 1);
        assert_eq!(desc.pending_children[0].workflow_id, "child");

        // Close the child through its own decision task.
        let child_runs = store.list("d", true);
        let child_exec = child_runs
            .iter()
            .find(|i| i.execution.as_ref().unwrap().workflow_id == "child")
            .unwrap()
            .execution
            .clone()
            .unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        assert_eq!(
            task.workflow_execution.as_ref().unwrap().workflow_id,
            "child"
        );
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![Decision {
                    decision_type: Some(DecisionType::CompleteWorkflowExecution),
                    ..Default::default()
                }],
                identity: "worker".to_string(),
                ..Default::default()
            })
            .unwrap();
        // Parent got the completion event plus a fresh decision.
        let kinds = event_kinds(&store, "d", &parent_exec);
        assert!(kinds.contains(&EventType::ChildWorkflowExecutionCompleted));
        let desc = store.describe("d", &parent_exec).unwrap();
        assert!(desc.pending_children.is_empty());
        let _ = child_exec;
    }

    #[test]
    fn parent_terminate_cascades_per_close_policy() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let parent = store.start(&start_req("d", "parent", "req-1")).unwrap();
        complete_with(
            &store,
            "d",
            "tl",
            vec![
                start_child_decision("d", "c1", ParentClosePolicy::Terminate),
                start_child_decision("d", "c2", ParentClosePolicy::Abandon),
            ],
        );
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(WorkflowExecution::new("parent", parent.run_id.clone())),
                ..Default::default()
            })
            .unwrap();
        // c1 terminated by policy; c2 abandoned (still open).
        let open: Vec<String> = store
            .list("d", true)
            .iter()
            .map(|i| i.execution.as_ref().unwrap().workflow_id.clone())
            .collect();
        assert!(!open.contains(&"c1".to_string()));
        assert!(open.contains(&"c2".to_string()));
        let closed: Vec<String> = store
            .list("d", false)
            .iter()
            .map(|i| i.execution.as_ref().unwrap().workflow_id.clone())
            .collect();
        assert!(closed.contains(&"parent".to_string()));
        assert!(closed.contains(&"c1".to_string()));
        // Parent history does NOT carry the child-terminated notification:
        // like Go, completions landing after the parent closed are dropped.
        let kinds = event_kinds(
            &store,
            "d",
            &WorkflowExecution::new("parent", parent.run_id),
        );
        assert!(!kinds.contains(&EventType::ChildWorkflowExecutionTerminated));
        // c1 itself was terminated by the policy.
        let c1 = store
            .list("d", false)
            .into_iter()
            .find(|i| i.execution.as_ref().unwrap().workflow_id == "c1")
            .unwrap();
        assert_eq!(
            c1.close_status,
            Some(cadence_api::enums::WorkflowExecutionCloseStatus::Terminated)
        );
    }

    #[test]
    fn continue_as_new_spawns_successor() {
        let store = InMemoryExecutionStore::new();
        let first = store.start(&start_req("d", "w", "req-1")).unwrap();
        complete_with(
            &store,
            "d",
            "tl",
            vec![Decision {
                decision_type: Some(DecisionType::ContinueAsNewWorkflowExecution),
                continue_as_new_workflow_execution_decision_attributes: Some(
                    ContinueAsNewWorkflowExecutionDecisionAttributes::default(),
                ),
                ..Default::default()
            }],
        );
        let open = store.list("d", true);
        assert_eq!(open.len(), 1);
        assert_ne!(open[0].execution.as_ref().unwrap().run_id, first.run_id);
        let kinds = event_kinds(&store, "d", &WorkflowExecution::new("w", first.run_id));
        assert!(kinds.contains(&EventType::WorkflowExecutionContinuedAsNew));
    }

    #[test]
    fn signal_external_delivers_or_fails() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let target = store.start(&start_req("d", "target", "req-t")).unwrap();
        store.start(&start_req("d", "source", "req-s")).unwrap();
        let signal = |wf: &str, run: &str| Decision {
            decision_type: Some(DecisionType::SignalExternalWorkflowExecution),
            signal_external_workflow_execution_decision_attributes: Some(
                SignalExternalWorkflowExecutionDecisionAttributes {
                    domain: "d".to_string(),
                    execution: Some(WorkflowExecution::new(wf.to_string(), run.to_string())),
                    signal_name: "ping".to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        // Poll source decision (FIFO may return target's first; drain both).
        complete_with(&store, "d", "tl", vec![signal("target", &target.run_id)]);
        let kinds = event_kinds(
            &store,
            "d",
            &WorkflowExecution::new("target", target.run_id),
        );
        assert!(kinds.contains(&EventType::WorkflowExecutionSignaled));
        // Unknown target records the Failed event on the source.
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        let source_run = task.workflow_execution.clone().unwrap().run_id.clone();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![signal("missing", "")],
                identity: "worker".to_string(),
                ..Default::default()
            })
            .unwrap();
        let kinds = event_kinds(
            &store,
            "d",
            &WorkflowExecution::new(task.workflow_execution.unwrap().workflow_id, source_run),
        );
        assert!(kinds.contains(&EventType::SignalExternalWorkflowExecutionFailed));
    }

    #[test]
    fn upsert_search_attributes_merges() {
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        complete_with(
            &store,
            "d",
            "tl",
            vec![Decision {
                decision_type: Some(DecisionType::UpsertWorkflowSearchAttributes),
                upsert_workflow_search_attributes_decision_attributes: Some(
                    UpsertWorkflowSearchAttributesDecisionAttributes {
                        search_attributes: Some(SearchAttributes {
                            indexed_fields: Some(
                                [("k".to_string(), b"v".to_vec())].into_iter().collect(),
                            ),
                        }),
                    },
                ),
                ..Default::default()
            }],
        );
        let desc = store
            .describe("d", &WorkflowExecution::new("w", started.run_id))
            .unwrap();
        let info = desc.workflow_execution_info.unwrap();
        assert!(info.history_length >= 4);
        let kinds = event_kinds(&store, "d", info.execution.as_ref().unwrap());
        assert!(kinds.contains(&EventType::UpsertWorkflowSearchAttributes));
    }

    #[test]
    fn query_buffers_and_answers_through_decision() {
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id);
        let qid = store
            .buffer_query("d", &exec, "state", Vec::new(), None)
            .unwrap();
        // Worker sees the query on its polled decision.
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        assert_eq!(
            task.query.as_ref().map(|q| q.query_type.clone()),
            Some("state".to_string())
        );
        assert_eq!(task.query_id, Some(qid.clone()));
        // Worker answers via queryResults.
        let mut results = std::collections::HashMap::new();
        results.insert(
            qid.clone(),
            WorkflowQueryResult {
                result_type: Some(cadence_api::enums::QueryResultType::Answered),
                answer: b"42".to_vec(),
                error_message: String::new(),
            },
        );
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![],
                identity: "worker".to_string(),
                query_results: Some(results),
                ..Default::default()
            })
            .unwrap();
        let answer = store.take_query_result(&qid).unwrap();
        assert_eq!(answer.answer, b"42");
    }

    #[test]
    fn query_on_closed_execution_rejects() {
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id);
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                ..Default::default()
            })
            .unwrap();
        let err = store
            .buffer_query("d", &exec, "state", Vec::new(), None)
            .unwrap_err();
        assert!(matches!(err, QueryError::Rejected(_)));
    }

    #[test]
    fn timers_fire_and_schedule_decisions() {
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        // Drain the initial decision without deciding.
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .fail_decision(&task.task_token, None, Vec::new(), "worker", "")
            .unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![Decision {
                    decision_type: Some(DecisionType::StartTimer),
                    start_timer_decision_attributes: Some(StartTimerDecisionAttributes {
                        timer_id: "t1".to_string(),
                        start_to_fire_timeout_seconds: Some(60),
                    }),
                    ..Default::default()
                }],
                identity: "worker".to_string(),
                ..Default::default()
            })
            .unwrap();
        // Not due yet.
        assert_eq!(store.fire_due_timers(0), 0);
        // Far future: fires.
        assert_eq!(store.fire_due_timers(i64::MAX / 2), 1);
        let kinds = event_kinds(&store, "d", &WorkflowExecution::new("w", started.run_id));
        assert!(kinds.contains(&EventType::TimerFired));
        // A new decision is available after the fire.
        assert!(store.poll_decision("d", "tl", "worker").is_some());
    }

    #[test]
    fn request_cancel_external_delivers() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let target = store.start(&start_req("d", "t", "req-t")).unwrap();
        store.start(&start_req("d", "s", "req-s")).unwrap();
        // Drain target's initial decision so the source decision is polled.
        let _ = store.poll_decision("d", "tl", "w");
        let task = store.poll_decision("d", "tl", "w").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![Decision {
                    decision_type: Some(DecisionType::RequestCancelExternalWorkflowExecution),
                    request_cancel_external_workflow_execution_decision_attributes: Some(
                        RequestCancelExternalWorkflowExecutionDecisionAttributes {
                            domain: "d".to_string(),
                            workflow_id: "t".to_string(),
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                }],
                identity: "w".to_string(),
                ..Default::default()
            })
            .unwrap();
        let kinds = event_kinds(&store, "d", &WorkflowExecution::new("t", target.run_id));
        assert!(kinds.contains(&EventType::WorkflowExecutionCancelRequested));
    }
}

#[cfg(test)]
mod activity_ext_tests {
    use super::*;
    use cadence_api::types::{TaskList, WorkflowType};

    fn start_req(domain: &str, wf: &str, request_id: &str) -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            domain: domain.to_string(),
            workflow_id: wf.to_string(),
            workflow_type: Some(WorkflowType {
                name: "wt".to_string(),
            }),
            task_list: Some(TaskList {
                name: "tl".to_string(),
                kind: None,
                base_name: String::new(),
            }),
            request_id: request_id.to_string(),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }
    }

    fn schedule_activity(id: &str) -> cadence_api::tasks::Decision {
        cadence_api::tasks::Decision {
            decision_type: Some(cadence_api::enums::DecisionType::ScheduleActivityTask),
            schedule_activity_task_decision_attributes: Some(
                cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                    activity_id: id.to_string(),
                    activity_type: Some(cadence_api::types::ActivityType {
                        name: "at".to_string(),
                    }),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }
    }

    fn polled_activity_token(
        store: &InMemoryExecutionStore,
        domain: &str,
        task_list: &str,
    ) -> Vec<u8> {
        store
            .poll_activity(domain, task_list, "worker")
            .unwrap()
            .task_token
    }

    fn setup_activity(store: &InMemoryExecutionStore) -> Vec<u8> {
        store.start(&start_req("d", "w", "req-1")).unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![schedule_activity("a1")],
                identity: "worker".to_string(),
                ..Default::default()
            })
            .unwrap();
        polled_activity_token(store, "d", "tl")
    }

    #[test]
    fn heartbeat_reports_cancel_requested() {
        let store = InMemoryExecutionStore::new();
        let token = setup_activity(&store);
        // No cancel requested yet.
        assert!(!store.record_heartbeat(&token, b"p1".to_vec()).unwrap());
        // Request cancel via a fresh decision, then heartbeat sees it.
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![cadence_api::tasks::Decision {
                    decision_type: Some(
                        cadence_api::enums::DecisionType::RequestCancelActivityTask,
                    ),
                    request_cancel_activity_task_decision_attributes: Some(
                        cadence_api::tasks::RequestCancelActivityTaskDecisionAttributes {
                            activity_id: "a1".to_string(),
                        },
                    ),
                    ..Default::default()
                }],
                identity: "worker".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert!(store.record_heartbeat(&token, b"p2".to_vec()).unwrap());
    }

    #[test]
    fn cancel_activity_appends_event() {
        use cadence_api::enums::EventType;
        let store = InMemoryExecutionStore::new();
        let token = setup_activity(&store);
        store
            .cancel_activity(&cadence_api::tasks::RespondActivityTaskCanceledRequest {
                task_token: token,
                details: b"bye".to_vec(),
                identity: "worker".to_string(),
            })
            .unwrap();
        let hist = store
            .history("d", &WorkflowExecution::new("w", String::new()))
            .unwrap()
            .history
            .unwrap();
        assert!(hist
            .events
            .iter()
            .any(|e| e.event_type == Some(EventType::ActivityTaskCanceled)));
    }

    #[test]
    fn history_pagination_pages_and_validates_tokens() {
        let store = InMemoryExecutionStore::new();
        store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", String::new());
        let p1 = store.history_page("d", &exec, 1, &[], None).unwrap();
        assert_eq!(p1.history.unwrap().events.len(), 1);
        assert!(!p1.next_page_token.is_empty());
        let p2 = store
            .history_page("d", &exec, 10, &p1.next_page_token, None)
            .unwrap();
        assert!(p2.next_page_token.is_empty());
        assert!(!p2.history.unwrap().events.is_empty());
        // Garbage token is a 400-style error like Go's Invalid NextPageToken.
        assert!(store.history_page("d", &exec, 10, b"nope!", None).is_err());
        assert!(store.history_page("d", &exec, 10, b"99", None).is_err());
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use cadence_api::enums::{EventType, FailureCategory};
    use cadence_api::types::FailureOptions;
    use cadence_api::types::{TaskList, WorkflowType};

    fn start_req(domain: &str, wf: &str, request_id: &str) -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            domain: domain.to_string(),
            workflow_id: wf.to_string(),
            workflow_type: Some(WorkflowType {
                name: "wt".to_string(),
            }),
            task_list: Some(TaskList {
                name: "tl".to_string(),
                kind: None,
                base_name: String::new(),
            }),
            request_id: request_id.to_string(),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }
    }

    fn retry_policy(max_attempts: i32) -> RetryPolicy {
        RetryPolicy {
            initial_interval_in_seconds: 1,
            backoff_coefficient: 2.0,
            maximum_attempts: max_attempts,
            expiration_interval_in_seconds: 60,
            ..Default::default()
        }
    }

    fn schedule_with_retry(policy: RetryPolicy) -> cadence_api::tasks::Decision {
        cadence_api::tasks::Decision {
            decision_type: Some(cadence_api::enums::DecisionType::ScheduleActivityTask),
            schedule_activity_task_decision_attributes: Some(
                cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                    activity_id: "a1".to_string(),
                    activity_type: Some(cadence_api::types::ActivityType {
                        name: "at".to_string(),
                    }),
                    retry_policy: Some(policy),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }
    }

    fn fail(token: Vec<u8>, reason: Option<&str>) -> RespondActivityTaskFailedRequest {
        RespondActivityTaskFailedRequest {
            task_token: token,
            reason: reason.map(str::to_string),
            identity: "worker".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn retryable_failure_reschedules_without_failed_event() {
        let store = InMemoryExecutionStore::new();
        store.start(&start_req("d", "w", "req-1")).unwrap();
        let task = store.poll_decision("d", "tl", "w").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![schedule_with_retry(retry_policy(3))],
                identity: "w".to_string(),
                ..Default::default()
            })
            .unwrap();
        let token = store.poll_activity("d", "tl", "w").unwrap().task_token;
        store.fail_activity(&fail(token, Some("boom"))).unwrap();
        // No Failed event; a fresh Scheduled event with attempt 1 exists.
        let hist = store
            .history("d", &WorkflowExecution::new("w", String::new()))
            .unwrap()
            .history
            .unwrap();
        assert!(hist
            .events
            .iter()
            .all(|e| e.event_type != Some(EventType::ActivityTaskFailed)));
        assert_eq!(
            hist.events
                .iter()
                .filter(|e| e.event_type == Some(EventType::ActivityTaskScheduled))
                .count(),
            2
        );
        // Not immediately repollable (backoff); due-loop requeues it.
        assert!(store.poll_activity("d", "tl", "w").is_none());
        assert_eq!(store.requeue_due_activities(i64::MAX / 2), 1);
        let retry = store.poll_activity("d", "tl", "w").unwrap();
        assert_eq!(retry.attempt, 1);
    }

    #[test]
    fn fatal_and_exhausted_failures_record_failed_event() {
        let store = InMemoryExecutionStore::new();
        store.start(&start_req("d", "w", "req-1")).unwrap();
        let task = store.poll_decision("d", "tl", "w").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![schedule_with_retry(retry_policy(1))],
                identity: "w".to_string(),
                ..Default::default()
            })
            .unwrap();
        // maxAttempts=1: first failure is final.
        let token = store.poll_activity("d", "tl", "w").unwrap().task_token;
        store.fail_activity(&fail(token, Some("boom"))).unwrap();
        let hist = store
            .history("d", &WorkflowExecution::new("w", String::new()))
            .unwrap()
            .history
            .unwrap();
        assert!(hist
            .events
            .iter()
            .any(|e| e.event_type == Some(EventType::ActivityTaskFailed)));

        // Fatal category also stops retries with room left.
        store.start(&start_req("d", "w2", "req-2")).unwrap();
        // Drain decisions until w2's initial decision arrives (FIFO is shared).
        let task = loop {
            let t = store.poll_decision("d", "tl", "w").unwrap();
            if t.workflow_execution.as_ref().unwrap().workflow_id == "w2" {
                break t;
            }
            store
                .complete_decision(&RespondDecisionTaskCompletedRequest {
                    task_token: t.task_token,
                    decisions: vec![],
                    identity: "w".to_string(),
                    ..Default::default()
                })
                .unwrap();
        };
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![schedule_with_retry(retry_policy(5))],
                identity: "w".to_string(),
                ..Default::default()
            })
            .unwrap();
        let token = store.poll_activity("d", "tl", "w").unwrap().task_token;
        store
            .fail_activity(&RespondActivityTaskFailedRequest {
                task_token: token,
                failure_options: Some(FailureOptions {
                    failure_category: Some(FailureCategory::Fatal),
                    next_retry_interval_seconds: None,
                }),
                identity: "w".to_string(),
                ..Default::default()
            })
            .unwrap();
        let hist = store
            .history("d", &WorkflowExecution::new("w2", String::new()))
            .unwrap()
            .history
            .unwrap();
        assert_eq!(
            hist.events
                .iter()
                .filter(|e| e.event_type == Some(EventType::ActivityTaskScheduled))
                .count(),
            1
        );
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "cadence-rust-test-{}-{}.json",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn execution_snapshot_round_trip() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let req = StartWorkflowExecutionRequest {
            domain: "d".to_string(),
            workflow_id: "w".to_string(),
            workflow_type: Some(WorkflowType { name: "wt".to_string() }),
            task_list: Some(TaskList {
                name: "tl".to_string(),
                kind: None,
                base_name: String::new(),
            }),
            request_id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        };
        let started = store.start(&req).unwrap();
        let path = tmp_path("exec");
        store.save_to_file(&path).unwrap();
        let loaded = InMemoryExecutionStore::load_from_file(&path).unwrap();
        std::fs::remove_file(&path).ok();
        // State survived: describe, history, and queued decision intact.
        let desc = loaded
            .describe("d", &WorkflowExecution::new("w", started.run_id.clone()))
            .unwrap();
        assert_eq!(
            desc.workflow_execution_info
                .unwrap()
                .history_length,
            2
        );
        assert!(loaded.poll_decision("d", "tl", "w").is_some());
        // Request-ID idempotency survived too.
        let again = loaded.start(&req).unwrap();
        assert_eq!(again.run_id, started.run_id);
    }
}
