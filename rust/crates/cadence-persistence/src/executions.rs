//! In-memory workflow execution store.
//!
//! Mirrors the observable semantics of the Go frontend + history path for
//! the core lifecycle: start (with request-ID idempotency and
//! `WorkflowIDReusePolicy`), describe, signal, request-cancel, terminate,
//! and history append/read. Each mutation appends a history event, so
//! `history_length` and `GetWorkflowExecutionHistory` behave like the Go
//! implementation for these operations.

use crate::version_history::{VersionHistoryExt, VersionHistoriesExt, EMPTY_VERSION};

fn empty_version() -> i64 { EMPTY_VERSION }

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use cadence_api::enums::{
    CancelExternalWorkflowExecutionFailedCause, ChildWorkflowExecutionFailedCause,
    DecisionTaskFailedCause, DecisionType, EventType, ParentClosePolicy, PendingActivityState,
    PendingDecisionState, SignalExternalWorkflowExecutionFailedCause, TimeoutType,
    WorkflowExecutionCloseStatus, WorkflowIDReusePolicy,
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

const HISTORY_PAGE_SIZE: usize = 1000;
#[derive(serde::Serialize, serde::Deserialize)]
struct HistoryCursor {
    domain: String,
    workflow_id: String,
    run_id: String,
    next: usize,
    end: usize,
    close_only: bool,
    #[serde(default)]
    transient_events: Vec<HistoryEvent>,
}
fn encode_history_cursor(
    domain: &str,
    execution: &WorkflowExecution,
    next: usize,
    end: usize,
    close_only: bool,
    transient_events: &[HistoryEvent],
) -> Vec<u8> {
    serde_json::to_vec(&HistoryCursor {
        domain: domain.to_string(),
        workflow_id: execution.workflow_id.clone(),
        run_id: execution.run_id.clone(),
        next,
        end,
        close_only,
        transient_events: transient_events.to_vec(),
    })
    .expect("history cursor contains only strings and integers")
}

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
    #[serde(default = "empty_version")]
    pub current_version: i64,
    #[serde(default)]
    pub version_histories: Option<cadence_api::types::VersionHistories>,
    #[serde(default)]
    pub(crate) buffered_events: Vec<HistoryEvent>,
    #[serde(default)]
    pub(crate) buffer_event_sequence: i64,
    pub(crate) pending_decision: Option<PendingDecision>,
    pub(crate) pending_activities: HashMap<i64, PendingActivity>,
    pub(crate) pending_timers: HashMap<String, PendingTimer>,
    pub(crate) pending_queries: std::collections::VecDeque<PendingQuery>,
    pub(crate) parent: Option<ParentRef>,
    pub(crate) prev_started_event_id: Option<i64>,
    #[serde(default)]
    pub(crate) signal_request_ids: std::collections::HashSet<String>,
    /// Go execution-info `CreateRequestID`: the request ID that created this
    /// run (start request ID, reset request ID on reset runs). Used for
    /// reset-request dedup.
    #[serde(default)]
    pub(crate) create_request_id: String,
    /// Go's `FirstDecisionTaskBackoffSeconds` for this run; the first decision
    /// task is only scheduled once it elapses.
    #[serde(default)]
    pub(crate) first_decision_task_backoff_seconds: i32,
    /// Go's execution-info `ExpirationTime`, nanoseconds since the epoch.
    #[serde(default)]
    pub(crate) expiration_time_nanos: Option<i64>,
    /// Go's execution-info `Attempt`: 0 for the first run and incremented on
    /// each retry continue-as-new.
    #[serde(default)]
    pub(crate) attempt: i32,
    /// Wall clock (millis) when the delayed first decision task becomes due.
    /// Cleared as soon as the decision task is scheduled.
    #[serde(default)]
    pub(crate) first_decision_due_millis: Option<i64>,
    #[serde(default)]
    pub(crate) jitter_start_seconds: i32,
    #[serde(default)]
    pub(crate) sticky_task_list: Option<String>,
    #[serde(default)]
    pub(crate) sticky_schedule_to_start_timeout_seconds: i32,
    #[serde(default)]
    pub(crate) last_updated_millis: i64,
}

/// Parent link for a child workflow, mirroring Go's parent execution info.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ParentRef {
    pub(crate) domain: String,
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    pub(crate) initiated_event_id: i64,
    #[serde(default)]
    pub(crate) started_event_id: i64,
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
    #[serde(default)]
    pub(crate) scheduled_time_millis: Option<i64>,
    #[serde(default)]
    pub(crate) started_time_millis: Option<i64>,
    #[serde(default)]
    pub(crate) identity: String,
    #[serde(default)]
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) task_list_name: Option<String>,
    #[serde(default)]
    pub(crate) sticky: bool,
    #[serde(default)]
    pub(crate) schedule_to_start_timeout_seconds: Option<i32>,
}

/// A scheduled activity, mirroring the pending-activity lifecycle.
/// Activity deadlines and transient retry state follow Go mutable state.
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
    #[serde(default)]
    pub(crate) schedule_to_start_timeout_seconds: Option<i32>,
    pub(crate) schedule_to_close_timeout_seconds: Option<i32>,
    pub(crate) start_to_close_timeout_seconds: Option<i32>,
    pub(crate) heartbeat_timeout_seconds: Option<i32>,
    pub(crate) scheduled_time_millis: i64,
    #[serde(default)]
    pub(crate) attempt_scheduled_time_millis: Option<i64>,
    #[serde(default)]
    pub(crate) started_time_millis: Option<i64>,
    #[serde(default)]
    pub(crate) last_heartbeat_time_millis: Option<i64>,
    #[serde(default)]
    pub(crate) started_identity: String,
    #[serde(default)]
    pub(crate) started_request_id: String,
    #[serde(default)]
    pub(crate) last_failure_reason: Option<String>,
    #[serde(default)]
    pub(crate) last_failure_details: Vec<u8>,
    #[serde(default)]
    pub(crate) last_failure_options: Option<cadence_api::types::FailureOptions>,
    pub(crate) retry_policy: Option<RetryPolicy>,
    pub(crate) header: Option<Header>,
}

impl ExecutionRecord {
    pub(crate) fn is_open(&self) -> bool {
        self.close_status.is_none()
    }

    /// The started-event attributes of the run's first history event.
    pub(crate) fn started_attributes(
        &self,
    ) -> cadence_api::service_types::WorkflowExecutionStartedEventAttributes {
        self.history
            .first()
            .and_then(|e| e.workflow_execution_started_event_attributes.clone())
            .unwrap_or_default()
    }

    /// The run's retry policy, mirroring Go's execution-info fields.
    pub(crate) fn retry_policy(&self) -> Option<RetryPolicy> {
        self.started_attributes().retry_policy.clone()
    }

    /// The cron schedule recorded on the start event, if any.
    pub(crate) fn cron_schedule(&self) -> String {
        self.started_attributes().cron_schedule.clone()
    }

    /// The cron overlap policy recorded on the start event, if any.
    pub(crate) fn cron_overlap_policy(&self) -> Option<cadence_api::enums::CronOverlapPolicy> {
        self.started_attributes().cron_overlap_policy
    }

    fn to_info(&self) -> WorkflowExecutionInfo {
        let started = self
            .history
            .first()
            .and_then(|e| e.workflow_execution_started_event_attributes.as_ref());
        let cron_schedule = started.map(|s| s.cron_schedule.clone());
        WorkflowExecutionInfo {
            execution: Some(self.execution.clone()),
            workflow_type: Some(self.workflow_type.clone()),
            start_time: Some(self.start_time_millis),
            close_time: self.close_time_millis,
            close_status: self.close_status,
            history_length: self.history.len() as i64,
            memo: self.memo.clone(),
            task_list: Some(self.task_list.clone()),
            search_attributes: self.search_attributes.clone(),
            parent_domain: started
                .and_then(|s| s.parent_workflow_domain.clone())
                .or_else(|| self.parent.as_ref().map(|p| p.domain.clone())),
            parent_execution: self
                .parent
                .as_ref()
                .map(|p| WorkflowExecution::new(&p.workflow_id, &p.run_id)),
            parent_initiated_id: self.parent.as_ref().map(|p| p.initiated_event_id),
            execution_time: Some(self.start_time_millis),
            is_cron: cron_schedule.as_ref().is_some_and(|s| !s.is_empty()),
            cron_schedule,
            cron_overlap_policy: started.and_then(|s| s.cron_overlap_policy),
            partition_config: started.and_then(|s| s.partition_config.clone()),
            update_time: self.history.last().and_then(|e| e.timestamp),
            execution_status: Some(
                self.close_status
                    .and_then(|s| {
                        cadence_api::enums::WorkflowExecutionStatus::from_i32(s as i32 + 2)
                    })
                    .unwrap_or(cadence_api::enums::WorkflowExecutionStatus::Started),
            ),
            ..Default::default()
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
                    state: Some(if p.cancel_requested {
                        PendingActivityState::CancelRequested
                    } else if p.started_event_id.is_some() {
                        PendingActivityState::Started
                    } else {
                        PendingActivityState::Scheduled
                    }),
                    attempt: p.attempt,
                    scheduled_timestamp: p
                        .attempt_scheduled_time_millis
                        .or(Some(p.scheduled_time_millis)),
                    last_started_timestamp: p.started_time_millis,
                    heartbeat_details: p.heartbeat_details.clone(),
                    maximum_attempts: p
                        .retry_policy
                        .as_ref()
                        .map(|r| r.maximum_attempts)
                        .unwrap_or(0),
                    schedule_id: p.scheduled_event_id,
                    last_failure_reason: p.last_failure_reason.clone(),
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
                    scheduled_timestamp: p.scheduled_time_millis,
                    started_timestamp: p.started_time_millis,
                    attempt: p.attempt,
                    schedule_id: p.scheduled_event_id,
                    original_scheduled_timestamp: None,
                }
            }),
        }
    }

    fn clear_stickiness(&mut self) {
        self.sticky_task_list = None;
        self.sticky_schedule_to_start_timeout_seconds = 0;
    }

    fn sticky_enabled(&self, now: i64) -> bool {
        // Go history.stickyTTL default is one year. Dynamic overrides remain
        // part of the configuration migration.
        self.sticky_task_list
            .as_ref()
            .is_some_and(|name| !name.is_empty())
            && now
                <= self
                    .last_updated_millis
                    .saturating_add(365 * 24 * 60 * 60 * 1000)
    }

    fn transient_decision_events(&self) -> Vec<HistoryEvent> {
        let Some(task) = self
            .pending_decision
            .as_ref()
            .filter(|task| task.attempt > 0 && task.started_event_id.is_some())
        else {
            return Vec::new();
        };
        vec![
            HistoryEvent {
                event_id: task.scheduled_event_id,
                version: self.current_version,
                timestamp: task.scheduled_time_millis,
                event_type: Some(EventType::DecisionTaskScheduled),
                decision_task_scheduled_event_attributes: Some(
                    DecisionTaskScheduledEventAttributes {
                        task_list: Some(self.task_list.clone()),
                        start_to_close_timeout_seconds: self.task_start_to_close_timeout_seconds,
                        attempt: task.attempt,
                    },
                ),
                ..Default::default()
            },
            HistoryEvent {
                event_id: task.started_event_id.unwrap(),
                version: self.current_version,
                timestamp: task.started_time_millis,
                event_type: Some(EventType::DecisionTaskStarted),
                decision_task_started_event_attributes: Some(DecisionTaskStartedEventAttributes {
                    scheduled_event_id: task.scheduled_event_id,
                    identity: task.identity.clone(),
                    request_id: task.request_id.clone(),
                }),
                ..Default::default()
            },
        ]
    }

    fn rebuild_version_history(&mut self, branch_token: Vec<u8>) {
        let items: Vec<_> = self.history.iter().map(|event| {
            crate::version_history::new_item(event.event_id, event.version)
        }).collect();
        self.version_histories = Some(cadence_api::types::VersionHistories {
            current_version_history_index: 0,
            histories: vec![crate::version_history::new_history(branch_token, &items)],
        });
    }

    fn record_version(&mut self, event_id: i64, version: i64) {
        if self.version_histories.is_none() {
            let token = crate::history_branch::new_token(&self.execution.run_id, &self.execution.run_id);
            self.rebuild_version_history(token);
        }
        let histories = self.version_histories.as_mut().unwrap();
        histories.histories[histories.current_version_history_index as usize]
            .add_or_update(&crate::version_history::new_item(event_id, version))
            .expect("committed events must extend the current version history");
    }

    fn materialize_transient_decision(&mut self, identity: &str) {
        let mut events = self.transient_decision_events();
        if let Some(started) = events.get_mut(1) {
            started
                .decision_task_started_event_attributes
                .as_mut()
                .unwrap()
                .identity = identity.to_owned();
        }
        for event in events {
            debug_assert_eq!(event.event_id, self.next_event_id());
            self.record_version(event.event_id, event.version);
            self.history.push(event);
        }
    }

    fn next_event_id(&self) -> i64 {
        self.history.len() as i64 + 1
    }

    fn push_event(&mut self, event_type: EventType, mut event: HistoryEvent) -> i64 {
        if event_type == EventType::WorkflowExecutionSignaled {
            if let Some(id) = event
                .workflow_execution_signaled_event_attributes
                .as_ref()
                .map(|a| &a.request_id)
                .filter(|id| !id.is_empty())
            {
                if !self.signal_request_ids.insert(id.clone()) {
                    return 0;
                }
            }
        }
        let buffer = self
            .pending_decision
            .as_ref()
            .is_some_and(|task| task.started_event_id.is_some())
            && !matches!(
                event_type,
                EventType::WorkflowExecutionStarted
                    | EventType::WorkflowExecutionCompleted
                    | EventType::WorkflowExecutionFailed
                    | EventType::WorkflowExecutionTimedOut
                    | EventType::WorkflowExecutionTerminated
                    | EventType::WorkflowExecutionContinuedAsNew
                    | EventType::WorkflowExecutionCanceled
                    | EventType::DecisionTaskScheduled
                    | EventType::DecisionTaskStarted
                    | EventType::DecisionTaskCompleted
                    | EventType::DecisionTaskFailed
                    | EventType::DecisionTaskTimedOut
                    | EventType::ActivityTaskScheduled
                    | EventType::ActivityTaskCancelRequested
                    | EventType::TimerStarted
                    | EventType::TimerCanceled
                    | EventType::CancelTimerFailed
                    | EventType::RequestCancelExternalWorkflowExecutionInitiated
                    | EventType::MarkerRecorded
                    | EventType::StartChildWorkflowExecutionInitiated
                    | EventType::SignalExternalWorkflowExecutionInitiated
                    | EventType::UpsertWorkflowSearchAttributes
            );
        // Private aliases let started/completion links remain unambiguous until
        // Go-style buffered events receive their durable IDs. These never appear
        // in public history or task tokens.
        event.event_id = if buffer {
            {
                let id = -1_000_000 - self.buffer_event_sequence;
                self.buffer_event_sequence += 1;
                id
            }
        } else {
            self.next_event_id()
        };
        let now = now_millis();
        event.timestamp = Some(now);
        event.event_type = Some(event_type);
        event.version = self.current_version;
        let id = event.event_id;
        if buffer {
            self.buffered_events.push(event);
        } else {
            self.record_version(event.event_id, event.version);
            self.history.push(event);
        }
        id
    }
}

/// Queue key: (domain, task-list name).
type QueueKey = (String, String);
/// Queued decision task: (workflow-id, run-id).
type QueuedDecision = (String, String);
/// Queued activity task: (workflow-id, run-id, scheduled-event-id).
type QueuedActivity = (String, String, i64);

/// An async workflow request waiting for the background consumer,
/// mirroring Go's Kafka `AsyncRequestMessage` payloads
/// (`StartWorkflowExecutionAsyncRequest` /
/// `SignalWithStartWorkflowExecutionAsyncRequest`). Attempts are bounded so a
/// poisoned request cannot spin the consumer forever.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AsyncRequest {
    pub request: AsyncRequestKind,
    pub attempts: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AsyncRequestKind {
    Start(cadence_api::types::StartWorkflowExecutionRequest),
    SignalWithStart(cadence_api::service_types::SignalWithStartWorkflowExecutionRequest),
}

impl AsyncRequest {
    pub const MAX_ATTEMPTS: u32 = 5;
}

/// Stored task-list partition config, mirroring Go matching's persisted
/// `TaskListPartitionConfig` (version plus read/write partition maps of
/// partition ID to isolation groups).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StoredPartitionConfig {
    pub version: i64,
    pub read_partitions: HashMap<i32, Vec<String>>,
    pub write_partitions: HashMap<i32, Vec<String>>,
}

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
    pub(crate) domain_ids: HashMap<String, String>,
    pub(crate) domain_names: HashMap<String, String>,
    pub(crate) domain_versions: HashMap<String, i64>,
    /// Durable async-request queue (FIFO), drained by the server's worker role.
    pub(crate) async_requests: std::collections::VecDeque<AsyncRequest>,
    /// (domain namespace, schedule ID) -> schedule record.
    pub(crate) schedules: HashMap<(String, String), crate::schedules::ScheduleRecord>,
    /// (domain namespace, semaphore name) -> metadata.
    pub(crate) semaphores: HashMap<(String, String), crate::semaphores::SemaphoreMetadata>,
    /// (domain namespace, semaphore name, bucket) -> granted holds.
    pub(crate) semaphore_holds: HashMap<(String, String, i32), Vec<crate::semaphores::SemaphoreHold>>,
    /// (domain namespace, task list, task-list type) -> partition config.
    pub(crate) partition_configs: HashMap<(String, String, i32), StoredPartitionConfig>,
}

impl Inner {
    pub(crate) fn domain_id(&self, value: &str) -> String {
        self.domain_ids
            .get(value)
            .cloned()
            .unwrap_or_else(|| value.to_owned())
    }
    pub(crate) fn domain_name(&self, value: &str) -> String {
        self.domain_names
            .get(value)
            .cloned()
            .unwrap_or_else(|| value.to_owned())
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryExecutionStore {
    pub(crate) inner: Arc<RwLock<Inner>>,
    domain_scope: Option<String>,
}

impl InMemoryExecutionStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin a request to the resolved UUID once, including across long polls.
    pub fn with_domain_id(&self, id: &str) -> Self {
        Self {
            inner: self.inner.clone(),
            domain_scope: Some(id.to_owned()),
        }
    }
    pub fn set_domain_version(&self, id: &str, version: i64) {
        self.inner.write().expect("execution store lock").domain_versions.insert(id.to_string(), version);
    }

    pub fn namespace_id(&self) -> Option<&str> {
        self.domain_scope.as_deref()
    }
    fn domain_id(&self, inner: &Inner, name: &str) -> String {
        self.domain_scope
            .clone()
            .unwrap_or_else(|| inner.domain_id(name))
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
        self.start_locked(&mut inner, req)
    }

    /// Enqueue an async start request, mirroring Go frontend
    /// `StartWorkflowExecutionAsync` publishing to the async queue. The
    /// request is validated by the caller before enqueueing and processed by
    /// the worker-role consumer.
    pub fn enqueue_async_start(&self, req: StartWorkflowExecutionRequest) {
        let mut inner = self.inner.write().expect("execution store lock");
        inner.async_requests.push_back(AsyncRequest {
            request: AsyncRequestKind::Start(req),
            attempts: 0,
        });
    }

    /// Enqueue an async signal-with-start request, mirroring Go frontend
    /// `SignalWithStartWorkflowExecutionAsync`.
    pub fn enqueue_async_signal_with_start(
        &self,
        req: cadence_api::service_types::SignalWithStartWorkflowExecutionRequest,
    ) {
        let mut inner = self.inner.write().expect("execution store lock");
        inner.async_requests.push_back(AsyncRequest {
            request: AsyncRequestKind::SignalWithStart(req),
            attempts: 0,
        });
    }

    /// Drain queued async requests for the worker-role consumer.
    pub fn drain_async_requests(&self) -> Vec<AsyncRequest> {
        let mut inner = self.inner.write().expect("execution store lock");
        inner.async_requests.drain(..).collect()
    }

    /// Process one async request like Go's async-queue consumer: apply the
    /// start (idempotent on request ID, so redelivery is safe). Returns
    /// `true` when the request is consumed and `false` when it must be
    /// requeued for another attempt.
    pub fn consume_async_request(&self, request: &AsyncRequest) -> bool {
        match &request.request {
            AsyncRequestKind::Start(req) => match self.start(req) {
                Ok(_) => true,
                // Idempotent redelivery: the run already exists.
                Err(StartError::AlreadyStarted(_)) => true,
                // Invalid requests never succeed; drop them like a poison
                // message after logging, instead of spinning forever.
                Err(StartError::InvalidArgument(message)) => {
                    eprintln!("warning: dropping invalid async start request: {message}");
                    true
                }
            },
            AsyncRequestKind::SignalWithStart(req) => match self.signal_with_start(req) {
                Ok(_) => true,
                Err(StartError::AlreadyStarted(_)) => true,
                Err(StartError::InvalidArgument(message)) => {
                    eprintln!("warning: dropping invalid async signal-with-start: {message}");
                    true
                }
            },
        }
    }

    /// Requeue a failed async request with bounded attempts, mirroring Go's
    /// NACK/redelivery with a poison-message limit.
    pub fn requeue_async_request(&self, mut request: AsyncRequest) {
        request.attempts += 1;
        if request.attempts >= AsyncRequest::MAX_ATTEMPTS {
            eprintln!("warning: dropping async request after {} attempts", request.attempts);
            return;
        }
        let mut inner = self.inner.write().expect("execution store lock");
        inner.async_requests.push_back(request);
    }

    /// Restart a workflow execution, mirroring Go frontend
    /// `RestartWorkflowExecution` + `constructRestartWorkflowRequest`: the new
    /// run reuses the first history event's started attributes (type, task
    /// list, input, timeouts, retry policy, cron schedule, header, memo,
    /// search attributes) with `TerminateIfRunning` reuse policy, so an open
    /// run is terminated and a closed one starts fresh. The reason is recorded
    /// nowhere, as in Go.
    pub fn restart(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        identity: &str,
    ) -> Result<StartWorkflowExecutionResponse, StartError> {
        use cadence_api::enums::WorkflowIDReusePolicy;
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        // Resolve the run like history reads do: an empty run ID targets the
        // latest run, so restarts work on both open and closed executions.
        let run_id = if execution.run_id.is_empty() {
            inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .and_then(|runs| runs.last().cloned())
                .ok_or_else(|| {
                    StartError::InvalidArgument(
                        "Workflow execution does not exist.".to_string(),
                    )
                })?
        } else {
            execution.run_id.clone()
        };
        let started = inner
            .by_run
            .get(&(
                domain.to_string(),
                execution.workflow_id.clone(),
                run_id,
            ))
            .and_then(|record| {
                record
                    .history
                    .first()
                    .and_then(|e| e.workflow_execution_started_event_attributes.clone())
            })
            .ok_or_else(|| {
                StartError::InvalidArgument("Workflow execution does not exist.".to_string())
            })?;
        let req = StartWorkflowExecutionRequest {
            domain: domain.to_string(),
            workflow_id: execution.workflow_id.clone(),
            workflow_type: started.workflow_type.clone(),
            task_list: started.task_list.clone(),
            input: started.input.clone(),
            execution_start_to_close_timeout_seconds: started
                .execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: started.task_start_to_close_timeout_seconds,
            identity: identity.to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            workflow_id_reuse_policy: Some(WorkflowIDReusePolicy::TerminateIfRunning),
            retry_policy: started.retry_policy.clone(),
            cron_schedule: started.cron_schedule.clone(),
            memo: started.memo.clone(),
            search_attributes: started.search_attributes.clone(),
            header: started.header.clone(),
            delay_start_seconds: Some(0),
            jitter_start_seconds: started.jitter_start_seconds,
            cron_overlap_policy: started.cron_overlap_policy,
            ..Default::default()
        };
        self.start_locked(&mut inner, &req)
    }

    fn start_locked(
        &self,
        inner: &mut Inner,
        req: &StartWorkflowExecutionRequest,
    ) -> Result<StartWorkflowExecutionResponse, StartError> {
        let mut request = req.clone();
        request.domain = self.domain_id(inner, &request.domain);
        let req = &request;
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
        if let Some(open_run) = self.open_run_id(inner, &req.domain, &req.workflow_id) {
            if policy == WorkflowIDReusePolicy::TerminateIfRunning {
                // Close the running execution as terminated, like Go does.
                Self::close_run_locked(
                    inner,
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
            let last = self.last_close_status(inner, &req.domain, &req.workflow_id);
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
        let start_millis = now_millis();
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
            start_time_millis: start_millis,
            close_time_millis: None,
            history: Vec::new(),
            current_version: inner.domain_versions.get(&req.domain).copied().unwrap_or(EMPTY_VERSION),
            version_histories: Some(cadence_api::types::VersionHistories {
                current_version_history_index: 0,
                histories: vec![cadence_api::types::VersionHistory {
                    branch_token: crate::history_branch::new_token(&run_id, &uuid::Uuid::new_v4().to_string()),
                    items: Vec::new(),
                }],
            }),
            buffered_events: Vec::new(),
            buffer_event_sequence: 0,
            sticky_task_list: None,
            sticky_schedule_to_start_timeout_seconds: 0,
            last_updated_millis: now_millis(),
            pending_decision: None,
            pending_activities: HashMap::new(),
            pending_timers: HashMap::new(),
            pending_queries: std::collections::VecDeque::new(),
            parent: None,
            prev_started_event_id: None,
            signal_request_ids: std::collections::HashSet::new(),
            create_request_id: String::new(),
            first_decision_task_backoff_seconds: 0,
            expiration_time_nanos: None,
            first_decision_due_millis: None,
            jitter_start_seconds: req.jitter_start_seconds.unwrap_or(0),
            attempt: 0,
        };
        let start_nanos = start_millis.saturating_mul(1_000_000);
        let jitter = cadence_common::cron::jitter_seconds(
            req.jitter_start_seconds.unwrap_or(0),
            &mut rand::thread_rng(),
        );
        let history_request = cadence_api::history_request::create_history_start_workflow_request(
            req,
            start_nanos,
            jitter,
        )
        .map_err(StartError::InvalidArgument)?;
        record.first_decision_task_backoff_seconds =
            history_request.first_decision_task_backoff_seconds;
        record.expiration_time_nanos = history_request.expiration_timestamp;
        record.attempt = 0;
        record.create_request_id = req.request_id.clone();
        let backoff_millis = i64::from(history_request.first_decision_task_backoff_seconds.max(0))
            .saturating_mul(1000);
        if backoff_millis > 0 {
            record.first_decision_due_millis = Some(start_millis + backoff_millis);
        }
        let started = HistoryEvent {
            workflow_execution_started_event_attributes: Some(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: req.workflow_type.clone(),
                    task_list: req.task_list.clone(),
                    input: req.input.clone(),
                    execution_start_to_close_timeout_seconds: req
                        .execution_start_to_close_timeout_seconds,
                    task_start_to_close_timeout_seconds: req.task_start_to_close_timeout_seconds,
                    original_execution_run_id: run_id.clone(),
                    first_execution_run_id: run_id.clone(),
                    expiration_timestamp: history_request.expiration_timestamp,
                    first_scheduled_time_nano: (!req.cron_schedule.is_empty()).then_some(
                        start_nanos.saturating_add(backoff_millis.saturating_mul(1_000_000)),
                    ),
                    identity: req.identity.clone(),
                    request_id: req.request_id.clone(),
                    retry_policy: req.retry_policy.clone(),
                    cron_schedule: req.cron_schedule.clone(),
                    cron_overlap_policy: req.cron_overlap_policy,
                    first_decision_task_backoff_seconds: Some(
                        history_request.first_decision_task_backoff_seconds,
                    ),
                    jitter_start_seconds: req.jitter_start_seconds,
                    memo: req.memo.clone(),
                    search_attributes: req.search_attributes.clone(),
                    header: req.header.clone(),
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
        // A non-zero first decision backoff (delay/jitter/cron/first-run-at)
        // delays the DecisionTaskScheduled event until it elapses, mirroring
        // Go's `WorkflowBackoffTimerTask`.
        if backoff_millis <= 0 {
            // Every new run starts with a decision task, like Go history does.
            Self::schedule_decision_locked(
                inner,
                &req.domain,
                &req.workflow_id,
                &run_id,
                &task_list.name,
                0,
            );
        }
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
            Some(r) if r.is_open() => {
                r.last_updated_millis = now_millis();
                return;
            }
            _ => return,
        };
        let sticky = record.sticky_enabled(now_millis());
        if !sticky {
            record.clear_stickiness();
        }
        let queue_name = record
            .sticky_task_list
            .clone()
            .unwrap_or_else(|| task_list.to_owned());
        let event = HistoryEvent {
            decision_task_scheduled_event_attributes: Some(DecisionTaskScheduledEventAttributes {
                task_list: Some(TaskList {
                    name: queue_name.clone(),
                    ..Default::default()
                }),
                start_to_close_timeout_seconds: record.task_start_to_close_timeout_seconds,
                attempt,
            }),
            ..Default::default()
        };
        let scheduled_time_millis = now_millis();
        let scheduled_event_id = record.next_event_id();
        if attempt == 0 {
            record.push_event(EventType::DecisionTaskScheduled, event);
        }
        record.pending_decision = Some(PendingDecision {
            scheduled_event_id,
            started_event_id: None,
            attempt,
            scheduled_time_millis: if attempt == 0 {
                record.history.last().and_then(|event| event.timestamp)
            } else {
                Some(scheduled_time_millis)
            },
            started_time_millis: None,
            identity: String::new(),
            request_id: String::new(),
            task_list_name: Some(queue_name.clone()),
            sticky,
            schedule_to_start_timeout_seconds: sticky
                .then_some(record.sticky_schedule_to_start_timeout_seconds),
        });
        record.last_updated_millis = now_millis();
        inner
            .decision_queues
            .entry((domain.to_string(), queue_name))
            .or_default()
            .push_back((workflow_id.to_string(), run_id.to_string()));
    }

    fn flush_buffered_events_locked(inner: &mut Inner, key: &(String, String, String)) {
        let record = inner.by_run.get_mut(key).expect("record exists");
        let mut events = std::mem::take(&mut record.buffered_events);
        let ids: HashMap<_, _> = events
            .iter()
            .enumerate()
            .map(|(index, event)| (event.event_id, record.next_event_id() + index as i64))
            .collect();
        for event in &mut events {
            event.event_id = ids[&event.event_id];
            macro_rules! remap {
                ($($field:ident),+ $(,)?) => {$(
                    if let Some(attributes) = event.$field.as_mut() {
                        if let Some(id) = ids.get(&attributes.started_event_id) {
                            attributes.started_event_id = *id;
                        }
                    }
                )+};
            }
            remap!(
                activity_task_completed_event_attributes,
                activity_task_failed_event_attributes,
                activity_task_canceled_event_attributes,
                activity_task_timed_out_event_attributes,
                child_workflow_execution_completed_event_attributes,
                child_workflow_execution_failed_event_attributes,
                child_workflow_execution_canceled_event_attributes,
                child_workflow_execution_timed_out_event_attributes,
                child_workflow_execution_terminated_event_attributes
            );
        }
        for activity in record.pending_activities.values_mut() {
            if let Some(id) = activity.started_event_id.and_then(|id| ids.get(&id)) {
                activity.started_event_id = Some(*id);
            }
        }
        for event in &events {
            record.record_version(event.event_id, event.version);
        }
        record.history.extend(events);
        for child in inner.by_run.values_mut() {
            if let Some(parent) = child.parent.as_mut() {
                if (&parent.domain, &parent.workflow_id, &parent.run_id) == (&key.0, &key.1, &key.2)
                {
                    if let Some(id) = ids.get(&parent.started_event_id) {
                        parent.started_event_id = *id;
                    }
                }
            }
        }
    }

    fn get_open_mut<'a>(
        &self,
        inner: &'a mut Inner,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<&'a mut ExecutionRecord, WorkflowMutationError> {
        let domain_key = self.domain_id(inner, domain);
        let domain = domain_key.as_str();
        if execution.run_id.is_empty() {
            // No run ID: target the open run, mirroring Go's current-execution lookup.
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| {
                    WorkflowMutationError::NotFound(EntityNotExistsError::new(
                        "Workflow execution does not exist.".to_string(),
                    ))
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
                    WorkflowMutationError::NotFound(EntityNotExistsError::new(
                        "Workflow execution does not exist.".to_string(),
                    ))
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
                WorkflowMutationError::NotFound(EntityNotExistsError::new(
                    "Workflow execution does not exist.".to_string(),
                ))
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
        let record = self.get_open_mut(&mut inner, &req.domain, &execution)?;
        if !req.request_id.is_empty() && record.signal_request_ids.contains(&req.request_id) {
            return Ok(());
        }
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
        let namespace = record.domain.clone();
        let workflow_id = record.execution.workflow_id.clone();
        let run_id = record.execution.run_id.clone();
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(
            &mut inner,
            &namespace,
            &workflow_id,
            &run_id,
            &task_list,
            0,
        );
        Ok(())
    }

    pub fn request_cancel(
        &self,
        req: &RequestCancelWorkflowExecutionRequest,
    ) -> Result<(), WorkflowMutationError> {
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut inner = self.inner.write().expect("execution store lock");
        let record = self.get_open_mut(&mut inner, &req.domain, &execution)?;
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
        let namespace = record.domain.clone();
        let workflow_id = record.execution.workflow_id.clone();
        let run_id = record.execution.run_id.clone();
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(
            &mut inner,
            &namespace,
            &workflow_id,
            &run_id,
            &task_list,
            0,
        );
        Ok(())
    }

    pub fn reset_sticky_task_list(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<(), WorkflowMutationError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let record = self.get_open_mut(&mut inner, domain, execution)?;
        record.clear_stickiness();
        record.last_updated_millis = now_millis();
        Ok(())
    }

    /// Reset a workflow execution to a decision-task boundary event,
    /// mirroring Go history `ResetWorkflowExecution` + `workflowResetter`
    /// (minus branches/versions, which a single host does not keep):
    /// validate the reset point, terminate the current run, fork the base
    /// history prefix into a new run, and reapply later signals unless
    /// skipped. Duplicate reset request IDs return the current run.
    pub fn reset(
        &self,
        req: &cadence_api::types::ResetWorkflowExecutionRequest,
    ) -> Result<StartWorkflowExecutionResponse, ResetError> {
        use cadence_api::enums::EventType;
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, &req.domain);
        let domain = domain_key.as_str();
        // Resolve the base run; an empty run ID targets the latest run.
        let base_run_id = if execution.run_id.is_empty() {
            inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .and_then(|runs| runs.last().cloned())
                .ok_or_else(|| {
                    ResetError::NotFound(EntityNotExistsError::new(
                        "Workflow execution does not exist.".to_string(),
                    ))
                })?
        } else {
            execution.run_id.clone()
        };
        let base_key = (
            domain.to_string(),
            execution.workflow_id.clone(),
            base_run_id.clone(),
        );
        let base = inner.by_run.get(&base_key).ok_or_else(|| {
            ResetError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        // Go requires at least one scheduled decision task.
        if !base.history.iter().any(|e| {
            e.event_type == Some(EventType::DecisionTaskScheduled)
        }) {
            return Err(ResetError::Invalid(
                "Cannot reset workflow without a decision task schedule.".to_string(),
            ));
        }
        let next_event_id = base.history.len() as i64 + 1;
        if req.decision_finish_event_id <= 1 || req.decision_finish_event_id > next_event_id {
            return Err(ResetError::Invalid(
                "Decision finish ID must be > 1 && <= workflow next event ID.".to_string(),
            ));
        }
        // Go `checkResetEventType`: only decision-task boundaries are resettable.
        let reset_event = &base.history[(req.decision_finish_event_id - 1) as usize];
        if !matches!(
            reset_event.event_type,
            Some(
                EventType::DecisionTaskStarted
                    | EventType::DecisionTaskTimedOut
                    | EventType::DecisionTaskFailed
                    | EventType::DecisionTaskCompleted
            )
        ) {
            return Err(ResetError::Invalid(format!(
                "reset event must be of type DecisionTaskStarted, DecisionTaskTimedOut, DecisionTaskFailed, or DecisionTaskCompleted. Attempting to reset on event type: {:?}",
                reset_event.event_type,
            )));
        }
        // Current run and request-ID dedup, like Go's CreateRequestID check.
        let current_run_id = inner
            .by_workflow
            .get(&(domain.to_string(), execution.workflow_id.clone()))
            .and_then(|runs| runs.last().cloned())
            .ok_or_else(|| {
                ResetError::NotFound(EntityNotExistsError::new(
                    "Workflow execution does not exist.".to_string(),
                ))
            })?;
        let current_key = (
            domain.to_string(),
            execution.workflow_id.clone(),
            current_run_id.clone(),
        );
        if !req.request_id.is_empty() {
            if let Some(current) = inner.by_run.get(&current_key) {
                if current.create_request_id == req.request_id {
                    return Ok(StartWorkflowExecutionResponse {
                        run_id: current_run_id,
                    });
                }
            }
        }
        // Snapshot the replay prefix and later signals before mutating.
        let prefix: Vec<HistoryEvent> = base.history
            [..req.decision_finish_event_id as usize]
            .to_vec();
        let mut signals: Vec<HistoryEvent> = base.history
            [req.decision_finish_event_id as usize..]
            .iter()
            .filter(|e| e.event_type == Some(EventType::WorkflowExecutionSignaled))
            .cloned()
            .collect();
        if current_run_id != base_run_id {
            if let Some(current) = inner.by_run.get(&current_key) {
                signals.extend(
                    current
                        .history
                        .iter()
                        .filter(|e| e.event_type == Some(EventType::WorkflowExecutionSignaled))
                        .cloned(),
                );
            }
        }
        let base_record = inner.by_run.get(&base_key).expect("base exists").clone();
        // Terminate the current run when open, like the resetter does.
        if inner
            .by_run
            .get(&current_key)
            .is_some_and(ExecutionRecord::is_open)
        {
            Self::close_run_locked(
                &mut inner,
                domain,
                &execution.workflow_id,
                &current_run_id,
                WorkflowExecutionCloseStatus::Terminated,
                None,
                Vec::new(),
                HistoryEvent {
                    workflow_execution_terminated_event_attributes: Some(
                        WorkflowExecutionTerminatedEventAttributes {
                            reason: if req.reason.is_empty() {
                                "reset workflow".to_string()
                            } else {
                                req.reason.clone()
                            },
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                },
                EventType::WorkflowExecutionTerminated,
            );
        }
        // Fork the prefix into the reset run.
        let reset_run_id = uuid::Uuid::new_v4().to_string();
        let mut record = ExecutionRecord {
            domain: domain.to_string(),
            execution: WorkflowExecution::new(
                execution.workflow_id.clone(),
                reset_run_id.clone(),
            ),
            workflow_type: base_record.workflow_type.clone(),
            task_list: base_record.task_list.clone(),
            execution_start_to_close_timeout_seconds: base_record
                .execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: base_record.task_start_to_close_timeout_seconds,
            memo: base_record.memo.clone(),
            search_attributes: base_record.search_attributes.clone(),
            close_status: None,
            start_time_millis: now_millis(),
            close_time_millis: None,
            history: prefix,
            current_version: inner.domain_versions.get(domain).copied().unwrap_or(base_record.current_version).max(base_record.current_version),
            version_histories: None,
            buffered_events: Vec::new(),
            buffer_event_sequence: 0,
            sticky_task_list: None,
            sticky_schedule_to_start_timeout_seconds: 0,
            last_updated_millis: now_millis(),
            pending_decision: None,
            pending_activities: HashMap::new(),
            pending_timers: HashMap::new(),
            pending_queries: std::collections::VecDeque::new(),
            parent: base_record.parent.clone(),
            prev_started_event_id: None,
            signal_request_ids: std::collections::HashSet::new(),
            create_request_id: req.request_id.clone(),
            first_decision_task_backoff_seconds: 0,
            expiration_time_nanos: None,
            first_decision_due_millis: None,
            jitter_start_seconds: 0,
            attempt: 0,
        };
        let source_branch = base_record.version_histories.as_ref()
            .and_then(|histories| histories.current_history().ok())
            .and_then(|history| crate::history_branch::decode(&history.branch_token).ok())
            .unwrap_or(cadence_api::types::HistoryBranch {
                tree_id: base_run_id.clone(), branch_id: base_run_id.clone(), ancestors: Vec::new(),
            });
        let fork = crate::history_branch::fork(&source_branch, record.next_event_id(), &uuid::Uuid::new_v4().to_string())
            .map_err(|e| ResetError::Invalid(e.message))?;
        record.rebuild_version_history(crate::history_branch::encode(&fork));
        // Rebuild signal dedup state from the forked prefix.
        for event in record.history.iter() {
            if let Some(id) = event
                .workflow_execution_signaled_event_attributes
                .as_ref()
                .map(|a| &a.request_id)
                .filter(|id| !id.is_empty())
            {
                record.signal_request_ids.insert(id.clone());
            }
        }
        // Reapply later signals unless skipped, like Go's signal reapply.
        if !req.skip_signal_reapply {
            for signal in signals {
                if let Some(attrs) = signal
                    .workflow_execution_signaled_event_attributes
                    .clone()
                {
                    let id = attrs.request_id.clone();
                    if !id.is_empty() && record.signal_request_ids.contains(&id) {
                        continue;
                    }
                    record.push_event(
                        EventType::WorkflowExecutionSignaled,
                        HistoryEvent {
                            workflow_execution_signaled_event_attributes: Some(attrs),
                            ..Default::default()
                        },
                    );
                }
            }
        }
        inner
            .by_workflow
            .entry((domain.to_string(), execution.workflow_id.clone()))
            .or_default()
            .push(reset_run_id.clone());
        inner.by_run.insert(
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                reset_run_id.clone(),
            ),
            record,
        );
        Self::schedule_decision_locked(
            &mut inner,
            domain,
            &execution.workflow_id,
            &reset_run_id,
            &base_record.task_list.name,
            0,
        );
        Ok(StartWorkflowExecutionResponse {
            run_id: reset_run_id,
        })
    }

    /// Regenerate dispatch queue entries for an execution, mirroring Go
    /// history `RefreshWorkflowTasks` + `mutableStateTaskRefresher`: timer
    /// state is scanned on the fly by the background loop, so the durable
    /// work here is requeueing unstarted decision and activity tasks that
    /// lost their queue entries. Closed runs are a no-op success, like Go
    /// regenerating (already complete) close tasks.
    pub fn refresh_tasks(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<(), WorkflowMutationError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = if execution.run_id.is_empty() {
            match self.open_run_id(&inner, domain, &execution.workflow_id) {
                Some(run_id) => (
                    domain.to_string(),
                    execution.workflow_id.clone(),
                    run_id,
                ),
                None => {
                    // Closed or unknown: Go still refreshes close tasks, which
                    // are already locked in; report unknown runs as not found.
                    if inner
                        .by_workflow
                        .get(&(domain.to_string(), execution.workflow_id.clone()))
                        .is_none_or(|runs| runs.is_empty())
                    {
                        return Err(WorkflowMutationError::NotFound(
                            EntityNotExistsError::new(
                                "Workflow execution does not exist.".to_string(),
                            ),
                        ));
                    }
                    return Ok(());
                }
            }
        } else {
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                execution.run_id.clone(),
            )
        };
        let record = inner.by_run.get(&key).ok_or_else(|| {
            WorkflowMutationError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Ok(());
        }
        // Requeue the unstarted decision task without duplicating entries.
        if record
            .pending_decision
            .as_ref()
            .is_some_and(|p| p.started_event_id.is_none())
        {
            let task_list = record.task_list.name.clone();
            let queue = inner
                .decision_queues
                .entry((domain.to_string(), task_list))
                .or_default();
            if !queue.iter().any(|(w, r)| {
                w == &execution.workflow_id && r == &key.2
            }) {
                queue.push_back((execution.workflow_id.clone(), key.2.clone()));
            }
        }
        // Requeue unstarted activities without duplicating entries.
        let pendings: Vec<(i64, String)> = inner
            .by_run
            .get(&key)
            .map(|record| {
                record
                    .pending_activities
                    .iter()
                    .filter(|(_, p)| p.started_event_id.is_none())
                    .map(|(id, p)| (*id, p.task_list.name.clone()))
                    .collect()
            })
            .unwrap_or_default();
        for (schedule_id, task_list) in pendings {
            let queue = inner
                .activity_queues
                .entry((domain.to_string(), task_list))
                .or_default();
            if !queue.iter().any(|(w, r, s)| {
                w == &execution.workflow_id && r == &key.2 && *s == schedule_id
            }) {
                queue.push_back((execution.workflow_id.clone(), key.2.clone(), schedule_id));
            }
        }
        Ok(())
    }

    /// Admin describe: shard placement plus the run's mutable state as
    /// JSON, mirroring Go admin `DescribeWorkflowExecution` (shard ID,
    /// history address, cached and stored state blobs).
    pub fn describe_admin(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<(i32, String, String), EntityNotExistsError> {
        let inner = self.inner.read().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = if execution.run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| {
                    EntityNotExistsError::new("Workflow execution does not exist.".to_string())
                })?;
            let latest = runs.last().ok_or_else(|| {
                EntityNotExistsError::new("Workflow execution does not exist.".to_string())
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
        let record = inner.by_run.get(&key).ok_or_else(|| {
            EntityNotExistsError::new("Workflow execution does not exist.".to_string())
        })?;
        let state_json = serde_json::to_string(record).unwrap_or_default();
        let hostname =
            std::env::var("HOSTNAME").unwrap_or_else(|_| "cadence-rust".to_string());
        Ok((0, hostname, state_json))
    }

    /// Read a raw event range for admin inspection, mirroring Go admin
    /// `GetWorkflowExecutionRawHistoryV2` minus version branches (single
    /// branch: versions are ignored and the full ID range is served with an
    /// opaque offset cursor).
    pub fn raw_history(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        start_event_id: Option<i64>,
        end_event_id: Option<i64>,
        page_size: usize,
        page_token: &[u8],
    ) -> Result<(Vec<HistoryEvent>, Vec<u8>), EntityNotExistsError> {
        let inner = self.inner.read().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = if execution.run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| {
                    EntityNotExistsError::new("Workflow execution does not exist.".to_string())
                })?;
            let latest = runs.last().ok_or_else(|| {
                EntityNotExistsError::new("Workflow execution does not exist.".to_string())
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
        let record = inner.by_run.get(&key).ok_or_else(|| {
            EntityNotExistsError::new("Workflow execution does not exist.".to_string())
        })?;
        let from = start_event_id.unwrap_or(1).max(1) as usize;
        let to = end_event_id.unwrap_or(i64::MAX).max(0) as usize;
        let offset: usize = if page_token.is_empty() {
            0
        } else {
            std::str::from_utf8(page_token)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0)
        };
        let events: Vec<HistoryEvent> = record
            .history
            .iter()
            .skip(from.saturating_sub(1))
            .take(to.saturating_sub(from.saturating_sub(1)))
            .skip(offset)
            .take(page_size.max(1))
            .cloned()
            .collect();
        let taken = offset + events.len();
        let remaining = record.history.len().saturating_sub(from.saturating_sub(1)).saturating_sub(taken);
        let next_page_token = if remaining > 0 {
            taken.to_string().into_bytes()
        } else {
            Vec::new()
        };
        Ok((events, next_page_token))
    }

    /// Inspect a run for history corruption, mirroring Go admin
    /// `MaintainCorruptWorkflow`'s query templates: a missing start event, a
    /// dangling activity reference, or a non-continuous event-ID sequence
    /// deletes the run and reports all-deleted; otherwise nothing is deleted.
    /// Uses Go's exact corruption messages.
    pub fn maintain_corrupt_workflow(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<(bool, bool, bool), WorkflowMutationError> {
        use cadence_api::enums::EventType;
        let corrupt = {
            let inner = self.inner.read().expect("execution store lock");
            let domain_key = self.domain_id(&inner, domain);
            let domain = domain_key.as_str();
            let key = if execution.run_id.is_empty() {
                match self.open_run_id(&inner, domain, &execution.workflow_id) {
                    Some(run_id) => (
                        domain.to_string(),
                        execution.workflow_id.clone(),
                        run_id,
                    ),
                    None => return Err(WorkflowMutationError::NotFound(
                        EntityNotExistsError::new(
                            "Workflow execution does not exist.".to_string(),
                        ),
                    )),
                }
            } else {
                (
                    domain.to_string(),
                    execution.workflow_id.clone(),
                    execution.run_id.clone(),
                )
            };
            let record = inner.by_run.get(&key).ok_or_else(|| {
                WorkflowMutationError::NotFound(EntityNotExistsError::new(
                    "Workflow execution does not exist.".to_string(),
                ))
            })?;
            // "unable to get workflow start event"
            let missing_start = record.history.first().is_none_or(|e| {
                e.event_type != Some(EventType::WorkflowExecutionStarted)
            });
            // "unable to get activity scheduled event": started/completed
            // activities referencing unknown schedule IDs.
            let scheduled: std::collections::HashSet<i64> = record
                .history
                .iter()
                .filter_map(|e| {
                    e.activity_task_scheduled_event_attributes
                        .as_ref()
                        .map(|_| e.event_id)
                })
                .collect();
            let dangling_activity = record.history.iter().any(|e| {
                let started = e
                    .activity_task_started_event_attributes
                    .as_ref()
                    .map(|a| a.scheduled_event_id);
                let completed = |id: Option<i64>| id.is_some_and(|id| !scheduled.contains(&id));
                started.is_some_and(|id| !scheduled.contains(&id))
                    || e.activity_task_completed_event_attributes
                        .as_ref()
                        .is_some_and(|a| completed(Some(a.scheduled_event_id)))
                    || e.activity_task_failed_event_attributes
                        .as_ref()
                        .is_some_and(|a| completed(Some(a.scheduled_event_id)))
                    || e.activity_task_timed_out_event_attributes
                        .as_ref()
                        .is_some_and(|a| completed(Some(a.scheduled_event_id)))
            });
            // "corrupted history event batch, eventID is not continuous"
            let discontinuous = record
                .history
                .iter()
                .enumerate()
                .any(|(i, e)| e.event_id != i as i64 + 1);
            if missing_start {
                Some("unable to get workflow start event")
            } else if dangling_activity {
                Some("unable to get activity scheduled event")
            } else if discontinuous {
                Some("corrupted history event batch, eventID is not continuous")
            } else {
                None
            }
        };
        if corrupt.is_none() {
            return Ok((false, false, false));
        }
        let (history, executions, visibility) = self.delete_run(domain, execution)?;
        Ok((history, executions, visibility))
    }

    /// Queue depths for admin `DescribeQueue` states.
    pub fn queue_depths(&self) -> (usize, usize, usize, usize, usize) {
        let inner = self.inner.read().expect("execution store lock");
        let decisions: usize = inner.decision_queues.values().map(|q| q.len()).sum();
        let activities: usize = inner.activity_queues.values().map(|q| q.len()).sum();
        let timers: usize = inner.by_run.values().map(|r| r.pending_timers.len()).sum();
        let backoffs: usize = inner
            .by_run
            .values()
            .filter(|r| r.first_decision_due_millis.is_some())
            .count();
        let schedules = inner.schedules.len();
        (decisions, activities, timers, backoffs, schedules)
    }

    /// Reapply signal events to the current open run, mirroring Go history
    /// `ReapplyEvents` for the single-branch case: same-version filtering and
    /// version branches do not exist here, so every signal not already
    /// present (by request ID) is appended and a decision is scheduled.
    /// Closed runs are reset to their last decision boundary first, like Go's
    /// reset-to-accept path, then receive the signals.
    pub fn reapply_signals(
        &self,
        domain: &str,
        workflow_id: &str,
        signals: Vec<HistoryEvent>,
    ) -> Result<(), WorkflowMutationError> {
        use cadence_api::enums::EventType;
        // Resolve the open run; reset a closed latest run to accept events.
        let open_key = {
            let inner = self.inner.read().expect("execution store lock");
            let domain_key = self.domain_id(&inner, domain);
            let domain = domain_key.as_str();
            match self.open_run_id(&inner, domain, workflow_id) {
                Some(run_id) => (
                    domain.to_string(),
                    workflow_id.to_string(),
                    run_id,
                ),
                None => {
                    // Mirror Go: reset the finished run to its last decision
                    // boundary so it can accept the reapplied signals.
                    let runs = inner
                        .by_workflow
                        .get(&(domain.to_string(), workflow_id.to_string()))
                        .ok_or_else(|| {
                            WorkflowMutationError::NotFound(EntityNotExistsError::new(
                                "Workflow execution does not exist.".to_string(),
                            ))
                        })?;
                    let base_run = runs.last().ok_or_else(|| {
                        WorkflowMutationError::NotFound(EntityNotExistsError::new(
                            "Workflow execution does not exist.".to_string(),
                        ))
                    })?.clone();
                    let boundary = inner
                        .by_run
                        .get(&(
                            domain.to_string(),
                            workflow_id.to_string(),
                            base_run.clone(),
                        ))
                        .and_then(|record| {
                            record.history.iter().rev().find(|e| {
                                matches!(
                                    e.event_type,
                                    Some(
                                        EventType::DecisionTaskCompleted
                                            | EventType::DecisionTaskStarted
                                            | EventType::DecisionTaskFailed
                                            | EventType::DecisionTaskTimedOut
                                    )
                                )
                            }).map(|e| e.event_id)
                        });
                    match boundary {
                        Some(id) => {
                            drop(inner);
                            let reset = self.reset(
                                &cadence_api::types::ResetWorkflowExecutionRequest {
                                    domain: domain.to_string(),
                                    workflow_execution: Some(WorkflowExecution::new(
                                        workflow_id.to_string(),
                                        base_run,
                                    )),
                                    reason: "reapply events".to_string(),
                                    decision_finish_event_id: id,
                                    request_id: uuid::Uuid::new_v4().to_string(),
                                    skip_signal_reapply: true,
                                },
                            ).map_err(|e| match e {
                                crate::ResetError::NotFound(inner) => {
                                    WorkflowMutationError::NotFound(inner)
                                }
                                crate::ResetError::Invalid(message) => {
                                    WorkflowMutationError::NotFound(EntityNotExistsError::new(
                                        message,
                                    ))
                                }
                            })?;
                            (
                                domain.to_string(),
                                workflow_id.to_string(),
                                reset.run_id,
                            )
                        }
                        // No decision boundary (e.g. never scheduled): Go
                        // skips reapply as a no-op in this case.
                        None => return Ok(()),
                    }
                }
            }
        };
        let mut inner = self.inner.write().expect("execution store lock");
        let record = inner.by_run.get_mut(&open_key).ok_or_else(|| {
            WorkflowMutationError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        for signal in signals {
            let Some(attrs) = signal.workflow_execution_signaled_event_attributes.clone() else {
                continue;
            };
            let id = attrs.request_id.clone();
            if !id.is_empty() && record.signal_request_ids.contains(&id) {
                continue;
            }
            record.push_event(
                EventType::WorkflowExecutionSignaled,
                HistoryEvent {
                    workflow_execution_signaled_event_attributes: Some(attrs),
                    ..Default::default()
                },
            );
        }
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(
            &mut inner,
            &open_key.0,
            &open_key.1,
            &open_key.2,
            &task_list,
            0,
        );
        Ok(())
    }

    /// A snapshot of user-visible mutable state, mirroring Go history
    /// `GetMutableState` fields including the durable current branch/version history.
    pub fn mutable_state_view(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<MutableStateView, EntityNotExistsError> {
        let inner = self.inner.read().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = if execution.run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| {
                    EntityNotExistsError::new("Workflow execution does not exist.".to_string())
                })?;
            let latest = runs.last().ok_or_else(|| {
                EntityNotExistsError::new("Workflow execution does not exist.".to_string())
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
        let record = inner.by_run.get(&key).ok_or_else(|| {
            EntityNotExistsError::new("Workflow execution does not exist.".to_string())
        })?;
        Ok(MutableStateView {
            workflow_id: record.execution.workflow_id.clone(),
            run_id: record.execution.run_id.clone(),
            workflow_type_name: record.workflow_type.name.clone(),
            next_event_id: record.history.len() as i64 + 1,
            previous_started_event_id: record.prev_started_event_id,
            task_list_name: record.task_list.name.clone(),
            sticky_task_list_name: record.sticky_task_list.clone().unwrap_or_default(),
            sticky_schedule_to_start_timeout_seconds: record
                .sticky_schedule_to_start_timeout_seconds,
            close_status: record.close_status,
            version_histories: record.version_histories.clone(),
            history_size_bytes: serde_json::to_string(&record.history)
                .map(|s| s.len() as i64)
                .unwrap_or(0),
        })
    }

    /// Record a decision task as started without queue involvement,
    /// mirroring Go's decision handler for history `RecordDecisionTaskStarted`:
    /// unknown schedules are `EntityNotExists`, re-records with the same
    /// request ID return current metadata, and a different request ID on an
    /// already-started task is an already-started error.
    pub fn record_decision_started(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        schedule_id: i64,
        request_id: &str,
        identity: &str,
    ) -> Result<RecordedDecision, RecordError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = (
            domain.to_string(),
            execution.workflow_id.clone(),
            execution.run_id.clone(),
        );
        let record = inner.by_run.get_mut(&key).ok_or_else(|| {
            RecordError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Err(RecordError::NotFound(EntityNotExistsError::new(
                "Decision task not found.".to_string(),
            )));
        }
        let mut pending = record.pending_decision.clone().ok_or_else(|| {
            RecordError::NotFound(EntityNotExistsError::new(
                "Decision task not found.".to_string(),
            ))
        })?;
        if pending.scheduled_event_id != schedule_id {
            return Err(RecordError::NotFound(EntityNotExistsError::new(
                "Decision task not found.".to_string(),
            )));
        }
        if let Some(started) = pending.started_event_id {
            if pending.request_id == request_id {
                return Ok(RecordedDecision::from_pending(
                    record,
                    &pending,
                    started,
                ));
            }
            return Err(RecordError::AlreadyStarted(
                "Decision task already started.".to_string(),
            ));
        }
        pending.started_time_millis = Some(now_millis());
        pending.identity = identity.to_string();
        pending.request_id = request_id.to_string();
        let started_event_id = record.push_event(
            EventType::DecisionTaskStarted,
            HistoryEvent {
                decision_task_started_event_attributes: Some(
                    DecisionTaskStartedEventAttributes {
                        scheduled_event_id: pending.scheduled_event_id,
                        identity: pending.identity.clone(),
                        request_id: pending.request_id.clone(),
                    },
                ),
                ..Default::default()
            },
        );
        pending.started_event_id = Some(started_event_id);
        record.pending_decision = Some(pending.clone());
        Ok(RecordedDecision::from_pending(
            record,
            &pending,
            started_event_id,
        ))
    }

    /// Record a child execution as completed on its parent, mirroring Go
    /// history `RecordChildExecutionCompleted`: the child must be pending
    /// under the initiated ID with a matching started workflow ID, otherwise
    /// `EntityNotExists`; stale IDs past the next event ID are stale-state
    /// errors. The completion event type selects the parent event appended.
    pub fn record_child_completed(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        initiated_id: i64,
        completed_execution: &WorkflowExecution,
        completion_event: &HistoryEvent,
    ) -> Result<(), RecordError> {
        use cadence_api::enums::EventType;
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = (
            domain.to_string(),
            execution.workflow_id.clone(),
            execution.run_id.clone(),
        );
        let next_event_id = inner
            .by_run
            .get(&key)
            .map(|r| r.history.len() as i64 + 1)
            .unwrap_or(1);
        let (is_open, started_event_id) = {
            let record = inner.by_run.get(&key).ok_or_else(|| {
                RecordError::NotFound(EntityNotExistsError::new(
                    "Workflow execution does not exist.".to_string(),
                ))
            })?;
            (
                record.is_open(),
                record
                    .history
                    .iter()
                    .filter_map(|e| {
                        e.child_workflow_execution_started_event_attributes
                            .as_ref()
                            .filter(|a| a.initiated_event_id == initiated_id)
                            .map(|_| e.event_id)
                    })
                    .next()
                    .unwrap_or(0),
            )
        };
        if !is_open {
            return Err(RecordError::NotFound(EntityNotExistsError::new(
                "Pending child execution not found.".to_string(),
            )));
        }
        let kids = inner.children.get(&key).cloned().unwrap_or_default();
        let child = kids.iter().find(|c| c.initiated_event_id == initiated_id);
        let Some(child) = child.cloned() else {
            if initiated_id >= next_event_id {
                return Err(RecordError::Stale(
                    "Encountered stale mutable state.".to_string(),
                ));
            }
            return Err(RecordError::NotFound(EntityNotExistsError::new(
                "Pending child execution not found.".to_string(),
            )));
        };
        if child.workflow_id != completed_execution.workflow_id {
            return Err(RecordError::NotFound(EntityNotExistsError::new(
                "Pending child execution workflowID mismatch.".to_string(),
            )));
        }
        // Started event ID for the parent event: the actual
        // ChildWorkflowExecutionStarted event when present.
        if started_event_id == 0 {
            return Err(RecordError::Stale(
                "Encountered stale mutable state.".to_string(),
            ));
        }
        let event_type = completion_event.event_type.unwrap_or(EventType::WorkflowExecutionCompleted);
        let mut event = completion_event.clone();
        // Stamp the parent-link IDs like Go's AddChildWorkflowExecution*
        // event constructors do.
        match &mut event {
            HistoryEvent {
                child_workflow_execution_completed_event_attributes: Some(attrs),
                ..
            } => {
                attrs.initiated_event_id = initiated_id;
                attrs.started_event_id = started_event_id;
            }
            HistoryEvent {
                child_workflow_execution_failed_event_attributes: Some(attrs),
                ..
            } => {
                attrs.initiated_event_id = initiated_id;
                attrs.started_event_id = started_event_id;
            }
            HistoryEvent {
                child_workflow_execution_canceled_event_attributes: Some(attrs),
                ..
            } => {
                attrs.initiated_event_id = initiated_id;
                attrs.started_event_id = started_event_id;
            }
            HistoryEvent {
                child_workflow_execution_terminated_event_attributes: Some(attrs),
                ..
            } => {
                attrs.initiated_event_id = initiated_id;
                attrs.started_event_id = started_event_id;
            }
            HistoryEvent {
                child_workflow_execution_timed_out_event_attributes: Some(attrs),
                ..
            } => {
                attrs.initiated_event_id = initiated_id;
                attrs.started_event_id = started_event_id;
            }
            _ => {
                return Err(RecordError::Invalid(
                    "Completion event must be a child workflow terminal event.".to_string(),
                ));
            }
        }
        let record = inner.by_run.get_mut(&key).expect("record exists");
        record.push_event(event_type, event);
        if let Some(kids) = inner.children.get_mut(&key) {
            kids.retain(|c| c.initiated_event_id != initiated_id);
        }
        Ok(())
    }

    /// Record an activity task as started without queue involvement,
    /// mirroring Go history `RecordActivityTaskStarted` outcomes: unknown
    /// schedules are `EntityNotExists`; already-started tasks return current
    /// metadata.
    pub fn record_activity_started(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        schedule_id: i64,
        identity: &str,
    ) -> Result<RecordedActivity, RecordError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = (
            domain.to_string(),
            execution.workflow_id.clone(),
            execution.run_id.clone(),
        );
        let record = inner.by_run.get_mut(&key).ok_or_else(|| {
            RecordError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Err(RecordError::NotFound(EntityNotExistsError::new(
                "Activity task not found.".to_string(),
            )));
        }
        let pending = record.pending_activities.get(&schedule_id).cloned().ok_or_else(|| {
            RecordError::NotFound(EntityNotExistsError::new(
                "Activity task not found.".to_string(),
            ))
        })?;
        if let Some(started) = pending.started_event_id {
            return Ok(RecordedActivity::from_pending(record, &pending, started));
        }
        let started_time = now_millis();
        let request_id = uuid::Uuid::new_v4().to_string();
        let started_event_id = record.push_event(
            EventType::ActivityTaskStarted,
            HistoryEvent {
                activity_task_started_event_attributes: Some(
                    ActivityTaskStartedEventAttributes {
                        scheduled_event_id: schedule_id,
                        identity: identity.to_string(),
                        request_id: request_id.clone(),
                        attempt: pending.attempt,
                        last_failure_reason: pending.last_failure_reason.clone(),
                        last_failure_details: pending.last_failure_details.clone(),
                        last_failure_options: pending.last_failure_options.clone(),
                    },
                ),
                ..Default::default()
            },
        );
        if let Some(p) = record.pending_activities.get_mut(&schedule_id) {
            p.started_event_id = Some(started_event_id);
            p.started_time_millis = Some(started_time);
            p.started_identity = identity.to_string();
        }
        let pending = record.pending_activities.get(&schedule_id).cloned().expect("pending exists");
        Ok(RecordedActivity::from_pending(record, &pending, started_event_id))
    }

    /// Schedule a decision task on an open run without duplicating an
    /// outstanding one, mirroring Go's decision handler for history
    /// `ScheduleDecisionTask`.
    pub fn schedule_decision_task(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<(), WorkflowMutationError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = if execution.run_id.is_empty() {
            match self.open_run_id(&inner, domain, &execution.workflow_id) {
                Some(run_id) => (
                    domain.to_string(),
                    execution.workflow_id.clone(),
                    run_id,
                ),
                None => {
                    return Err(WorkflowMutationError::NotFound(EntityNotExistsError::new(
                        "Workflow execution does not exist.".to_string(),
                    )));
                }
            }
        } else {
            (
                domain.to_string(),
                execution.workflow_id.clone(),
                execution.run_id.clone(),
            )
        };
        let record = inner.by_run.get(&key).ok_or_else(|| {
            WorkflowMutationError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Err(WorkflowMutationError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError::new(
                    "Workflow execution already completed.".to_string(),
                ),
            ));
        }
        if record.pending_decision.is_some() {
            return Ok(());
        }
        let task_list = record.task_list.name.clone();
        Self::schedule_decision_locked(&mut inner, domain, &key.1, &key.2, &task_list, 0);
        Ok(())
    }

    /// Delete a buffered signal request ID, mirroring Go history
    /// `RemoveSignalMutableState` (`DeleteSignalRequested`). Closed runs
    /// report not-exists like Go's `ErrNotExists`.
    pub fn remove_signal(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        request_id: &str,
    ) -> Result<(), WorkflowMutationError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = (
            domain.to_string(),
            execution.workflow_id.clone(),
            execution.run_id.clone(),
        );
        let record = inner.by_run.get_mut(&key).ok_or_else(|| {
            WorkflowMutationError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Err(WorkflowMutationError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            )));
        }
        record.signal_request_ids.remove(request_id);
        Ok(())
    }

    /// Apply a replicated activity heartbeat/details update, mirroring Go NDC
    /// `SyncActivity` minus version gating (single branch: versions do not
    /// exist). Unknown runs or activities are a success no-op like Go's
    /// out-of-order path.
    pub fn sync_activity(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        scheduled_id: i64,
        details: Vec<u8>,
        attempt: i32,
    ) {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let key = (
            domain_key,
            execution.workflow_id.clone(),
            execution.run_id.clone(),
        );
        if let Some(record) = inner.by_run.get_mut(&key) {
            if let Some(pending) = record.pending_activities.get_mut(&scheduled_id) {
                pending.heartbeat_details = details;
                pending.attempt = attempt;
            }
        }
    }

    /// Delete a workflow run's data, mirroring Go admin `DeleteWorkflow`
    /// (history + execution + visibility stores are one store here, so all
    /// three report deleted). Unknown runs are `EntityNotExists`, like Go's
    /// describe-first path.
    pub fn delete_run(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
    ) -> Result<(bool, bool, bool), WorkflowMutationError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let run_id = if execution.run_id.is_empty() {
            self.open_run_id(&inner, domain, &execution.workflow_id)
                .ok_or_else(|| {
                    WorkflowMutationError::NotFound(EntityNotExistsError::new(
                        "Workflow execution does not exist.".to_string(),
                    ))
                })?
        } else {
            execution.run_id.clone()
        };
        let key = (
            domain.to_string(),
            execution.workflow_id.clone(),
            run_id.clone(),
        );
        if !inner.by_run.contains_key(&key) {
            return Err(WorkflowMutationError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            )));
        }
        inner.by_run.remove(&key);
        if let Some(runs) = inner
            .by_workflow
            .get_mut(&(domain.to_string(), execution.workflow_id.clone()))
        {
            runs.retain(|r| r != &run_id);
        }
        inner
            .by_request_id
            .retain(|_, r| r != &run_id);
        for queue in inner.decision_queues.values_mut() {
            queue.retain(|(w, r)| {
                w != &execution.workflow_id || r != &run_id
            });
        }
        for queue in inner.activity_queues.values_mut() {
            queue.retain(|(w, r, _)| {
                w != &execution.workflow_id || r != &run_id
            });
        }
        inner.children.remove(&key);
        Ok((true, true, true))
    }

    pub fn terminate(
        &self,
        req: &TerminateWorkflowExecutionRequest,
    ) -> Result<(), WorkflowMutationError> {
        let execution = req.workflow_execution.clone().unwrap_or_default();
        let mut inner = self.inner.write().expect("execution store lock");
        // Resolve the open run first (get_open_mut validates openness).
        let (domain_id, workflow_id, run_id) = {
            let record = self.get_open_mut(&mut inner, &req.domain, &execution)?;
            (
                record.domain.clone(),
                record.execution.workflow_id.clone(),
                record.execution.run_id.clone(),
            )
        };
        Self::close_run_locked(
            &mut inner,
            &domain_id,
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
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let missing =
            || EntityNotExistsError::new("Workflow execution does not exist.".to_string());
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
                            domain: inner.domain_name(&c.domain),
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
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = if execution.run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| {
                    EntityNotExistsError::new("Workflow execution does not exist.".to_string())
                })?;
            let latest = runs.last().ok_or_else(|| {
                EntityNotExistsError::new("Workflow execution does not exist.".to_string())
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
            .ok_or_else(|| {
                EntityNotExistsError::new("Workflow execution does not exist.".to_string())
            })
    }

    /// List executions for a domain; `open_only=true` lists running workflows.
    #[must_use]
    pub fn list(&self, domain: &str, open_only: bool) -> Vec<WorkflowExecutionInfo> {
        let inner = self.inner.read().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let mut out: Vec<WorkflowExecutionInfo> = inner
            .by_run
            .values()
            .filter(|r| r.domain == domain && r.is_open() == open_only)
            .map(ExecutionRecord::to_info)
            .collect();
        out.sort_by_key(|i| i.start_time.unwrap_or(0));
        out
    }

    pub fn query_visible(
        &self,
        domain: &str,
        query: &str,
    ) -> Result<Vec<WorkflowExecutionInfo>, BadRequestError> {
        let mut rows = self.list(domain, true);
        rows.extend(self.list(domain, false));
        crate::visibility::apply(&mut rows, query)?;
        Ok(rows)
    }
    pub fn visibility_page(
        &self,
        request: &cadence_api::service_types::ListWorkflowExecutionsRequest,
    ) -> Result<cadence_api::service_types::ListWorkflowExecutionsResponse, BadRequestError> {
        let rows = self.query_visible(&request.domain, &request.query)?;
        crate::visibility::page(rows, request.page_size, &request.next_page_token)
    }

    // -----------------------------------------------------------------------
    // Matching: decision tasks
    // -----------------------------------------------------------------------

    fn encode_task_token(token: &TaskToken) -> Vec<u8> {
        JsonTaskTokenSerializer::serialize(token).unwrap_or_default()
    }

    /// Resolve the same pending activity used by token-based APIs. Cadence's
    /// ByID APIs target the current open run when no run ID is supplied.
    pub fn activity_token_by_id(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        activity_id: &str,
    ) -> Result<Vec<u8>, TaskError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let record = self
            .get_open_mut(&mut inner, domain, execution)
            .map_err(|e| match e {
                WorkflowMutationError::NotFound(e) => TaskError::NotFound(e),
                WorkflowMutationError::AlreadyCompleted(e) => TaskError::AlreadyCompleted(e),
            })?;
        let pending = record
            .pending_activities
            .values()
            .find(|p| p.activity_id == activity_id)
            .ok_or_else(|| {
                TaskError::NotFound(EntityNotExistsError::new("Activity task not found."))
            })?;
        let token = TaskToken {
            domain_id: domain.to_string(),
            workflow_id: record.execution.workflow_id.clone(),
            run_id: record.execution.run_id.clone(),
            workflow_type: record.workflow_type.name.clone(),
            schedule_id: pending.scheduled_event_id,
            schedule_attempt: i64::from(pending.attempt),
            activity_id: pending.activity_id.clone(),
            activity_type: pending.activity_type.name.clone(),
        };
        Ok(Self::encode_task_token(&token))
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
        let domain_key = self.domain_id(&inner, domain);
        let key = (domain_key.clone(), task_list.to_owned());
        loop {
            let (workflow_id, run_id) = inner.decision_queues.get_mut(&key)?.pop_front()?;
            if !inner
                .by_run
                .get(&(domain_key.clone(), workflow_id.clone(), run_id.clone()))
                .is_some_and(|record| {
                    record.pending_decision.as_ref().is_some_and(|task| {
                        task.task_list_name
                            .as_deref()
                            .unwrap_or(&record.task_list.name)
                            == task_list
                    })
                })
            {
                continue;
            }
            let backlog = inner
                .decision_queues
                .get(&key)
                .map_or(0, |queue| queue.len());
            if let Some(task) = Self::start_decision_locked(
                &mut inner,
                &domain_key,
                workflow_id,
                run_id,
                identity,
                backlog,
                false,
            ) {
                return Some(task);
            }
        }
    }

    fn start_decision_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: String,
        run_id: String,
        identity: &str,
        backlog: usize,
        inline: bool,
    ) -> Option<PollForDecisionTaskResponse> {
        let (pending, started_event_id) = {
            let record =
                inner
                    .by_run
                    .get_mut(&(domain.to_owned(), workflow_id.clone(), run_id.clone()))?;
            let mut pending = record.pending_decision.clone()?;
            if !record.is_open() || pending.started_event_id.is_some() {
                return None;
            }
            if pending.attempt > 0 && pending.scheduled_event_id != record.next_event_id() {
                pending.attempt = 0;
                pending.scheduled_event_id = record.push_event(
                    EventType::DecisionTaskScheduled,
                    HistoryEvent {
                        decision_task_scheduled_event_attributes: Some(
                            DecisionTaskScheduledEventAttributes {
                                task_list: Some(record.task_list.clone()),
                                start_to_close_timeout_seconds: record
                                    .task_start_to_close_timeout_seconds,
                                attempt: 0,
                            },
                        ),
                        ..Default::default()
                    },
                );
                pending.scheduled_time_millis =
                    record.history.last().and_then(|event| event.timestamp);
            }
            pending.started_time_millis = Some(now_millis());
            record.last_updated_millis = now_millis();
            pending.identity = identity.to_owned();
            pending.request_id = if inline {
                "request-from-RespondDecisionTaskCompleted".into()
            } else {
                uuid::Uuid::new_v4().to_string()
            };
            let started_event_id = if pending.attempt > 0 {
                pending.scheduled_event_id + 1
            } else {
                record.push_event(
                    EventType::DecisionTaskStarted,
                    HistoryEvent {
                        decision_task_started_event_attributes: Some(
                            DecisionTaskStartedEventAttributes {
                                scheduled_event_id: pending.scheduled_event_id,
                                identity: pending.identity.clone(),
                                request_id: pending.request_id.clone(),
                            },
                        ),
                        ..Default::default()
                    },
                )
            };
            if pending.attempt == 0 {
                pending.started_time_millis =
                    record.history.last().and_then(|event| event.timestamp);
            }
            pending.started_event_id = Some(started_event_id);
            record.pending_decision = Some(pending.clone());
            (pending, started_event_id)
        };
        let history_start = if inline
            || inner.by_run[&(domain.to_owned(), workflow_id.clone(), run_id.clone())]
                .sticky_task_list
                .as_ref()
                .is_some_and(|name| !name.is_empty())
        {
            inner.by_run[&(domain.to_owned(), workflow_id.clone(), run_id.clone())]
                .prev_started_event_id
                .unwrap_or(0)
                .max(0) as usize
        } else {
            0
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
                record
                    .history
                    .iter()
                    .skip(history_start)
                    .take(HISTORY_PAGE_SIZE)
                    .cloned()
                    .collect::<Vec<_>>(),
                record.history.len(),
                pending.scheduled_time_millis,
                pending.started_time_millis,
                Self::head_query(record),
                Self::head_query_id(record),
                Self::all_queries(record),
            )
        };
        let transient_events = inner.by_run
            [&(domain.to_owned(), workflow_id.clone(), run_id.clone())]
            .transient_decision_events();
        let (
            workflow_type,
            task_list,
            prev_started,
            mut history,
            history_len,
            scheduled_timestamp,
            started_timestamp,
            head_query,
            head_query_id,
            queries,
        ) = response_data;
        let page_end = history_start
            .saturating_add(HISTORY_PAGE_SIZE)
            .min(history_len);
        if page_end == history_len {
            history.extend(transient_events.clone());
        }
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
        Some(PollForDecisionTaskResponse {
            task_token: Self::encode_task_token(&token),
            workflow_execution: Some(WorkflowExecution::new(workflow_id.clone(), run_id.clone())),
            workflow_type: Some(workflow_type),
            previous_started_event_id: prev_started,
            started_event_id,
            attempt: pending.attempt,
            backlog_count_hint: backlog as i64,
            history: Some(History { events: history }),
            next_page_token: if page_end < history_len {
                encode_history_cursor(
                    domain,
                    &WorkflowExecution::new(&workflow_id, &run_id),
                    page_end,
                    history_len,
                    false,
                    &transient_events,
                )
            } else {
                Vec::new()
            },
            next_event_id: if pending.attempt > 0 {
                pending.scheduled_event_id
            } else {
                started_event_id + 1
            },
            scheduled_timestamp,
            started_timestamp,
            workflow_execution_task_list: Some(task_list),
            query: head_query,
            query_id: head_query_id,
            queries,
        })
    }

    fn inline_decision_response_locked(
        inner: &mut Inner,
        key: &(String, String, String),
        task_list: &str,
        request: &RespondDecisionTaskCompletedRequest,
    ) -> RespondDecisionTaskCompletedResponse {
        if !request.return_new_decision_task
            || !inner.by_run.get(key).is_some_and(|record| {
                record.is_open()
                    && record
                        .pending_decision
                        .as_ref()
                        .is_some_and(|task| task.started_event_id.is_none())
            })
        {
            return RespondDecisionTaskCompletedResponse::default();
        }
        // Go skips transfer when it starts an inline task. Remove only this
        // execution's queue entries; another workflow must still be polled.
        let queue_name = inner.by_run[key]
            .pending_decision
            .as_ref()
            .and_then(|task| task.task_list_name.clone())
            .unwrap_or_else(|| task_list.to_owned());
        if let Some(queue) = inner.decision_queues.get_mut(&(key.0.clone(), queue_name)) {
            queue.retain(|(workflow, run)| (workflow, run) != (&key.1, &key.2));
        }
        RespondDecisionTaskCompletedResponse {
            decision_task: Self::start_decision_locked(
                inner,
                &key.0,
                key.1.clone(),
                key.2.clone(),
                &request.identity,
                0,
                true,
            ),
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
        let token = InMemoryExecutionStore::decode_task_token(&req.task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let record = inner.by_run.get(&record_key).ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Err(TaskError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError {
                    message: "Workflow execution already completed.".to_string(),
                },
            ));
        }
        let pending = record.pending_decision.clone().ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new("Decision task not found."))
        })?;
        if pending.started_event_id.is_none()
            || pending.scheduled_event_id != token.schedule_id
            || pending.attempt != token.schedule_attempt
        {
            return Err(TaskError::NotFound(EntityNotExistsError::new(
                "Decision task not found.",
            )));
        }
        Self::validate_decisions(&inner, &record_key, &req.decisions)?;

        let first_close = req.decisions.iter().position(|decision| {
            matches!(
                decision.decision_type,
                Some(
                    DecisionType::CompleteWorkflowExecution
                        | DecisionType::FailWorkflowExecution
                        | DecisionType::CancelWorkflowExecution
                        | DecisionType::ContinueAsNewWorkflowExecution
                )
            )
        });
        let canceled_before_close: std::collections::HashSet<_> = req
            .decisions
            .iter()
            .take(first_close.unwrap_or(req.decisions.len()))
            .filter(|decision| decision.decision_type == Some(DecisionType::CancelTimer))
            .filter_map(|decision| {
                decision
                    .cancel_timer_decision_attributes
                    .as_ref()
                    .map(|attrs| attrs.timer_id.as_str())
            })
            .collect();
        let has_unhandled_before_close =
            inner.by_run[&record_key]
                .buffered_events
                .iter()
                .any(|event| {
                    !event
                        .timer_fired_event_attributes
                        .as_ref()
                        .is_some_and(|fired| {
                            canceled_before_close.contains(fired.timer_id.as_str())
                        })
                });
        if has_unhandled_before_close
            && req.decisions.iter().any(|decision| {
                matches!(
                    decision.decision_type,
                    Some(
                        DecisionType::CompleteWorkflowExecution
                            | DecisionType::FailWorkflowExecution
                            | DecisionType::CancelWorkflowExecution
                            | DecisionType::ContinueAsNewWorkflowExecution
                    )
                )
            })
        {
            let record = inner.by_run.get_mut(&record_key).unwrap();
            if pending.attempt == 0 {
                record.push_event(
                    EventType::DecisionTaskFailed,
                    HistoryEvent {
                        decision_task_failed_event_attributes: Some(
                            cadence_api::tasks::DecisionTaskFailedEventAttributes {
                                scheduled_event_id: pending.scheduled_event_id,
                                started_event_id: pending.started_event_id.unwrap(),
                                cause: Some(DecisionTaskFailedCause::UnhandledDecision),
                                identity: req.identity.clone(),
                                ..Default::default()
                            },
                        ),
                        ..Default::default()
                    },
                );
            }
            record.clear_stickiness();
            record.pending_decision = None;
            let task_list = record.task_list.name.clone();
            Self::flush_buffered_events_locked(&mut inner, &record_key);
            Self::schedule_decision_locked(
                &mut inner,
                &token.domain_id,
                &token.workflow_id,
                &token.run_id,
                &task_list,
                0,
            );
            return Ok(Self::inline_decision_response_locked(
                &mut inner,
                &record_key,
                &task_list,
                req,
            ));
        }
        // All valid: record completion.
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        record.materialize_transient_decision(&req.identity);
        if let Some(attributes) = req
            .sticky_attributes
            .as_ref()
            .filter(|attributes| attributes.worker_task_list.is_some())
        {
            record.sticky_task_list = attributes
                .worker_task_list
                .as_ref()
                .map(|task_list| task_list.name.clone());
            record.sticky_schedule_to_start_timeout_seconds =
                attributes.schedule_to_start_timeout_seconds.unwrap_or(0);
        } else {
            record.clear_stickiness();
        }

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
        Self::apply_decisions_locked(
            &mut inner,
            &domain,
            &workflow_id,
            &run_id,
            &req.decisions,
            completed_event_id,
            &req.identity,
        )?;
        let has_new_events = !inner.by_run[&record_key].buffered_events.is_empty();
        Self::flush_buffered_events_locked(&mut inner, &record_key);
        // Go creates another decision only for unhandled external events,
        // an explicit heartbeat/force request, or an immediate asynchronous
        // result produced by the local child/external-workflow engine.
        let has_immediate_result = inner.by_run[&record_key]
            .history
            .iter()
            .filter(|event| event.event_id > completed_event_id)
            .any(|event| {
                matches!(
                    event.event_type,
                    Some(
                        EventType::ChildWorkflowExecutionStarted
                            | EventType::StartChildWorkflowExecutionFailed
                            | EventType::ExternalWorkflowExecutionSignaled
                            | EventType::SignalExternalWorkflowExecutionFailed
                            | EventType::ExternalWorkflowExecutionCancelRequested
                            | EventType::RequestCancelExternalWorkflowExecutionFailed
                            | EventType::ActivityTaskCanceled
                    )
                )
            });
        if (has_new_events || has_immediate_result || req.force_create_new_decision_task)
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
        inner
            .by_run
            .get_mut(&record_key)
            .unwrap()
            .last_updated_millis = now_millis();
        Ok(Self::inline_decision_response_locked(
            &mut inner,
            &record_key,
            &task_list,
            req,
        ))
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
                    cadence_api::validation::validate_retry_policy(a.retry_policy.as_ref())
                        .map_err(TaskError::BadDecision)?;
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
                    cadence_api::validation::validate_retry_policy(c.retry_policy.as_ref())
                        .map_err(TaskError::BadDecision)?;
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
        let domain_name = inner.domain_name(domain);
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
                                    domain_name.clone()
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
                            schedule_to_start_timeout_seconds: a.schedule_to_start_timeout_seconds,
                            schedule_to_close_timeout_seconds: a.schedule_to_close_timeout_seconds,
                            start_to_close_timeout_seconds: a.start_to_close_timeout_seconds,
                            heartbeat_timeout_seconds: a.heartbeat_timeout_seconds,
                            scheduled_time_millis: now_millis(),
                            attempt_scheduled_time_millis: None,
                            started_time_millis: None,
                            last_heartbeat_time_millis: None,
                            started_identity: String::new(),
                            started_request_id: String::new(),
                            last_failure_reason: None,
                            last_failure_details: Vec::new(),
                            last_failure_options: None,
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
                    if Self::retry_or_cron_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        now_millis(),
                        completed_event_id,
                        None,
                        &[],
                        Some(&result),
                    ) {
                        continue;
                    }
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
                    if record.history.iter().any(|event| {
                        event.event_type == Some(EventType::WorkflowExecutionCancelRequested)
                    }) {
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
                                        details,
                                        decision_task_completed_event_id: completed_event_id,
                                    },
                                ),
                                ..Default::default()
                            },
                            EventType::WorkflowExecutionCanceled,
                        );
                        continue;
                    }
                    if Self::retry_or_cron_locked(
                        inner,
                        domain,
                        workflow_id,
                        run_id,
                        now_millis(),
                        completed_event_id,
                        reason.as_deref(),
                        &details,
                        None,
                    ) {
                        continue;
                    }
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
                    let attrs = d
                        .cancel_timer_decision_attributes
                        .as_ref()
                        .expect("validated");
                    let started_event_id = record
                        .pending_timers
                        .remove(&attrs.timer_id)
                        .map(|timer| timer.started_event_id)
                        .or_else(|| {
                            let index = record.buffered_events.iter().position(|event| {
                                event
                                    .timer_fired_event_attributes
                                    .as_ref()
                                    .is_some_and(|fired| fired.timer_id == attrs.timer_id)
                            })?;
                            record
                                .buffered_events
                                .remove(index)
                                .timer_fired_event_attributes
                                .map(|fired| fired.started_event_id)
                        });
                    if let Some(started_event_id) = started_event_id {
                        record.push_event(
                            EventType::TimerCanceled,
                            HistoryEvent {
                                timer_canceled_event_attributes: Some(
                                    TimerCanceledEventAttributes {
                                        timer_id: attrs.timer_id.clone(),
                                        started_event_id,
                                        decision_task_completed_event_id: completed_event_id,
                                        identity: identity.to_owned(),
                                    },
                                ),
                                ..Default::default()
                            },
                        );
                    } else {
                        record.push_event(
                            EventType::CancelTimerFailed,
                            HistoryEvent {
                                cancel_timer_failed_event_attributes: Some(
                                    cadence_api::tasks::CancelTimerFailedEventAttributes {
                                        timer_id: attrs.timer_id.clone(),
                                        cause: "TIMER_ID_UNKNOWN".into(),
                                        decision_task_completed_event_id: completed_event_id,
                                        identity: identity.to_owned(),
                                    },
                                ),
                                ..Default::default()
                            },
                        );
                    }
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
                        attrs.backoff_start_interval_in_seconds,
                        None,
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
    #[allow(clippy::too_many_arguments)]
    fn continue_as_new_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        attrs: &cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes,
        completed_event_id: i64,
        first_decision_backoff_seconds: Option<i32>,
        expiration_timestamp: Option<i64>,
    ) {
        use cadence_api::tasks::WorkflowExecutionContinuedAsNewEventAttributes;
        let new_run_id = uuid::Uuid::new_v4().to_string();
        let (workflow_type, task_list, timeouts, memo, search_attributes, parent, attempt) = {
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
                old.attempt,
            )
        };
        let previous_start = inner.by_run
            [&(domain.to_owned(), workflow_id.to_owned(), run_id.to_owned())]
            .started_attributes()
            .clone();
        let first_run_id = if previous_start.first_execution_run_id.is_empty() {
            run_id.to_owned()
        } else {
            previous_start.first_execution_run_id.clone()
        };
        let initiator = Some(
            attrs
                .initiator
                .unwrap_or(cadence_api::enums::ContinueAsNewInitiator::Decider),
        );
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
                        initiator,
                        backoff_start_interval_in_seconds: Some(
                            attrs.backoff_start_interval_in_seconds.unwrap_or(0),
                        ),
                        failure_reason: attrs.failure_reason.clone(),
                        failure_details: attrs.failure_details.clone(),
                        last_completion_result: attrs.last_completion_result.clone(),
                        header: attrs.header.clone(),
                        memo: attrs.memo.clone(),
                        search_attributes: attrs.search_attributes.clone(),
                        cron_overlap_policy: attrs.cron_overlap_policy,
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
            current_version: inner.domain_versions.get(domain).copied().unwrap_or_else(|| inner.by_run[&old_key].current_version),
            version_histories: Some(cadence_api::types::VersionHistories {
                current_version_history_index: 0,
                histories: vec![cadence_api::types::VersionHistory {
                    branch_token: crate::history_branch::new_token(&new_run_id, &uuid::Uuid::new_v4().to_string()),
                    items: Vec::new(),
                }],
            }),
            buffered_events: Vec::new(),
            buffer_event_sequence: 0,
            sticky_task_list: None,
            sticky_schedule_to_start_timeout_seconds: 0,
            last_updated_millis: now_millis(),
            pending_decision: None,
            pending_activities: HashMap::new(),
            pending_timers: HashMap::new(),
            pending_queries: std::collections::VecDeque::new(),
            parent,
            prev_started_event_id: None,
            signal_request_ids: std::collections::HashSet::new(),
            create_request_id: String::new(),
            first_decision_task_backoff_seconds: first_decision_backoff_seconds.unwrap_or(0),
            expiration_time_nanos: expiration_timestamp,
            first_decision_due_millis: None,
            jitter_start_seconds: attrs.jitter_start_seconds.unwrap_or(0),
            attempt,
        };
        let backoff_seconds = first_decision_backoff_seconds;
        let backoff_millis = i64::from(backoff_seconds.unwrap_or(0).max(0)).saturating_mul(1000);
        if backoff_millis > 0 {
            record.first_decision_due_millis = Some(record.start_time_millis + backoff_millis);
        }
        record.push_event(
            EventType::WorkflowExecutionStarted,
            HistoryEvent {
                workflow_execution_started_event_attributes: Some(
                    WorkflowExecutionStartedEventAttributes {
                        workflow_type: Some(record.workflow_type.clone()),
                        task_list: Some(record.task_list.clone()),
                        input: attrs.input.clone(),
                        execution_start_to_close_timeout_seconds: timeouts.0,
                        task_start_to_close_timeout_seconds: timeouts.1,
                        retry_policy: attrs.retry_policy.clone(),
                        cron_schedule: attrs.cron_schedule.clone(),
                        cron_overlap_policy: attrs.cron_overlap_policy,
                        first_decision_task_backoff_seconds: backoff_seconds,
                        memo: attrs.memo.clone(),
                        search_attributes: attrs.search_attributes.clone(),
                        header: attrs.header.clone(),
                        continued_execution_run_id: run_id.to_owned(),
                        original_execution_run_id: new_run_id.clone(),
                        first_execution_run_id: first_run_id,
                        first_scheduled_time_nano: previous_start.first_scheduled_time_nano,
                        expiration_timestamp,
                        initiator,
                        continued_failure_reason: attrs.failure_reason.clone().unwrap_or_default(),
                        continued_failure_details: attrs.failure_details.clone(),
                        last_completion_result: attrs.last_completion_result.clone(),
                        attempt,
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
        if backoff_millis <= 0 {
            Self::schedule_decision_locked(
                inner,
                domain,
                workflow_id,
                &new_run_id,
                &task_list.name,
                0,
            );
        }
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
        let child_domain = inner.domain_id(&attrs.domain);
        let parent_domain_name = inner.domain_name(domain);
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
            .get(&(child_domain.clone(), attrs.workflow_id.clone()))
            .is_some_and(|runs| {
                runs.iter().any(|r| {
                    inner
                        .by_run
                        .get(&(child_domain.clone(), attrs.workflow_id.clone(), r.clone()))
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
            domain: child_domain.clone(),
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
            current_version: inner.domain_versions.get(&child_domain).copied().unwrap_or(EMPTY_VERSION),
            version_histories: Some(cadence_api::types::VersionHistories {
                current_version_history_index: 0,
                histories: vec![cadence_api::types::VersionHistory {
                    branch_token: crate::history_branch::new_token(&child_run_id, &uuid::Uuid::new_v4().to_string()),
                    items: Vec::new(),
                }],
            }),
            buffered_events: Vec::new(),
            buffer_event_sequence: 0,
            sticky_task_list: None,
            sticky_schedule_to_start_timeout_seconds: 0,
            last_updated_millis: now_millis(),
            pending_decision: None,
            pending_activities: HashMap::new(),
            pending_timers: HashMap::new(),
            pending_queries: std::collections::VecDeque::new(),
            parent: Some(ParentRef {
                started_event_id: 0,
                domain: domain.to_string(),
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                initiated_event_id,
            }),
            prev_started_event_id: None,
            signal_request_ids: std::collections::HashSet::new(),
            create_request_id: String::new(),
            first_decision_task_backoff_seconds: 0,
            expiration_time_nanos: None,
            first_decision_due_millis: None,
            jitter_start_seconds: 0,
            attempt: 0,
        };
        child.push_event(
            EventType::WorkflowExecutionStarted,
            HistoryEvent {
                workflow_execution_started_event_attributes: Some(
                    WorkflowExecutionStartedEventAttributes {
                        workflow_type: attrs.workflow_type.clone(),
                        parent_workflow_domain: Some(parent_domain_name),
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
            .entry((child_domain.clone(), attrs.workflow_id.clone()))
            .or_default()
            .push(child_run_id.clone());
        inner.by_run.insert(
            (
                child_domain.clone(),
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
                domain: child_domain.clone(),
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
        let child_started_event_id = parent.history.len() as i64;
        if let Some(link) = inner
            .by_run
            .get_mut(&(
                child_domain.clone(),
                attrs.workflow_id.clone(),
                child_run_id.clone(),
            ))
            .and_then(|child| child.parent.as_mut())
        {
            link.started_event_id = child_started_event_id;
        }
        Self::schedule_decision_locked(
            inner,
            &child_domain,
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
        let domain_key = inner.domain_id(domain);
        let domain = domain_key.as_str();
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
                let is_child = inner.children.get(&source_key).is_some_and(|kids| {
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
    pub(crate) fn close_run_locked(
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
        // Go trims buffered events after an external close event. Their
        // unhandled payloads must not be published before termination/timeout.
        inner
            .by_run
            .get_mut(&(domain.to_owned(), workflow_id.to_owned(), run_id.to_owned()))
            .expect("record exists")
            .buffered_events
            .clear();
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
            record.last_updated_millis = record.close_time_millis.unwrap();
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
        let child_domain_name = inner.domain_name(child_domain);
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
        let started_event_id = if parent.started_event_id > 0 {
            parent.started_event_id
        } else {
            // Older snapshots did not persist the parent start link.
            inner
                .by_run
                .get(&parent_key)
                .and_then(|record| {
                    record
                        .history
                        .iter()
                        .find(|event| {
                            event
                                .child_workflow_execution_started_event_attributes
                                .as_ref()
                                .is_some_and(|a| a.initiated_event_id == parent.initiated_event_id)
                        })
                        .map(|event| event.event_id)
                })
                .unwrap_or(0)
        };
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
                            domain: child_domain_name.clone(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id,
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
                            domain: child_domain_name.clone(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id,
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
                            domain: child_domain_name.clone(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id,
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
                            domain: child_domain_name.clone(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id,
                        },
                    ),
                    ..Default::default()
                },
                EventType::ChildWorkflowExecutionTerminated,
            ),
            WorkflowExecutionCloseStatus::TimedOut => (
                HistoryEvent {
                    child_workflow_execution_timed_out_event_attributes: Some(
                        cadence_api::tasks::ChildWorkflowExecutionTimedOutEventAttributes {
                            timeout_type: Some(TimeoutType::StartToClose),
                            domain: child_domain_name.clone(),
                            workflow_execution: Some(child_execution),
                            workflow_type: Some(child_type),
                            initiated_event_id: parent.initiated_event_id,
                            started_event_id,
                        },
                    ),
                    ..Default::default()
                },
                EventType::ChildWorkflowExecutionTimedOut,
            ),
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
        let token = InMemoryExecutionStore::decode_task_token(task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let record = inner.by_run.get(&record_key).ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Err(TaskError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError {
                    message: "Workflow execution already completed.".to_string(),
                },
            ));
        }
        let pending = record.pending_decision.clone().ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new("Decision task not found."))
        })?;
        let started_event_id = pending.started_event_id.ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new("Decision task not found."))
        })?;
        if pending.scheduled_event_id != token.schedule_id
            || pending.attempt != token.schedule_attempt
        {
            return Err(TaskError::NotFound(EntityNotExistsError::new(
                "Decision task not found.",
            )));
        }
        let attempt = if record.buffered_events.is_empty()
            && !matches!(
                cause,
                Some(
                    DecisionTaskFailedCause::ResetWorkflow
                        | DecisionTaskFailedCause::FailoverCloseDecision
                )
            ) {
            pending.attempt + 1
        } else {
            0
        };
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        if pending.attempt == 0 {
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
        }
        record.clear_stickiness();
        record.last_updated_millis = now_millis();
        record.pending_decision = None;
        let task_list = record.task_list.name.clone();
        Self::flush_buffered_events_locked(&mut inner, &record_key);
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
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let workflow_domain = inner.domain_name(domain);
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
            let Some(pending) = record.pending_activities.get(&scheduled_event_id).cloned() else {
                continue;
            };
            if !pending.queued || pending.retry_ready_millis.is_some() {
                continue;
            }
            if pending.started_event_id.is_some() {
                continue; // already dispatched
            }
            let started_time = now_millis();
            let request_id = uuid::Uuid::new_v4().to_string();
            // Go records retry-policy starts transiently; only the final
            // attempt's Started event belongs in workflow history.
            let started_event_id = if pending.retry_policy.is_some() {
                -124
            } else {
                record.push_event(
                    EventType::ActivityTaskStarted,
                    HistoryEvent {
                        activity_task_started_event_attributes: Some(
                            ActivityTaskStartedEventAttributes {
                                scheduled_event_id,
                                identity: identity.to_string(),
                                request_id: request_id.clone(),
                                attempt: pending.attempt,
                                last_failure_reason: pending.last_failure_reason.clone(),
                                last_failure_details: pending.last_failure_details.clone(),
                                last_failure_options: pending.last_failure_options.clone(),
                            },
                        ),
                        ..Default::default()
                    },
                )
            };
            record.last_updated_millis = started_time;
            if let Some(p) = record.pending_activities.get_mut(&scheduled_event_id) {
                p.started_event_id = Some(started_event_id);
                p.started_time_millis = Some(started_time);
                p.last_heartbeat_time_millis = None;
                p.started_identity = identity.to_string();
                p.started_request_id = request_id;
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
                started_timestamp: Some(started_time),
                start_to_close_timeout_seconds: pending.start_to_close_timeout_seconds,
                heartbeat_timeout_seconds: pending.heartbeat_timeout_seconds,
                attempt: pending.attempt,
                scheduled_timestamp_of_this_attempt: pending
                    .attempt_scheduled_time_millis
                    .or(Some(pending.scheduled_time_millis)),
                heartbeat_details: pending.heartbeat_details,
                workflow_type: Some(record.workflow_type.clone()),
                workflow_domain: workflow_domain.clone(),
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
                TaskError::NotFound(EntityNotExistsError::new(
                    "Workflow execution does not exist.".to_string(),
                ))
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
        if pending.activity_id != token.activity_id
            || i64::from(pending.attempt) != token.schedule_attempt
        {
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
        let token = InMemoryExecutionStore::decode_task_token(&req.task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let scheduled_event_id = token.schedule_id;
        Self::resolve_activity_mut(&mut inner, &token)?;
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        let started_event_id = Self::materialize_activity_started(record, scheduled_event_id);
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
        let token = InMemoryExecutionStore::decode_task_token(&req.task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let scheduled_event_id = token.schedule_id;
        let (attempt, activity_snapshot) = {
            let pending = Self::resolve_activity_mut(&mut inner, &token)?;
            (pending.attempt, pending.clone())
        };
        if let Some(ready) = Self::activity_retry_time(req, &activity_snapshot, now_millis()) {
            let record = inner.by_run.get_mut(&record_key).expect("record exists");
            record.last_updated_millis = now_millis();
            let pending = record
                .pending_activities
                .get_mut(&scheduled_event_id)
                .expect("activity exists");
            pending.started_event_id = None;
            pending.started_time_millis = None;
            pending.last_heartbeat_time_millis = None;
            pending.started_identity.clear();
            pending.started_request_id.clear();
            pending.attempt = attempt + 1;
            pending.queued = false;
            pending.retry_ready_millis = Some(ready);
            pending.attempt_scheduled_time_millis = Some(ready);
            if !req.heartbeat_details.is_empty() {
                pending.heartbeat_details = req.heartbeat_details.clone();
            }
            pending.last_failure_reason = req.reason.clone();
            pending.last_failure_details = req.details.clone();
            pending.last_failure_options = req.failure_options.clone();
            return Ok(());
        }
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        let started_event_id = Self::materialize_activity_started(record, scheduled_event_id);
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

    /// Go execution/retry.go and mutable_state_builder_methods_activity.go:
    /// preserve the ScheduleID, honor fatal/cancel/non-retryable failures,
    /// attempt and expiration limits, jitter and the caller's backoff override.
    fn activity_retry_time(
        req: &RespondActivityTaskFailedRequest,
        pending: &PendingActivity,
        now: i64,
    ) -> Option<i64> {
        use cadence_api::enums::FailureCategory;
        let policy = pending.retry_policy.as_ref()?;
        if pending.cancel_requested {
            return None;
        }
        let category = req
            .failure_options
            .as_ref()
            .map(|o| o.failure_category_or_default())
            .unwrap_or(FailureCategory::Standard);
        if category == FailureCategory::Fatal {
            return None;
        }
        if policy.maximum_attempts == 0 && policy.expiration_interval_in_seconds == 0 {
            return None;
        }
        if policy.maximum_attempts > 0 && pending.attempt >= policy.maximum_attempts - 1 {
            return None;
        }
        if let Some(reason) = req.reason.as_deref() {
            if !policy.is_retriable_reason(reason)
                || matches!(
                    reason,
                    "CANCEL_DETAILS_EXCEEDS_LIMIT"
                        | "COMPLETE_RESULT_EXCEEDS_LIMIT"
                        | "HEARTBEAT_EXCEEDS_LIMIT"
                        | "DECISION_BLOB_SIZE_EXCEEDS_LIMIT"
                )
            {
                return None;
            }
        }
        let maximum = i64::MAX / 1_000_000_000;
        let value = f64::from(policy.initial_interval_in_seconds)
            * policy.backoff_coefficient.powi(pending.attempt);
        let mut seconds = if !value.is_finite() || value <= 0.0 || value > maximum as f64 {
            if policy.maximum_interval_in_seconds > 0 {
                i64::from(policy.maximum_interval_in_seconds)
            } else {
                return None;
            }
        } else {
            value as i64
        };
        if policy.maximum_interval_in_seconds > 0 {
            seconds = seconds.min(i64::from(policy.maximum_interval_in_seconds));
        }
        let mut delay = seconds as f64 * 1000.0;
        if policy.jitter_coefficient > 0.0 && policy.jitter_coefficient <= 1.0 {
            delay *= 1.0 - policy.jitter_coefficient * rand::random::<f64>();
        }
        if let Some(override_seconds) = req
            .failure_options
            .as_ref()
            .and_then(|o| o.next_retry_interval_seconds)
            .filter(|s| *s > 0)
        {
            delay = f64::from(override_seconds) * 1000.0;
        }
        let ready = now.checked_add(delay as i64)?;
        if policy.expiration_interval_in_seconds > 0
            && ready
                > pending
                    .scheduled_time_millis
                    .saturating_add(i64::from(policy.expiration_interval_in_seconds) * 1000)
        {
            return None;
        }
        Some(ready)
    }

    fn materialize_activity_started(record: &mut ExecutionRecord, scheduled_id: i64) -> i64 {
        let pending = record
            .pending_activities
            .get(&scheduled_id)
            .expect("activity exists")
            .clone();
        if pending.started_event_id != Some(-124) {
            return pending.started_event_id.unwrap_or(0);
        }
        let id = record.push_event(
            EventType::ActivityTaskStarted,
            HistoryEvent {
                activity_task_started_event_attributes: Some(ActivityTaskStartedEventAttributes {
                    scheduled_event_id: scheduled_id,
                    identity: pending.started_identity,
                    request_id: pending.started_request_id,
                    attempt: pending.attempt,
                    last_failure_reason: pending.last_failure_reason,
                    last_failure_details: pending.last_failure_details,
                    last_failure_options: pending.last_failure_options,
                }),
                ..Default::default()
            },
        );
        if let Some(time) = pending.started_time_millis {
            record
                .history
                .iter_mut()
                .chain(record.buffered_events.iter_mut())
                .find(|event| event.event_id == id)
                .expect("started event")
                .timestamp = Some(time);
        }
        record
            .pending_activities
            .get_mut(&scheduled_id)
            .expect("activity exists")
            .started_event_id = Some(id);
        id
    }

    pub fn expire_due_decisions(&self, now: i64) -> usize {
        let mut inner = self.inner.write().expect("execution store lock");
        let due: Vec<_> = inner
            .by_run
            .iter()
            .filter_map(|(key, record)| {
                if !record.is_open() {
                    return None;
                }
                let task = record.pending_decision.as_ref()?;
                let (deadline, kind) = if task.started_event_id.is_some() {
                    let timeout = record
                        .task_start_to_close_timeout_seconds
                        .filter(|seconds| *seconds > 0)?;
                    (
                        task.started_time_millis?
                            .saturating_add(i64::from(timeout) * 1000),
                        TimeoutType::StartToClose,
                    )
                } else if task.sticky {
                    (
                        task.scheduled_time_millis?.saturating_add(
                            i64::from(task.schedule_to_start_timeout_seconds?) * 1000,
                        ),
                        TimeoutType::ScheduleToStart,
                    )
                } else {
                    return None;
                };
                (deadline <= now).then(|| (key.clone(), kind))
            })
            .collect();
        for (key, kind) in &due {
            let record = inner.by_run.get_mut(key).unwrap();
            let task = record.pending_decision.clone().unwrap();
            if task.attempt == 0 {
                record.push_event(
                    EventType::DecisionTaskTimedOut,
                    HistoryEvent {
                        decision_task_timed_out_event_attributes: Some(
                            cadence_api::tasks::DecisionTaskTimedOutEventAttributes {
                                scheduled_event_id: task.scheduled_event_id,
                                started_event_id: task.started_event_id.unwrap_or(0),
                                timeout_type: Some(*kind),
                                cause: Some(cadence_api::enums::DecisionTaskTimedOutCause::Timeout),
                                ..Default::default()
                            },
                        ),
                        ..Default::default()
                    },
                );
            }
            let attempt = if *kind == TimeoutType::StartToClose && record.buffered_events.is_empty()
            {
                task.attempt + 1
            } else {
                0
            };
            record.clear_stickiness();
            record.pending_decision = None;
            record.last_updated_millis = now;
            let task_list = record.task_list.name.clone();
            Self::flush_buffered_events_locked(&mut inner, key);
            Self::schedule_decision_locked(&mut inner, &key.0, &key.1, &key.2, &task_list, attempt);
        }
        due.len()
    }

    /// Expire activity deadlines in chronological order, following Go's
    /// timer sequence and RetryActivity path. Intermediate retry timeouts are
    /// transient; only the final attempt contributes a timeout history event.
    pub fn expire_due_activities(&self, now: i64) -> usize {
        let mut inner = self.inner.write().expect("execution store lock");
        let mut due = Vec::new();
        for (key, record) in &inner.by_run {
            if !record.is_open() {
                continue;
            }
            for (id, activity) in &record.pending_activities {
                let scheduled = activity
                    .attempt_scheduled_time_millis
                    .unwrap_or(activity.scheduled_time_millis);
                let mut deadlines = Vec::new();
                let mut add = |base: i64, seconds: Option<i32>, kind| {
                    if let Some(seconds) = seconds.filter(|seconds| *seconds > 0) {
                        deadlines.push((base.saturating_add(i64::from(seconds) * 1000), kind));
                    }
                };
                add(
                    scheduled,
                    activity.schedule_to_close_timeout_seconds,
                    TimeoutType::ScheduleToClose,
                );
                if let Some(started) = activity.started_time_millis {
                    add(
                        started,
                        activity.start_to_close_timeout_seconds,
                        TimeoutType::StartToClose,
                    );
                    add(
                        activity.last_heartbeat_time_millis.unwrap_or(started),
                        activity.heartbeat_timeout_seconds,
                        TimeoutType::Heartbeat,
                    );
                } else {
                    add(
                        scheduled,
                        activity.schedule_to_start_timeout_seconds,
                        TimeoutType::ScheduleToStart,
                    );
                }
                if let Some((deadline, kind)) = deadlines
                    .into_iter()
                    .min_by_key(|(deadline, kind)| (*deadline, *kind as i32))
                {
                    if deadline <= now {
                        due.push((deadline, key.clone(), *id, kind));
                    }
                }
            }
        }
        due.sort_by(|left, right| (&left.0, &left.1, &left.2).cmp(&(&right.0, &right.1, &right.2)));
        let count = due.len();
        for (_, key, id, kind) in due {
            let snapshot = inner.by_run[&key].pending_activities[&id].clone();
            let failure = RespondActivityTaskFailedRequest {
                reason: Some(format!("cadenceInternal:Timeout {kind}")),
                ..Default::default()
            };
            if let Some(ready) = Self::activity_retry_time(&failure, &snapshot, now) {
                let record = inner.by_run.get_mut(&key).unwrap();
                record.last_updated_millis = now;
                let pending = record.pending_activities.get_mut(&id).unwrap();
                pending.attempt += 1;
                pending.started_event_id = None;
                pending.started_time_millis = None;
                pending.last_heartbeat_time_millis = None;
                pending.started_identity.clear();
                pending.started_request_id.clear();
                pending.queued = false;
                pending.retry_ready_millis = Some(ready);
                pending.attempt_scheduled_time_millis = Some(ready);
                pending.last_failure_reason = failure.reason;
                pending.last_failure_details.clear();
                pending.last_failure_options = None;
                continue;
            }
            let record = inner.by_run.get_mut(&key).unwrap();
            let started_event_id = Self::materialize_activity_started(record, id);
            record.push_event(
                EventType::ActivityTaskTimedOut,
                HistoryEvent {
                    activity_task_timed_out_event_attributes: Some(
                        cadence_api::tasks::ActivityTaskTimedOutEventAttributes {
                            scheduled_event_id: id,
                            started_event_id,
                            timeout_type: Some(kind),
                            details: snapshot.heartbeat_details,
                            last_failure_reason: snapshot.last_failure_reason,
                            last_failure_details: snapshot.last_failure_details,
                            last_failure_options: snapshot.last_failure_options,
                        },
                    ),
                    ..Default::default()
                },
            );
            record.pending_activities.remove(&id);
            let task_list = record.task_list.name.clone();
            Self::schedule_decision_locked(&mut inner, &key.0, &key.1, &key.2, &task_list, 0);
        }
        count
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
        let token = InMemoryExecutionStore::decode_task_token(task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let pending = Self::resolve_activity_mut(&mut inner, &token)?;
        pending.heartbeat_details = details;
        pending.last_heartbeat_time_millis = Some(now_millis());
        let canceled = pending.cancel_requested;
        inner
            .by_run
            .get_mut(&(token.domain_id, token.workflow_id, token.run_id))
            .unwrap()
            .last_updated_millis = now_millis();
        Ok(canceled)
    }

    /// Respond to an activity task as canceled, mirroring Go history.
    pub fn cancel_activity(
        &self,
        req: &cadence_api::tasks::RespondActivityTaskCanceledRequest,
    ) -> Result<(), TaskError> {
        let token = InMemoryExecutionStore::decode_task_token(&req.task_token)?;
        let mut inner = self.inner.write().expect("execution store lock");
        let record_key = (
            token.domain_id.clone(),
            token.workflow_id.clone(),
            token.run_id.clone(),
        );
        let scheduled_event_id = token.schedule_id;
        let cancel_requested_event_id = {
            let pending = Self::resolve_activity_mut(&mut inner, &token)?;
            pending.cancel_requested_event_id.unwrap_or(0)
        };
        let record = inner.by_run.get_mut(&record_key).expect("record exists");
        let started_event_id = Self::materialize_activity_started(record, scheduled_event_id);
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

    /// Read a bounded history snapshot. The cursor freezes the run and end
    /// event so signals arriving during a worker replay cannot leak into it.
    /// Numeric legacy cursors remain readable for existing callers.
    pub fn history_page(
        &self,
        domain: &str,
        execution: &WorkflowExecution,
        max_page_size: i32,
        next_page_token: &[u8],
        filter: Option<cadence_api::enums::HistoryEventFilterType>,
    ) -> Result<GetWorkflowExecutionHistoryResponse, HistoryPageError> {
        let inner = self.inner.read().expect("execution store lock");
        let supplied_domain = domain;
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let invalid =
            || HistoryPageError::InvalidToken(BadRequestError::new("Invalid NextPageToken."));
        let close_only = filter == Some(cadence_api::enums::HistoryEventFilterType::CloseEvent);
        let cursor = if next_page_token.is_empty() {
            None
        } else {
            serde_json::from_slice::<HistoryCursor>(next_page_token).ok()
        };
        let legacy = if !next_page_token.is_empty() && cursor.is_none() {
            Some(
                std::str::from_utf8(next_page_token)
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .ok_or_else(invalid)?,
            )
        } else {
            None
        };
        if let Some(c) = &cursor {
            if (c.domain != domain && c.domain != supplied_domain)
                || c.workflow_id != execution.workflow_id
                || (!execution.run_id.is_empty() && c.run_id != execution.run_id)
                || c.close_only != close_only
            {
                return Err(invalid());
            }
        }
        let run = if let Some(c) = &cursor {
            c.run_id.clone()
        } else if !execution.run_id.is_empty() {
            execution.run_id.clone()
        } else {
            inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .and_then(|runs| runs.last())
                .cloned()
                .ok_or_else(|| {
                    HistoryPageError::NotFound(EntityNotExistsError::new(
                        "Workflow execution does not exist.",
                    ))
                })?
        };
        let record = inner
            .by_run
            .get(&(
                domain.to_string(),
                execution.workflow_id.clone(),
                run.clone(),
            ))
            .ok_or_else(|| {
                HistoryPageError::NotFound(EntityNotExistsError::new(
                    "Workflow execution does not exist.",
                ))
            })?;
        let events = if close_only {
            if record.close_status.is_some() {
                record
                    .history
                    .last()
                    .map(std::slice::from_ref)
                    .unwrap_or(&[])
            } else {
                &[]
            }
        } else {
            record.history.as_slice()
        };
        let offset = cursor.as_ref().map(|c| c.next).or(legacy).unwrap_or(0);
        let bound = cursor.as_ref().map_or(events.len(), |c| c.end);
        if offset > bound || bound > events.len() {
            return Err(invalid());
        }
        let limit = if max_page_size <= 0 {
            HISTORY_PAGE_SIZE
        } else {
            (max_page_size as usize).min(HISTORY_PAGE_SIZE)
        };
        let transient = cursor
            .as_ref()
            .map(|cursor| cursor.transient_events.as_slice())
            .unwrap_or(&[]);
        if !transient.is_empty()
            && (close_only
                || transient.len() != 2
                || transient[0].event_id != bound as i64 + 1
                || transient[1].event_id != bound as i64 + 2
                || transient[0].event_type != Some(EventType::DecisionTaskScheduled)
                || transient[1].event_type != Some(EventType::DecisionTaskStarted))
        {
            return Err(invalid());
        }
        let end = offset.saturating_add(limit).min(bound);
        let token = if end < bound {
            encode_history_cursor(
                domain,
                &WorkflowExecution::new(&execution.workflow_id, run),
                end,
                bound,
                close_only,
                transient,
            )
        } else {
            Vec::new()
        };
        let mut page = events[offset..end].to_vec();
        if end == bound {
            page.extend_from_slice(transient);
        }
        Ok(GetWorkflowExecutionHistoryResponse {
            history: Some(History { events: page }),
            next_page_token: token,
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
        let mut inner = self.inner.write().expect("execution store lock");
        let namespace = self.domain_id(&inner, &req.domain);
        let open_run = self.open_run_id(&inner, &namespace, &req.workflow_id);
        if let Some(run_id) = open_run {
            let record = inner
                .by_run
                .get_mut(&(namespace.clone(), req.workflow_id.clone(), run_id.clone()))
                .expect("open run exists");
            if !req.request_id.is_empty() && record.signal_request_ids.contains(&req.request_id) {
                return Ok(StartWorkflowExecutionResponse { run_id });
            }
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
            let task_list = record.task_list.name.clone();
            Self::schedule_decision_locked(
                &mut inner,
                &namespace,
                &req.workflow_id,
                &run_id,
                &task_list,
                0,
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
        let resp = self.start_locked(&mut inner, &start_req)?;
        {
            let record = inner
                .by_run
                .get_mut(&(
                    namespace.clone(),
                    req.workflow_id.clone(),
                    resp.run_id.clone(),
                ))
                .expect("new run exists");
            if !record.is_open() {
                return Ok(resp);
            }
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

    /// Legacy standalone-store helper. Server code supplies the real UUID.
    pub fn register_domain_name(&self, name: &str) {
        self.register_domain(name, name);
    }

    pub fn register_domain(&self, name: &str, id: &str) {
        let mut inner = self.inner.write().expect("execution store lock");
        let migrate_legacy = inner
            .domain_names
            .get(name)
            .is_none_or(|display| display == name);
        inner.domain_ids.insert(name.to_owned(), id.to_owned());
        inner.domain_names.insert(id.to_owned(), name.to_owned());
        inner.known_domains.insert(name.to_owned());
        inner.known_domains.insert(id.to_owned());
        if name == id || !migrate_legacy || !inner.by_run.keys().any(|k| k.0 == name) {
            return;
        }
        // Upgrade development snapshots produced before UUID namespacing.
        // Recreating a domain never moves records from an OLD UUID to a NEW UUID.
        let map2 = |(mut d, w): (String, String)| {
            if d == name {
                d = id.to_owned();
            }
            (d, w)
        };
        let map3 = |(mut d, w, r): (String, String, String)| {
            if d == name {
                d = id.to_owned();
            }
            (d, w, r)
        };
        inner.by_run = inner
            .by_run
            .drain()
            .map(|(key, mut record)| {
                if record.domain == name {
                    record.domain = id.to_owned();
                }
                if let Some(parent) = record.parent.as_mut() {
                    if parent.domain == name {
                        parent.domain = id.to_owned();
                    }
                }
                (map3(key), record)
            })
            .collect();
        inner.by_workflow = inner
            .by_workflow
            .drain()
            .map(|(key, v)| (map2(key), v))
            .collect();
        inner.by_request_id = inner
            .by_request_id
            .drain()
            .map(|(key, v)| (map3(key), v))
            .collect();
        inner.decision_queues = inner
            .decision_queues
            .drain()
            .map(|(key, v)| (map2(key), v))
            .collect();
        inner.activity_queues = inner
            .activity_queues
            .drain()
            .map(|(key, v)| (map2(key), v))
            .collect();
        inner.children = inner
            .children
            .drain()
            .map(|(key, mut children)| {
                for child in &mut children {
                    if child.domain == name {
                        child.domain = id.to_owned();
                    }
                }
                (map3(key), children)
            })
            .collect();
    }

    pub fn sync_domains(&self, domains: &[(String, String)]) {
        {
            let mut inner = self.inner.write().expect("execution store lock");
            inner.domain_ids.clear();
            inner.known_domains.clear();
        }
        for (name, id) in domains {
            self.register_domain(name, id);
        }
    }

    pub fn unregister_domain(&self, name: &str) {
        let mut inner = self.inner.write().expect("execution store lock");
        if let Some(id) = inner.domain_ids.remove(name) {
            inner.known_domains.remove(&id);
        }
        inner.known_domains.remove(name);
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
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = if execution.run_id.is_empty() {
            let runs = inner
                .by_workflow
                .get(&(domain.to_string(), execution.workflow_id.clone()))
                .ok_or_else(|| {
                    QueryError::NotFound(EntityNotExistsError::new(
                        "Workflow execution does not exist.".to_string(),
                    ))
                })?;
            let latest = runs.last().ok_or_else(|| {
                QueryError::NotFound(EntityNotExistsError::new(
                    "Workflow execution does not exist.".to_string(),
                ))
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
            QueryError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
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

    /// Answer a directly-dispatched query task, mirroring Go matching
    /// `RespondQueryTaskCompleted` → `deliverQueryResult`. The task token is
    /// a `QueryTaskToken` whose `taskId` is the buffered query ID. Unknown
    /// IDs return `EntityNotExists` ("query task not found, or already
    /// expired"); malformed tokens return `BadRequest`.
    pub fn answer_query_task(
        &self,
        req: &cadence_api::tasks::RespondQueryTaskCompletedRequest,
    ) -> Result<(), QueryError> {
        use cadence_api::enums::{QueryResultType, QueryTaskCompletedType};
        let token = JsonTaskTokenSerializer::deserialize_query_token(&req.task_token)
            .map_err(|_| {
                QueryError::Invalid(BadRequestError::new("Invalid TaskToken.".to_string()))
            })?;
        if token.domain_id.is_empty() || token.task_list.is_empty() || token.task_id.is_empty() {
            return Err(QueryError::Invalid(BadRequestError::new(
                "Invalid TaskToken.".to_string(),
            )));
        }
        // The token domain must be a known namespace, like Go's domain cache
        // lookup by UUID.
        {
            let inner = self.inner.read().expect("execution store lock");
            if !inner.domain_ids.values().any(|id| id == &token.domain_id)
                && !inner.domain_names.contains_key(&token.domain_id)
            {
                return Err(QueryError::NotFound(EntityNotExistsError::new(
                    "query task not found, or already expired".to_string(),
                )));
            }
        }
        let mut inner = self.inner.write().expect("execution store lock");
        let query = inner
            .by_run
            .values_mut()
            .flat_map(|r| r.pending_queries.iter_mut())
            .find(|q| q.query_id == token.task_id);
        let query = match query {
            Some(q) => q,
            None => {
                return Err(QueryError::NotFound(EntityNotExistsError::new(
                    "query task not found, or already expired".to_string(),
                )));
            }
        };
        if query.answered.is_some() {
            return Err(QueryError::NotFound(EntityNotExistsError::new(
                "query task not found, or already expired".to_string(),
            )));
        }
        match req.completed_type.unwrap_or(QueryTaskCompletedType::Completed) {
            QueryTaskCompletedType::Completed => {
                query.answered = Some(WorkflowQueryResult {
                    result_type: Some(QueryResultType::Answered),
                    answer: req.query_result.clone(),
                    error_message: String::new(),
                });
            }
            QueryTaskCompletedType::Failed => {
                let message = if req.error_message.is_empty() {
                    "Query failed.".to_string()
                } else {
                    req.error_message.clone()
                };
                query.answered = Some(WorkflowQueryResult {
                    result_type: Some(QueryResultType::Failed),
                    answer: Vec::new(),
                    error_message: message,
                });
            }
            _ => {
                return Err(QueryError::Invalid(BadRequestError::new(
                    "Unknown query completed type.".to_string(),
                )));
            }
        }
        Ok(())
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
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
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

    /// Schedule the first decision task for runs whose
    /// `FirstDecisionTaskBackoffSeconds` has elapsed, mirroring Go's
    /// `WorkflowBackoffTimerTask` (retry and cron backoff types).
    pub fn fire_due_backoffs(&self, now: i64) -> usize {
        let mut inner = self.inner.write().expect("execution store lock");
        let due: Vec<(String, String, String, String)> = inner
            .by_run
            .iter()
            .filter_map(|(key, record)| {
                let is_open = record.is_open();
                let no_decision = record.pending_decision.is_none();
                let due = record.first_decision_due_millis.is_some_and(|d| d <= now);
                (is_open && no_decision && due).then(|| {
                    (
                        key.0.clone(),
                        key.1.clone(),
                        key.2.clone(),
                        record.task_list.name.clone(),
                    )
                })
            })
            .collect();
        let mut count = 0;
        for (domain, workflow, run, task_list) in due {
            {
                let record =
                    match inner
                        .by_run
                        .get_mut(&(domain.clone(), workflow.clone(), run.clone()))
                    {
                        Some(r) if r.is_open() && r.pending_decision.is_none() => r,
                        _ => continue,
                    };
                if !record.first_decision_due_millis.is_some_and(|d| d <= now) {
                    continue;
                }
                record.first_decision_due_millis = None;
            }
            Self::schedule_decision_locked(&mut inner, &domain, &workflow, &run, &task_list, 0);
            count += 1;
        }
        count
    }

    /// Go's workflow start timer: `start + execution timeout + first decision
    /// backoff`. Runs with a retry policy or a cron schedule are continued as
    /// new instead of being closed, mirroring Go's
    /// `processWorkflowTimeout` → `retryWorkflow`.
    pub fn expire_due_workflows(&self, now: i64) -> usize {
        let mut inner = self.inner.write().expect("execution store lock");
        let due: Vec<(String, String, String)> = inner
            .by_run
            .iter()
            .filter_map(|(key, record)| {
                if !record.is_open() {
                    return None;
                }
                let start = record
                    .history
                    .first()?
                    .workflow_execution_started_event_attributes
                    .as_ref()?;
                let timeout = i64::from(
                    record
                        .execution_start_to_close_timeout_seconds
                        .filter(|v| *v > 0)?,
                );
                let backoff = i64::from(record.first_decision_task_backoff_seconds.max(0));
                let mut deadline = record
                    .start_time_millis
                    .saturating_add(timeout.saturating_add(backoff).saturating_mul(1000));
                // Go: a retried attempt must not outlive the retry expiration.
                if record.attempt > 0 {
                    if let Some(expiration_nanos) = record.expiration_time_nanos {
                        let expiration_millis = expiration_nanos / 1_000_000;
                        deadline = deadline.min(expiration_millis);
                    }
                }
                let _ = start;
                (deadline <= now).then(|| key.clone())
            })
            .collect();
        let mut count = 0;
        for (domain, workflow, run) in due {
            // An earlier parent's timeout may have already closed this child.
            if !inner
                .by_run
                .get(&(domain.clone(), workflow.clone(), run.clone()))
                .is_some_and(ExecutionRecord::is_open)
            {
                continue;
            }
            if Self::retry_or_cron_locked(
                &mut inner,
                &domain,
                &workflow,
                &run,
                now,
                -1,
                Some("cadenceInternal:Timeout START_TO_CLOSE"),
                &[],
                None,
            ) {
                count += 1;
                continue;
            }
            Self::close_run_locked(
                &mut inner,
                &domain,
                &workflow,
                &run,
                WorkflowExecutionCloseStatus::TimedOut,
                None,
                Vec::new(),
                HistoryEvent {
                    workflow_execution_timed_out_event_attributes: Some(
                        cadence_api::tasks::WorkflowExecutionTimedOutEventAttributes {
                            timeout_type: Some(TimeoutType::StartToClose),
                        },
                    ),
                    ..Default::default()
                },
                EventType::WorkflowExecutionTimedOut,
            );
            count += 1;
        }
        count
    }

    /// Continue a failed/timed-out run through retry then cron, or a successful
    /// run through cron. Return false when the caller should close it normally.
    ///
    /// Mirrors Go's `GetRetryBackoffDuration` then `GetCronBackoffDuration`
    /// fallback in `processWorkflowTimeout`.
    #[allow(clippy::too_many_arguments)]
    fn retry_or_cron_locked(
        inner: &mut Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        now: i64,
        completed_event_id: i64,
        failure_reason: Option<&str>,
        failure_details: &[u8],
        completion_result: Option<&[u8]>,
    ) -> bool {
        use cadence_api::enums::ContinueAsNewInitiator;
        let mut attrs = match inner.by_run.get(&(
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        )) {
            Some(record) => Self::start_attributes(record),
            None => return false,
        };
        if inner.by_run[&(domain.to_owned(), workflow_id.to_owned(), run_id.to_owned())]
            .history
            .iter()
            .chain(
                &inner.by_run[&(domain.to_owned(), workflow_id.to_owned(), run_id.to_owned())]
                    .buffered_events,
            )
            .any(|event| event.event_type == Some(EventType::WorkflowExecutionCancelRequested))
        {
            return false;
        }
        // Successful completion only recurs through cron; failure retries first.
        let mut backoff_seconds = None;
        let mut initiator = ContinueAsNewInitiator::RetryPolicy;
        if let Some(retry) = attrs
            .retry_policy
            .clone()
            .filter(|_| completion_result.is_none())
        {
            if let Some(backoff) = Self::retry_backoff_duration(
                inner,
                domain,
                workflow_id,
                run_id,
                now,
                &retry,
                failure_reason.unwrap_or_default(),
            ) {
                backoff_seconds = Some(backoff);
                initiator = ContinueAsNewInitiator::RetryPolicy;
            }
        }
        if backoff_seconds.is_none() && !attrs.cron_schedule.is_empty() {
            match Self::cron_backoff_duration(inner, domain, workflow_id, run_id, now) {
                Ok(Some(backoff)) => {
                    backoff_seconds = Some(backoff);
                    initiator = ContinueAsNewInitiator::CronSchedule;
                }
                Ok(None) => {}
                // An unparseable schedule closes the run, as in Go.
                Err(_) => return false,
            }
        }
        let backoff_seconds = match backoff_seconds {
            Some(v) => v,
            None => return false,
        };
        let mut expired_expiration = None;
        {
            let record = inner
                .by_run
                .get_mut(&(
                    domain.to_string(),
                    workflow_id.to_string(),
                    run_id.to_string(),
                ))
                .expect("record exists");
            // Retry continue-as-new increments the attempt and keeps the
            // original expiration timestamp.
            if initiator == ContinueAsNewInitiator::RetryPolicy {
                record.attempt += 1;
                expired_expiration = record.expiration_time_nanos;
            } else {
                record.attempt = 0;
                if let Some(policy) = record.retry_policy() {
                    if policy.expiration_interval_in_seconds > 0 {
                        expired_expiration = Some(
                            now.saturating_mul(1_000_000).saturating_add(
                                (i64::from(policy.expiration_interval_in_seconds)
                                    + i64::from(backoff_seconds))
                                .saturating_mul(1_000_000_000),
                            ),
                        );
                    }
                }
            }
            attrs.last_completion_result = completion_result
                .map(Vec::from)
                .unwrap_or_else(|| record.started_attributes().last_completion_result.clone());
            attrs.retry_policy = record.retry_policy();
        }
        attrs.backoff_start_interval_in_seconds = Some(backoff_seconds);
        attrs.initiator = Some(initiator);
        attrs.failure_reason = failure_reason.map(str::to_owned);
        attrs.failure_details = failure_details.to_vec();
        Self::continue_as_new_locked(
            inner,
            domain,
            workflow_id,
            run_id,
            &attrs,
            completed_event_id,
            Some(backoff_seconds),
            expired_expiration,
        );
        true
    }

    /// The started-event attributes of a run's first history event.
    fn start_attributes(
        record: &ExecutionRecord,
    ) -> cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes {
        let start = record
            .history
            .first()
            .and_then(|e| e.workflow_execution_started_event_attributes.clone())
            .unwrap_or_default();
        cadence_api::tasks::ContinueAsNewWorkflowExecutionDecisionAttributes {
            workflow_type: start.workflow_type.clone(),
            task_list: start.task_list.clone(),
            input: start.input.clone(),
            execution_start_to_close_timeout_seconds: start
                .execution_start_to_close_timeout_seconds,
            task_start_to_close_timeout_seconds: start.task_start_to_close_timeout_seconds,
            retry_policy: start.retry_policy.clone(),
            cron_schedule: start.cron_schedule.clone(),
            cron_overlap_policy: start.cron_overlap_policy,
            memo: start.memo.clone(),
            search_attributes: start.search_attributes.clone(),
            header: start.header.clone(),
            jitter_start_seconds: Some(record.jitter_start_seconds),
            ..Default::default()
        }
    }

    /// Port of `getBackoffInterval` + `shouldRetry` from
    /// `go/service/history/execution/retry.go`.
    fn retry_backoff_duration(
        inner: &Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        now: i64,
        retry: &cadence_api::types::RetryPolicy,
        failure_reason: &str,
    ) -> Option<i32> {
        let coefficient = retry.backoff_coefficient;
        let attempt = inner
            .by_run
            .get(&(
                domain.to_string(),
                workflow_id.to_string(),
                run_id.to_string(),
            ))
            .map_or(0, |r| r.attempt);
        let expiration_nanos = inner
            .by_run
            .get(&(
                domain.to_string(),
                workflow_id.to_string(),
                run_id.to_string(),
            ))
            .and_then(|r| r.expiration_time_nanos);
        Self::retry_backoff_seconds(
            attempt,
            retry.initial_interval_in_seconds,
            retry.maximum_interval_in_seconds,
            coefficient,
            retry.jitter_coefficient,
            retry.maximum_attempts,
            retry.expiration_interval_in_seconds,
            expiration_nanos,
            now,
            &retry.non_retriable_error_reasons,
            failure_reason,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn retry_backoff_seconds(
        attempt: i32,
        initial_interval: i32,
        maximum_interval: i32,
        backoff_coefficient: f64,
        jitter_coefficient: f64,
        maximum_attempts: i32,
        expiration_interval: i32,
        expiration_nanos: Option<i64>,
        now: i64,
        non_retriable_reasons: &[String],
        failure_reason: &str,
    ) -> Option<i32> {
        const MAX_BACKOFF_INTERVAL_SECONDS: i64 = i64::MAX / 1_000_000_000;
        // getBackoffInterval
        let next_interval =
            (f64::from(initial_interval) * backoff_coefficient.powi(attempt)) as i64;
        let mut next_interval =
            if next_interval <= 0 || next_interval > MAX_BACKOFF_INTERVAL_SECONDS {
                if maximum_interval > 0 {
                    i64::from(maximum_interval)
                } else {
                    return None;
                }
            } else {
                next_interval
            };
        if maximum_interval > 0 && next_interval > i64::from(maximum_interval) {
            next_interval = i64::from(maximum_interval);
        }
        let mut next_seconds = next_interval as f64;
        if jitter_coefficient > 0.0 && jitter_coefficient <= 1.0 {
            next_seconds *= 1.0 - jitter_coefficient * rand::random::<f64>();
        }
        // Go rounds up so a sub-second jittered interval is not an immediate retry.
        let backoff_seconds = (next_seconds.ceil() as i64).max(0);
        // shouldRetry: a retry policy needs either attempts or an expiration.
        if maximum_attempts == 0 && expiration_interval == 0 && expiration_nanos.is_none() {
            return None;
        }
        if maximum_attempts > 0 && attempt >= maximum_attempts - 1 {
            return None;
        }
        if let Some(expiration_nanos) = expiration_nanos {
            let next_scheduled_nanos = now
                .saturating_mul(1_000_000)
                .saturating_add(backoff_seconds.saturating_mul(1_000_000_000));
            if next_scheduled_nanos > expiration_nanos {
                return None;
            }
        }
        if matches!(
            failure_reason,
            "CANCEL_DETAILS_EXCEEDS_LIMIT"
                | "COMPLETE_RESULT_EXCEEDS_LIMIT"
                | "HEARTBEAT_EXCEEDS_LIMIT"
                | "DECISION_BLOB_SIZE_EXCEEDS_LIMIT"
        ) || non_retriable_reasons.iter().any(|r| r == failure_reason)
        {
            return None;
        }
        i32::try_from(backoff_seconds).ok()
    }

    /// Port of Go's `GetCronBackoffDuration`.
    fn cron_backoff_duration(
        inner: &Inner,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        now: i64,
    ) -> Result<Option<i32>, String> {
        let key = (
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        let record = match inner.by_run.get(&key) {
            Some(r) => r,
            None => return Ok(None),
        };
        if record.cron_schedule().is_empty() {
            return Ok(None);
        }
        let schedule = record.cron_schedule();
        let overlap = record.cron_overlap_policy().map(|p| p as i32);
        // Go uses the run start time plus the first decision backoff.
        let execution_nanos = record
            .start_time_millis
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(record.first_decision_task_backoff_seconds) * 1_000_000_000);
        let backoff = cadence_common::cron::get_backoff_for_next_schedule_in_seconds(
            &schedule,
            cadence_common::cron::naive_from_nanos(execution_nanos),
            cadence_common::cron::naive_from_nanos(now.saturating_mul(1_000_000)),
            overlap,
            cadence_common::cron::naive_from_nanos(now.saturating_mul(1_000_000)),
            cadence_common::cron::jitter_seconds(
                record.jitter_start_seconds,
                &mut rand::thread_rng(),
            ),
        )?;
        Ok(Some(backoff))
    }

    /// Backlog sizes per task list for `DescribeTaskList`.
    pub fn task_list_backlog(&self, domain: &str, task_list: &str) -> (usize, usize) {
        let inner = self.inner.read().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
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

    /// Task-list names with queued or recently-seen tasks for a domain,
    /// mirroring Go matching's task-list registry scan for
    /// `GetTaskListsByDomain`. Returns `(decision_lists, activity_lists)`.
    pub fn task_lists(&self, domain: &str) -> (Vec<String>, Vec<String>) {
        let inner = self.inner.read().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let mut decisions: Vec<String> = inner
            .decision_queues
            .keys()
            .filter(|(d, _)| d == domain)
            .map(|(_, tl)| tl.clone())
            .collect();
        decisions.sort();
        decisions.dedup();
        let mut activities: Vec<String> = inner
            .activity_queues
            .keys()
            .filter(|(d, _)| d == domain)
            .map(|(_, tl)| tl.clone())
            .collect();
        activities.sort();
        activities.dedup();
        (decisions, activities)
    }

    /// Store a task-list partition config, mirroring Go matching
    /// `UpdateTaskListPartitionConfig` persistence. Only normal task lists
    /// accept updates; the config must be present. Single host: the stored
    /// config is served back verbatim by describe/list calls.
    pub fn update_partition_config(
        &self,
        domain: &str,
        task_list: &str,
        task_list_type: i32,
        kind_is_normal: bool,
        config: StoredPartitionConfig,
    ) -> Result<(), TaskError> {
        if !kind_is_normal {
            return Err(TaskError::InvalidToken(
                cadence_api::errors::BadRequestError::new(
                    "Only normal tasklist's partition config can be updated.".to_string(),
                ),
            ));
        }
        let mut inner = self.inner.write().expect("execution store lock");
        let namespace = inner.domain_id(domain);
        inner.partition_configs.insert(
            (namespace, task_list.to_string(), task_list_type),
            config,
        );
        Ok(())
    }

    /// Refresh a cached partition config, mirroring Go matching
    /// `RefreshTaskListPartitionConfig`. Single process shares state, so this
    /// verifies the domain and stored entry exist.
    pub fn refresh_partition_config(
        &self,
        domain: &str,
        task_list: &str,
        task_list_type: i32,
    ) -> Result<(), TaskError> {
        let inner = self.inner.read().expect("execution store lock");
        let namespace = inner.domain_id(domain);
        if !inner
            .domain_ids
            .values()
            .any(|id| id == &namespace)
            && !inner.domain_names.contains_key(&namespace)
        {
            return Err(TaskError::InvalidToken(
                cadence_api::errors::BadRequestError::new("Domain does not exist.".to_string()),
            ));
        }
        let _ = (task_list, task_list_type);
        Ok(())
    }

    /// Stored partition config for describe/list responses, if any.
    pub fn partition_config(
        &self,
        domain: &str,
        task_list: &str,
        task_list_type: i32,
    ) -> Option<StoredPartitionConfig> {
        let inner = self.inner.read().expect("execution store lock");
        let namespace = inner.domain_id(domain);
        inner
            .partition_configs
            .get(&(namespace, task_list.to_string(), task_list_type))
            .cloned()
    }

    /// Requeue a scheduled-but-unstarted decision task, mirroring Go
    /// matching `AddDecisionTask`: the run and its pending decision with the
    /// given schedule ID must exist, otherwise `EntityNotExists` is returned.
    /// An already-queued entry is not duplicated.
    pub fn requeue_decision_task(
        &self,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        schedule_id: i64,
    ) -> Result<(), TaskError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = (
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        let record = inner.by_run.get(&key).ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Err(TaskError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError::new(
                    "Workflow execution already completed.".to_string(),
                ),
            ));
        }
        let pending = record.pending_decision.clone().ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new(
                "Decision task not found.".to_string(),
            ))
        })?;
        if pending.scheduled_event_id != schedule_id || pending.started_event_id.is_some() {
            return Err(TaskError::NotFound(EntityNotExistsError::new(
                "Decision task not found.".to_string(),
            )));
        }
        let task_list = record.task_list.name.clone();
        let queue = inner
            .decision_queues
            .entry((domain.to_string(), task_list))
            .or_default();
        if !queue
            .iter()
            .any(|(w, r)| w == workflow_id && r == run_id)
        {
            queue.push_back((workflow_id.to_string(), run_id.to_string()));
        }
        Ok(())
    }

    /// Requeue a scheduled activity task, mirroring Go matching
    /// `AddActivityTask`. Unknown runs or schedule IDs return
    /// `EntityNotExists`; an already-queued entry is not duplicated.
    pub fn requeue_activity_task(
        &self,
        domain: &str,
        workflow_id: &str,
        run_id: &str,
        schedule_id: i64,
    ) -> Result<(), TaskError> {
        let mut inner = self.inner.write().expect("execution store lock");
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
        let key = (
            domain.to_string(),
            workflow_id.to_string(),
            run_id.to_string(),
        );
        let record = inner.by_run.get(&key).ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new(
                "Workflow execution does not exist.".to_string(),
            ))
        })?;
        if !record.is_open() {
            return Err(TaskError::AlreadyCompleted(
                WorkflowExecutionAlreadyCompletedError::new(
                    "Workflow execution already completed.".to_string(),
                ),
            ));
        }
        let pending = record.pending_activities.get(&schedule_id).ok_or_else(|| {
            TaskError::NotFound(EntityNotExistsError::new(
                "Activity task not found.".to_string(),
            ))
        })?;
        // Already-dispatched tasks are not requeued, like started decisions.
        if pending.started_event_id.is_some() {
            return Err(TaskError::NotFound(EntityNotExistsError::new(
                "Activity task not found.".to_string(),
            )));
        }
        let task_list = pending.task_list.name.clone();
        let queue = inner
            .activity_queues
            .entry((domain.to_string(), task_list))
            .or_default();
        if !queue
            .iter()
            .any(|(w, r, s)| w == workflow_id && r == run_id && *s == schedule_id)
        {
            queue.push_back((workflow_id.to_string(), run_id.to_string(), schedule_id));
        }
        Ok(())
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
        let domain_key = self.domain_id(&inner, domain);
        let domain = domain_key.as_str();
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
    /// The request cannot be started at all (for example an invalid cron
    /// schedule), mirroring Go's BadRequestError from the frontend.
    #[error("{0}")]
    InvalidArgument(String),
}

/// Metadata for a recorded-as-started decision task.
#[derive(Debug, Clone)]
pub struct RecordedDecision {
    pub workflow_type_name: String,
    pub scheduled_event_id: i64,
    pub started_event_id: i64,
    pub next_event_id: i64,
    pub attempt: i64,
    pub previous_started_event_id: Option<i64>,
    pub task_list_name: String,
    pub scheduled_time_millis: Option<i64>,
    pub started_time_millis: Option<i64>,
}

impl RecordedDecision {
    fn from_pending(
        record: &ExecutionRecord,
        pending: &PendingDecision,
        started_event_id: i64,
    ) -> Self {
        Self {
            workflow_type_name: record.workflow_type.name.clone(),
            scheduled_event_id: pending.scheduled_event_id,
            started_event_id,
            next_event_id: record.history.len() as i64 + 1,
            attempt: pending.attempt,
            previous_started_event_id: record.prev_started_event_id,
            task_list_name: record.task_list.name.clone(),
            scheduled_time_millis: pending.scheduled_time_millis,
            started_time_millis: pending.started_time_millis,
        }
    }
}

/// Metadata for a recorded-as-started activity task.
#[derive(Debug, Clone)]
pub struct RecordedActivity {
    pub scheduled_event: HistoryEvent,
    pub started_time_millis: i64,
    pub attempt: i32,
    pub heartbeat_details: Vec<u8>,
}

impl RecordedActivity {
    fn from_pending(
        record: &ExecutionRecord,
        pending: &PendingActivity,
        started_event_id: i64,
    ) -> Self {
        let scheduled_event = record
            .history
            .iter()
            .find(|e| e.event_id == pending.scheduled_event_id)
            .cloned()
            .unwrap_or_default();
        let _ = started_event_id;
        Self {
            scheduled_event,
            started_time_millis: pending.started_time_millis.unwrap_or_else(now_millis),
            attempt: pending.attempt,
            heartbeat_details: pending.heartbeat_details.clone(),
        }
    }
}

/// Errors for task recording: unknown tasks are `EntityNotExists`,
/// double-starts with a different request are already-started failures,
/// stale IDs are retryable stale-state errors.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error(transparent)]
    NotFound(#[from] EntityNotExistsError),
    #[error("{0}")]
    AlreadyStarted(String),
    #[error("{0}")]
    Stale(String),
    #[error("{0}")]
    Invalid(String),
}

/// User-visible mutable-state snapshot for history/introspection APIs.
#[derive(Debug, Clone)]
pub struct MutableStateView {
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type_name: String,
    pub next_event_id: i64,
    pub previous_started_event_id: Option<i64>,
    pub task_list_name: String,
    pub sticky_task_list_name: String,
    pub sticky_schedule_to_start_timeout_seconds: i32,
    pub close_status: Option<WorkflowExecutionCloseStatus>,
    pub history_size_bytes: i64,
    pub version_histories: Option<cadence_api::types::VersionHistories>,
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

/// Errors for workflow reset, mirroring Go's history reset mapping:
/// unknown runs are `EntityNotExists`, invalid reset points are `BadRequest`.
#[derive(Debug, thiserror::Error)]
pub enum ResetError {
    #[error(transparent)]
    NotFound(#[from] EntityNotExistsError),
    #[error("{0}")]
    Invalid(String),
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
    #[serde(default)]
    domain_ids: HashMap<String, String>,
    #[serde(default)]
    domain_names: HashMap<String, String>,
    #[serde(default)]
    domain_versions: HashMap<String, i64>,
    #[serde(default)]
    async_requests: Vec<AsyncRequest>,
    #[serde(default)]
    schedules: Vec<((String, String), crate::schedules::ScheduleRecord)>,
    #[serde(default)]
    semaphores: Vec<((String, String), crate::semaphores::SemaphoreMetadata)>,
    #[serde(default)]
    semaphore_holds: Vec<((String, String, i32), Vec<crate::semaphores::SemaphoreHold>)>,
    #[serde(default)]
    partition_configs: Vec<((String, String, i32), StoredPartitionConfig)>,
}

impl InMemoryExecutionStore {
    fn snapshot(&self) -> ExecutionSnapshot {
        let inner = self.inner.read().expect("execution store lock");
        ExecutionSnapshot {
            runs: inner
                .by_run
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            by_workflow: inner
                .by_workflow
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            by_request_id: inner
                .by_request_id
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
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
            children: inner
                .children
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            known_domains: inner.known_domains.iter().cloned().collect(),
            domain_ids: inner.domain_ids.clone(),
            domain_names: inner.domain_names.clone(),
            domain_versions: inner.domain_versions.clone(),
            async_requests: inner.async_requests.iter().cloned().collect(),
            schedules: inner
                .schedules
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            semaphores: inner
                .semaphores
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            semaphore_holds: inner
                .semaphore_holds
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            partition_configs: inner
                .partition_configs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    fn restore(snapshot: ExecutionSnapshot) -> Self {
        let mut runs = snapshot.runs;
        for (_, record) in &mut runs {
            if record.version_histories.is_none() {
                // Older Rust snapshots emitted version zero. Preserve their
                // recorded stream instead of retroactively rewriting versions.
                record.current_version = record.history.last().map(|e| e.version).unwrap_or(EMPTY_VERSION);
                let token = crate::history_branch::new_token(&record.execution.run_id, &record.execution.run_id);
                record.rebuild_version_history(token);
            }
            if let Some(task) = record.pending_decision.as_mut() {
                if task.task_list_name.is_none() {
                    task.task_list_name = record
                        .history
                        .get(task.scheduled_event_id.saturating_sub(1) as usize)
                        .and_then(|event| event.decision_task_scheduled_event_attributes.as_ref())
                        .and_then(|attributes| attributes.task_list.as_ref())
                        .map(|task_list| task_list.name.clone())
                        .or_else(|| Some(record.task_list.name.clone()));
                }
                task.scheduled_time_millis = task.scheduled_time_millis.or_else(|| {
                    record
                        .history
                        .get(task.scheduled_event_id.saturating_sub(1) as usize)
                        .and_then(|event| event.timestamp)
                });
                task.started_time_millis = task.started_time_millis.or_else(|| {
                    task.started_event_id
                        .and_then(|id| record.history.get(id.saturating_sub(1) as usize))
                        .and_then(|event| event.timestamp)
                });
            }
            record.buffer_event_sequence = record.buffer_event_sequence.max(
                record
                    .buffered_events
                    .iter()
                    .map(|event| (-event.event_id - 1_000_000 + 1).max(0))
                    .max()
                    .unwrap_or(0),
            );
            for event in record.history.iter().chain(&record.buffered_events) {
                if let Some(id) = event
                    .workflow_execution_signaled_event_attributes
                    .as_ref()
                    .map(|a| &a.request_id)
                    .filter(|id| !id.is_empty())
                {
                    record.signal_request_ids.insert(id.clone());
                }
            }
        }
        let inner = Inner {
            by_run: runs.into_iter().collect(),
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
            domain_ids: snapshot.domain_ids,
            domain_names: snapshot.domain_names,
            domain_versions: snapshot.domain_versions,
            async_requests: snapshot.async_requests.into_iter().collect(),
            schedules: snapshot.schedules.into_iter().collect(),
            semaphores: snapshot.semaphores.into_iter().collect(),
            semaphore_holds: snapshot.semaphore_holds.into_iter().collect(),
            partition_configs: snapshot.partition_configs.into_iter().collect(),
        };
        Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(inner)),
            domain_scope: None,
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
        let store = Self::restore(snapshot);
        store.resume_schedules_after_restart();
        Ok(store)
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
        assert_eq!(retry.previous_started_event_id, None);
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
        assert_eq!(hist.events.len(), 3); // one signal for an idempotent request ID
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
    use cadence_api::types::{
        ResetWorkflowExecutionRequest, SearchAttributes, TaskList, WorkflowType,
    };

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
    fn query_task_completed_answers_by_token() {
        use cadence_api::enums::QueryTaskCompletedType;
        use cadence_api::tasktoken::{JsonTaskTokenSerializer, QueryTaskToken};
        let store = InMemoryExecutionStore::new();
        store.register_domain("d", &uuid::Uuid::new_v4().to_string());
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        let qid = store
            .buffer_query("d", &exec, "state", Vec::new(), None)
            .unwrap();
        let inner = store.inner.read().expect("execution store lock");
        let domain_id = store.domain_id(&inner, "d");
        drop(inner);
        // A directly-dispatched query task carries a QueryTaskToken whose
        // taskId is the buffered query ID, as Go matching generates.
        let token = QueryTaskToken {
            domain_id,
            workflow_id: "w".to_string(),
            run_id: started.run_id.clone(),
            task_list: "tl".to_string(),
            task_id: qid.clone(),
        };
        let token_bytes = JsonTaskTokenSerializer::serialize_query_token(&token).unwrap();
        store
            .answer_query_task(&cadence_api::tasks::RespondQueryTaskCompletedRequest {
                task_token: token_bytes,
                completed_type: Some(QueryTaskCompletedType::Completed),
                query_result: b"42".to_vec(),
                ..Default::default()
            })
            .unwrap();
        let answer = store.take_query_result(&qid).unwrap();
        assert_eq!(answer.answer, b"42");
        // Answering twice reports the query as expired, like Go's
        // deliverQueryResult on an already-removed channel.
        let token2 = QueryTaskToken {
            domain_id: token.domain_id.clone(),
            workflow_id: "w".to_string(),
            run_id: started.run_id.clone(),
            task_list: "tl".to_string(),
            task_id: qid.clone(),
        };
        let token2_bytes = JsonTaskTokenSerializer::serialize_query_token(&token2).unwrap();
        let err = store
            .answer_query_task(&cadence_api::tasks::RespondQueryTaskCompletedRequest {
                task_token: token2_bytes,
                completed_type: Some(QueryTaskCompletedType::Completed),
                query_result: b"43".to_vec(),
                ..Default::default()
            })
            .unwrap_err();
        assert!(matches!(err, QueryError::NotFound(_)));
    }

    #[test]
    fn query_task_completed_failure_and_unknown_tokens() {
        use cadence_api::enums::QueryTaskCompletedType;
        use cadence_api::tasktoken::{JsonTaskTokenSerializer, QueryTaskToken};
        let store = InMemoryExecutionStore::new();
        let domain_id = uuid::Uuid::new_v4().to_string();
        store.register_domain("d", &domain_id);
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        let qid = store
            .buffer_query("d", &exec, "state", Vec::new(), None)
            .unwrap();
        // Failed completions surface QueryFailedError to the waiter.
        let token = QueryTaskToken {
            domain_id: domain_id.clone(),
            workflow_id: "w".to_string(),
            run_id: started.run_id.clone(),
            task_list: "tl".to_string(),
            task_id: qid.clone(),
        };
        store
            .answer_query_task(&cadence_api::tasks::RespondQueryTaskCompletedRequest {
                task_token: JsonTaskTokenSerializer::serialize_query_token(&token).unwrap(),
                completed_type: Some(QueryTaskCompletedType::Failed),
                error_message: "boom".to_string(),
                ..Default::default()
            })
            .unwrap();
        let answer = store.take_query_result(&qid).unwrap();
        assert_eq!(answer.error_message, "boom");
        // Unknown query IDs are EntityNotExists, not BadRequest.
        let ghost = QueryTaskToken {
            domain_id,
            workflow_id: "w".to_string(),
            run_id: started.run_id,
            task_list: "tl".to_string(),
            task_id: "no-such-query".to_string(),
        };
        let err = store
            .answer_query_task(&cadence_api::tasks::RespondQueryTaskCompletedRequest {
                task_token: JsonTaskTokenSerializer::serialize_query_token(&ghost).unwrap(),
                ..Default::default()
            })
            .unwrap_err();
        assert!(matches!(err, QueryError::NotFound(_)));
        // Malformed tokens are BadRequest.
        let err = store
            .answer_query_task(&cadence_api::tasks::RespondQueryTaskCompletedRequest {
                task_token: b"not-json".to_vec(),
                ..Default::default()
            })
            .unwrap_err();
        assert!(matches!(err, QueryError::Invalid(_)));
    }

    #[test]
    fn query_on_closed_execution_rejects() {        let store = InMemoryExecutionStore::new();
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
    fn requeue_decision_task_restores_queue_entry() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        // Drain the queue without starting the task.
        {
            let mut inner = store.inner.write().expect("execution store lock");
            let domain = store.domain_id(&inner, "d");
            inner
                .decision_queues
                .get_mut(&(domain, "tl".to_string()))
                .unwrap()
                .pop_front();
        }
        assert!(store.poll_decision("d", "tl", "worker").is_none());
        // The pending decision still exists with schedule ID 2.
        store
            .requeue_decision_task("d", "w", &started.run_id, 2)
            .unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        assert_eq!(task.workflow_execution.unwrap().run_id, started.run_id);
        // Unknown schedule IDs and runs are EntityNotExists.
        assert!(store.requeue_decision_task("d", "w", &started.run_id, 99).is_err());
        assert!(store.requeue_decision_task("d", "w", "nope", 2).is_err());
        // Started tasks cannot be requeued.
        assert!(store.requeue_decision_task("d", "w", &started.run_id, 2).is_err());
    }

    #[test]
    fn requeue_activity_task_restores_queue_entry() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        complete_with(
            &store,
            "d",
            "tl",
            vec![Decision {
                decision_type: Some(DecisionType::ScheduleActivityTask),
                schedule_activity_task_decision_attributes: Some(
                    cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                        activity_id: "a1".to_string(),
                        activity_type: Some(cadence_api::types::ActivityType {
                            name: "at".to_string(),
                        }),
                        task_list: Some(cadence_api::types::TaskList {
                            name: "tl".to_string(),
                            ..Default::default()
                        }),
                        schedule_to_close_timeout_seconds: Some(60),
                        schedule_to_start_timeout_seconds: Some(60),
                        start_to_close_timeout_seconds: Some(60),
                        ..Default::default()
                    },
                ),
                ..Default::default()
            }],
        );
        // Find the activity schedule ID, drain its queue entry, requeue it.
        let schedule_id = {
            let inner = store.inner.read().expect("execution store lock");
            let domain = store.domain_id(&inner, "d");
            let record = inner
                .by_run
                .get(&(domain, "w".to_string(), started.run_id.clone()))
                .unwrap();
            *record.pending_activities.keys().next().unwrap()
        };
        {
            let mut inner = store.inner.write().expect("execution store lock");
            let domain = store.domain_id(&inner, "d");
            inner
                .activity_queues
                .get_mut(&(domain, "tl".to_string()))
                .unwrap()
                .pop_front();
        }
        store
            .requeue_activity_task("d", "w", &started.run_id, schedule_id)
            .unwrap();
        assert!(store.poll_activity("d", "tl", "worker").is_some());
        assert!(store
            .requeue_activity_task("d", "w", &started.run_id, 9999)
            .is_err());
    }

    #[test]
    fn task_lists_reports_known_queues() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        store.start(&start_req("d", "w", "req-1")).unwrap();
        let (decisions, activities) = store.task_lists("d");
        assert_eq!(decisions, vec!["tl".to_string()]);
        assert!(activities.is_empty());
    }

    #[test]
    fn restart_terminates_open_run_and_starts_new() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let first = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", first.run_id.clone());
        let restarted = store.restart("d", &exec, "tester").unwrap();
        assert_ne!(restarted.run_id, first.run_id);
        // The open run was terminated by the TerminateIfRunning reuse policy.
        let kinds = event_kinds(&store, "d", &exec);
        assert!(kinds.contains(&EventType::WorkflowExecutionTerminated));
        // The new run carries the original type and a fresh decision.
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        assert_eq!(task.workflow_execution.unwrap().run_id, restarted.run_id);
    }

    #[test]
    fn restart_closed_run_starts_fresh() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let first = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", first.run_id.clone());
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                ..Default::default()
            })
            .unwrap();
        let restarted = store.restart("d", &exec, "tester").unwrap();
        assert_ne!(restarted.run_id, first.run_id);
        let inner = store.inner.read().expect("execution store lock");
        let domain = store.domain_id(&inner, "d");
        let record = inner
            .by_run
            .get(&(domain, "w".to_string(), restarted.run_id.clone()))
            .unwrap();
        let started = record.started_attributes();
        assert_eq!(started.workflow_type.unwrap().name, "wt");
        assert_eq!(started.identity, "tester");
    }

    #[test]
    fn restart_unknown_execution_is_invalid() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let err = store
            .restart("d", &WorkflowExecution::new("w", "nope"), "tester")
            .unwrap_err();
        assert!(matches!(err, StartError::InvalidArgument(_)));
    }

    #[test]
    fn async_requests_are_consumed_into_runs() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        store.enqueue_async_start(start_req("d", "w", "req-async"));
        let drained = store.drain_async_requests();
        assert_eq!(drained.len(), 1);
        assert!(store.consume_async_request(&drained[0]));
        // The run exists and has a pollable decision.
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        assert_eq!(task.workflow_execution.unwrap().workflow_id, "w");
        // Redelivery is idempotent through request-ID dedup.
        assert!(store.consume_async_request(&drained[0]));
        // Invalid requests are dropped, never requeued.
        let bad = AsyncRequest {
            request: AsyncRequestKind::Start(StartWorkflowExecutionRequest {
                domain: "d".to_string(),
                ..Default::default()
            }),
            attempts: 0,
        };
        assert!(store.consume_async_request(&bad));
        // Requeue bounds attempts like a poison-message limit.
        let mut req = drained[0].clone();
        req.attempts = AsyncRequest::MAX_ATTEMPTS - 1;
        store.requeue_async_request(req);
        assert!(store.drain_async_requests().is_empty());
    }

    #[test]
    fn reset_forks_history_at_decision_boundary() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let first = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", first.run_id.clone());
        // Drive one decision with a signal in between: history is
        // Started(1), DecisionScheduled(2), DecisionStarted(3), Signaled(4).
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                signal_name: "ping".to_string(),
                request_id: "sig-1".to_string(),
                ..Default::default()
            })
            .unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                ..Default::default()
            })
            .unwrap();
        // Reset to the DecisionTaskStarted boundary (event 3).
        let reset = store
            .reset(&ResetWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                reason: "test".to_string(),
                decision_finish_event_id: 3,
                request_id: "reset-1".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert_ne!(reset.run_id, first.run_id);
        // Old run terminated; reset run replays the 3-event prefix and
        // reapplies the later signal.
        let inner = store.inner.read().expect("execution store lock");
        let domain = store.domain_id(&inner, "d");
        let old = inner
            .by_run
            .get(&(domain.clone(), "w".to_string(), first.run_id.clone()))
            .unwrap();
        assert_eq!(
            old.close_status,
            Some(WorkflowExecutionCloseStatus::Terminated)
        );
        let new = inner
            .by_run
            .get(&(domain, "w".to_string(), reset.run_id.clone()))
            .unwrap();
        // Prefix (3) + reapplied signal (1) + freshly scheduled decision (1).
        assert_eq!(new.history.len(), 5);
        assert_eq!(
            new.history[3].event_type,
            Some(EventType::WorkflowExecutionSignaled)
        );
        assert!(new.pending_decision.is_some());
        drop(inner);
        // Duplicate request IDs return the reset run without forking again.
        let again = store
            .reset(&ResetWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                decision_finish_event_id: 3,
                request_id: "reset-1".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(again.run_id, reset.run_id);
    }

    #[test]
    fn reset_rejects_bad_points_and_runs() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let req = ResetWorkflowExecutionRequest {
            domain: "d".to_string(),
            workflow_execution: Some(WorkflowExecution::new("w", "nope")),
            decision_finish_event_id: 2,
            request_id: "r".to_string(),
            ..Default::default()
        };
        assert!(matches!(store.reset(&req), Err(ResetError::NotFound(_))));
        let first = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", first.run_id.clone());
        // Event 1 is WorkflowExecutionStarted: not a reset boundary.
        let bad_type = ResetWorkflowExecutionRequest {
            domain: "d".to_string(),
            workflow_execution: Some(exec.clone()),
            decision_finish_event_id: 1,
            request_id: "r".to_string(),
            ..Default::default()
        };
        assert!(matches!(store.reset(&bad_type), Err(ResetError::Invalid(_))));
        // Out of range.
        let bad_range = ResetWorkflowExecutionRequest {
            domain: "d".to_string(),
            workflow_execution: Some(exec),
            decision_finish_event_id: 99,
            request_id: "r".to_string(),
            ..Default::default()
        };
        assert!(matches!(store.reset(&bad_range), Err(ResetError::Invalid(_))));
    }

    #[test]
    fn reset_can_skip_signal_reapply() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let first = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", first.run_id.clone());
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                signal_name: "ping".to_string(),
                request_id: "sig-1".to_string(),
                ..Default::default()
            })
            .unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                ..Default::default()
            })
            .unwrap();
        let reset = store
            .reset(&ResetWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec),
                decision_finish_event_id: 3,
                request_id: "reset-1".to_string(),
                skip_signal_reapply: true,
                ..Default::default()
            })
            .unwrap();
        let inner = store.inner.read().expect("execution store lock");
        let domain = store.domain_id(&inner, "d");
        let new = inner
            .by_run
            .get(&(domain, "w".to_string(), reset.run_id))
            .unwrap();
        // Prefix (3) + freshly scheduled decision (1); the signal was skipped.
        assert_eq!(new.history.len(), 4);
    }

    #[test]
    fn refresh_tasks_requeues_lost_queue_entries() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        // Drop the decision queue entry; refresh restores it.
        {
            let mut inner = store.inner.write().expect("execution store lock");
            let domain = store.domain_id(&inner, "d");
            inner
                .decision_queues
                .get_mut(&(domain, "tl".to_string()))
                .unwrap()
                .pop_front();
        }
        assert!(store.poll_decision("d", "tl", "worker").is_none());
        store.refresh_tasks("d", &exec).unwrap();
        assert!(store.poll_decision("d", "tl", "worker").is_some());
        // Unknown runs are not found; closed runs are a no-op success.
        assert!(store
            .refresh_tasks("d", &WorkflowExecution::new("w", "nope"))
            .is_err());
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                ..Default::default()
            })
            .unwrap();
        store.refresh_tasks("d", &exec).unwrap();
    }

    #[test]
    fn partition_config_round_trips_through_store() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let config = StoredPartitionConfig {
            version: 3,
            read_partitions: [(0, vec!["a".to_string()])].into_iter().collect(),
            write_partitions: HashMap::new(),
        };
        store
            .update_partition_config("d", "tl", 1, true, config)
            .unwrap();
        let got = store.partition_config("d", "tl", 1).unwrap();
        assert_eq!(got.version, 3);
        assert_eq!(got.read_partitions[&0], vec!["a".to_string()]);
        store.refresh_partition_config("d", "tl", 1).unwrap();
        // Non-normal kinds are rejected like Go.
        assert!(store
            .update_partition_config(
                "d",
                "tl",
                1,
                false,
                StoredPartitionConfig::default()
            )
            .is_err());
        // Unknown lists simply have no stored config.
        assert!(store.partition_config("d", "other", 1).is_none());
    }

    #[test]
    fn maintain_healthy_run_deletes_nothing() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        assert_eq!(
            store.maintain_corrupt_workflow("d", &exec).unwrap(),
            (false, false, false)
        );
        // Unknown runs are not found.
        assert!(store
            .maintain_corrupt_workflow("d", &WorkflowExecution::new("w", "nope"))
            .is_err());
    }

    #[test]
    fn raw_history_pages_by_event_id() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        let (events, token) = store.raw_history("d", &exec, None, None, 10, &[]).unwrap();
        assert_eq!(events.len(), 2);
        assert!(token.is_empty());
        let (page, token) = store.raw_history("d", &exec, None, None, 1, &[]).unwrap();
        assert_eq!(page.len(), 1);
        assert!(!token.is_empty());
        let (rest, token2) = store.raw_history("d", &exec, None, None, 10, &token).unwrap();
        assert_eq!(rest.len(), 1);
        assert!(token2.is_empty());
        // Start/end bounds select the decision-scheduled event only.
        let (bounded, _) = store
            .raw_history("d", &exec, Some(2), Some(2), 10, &[])
            .unwrap();
        assert_eq!(bounded.len(), 1);
        assert!(store
            .raw_history("d", &WorkflowExecution::new("w", "nope"), None, None, 10, &[])
            .is_err());
    }

    #[test]
    fn reapply_signals_appends_and_dedups() {
        use cadence_api::service_types::HistoryEvent;
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let signal = |id: &str| HistoryEvent {
            workflow_execution_signaled_event_attributes: Some(
                cadence_api::service_types::WorkflowExecutionSignaledEventAttributes {
                    signal_name: "ping".to_string(),
                    request_id: id.to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        store
            .reapply_signals("d", "w", vec![signal("s-1"), signal("s-2")])
            .unwrap();
        store
            .reapply_signals("d", "w", vec![signal("s-1")])
            .unwrap();
        let inner = store.inner.read().expect("execution store lock");
        let domain = store.domain_id(&inner, "d");
        let record = inner
            .by_run
            .get(&(domain, "w".to_string(), started.run_id))
            .unwrap();
        let signals = record
            .history
            .iter()
            .filter(|e| {
                e.event_type == Some(cadence_api::enums::EventType::WorkflowExecutionSignaled)
            })
            .count();
        assert_eq!(signals, 2);
    }

    #[test]
    fn mutable_state_view_reports_run_facts() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let view = store
            .mutable_state_view("d", &WorkflowExecution::new("w", started.run_id.clone()))
            .unwrap();
        assert_eq!(view.workflow_id, "w");
        assert_eq!(view.next_event_id, 3);
        assert_eq!(view.task_list_name, "tl");
        assert!(store
            .mutable_state_view("d", &WorkflowExecution::new("w", "nope"))
            .is_err());
    }

    #[test]
    fn schedule_and_remove_signal_round_trip() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        // A pending decision already exists; scheduling is idempotent.
        store.schedule_decision_task("d", &exec).unwrap();
        // Buffered signal IDs can be removed; unknown runs fail.
        {
            let mut inner = store.inner.write().expect("execution store lock");
            let domain = store.domain_id(&inner, "d");
            inner
                .by_run
                .get_mut(&(domain, "w".to_string(), started.run_id.clone()))
                .unwrap()
                .signal_request_ids
                .insert("sig-1".to_string());
        }
        store.remove_signal("d", &exec, "sig-1").unwrap();
        // Closed runs report not-exists like Go's ErrNotExists.
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(exec.clone()),
                ..Default::default()
            })
            .unwrap();
        assert!(store.remove_signal("d", &exec, "sig-1").is_err());
        assert!(store
            .schedule_decision_task("d", &WorkflowExecution::new("w", "nope"))
            .is_err());
    }

    #[test]
    fn sync_activity_updates_known_activities() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        // Unknown runs and activities are a success no-op.
        store.sync_activity("d", &exec, 99, b"hb".to_vec(), 2);
        store.sync_activity("d", &WorkflowExecution::new("w", "nope"), 1, vec![], 1);
    }

    #[test]
    fn record_decision_started_paths() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        // Unknown schedule IDs are not found.
        assert!(store
            .record_decision_started("d", &exec, 99, "r1", "worker")
            .is_err());
        let recorded = store
            .record_decision_started("d", &exec, 2, "r1", "worker")
            .unwrap();
        assert_eq!(recorded.scheduled_event_id, 2);
        assert_eq!(recorded.workflow_type_name, "wt");
        // Same request ID re-records idempotently.
        let again = store
            .record_decision_started("d", &exec, 2, "r1", "worker")
            .unwrap();
        assert_eq!(again.started_event_id, recorded.started_event_id);
        // A different request ID on a started task is already-started.
        assert!(matches!(
            store.record_decision_started("d", &exec, 2, "r2", "worker"),
            Err(RecordError::AlreadyStarted(_))
        ));
    }

    #[test]
    fn record_activity_started_paths() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let started = store.start(&start_req("d", "w", "req-1")).unwrap();
        let exec = WorkflowExecution::new("w", started.run_id.clone());
        complete_with(
            &store,
            "d",
            "tl",
            vec![cadence_api::tasks::Decision {
                decision_type: Some(cadence_api::enums::DecisionType::ScheduleActivityTask),
                schedule_activity_task_decision_attributes: Some(
                    cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                        activity_id: "a1".to_string(),
                        activity_type: Some(cadence_api::types::ActivityType {
                            name: "at".to_string(),
                        }),
                        ..Default::default()
                    },
                ),
                ..Default::default()
            }],
        );
        let schedule_id = {
            let inner = store.inner.read().expect("execution store lock");
            let domain = store.domain_id(&inner, "d");
            *inner
                .by_run
                .get(&(domain, "w".to_string(), started.run_id.clone()))
                .unwrap()
                .pending_activities
                .keys()
                .next()
                .unwrap()
        };
        let recorded = store
            .record_activity_started("d", &exec, schedule_id, "worker")
            .unwrap();
        assert!(recorded.scheduled_event.event_id > 0);
        // Re-record returns current metadata.
        let again = store
            .record_activity_started("d", &exec, schedule_id, "worker")
            .unwrap();
        assert_eq!(again.attempt, recorded.attempt);
        assert!(store
            .record_activity_started("d", &exec, 9999, "worker")
            .is_err());
    }

    #[test]
    fn record_child_completed_paths() {
        use cadence_api::service_types::HistoryEvent;
        use cadence_api::tasks::ChildWorkflowExecutionCompletedEventAttributes;
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let parent = store.start(&start_req("d", "parent", "req-1")).unwrap();
        let parent_exec = WorkflowExecution::new("parent", parent.run_id.clone());
        complete_with(
            &store,
            "d",
            "tl",
            vec![start_child_decision("d", "child", ParentClosePolicy::Abandon)],
        );
        let (initiated, child_exec) = {
            let inner = store.inner.read().expect("execution store lock");
            let domain = store.domain_id(&inner, "d");
            let child = &inner
                .children
                .get(&(domain, "parent".to_string(), parent.run_id.clone()))
                .expect("pending child link")[0];
            (
                child.initiated_event_id,
                WorkflowExecution::new(&child.workflow_id, &child.run_id),
            )
        };
        let completed_event = || HistoryEvent {
            event_type: Some(EventType::ChildWorkflowExecutionCompleted),
            child_workflow_execution_completed_event_attributes: Some(
                ChildWorkflowExecutionCompletedEventAttributes {
                    domain: "d".to_string(),
                    workflow_execution: Some(cadence_api::types::WorkflowExecution {
                        workflow_id: child_exec.workflow_id.clone(),
                        run_id: child_exec.run_id.clone(),
                    }),
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        // A non-terminal event is invalid.
        assert!(store
            .record_child_completed(
                "d",
                &parent_exec,
                initiated,
                &child_exec,
                &HistoryEvent {
                    event_type: Some(EventType::WorkflowExecutionSignaled),
                    ..Default::default()
                },
            )
            .is_err());
        // Unknown initiated IDs are not found.
        assert!(store
            .record_child_completed("d", &parent_exec, 9999, &child_exec, &completed_event())
            .is_err());
        // Mismatched child workflow IDs are not found.
        assert!(store
            .record_child_completed(
                "d",
                &parent_exec,
                initiated,
                &WorkflowExecution::new("other", &child_exec.run_id),
                &completed_event(),
            )
            .is_err());
        store
            .record_child_completed("d", &parent_exec, initiated, &child_exec, &completed_event())
            .unwrap();
        let kinds = event_kinds(&store, "d", &parent_exec);
        assert!(kinds.contains(&EventType::ChildWorkflowExecutionCompleted));
        // The link is consumed; a second completion is not found.
        assert!(store
            .record_child_completed("d", &parent_exec, initiated, &child_exec, &completed_event())
            .is_err());
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
    fn cancel_timer_consumes_buffered_fire_and_can_complete_workflow() {
        for close in [false, true] {
            let store = InMemoryExecutionStore::new();
            let started = store.start(&start_req("d", "w", "timer-cancel")).unwrap();
            let task = store.poll_decision("d", "tl", "worker").unwrap();
            store
                .complete_decision(&RespondDecisionTaskCompletedRequest {
                    task_token: task.task_token,
                    decisions: vec![Decision {
                        decision_type: Some(DecisionType::StartTimer),
                        start_timer_decision_attributes: Some(StartTimerDecisionAttributes {
                            timer_id: "timer".into(),
                            start_to_fire_timeout_seconds: Some(1),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .unwrap();
            store
                .signal(&SignalWorkflowExecutionRequest {
                    domain: "d".into(),
                    workflow_execution: Some(WorkflowExecution::new("w", &started.run_id)),
                    signal_name: "cancel".into(),
                    ..Default::default()
                })
                .unwrap();
            let task = store.poll_decision("d", "tl", "worker").unwrap();
            assert_eq!(store.fire_due_timers(i64::MAX / 2), 1);
            let mut decisions = vec![Decision {
                decision_type: Some(DecisionType::CancelTimer),
                cancel_timer_decision_attributes: Some(
                    cadence_api::tasks::CancelTimerDecisionAttributes {
                        timer_id: "timer".into(),
                    },
                ),
                ..Default::default()
            }];
            if close {
                decisions.push(Decision {
                    decision_type: Some(DecisionType::CompleteWorkflowExecution),
                    complete_workflow_execution_decision_attributes: Some(Default::default()),
                    ..Default::default()
                });
            }
            store
                .complete_decision(&RespondDecisionTaskCompletedRequest {
                    task_token: task.task_token,
                    decisions,
                    ..Default::default()
                })
                .unwrap();
            let kinds = event_kinds(&store, "d", &WorkflowExecution::new("w", &started.run_id));
            assert!(kinds.contains(&EventType::TimerCanceled));
            assert!(!kinds.contains(&EventType::TimerFired));
            assert!(!kinds.contains(&EventType::DecisionTaskFailed));
            assert_eq!(
                kinds.contains(&EventType::WorkflowExecutionCompleted),
                close
            );
            assert!(store.poll_decision("d", "tl", "worker").is_none());
        }
    }

    #[test]
    fn cancel_unknown_timer_records_typed_failure() {
        let store = InMemoryExecutionStore::new();
        let started = store.start(&start_req("d", "w", "unknown-timer")).unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![Decision {
                    decision_type: Some(DecisionType::CancelTimer),
                    cancel_timer_decision_attributes: Some(
                        cadence_api::tasks::CancelTimerDecisionAttributes {
                            timer_id: "missing".into(),
                        },
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
        let response = store
            .history("d", &WorkflowExecution::new("w", started.run_id))
            .unwrap();
        let event = response.history.unwrap().events.last().unwrap().clone();
        assert_eq!(event.event_type, Some(EventType::CancelTimerFailed));
        assert_eq!(
            event.cancel_timer_failed_event_attributes.unwrap().cause,
            "TIMER_ID_UNKNOWN"
        );
    }

    #[test]
    fn request_cancel_external_delivers() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("d");
        let target = store.start(&start_req("d", "t", "req-t")).unwrap();
        store.start(&start_req("d", "s", "req-s")).unwrap();
        // Drain target's initial decision so the source decision is polled.
        let target_task = store.poll_decision("d", "tl", "w").unwrap();
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
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: target_task.task_token,
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
    fn external_termination_drops_unhandled_buffered_events() {
        let store = InMemoryExecutionStore::new();
        let run = store
            .start(&start_req("d", "w", "buffered-terminate"))
            .unwrap();
        store.poll_decision("d", "tl", "worker").unwrap();
        let execution = WorkflowExecution::new("w", run.run_id);
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_execution: Some(execution.clone()),
                signal_name: "unhandled".into(),
                ..Default::default()
            })
            .unwrap();
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_execution: Some(execution.clone()),
                ..Default::default()
            })
            .unwrap();
        let history = store.history("d", &execution).unwrap().history.unwrap();
        assert_eq!(history.events.len(), 4);
        assert_eq!(
            history.events.last().unwrap().event_type,
            Some(EventType::WorkflowExecutionTerminated)
        );
        assert!(!history
            .events
            .iter()
            .any(|event| event.event_type == Some(EventType::WorkflowExecutionSignaled)));
    }

    #[test]
    fn buffered_signal_snapshot_restores_dedup_and_private_id_sequence() {
        let store = InMemoryExecutionStore::new();
        let run = store
            .start(&start_req("d", "w", "buffered-restore"))
            .unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        let signal = SignalWorkflowExecutionRequest {
            domain: "d".into(),
            workflow_execution: Some(WorkflowExecution::new("w", &run.run_id)),
            signal_name: "buffered".into(),
            request_id: "same".into(),
            ..Default::default()
        };
        store.signal(&signal).unwrap();
        let mut snapshot = store.snapshot();
        snapshot.runs[0].1.signal_request_ids.clear();
        snapshot.runs[0].1.buffer_event_sequence = 0;
        let restored = InMemoryExecutionStore::restore(snapshot);
        restored.signal(&signal).unwrap();
        restored
            .signal(&SignalWorkflowExecutionRequest {
                request_id: "new".into(),
                ..signal
            })
            .unwrap();
        {
            let inner = restored.inner.read().unwrap();
            let record = &inner.by_run[&("d".to_owned(), "w".to_owned(), run.run_id.clone())];
            assert_eq!(record.buffered_events.len(), 2);
            assert_ne!(
                record.buffered_events[0].event_id,
                record.buffered_events[1].event_id
            );
        }
        restored
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                ..Default::default()
            })
            .unwrap();
        let history = restored
            .history("d", &WorkflowExecution::new("w", run.run_id))
            .unwrap()
            .history
            .unwrap();
        assert_eq!(
            history
                .events
                .iter()
                .filter(|event| event.event_type == Some(EventType::WorkflowExecutionSignaled))
                .count(),
            2
        );
    }

    #[test]
    fn buffered_signal_prevents_workflow_completion_until_replayed() {
        let store = InMemoryExecutionStore::new();
        let run = store.start(&start_req("d", "w", "buffered-close")).unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_execution: Some(WorkflowExecution::new("w", &run.run_id)),
                signal_name: "late".into(),
                request_id: "late-request".into(),
                ..Default::default()
            })
            .unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![cadence_api::tasks::Decision {
                    decision_type: Some(DecisionType::CompleteWorkflowExecution),
                    complete_workflow_execution_decision_attributes: Some(Default::default()),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
        let key = ("d".to_owned(), "w".to_owned(), run.run_id);
        let inner = store.inner.read().unwrap();
        let record = &inner.by_run[&key];
        assert!(record.is_open());
        assert_eq!(
            record.history[3].event_type,
            Some(EventType::DecisionTaskFailed)
        );
        assert_eq!(
            record.history[3]
                .decision_task_failed_event_attributes
                .as_ref()
                .unwrap()
                .cause,
            Some(DecisionTaskFailedCause::UnhandledDecision)
        );
        assert_eq!(
            record.history[4].event_type,
            Some(EventType::WorkflowExecutionSignaled)
        );
        assert_eq!(
            record.history[5].event_type,
            Some(EventType::DecisionTaskScheduled)
        );
        assert!(record.buffered_events.is_empty());
    }

    #[test]
    fn activity_start_and_completion_buffer_and_receive_linked_durable_ids() {
        let store = InMemoryExecutionStore::new();
        let run = store
            .start(&start_req("d", "w", "buffered-activity"))
            .unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![schedule_activity("a")],
                ..Default::default()
            })
            .unwrap();
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_execution: Some(WorkflowExecution::new("w", &run.run_id)),
                signal_name: "wake".into(),
                ..Default::default()
            })
            .unwrap();
        let decision = store.poll_decision("d", "tl", "worker").unwrap();
        let activity = store.poll_activity("d", "tl", "worker").unwrap();
        store
            .complete_activity(&RespondActivityTaskCompletedRequest {
                task_token: activity.task_token,
                result: b"done".to_vec(),
                ..Default::default()
            })
            .unwrap();
        let key = ("d".to_owned(), "w".to_owned(), run.run_id);
        {
            let inner = store.inner.read().unwrap();
            let record = &inner.by_run[&key];
            assert_eq!(record.buffered_events.len(), 2);
            assert!(!record
                .history
                .iter()
                .any(|event| event.event_type == Some(EventType::ActivityTaskStarted)));
        }
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: decision.task_token,
                ..Default::default()
            })
            .unwrap();
        let inner = store.inner.read().unwrap();
        let record = &inner.by_run[&key];
        let started = record
            .history
            .iter()
            .find(|event| event.event_type == Some(EventType::ActivityTaskStarted))
            .unwrap();
        let completed = record
            .history
            .iter()
            .find_map(|event| event.activity_task_completed_event_attributes.as_ref())
            .unwrap();
        assert!(started.event_id > decision.started_event_id);
        assert_eq!(completed.started_event_id, started.event_id);
        assert!(record.history.iter().all(|event| event.event_id > 0));
    }

    #[test]
    fn scheduling_activity_does_not_create_an_extra_decision() {
        let store = InMemoryExecutionStore::new();
        setup_activity(&store);
        assert!(store.poll_decision("d", "tl", "worker").is_none());
    }

    #[test]
    fn force_request_creates_another_decision() {
        let store = InMemoryExecutionStore::new();
        store.start(&start_req("d", "w", "forced-task")).unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                force_create_new_decision_task: true,
                ..Default::default()
            })
            .unwrap();
        assert!(store.poll_decision("d", "tl", "worker").is_some());
    }

    #[test]
    fn heartbeat_reports_cancel_requested() {
        let store = InMemoryExecutionStore::new();
        let token = setup_activity(&store);
        // No cancel requested yet.
        assert!(!store.record_heartbeat(&token, b"p1".to_vec()).unwrap());
        // An external signal wakes the workflow so it can request activity cancellation.
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_execution: Some(WorkflowExecution::new("w", "")),
                signal_name: "cancel-activity".into(),
                request_id: "wake-cancel".into(),
                ..Default::default()
            })
            .unwrap();
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

    fn pending_for_retry(
        policy: Option<RetryPolicy>,
        attempt: i32,
        scheduled: i64,
    ) -> PendingActivity {
        PendingActivity {
            activity_id: "a".into(),
            activity_type: cadence_api::types::ActivityType {
                name: "activity".into(),
            },
            input: Vec::new(),
            task_list: TaskList {
                name: "tl".into(),
                kind: None,
                base_name: String::new(),
            },
            scheduled_event_id: 1,
            started_event_id: Some(2),
            attempt,
            cancel_requested: false,
            cancel_requested_event_id: None,
            heartbeat_details: Vec::new(),
            queued: false,
            retry_ready_millis: None,
            schedule_to_start_timeout_seconds: None,
            schedule_to_close_timeout_seconds: None,
            start_to_close_timeout_seconds: None,
            heartbeat_timeout_seconds: None,
            scheduled_time_millis: scheduled,
            attempt_scheduled_time_millis: None,
            started_time_millis: None,
            last_heartbeat_time_millis: None,
            started_identity: String::new(),
            started_request_id: String::new(),
            last_failure_reason: None,
            last_failure_details: Vec::new(),
            last_failure_options: None,
            retry_policy: policy,
            header: None,
        }
    }

    #[test]
    fn activity_retry_policy_is_validated_before_history_mutation() {
        let store = InMemoryExecutionStore::new();
        store.start(&start_req("d", "w", "invalid-retry")).unwrap();
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        let policy = RetryPolicy {
            initial_interval_in_seconds: 1,
            maximum_attempts: 3,
            ..Default::default()
        };
        let request = RespondDecisionTaskCompletedRequest {
            task_token: task.task_token,
            decisions: vec![schedule_with_retry(policy)],
            ..Default::default()
        };
        let key = (
            "d".to_owned(),
            "w".to_owned(),
            task.workflow_execution.unwrap().run_id,
        );
        let before = store.inner.read().unwrap().by_run[&key].history.len();
        let error = store.complete_decision(&request).unwrap_err();
        assert!(format!("{error:?}").contains("BackoffCoefficient cannot be less than 1"));
        assert_eq!(
            store.inner.read().unwrap().by_run[&key].history.len(),
            before
        );
        let mut corrected = request;
        corrected.decisions[0]
            .schedule_activity_task_decision_attributes
            .as_mut()
            .unwrap()
            .retry_policy
            .as_mut()
            .unwrap()
            .backoff_coefficient = 2.0;
        store.complete_decision(&corrected).unwrap();
        assert!(store.poll_activity("d", "tl", "worker").is_some());
    }

    fn timeout_activity(
        store: &InMemoryExecutionStore,
        attrs: cadence_api::tasks::ScheduleActivityTaskDecisionAttributes,
    ) -> String {
        let run = store
            .start(&start_req("d", "w", "timeout-test"))
            .unwrap()
            .run_id;
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![cadence_api::tasks::Decision {
                    decision_type: Some(DecisionType::ScheduleActivityTask),
                    schedule_activity_task_decision_attributes: Some(
                        cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                            activity_id: "a".into(),
                            activity_type: Some(ActivityType {
                                name: "activity".into(),
                            }),
                            ..attrs
                        },
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
        run
    }

    #[test]
    fn queued_activity_timeouts_have_no_started_event_and_skip_stale_queue_entries() {
        for kind in [TimeoutType::ScheduleToStart, TimeoutType::ScheduleToClose] {
            let store = InMemoryExecutionStore::new();
            let run = timeout_activity(
                &store,
                cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                    schedule_to_start_timeout_seconds: (kind == TimeoutType::ScheduleToStart)
                        .then_some(1),
                    schedule_to_close_timeout_seconds: (kind == TimeoutType::ScheduleToClose)
                        .then_some(1),
                    ..Default::default()
                },
            );
            let key = ("d".to_owned(), "w".to_owned(), run);
            let scheduled = store.inner.read().unwrap().by_run[&key]
                .pending_activities
                .values()
                .next()
                .unwrap()
                .scheduled_time_millis;
            assert_eq!(store.expire_due_activities(scheduled + 999), 0);
            assert_eq!(store.expire_due_activities(scheduled + 1000), 1);
            assert_eq!(store.expire_due_activities(scheduled + 2000), 0);
            assert!(store.poll_activity("d", "tl", "worker").is_none());
            let inner = store.inner.read().unwrap();
            let event = inner.by_run[&key]
                .history
                .iter()
                .find_map(|event| event.activity_task_timed_out_event_attributes.as_ref())
                .unwrap();
            assert_eq!(event.started_event_id, 0);
            assert_eq!(event.timeout_type, Some(kind));
        }
    }

    #[test]
    fn heartbeat_deadline_moves_and_preserves_details() {
        let store = InMemoryExecutionStore::new();
        let run = timeout_activity(
            &store,
            cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                heartbeat_timeout_seconds: Some(1),
                start_to_close_timeout_seconds: Some(30),
                ..Default::default()
            },
        );
        let task = store.poll_activity("d", "tl", "worker").unwrap();
        store
            .record_heartbeat(&task.task_token, b"checkpoint".to_vec())
            .unwrap();
        let key = ("d".to_owned(), "w".to_owned(), run);
        let heartbeat = store.inner.read().unwrap().by_run[&key]
            .pending_activities
            .values()
            .next()
            .unwrap()
            .last_heartbeat_time_millis
            .unwrap();
        assert_eq!(store.expire_due_activities(heartbeat + 999), 0);
        assert_eq!(store.expire_due_activities(heartbeat + 1000), 1);
        assert!(store.record_heartbeat(&task.task_token, vec![]).is_err());
        let inner = store.inner.read().unwrap();
        let event = inner.by_run[&key]
            .history
            .iter()
            .find_map(|event| event.activity_task_timed_out_event_attributes.as_ref())
            .unwrap();
        assert_eq!(event.timeout_type, Some(TimeoutType::Heartbeat));
        assert_eq!(event.details, b"checkpoint");
    }

    #[test]
    fn activity_timeout_retries_are_transient_and_old_tokens_expire() {
        let store = InMemoryExecutionStore::new();
        let run = timeout_activity(
            &store,
            cadence_api::tasks::ScheduleActivityTaskDecisionAttributes {
                start_to_close_timeout_seconds: Some(1),
                retry_policy: Some(retry_policy(2)),
                ..Default::default()
            },
        );
        let task = store.poll_activity("d", "tl", "worker").unwrap();
        let key = ("d".to_owned(), "w".to_owned(), run);
        assert_eq!(
            store.expire_due_activities(task.started_timestamp.unwrap() + 1000),
            1
        );
        assert!(store.record_heartbeat(&task.task_token, vec![]).is_err());
        let ready = store.inner.read().unwrap().by_run[&key]
            .pending_activities
            .values()
            .next()
            .unwrap()
            .retry_ready_millis
            .unwrap();
        assert_eq!(store.requeue_due_activities(ready - 1), 0);
        assert_eq!(store.requeue_due_activities(ready), 1);
        let retry = store.poll_activity("d", "tl", "worker").unwrap();
        assert_eq!(retry.attempt, 1);
        assert_eq!(
            store.expire_due_activities(retry.started_timestamp.unwrap() + 1000),
            1
        );
        let inner = store.inner.read().unwrap();
        let record = &inner.by_run[&key];
        assert_eq!(
            record
                .history
                .iter()
                .filter(|event| event.event_type == Some(EventType::ActivityTaskStarted))
                .count(),
            1
        );
        let timeouts: Vec<_> = record
            .history
            .iter()
            .filter_map(|event| event.activity_task_timed_out_event_attributes.as_ref())
            .collect();
        assert_eq!(timeouts.len(), 1);
        assert_eq!(
            timeouts[0].last_failure_reason.as_deref(),
            Some("cadenceInternal:Timeout START_TO_CLOSE")
        );
        assert_eq!(timeouts[0].timeout_type, Some(TimeoutType::StartToClose));
        assert!(timeouts[0].started_event_id > timeouts[0].scheduled_event_id);
    }

    #[test]
    fn retry_backoff_and_overrides_match_go_policy() {
        let policy = RetryPolicy {
            initial_interval_in_seconds: 3,
            backoff_coefficient: 2.0,
            maximum_attempts: 5,
            expiration_interval_in_seconds: 60,
            ..Default::default()
        };
        // The SDK sends its default coefficient explicitly; the server does not default it.
        let pending = pending_for_retry(Some(policy.clone()), 1, 10_000);
        assert_eq!(
            InMemoryExecutionStore::activity_retry_time(
                &fail(Vec::new(), Some("boom")),
                &pending,
                20_000
            ),
            Some(26_000)
        );
        let override_req = RespondActivityTaskFailedRequest {
            failure_options: Some(FailureOptions {
                next_retry_interval_seconds: Some(7),
                ..Default::default()
            }),
            ..fail(Vec::new(), Some("boom"))
        };
        assert_eq!(
            InMemoryExecutionStore::activity_retry_time(&override_req, &pending, 20_000),
            Some(27_000)
        );
        assert_eq!(
            InMemoryExecutionStore::activity_retry_time(
                &fail(Vec::new(), Some("boom")),
                &pending_for_retry(None, 0, 10_000),
                20_000
            ),
            None
        );
        assert_eq!(
            InMemoryExecutionStore::activity_retry_time(
                &fail(Vec::new(), Some("boom")),
                &pending_for_retry(Some(policy), 0, -60_000),
                20_000
            ),
            None
        );
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
        let scheduled_id = InMemoryExecutionStore::decode_task_token(&token)
            .unwrap()
            .schedule_id;
        store
            .fail_activity(&fail(token.clone(), Some("boom")))
            .unwrap();
        // Go keeps one Scheduled event and buffers retry starts until terminal.
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
            1
        );
        assert!(!hist
            .events
            .iter()
            .any(|e| e.event_type == Some(EventType::ActivityTaskStarted)));
        // Not immediately repollable (backoff); due-loop requeues it.
        assert!(store.poll_activity("d", "tl", "w").is_none());
        assert_eq!(store.requeue_due_activities(i64::MAX / 2), 1);
        // Tokens from the previous attempt cannot complete a retried activity.
        assert!(store
            .complete_activity(&RespondActivityTaskCompletedRequest {
                task_token: token,
                identity: "worker".into(),
                ..Default::default()
            })
            .is_err());
        let retry = store.poll_activity("d", "tl", "w").unwrap();
        assert_eq!(retry.attempt, 1);
        let retry_token = retry.task_token.clone();
        assert_eq!(
            InMemoryExecutionStore::decode_task_token(&retry_token)
                .unwrap()
                .schedule_id,
            scheduled_id
        );
        assert!(
            retry.scheduled_timestamp_of_this_attempt.unwrap() > retry.scheduled_timestamp.unwrap()
        );
        store
            .complete_activity(&RespondActivityTaskCompletedRequest {
                task_token: retry_token,
                identity: "worker".into(),
                ..Default::default()
            })
            .unwrap();
        let hist = store
            .history("d", &WorkflowExecution::new("w", String::new()))
            .unwrap()
            .history
            .unwrap();
        let started: Vec<_> = hist
            .events
            .iter()
            .filter_map(|e| e.activity_task_started_event_attributes.as_ref())
            .collect();
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].attempt, 1);
        assert_eq!(started[0].scheduled_event_id, scheduled_id);
        assert_eq!(
            hist.events
                .iter()
                .filter(|e| e.event_type == Some(EventType::ActivityTaskScheduled))
                .count(),
            1
        );
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
            workflow_type: Some(WorkflowType {
                name: "wt".to_string(),
            }),
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
        assert_eq!(desc.workflow_execution_info.unwrap().history_length, 2);
        assert!(loaded.poll_decision("d", "tl", "w").is_some());
        // Request-ID idempotency survived too.
        let again = loaded.start(&req).unwrap();
        assert_eq!(again.run_id, started.run_id);
    }
}

#[cfg(test)]
mod history_cursor_tests {
    use super::*;
    fn request(id: &str, rid: &str) -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            domain: "d".into(),
            workflow_id: id.into(),
            request_id: rid.into(),
            workflow_type: Some(WorkflowType {
                name: "type".into(),
            }),
            task_list: Some(TaskList {
                name: "tasks".into(),
                ..Default::default()
            }),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }
    }
    #[test]
    fn worker_history_pages_freeze_before_signals_arriving_during_replay() {
        let store = InMemoryExecutionStore::new();
        let run = store.start(&request("workflow", "start")).unwrap();
        let execution = WorkflowExecution::new("workflow", &run.run_id);
        for n in 0..1001 {
            store
                .signal(&SignalWorkflowExecutionRequest {
                    domain: "d".into(),
                    workflow_execution: Some(execution.clone()),
                    signal_name: "before-poll".into(),
                    request_id: format!("signal-{n}"),
                    ..Default::default()
                })
                .unwrap();
        }
        let task = store.poll_decision("d", "tasks", "worker").unwrap();
        assert_eq!(task.history.as_ref().unwrap().events.len(), 1000);
        assert_eq!(task.next_event_id, task.started_event_id + 1);
        assert!(task.scheduled_timestamp.is_some());
        assert!(task.started_timestamp.is_some());
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_execution: Some(execution.clone()),
                signal_name: "after-poll".into(),
                request_id: "late".into(),
                ..Default::default()
            })
            .unwrap();
        let rest = store
            .history_page("d", &execution, 5000, &task.next_page_token, None)
            .unwrap();
        let rest = rest.history.unwrap().events;
        assert_eq!(rest.last().unwrap().event_id, task.started_event_id);
        assert_eq!(
            rest.last().unwrap().event_type,
            Some(EventType::DecisionTaskStarted)
        );
        assert_eq!(rest.len(), 4);
        assert!(store
            .history_page("other", &execution, 1000, &task.next_page_token, None)
            .is_err());
        let fresh = store.history_page("d", &execution, 0, &[], None).unwrap();
        assert_eq!(fresh.history.unwrap().events.len(), 1000);
        let rest = store
            .history_page("d", &execution, 0, &fresh.next_page_token, None)
            .unwrap();
        assert_eq!(
            rest.history.unwrap().events.last().unwrap().event_type,
            Some(EventType::DecisionTaskStarted)
        );
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                ..Default::default()
            })
            .unwrap();
        let inner = store.inner.read().unwrap();
        let record = &inner.by_run[&("d".to_owned(), "workflow".to_owned(), run.run_id)];
        let signal = record
            .history
            .iter()
            .find(|event| {
                event
                    .workflow_execution_signaled_event_attributes
                    .as_ref()
                    .is_some_and(|attrs| attrs.request_id == "late")
            })
            .unwrap();
        assert_eq!(signal.event_id, task.started_event_id + 2);
        assert!(record.buffered_events.is_empty());
    }
    #[test]
    fn continuation_without_run_id_keeps_original_run_after_reuse() {
        let store = InMemoryExecutionStore::new();
        let run = store.start(&request("workflow", "first")).unwrap();
        let execution = WorkflowExecution::new("workflow", &run.run_id);
        let first = store
            .history_page("d", &WorkflowExecution::new("workflow", ""), 1, &[], None)
            .unwrap();
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_execution: Some(execution),
                ..Default::default()
            })
            .unwrap();
        let mut second = request("workflow", "second");
        second.workflow_id_reuse_policy = Some(WorkflowIDReusePolicy::AllowDuplicate);
        let next_run = store.start(&second).unwrap();
        assert_ne!(next_run.run_id, run.run_id);
        let rest = store
            .history_page(
                "d",
                &WorkflowExecution::new("workflow", ""),
                1000,
                &first.next_page_token,
                None,
            )
            .unwrap();
        assert_eq!(rest.history.unwrap().events.len(), 1);
        assert!(rest.next_page_token.is_empty());
    }
}

#[cfg(test)]
mod workflow_timeout_tests {
    use super::*;

    #[test]
    fn workflow_timeout_closes_once_and_rejects_stale_task_completion() {
        let store = InMemoryExecutionStore::new();
        let response = store
            .start(&StartWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_id: "timeout".into(),
                request_id: "request".into(),
                workflow_type: Some(WorkflowType {
                    name: "workflow".into(),
                }),
                task_list: Some(TaskList {
                    name: "tasks".into(),
                    ..Default::default()
                }),
                execution_start_to_close_timeout_seconds: Some(2),
                task_start_to_close_timeout_seconds: Some(1),
                ..Default::default()
            })
            .unwrap();
        let execution = WorkflowExecution::new("timeout", response.run_id);
        let task = store.poll_decision("d", "tasks", "worker").unwrap();
        let start = store
            .describe("d", &execution)
            .unwrap()
            .workflow_execution_info
            .unwrap()
            .start_time
            .unwrap();
        assert_eq!(store.expire_due_workflows(start + 1999), 0);
        assert_eq!(store.expire_due_workflows(start + 2000), 1);
        assert_eq!(store.expire_due_workflows(start + 3000), 0);
        assert_eq!(
            store
                .describe("d", &execution)
                .unwrap()
                .workflow_execution_info
                .unwrap()
                .close_status,
            Some(WorkflowExecutionCloseStatus::TimedOut)
        );
        assert!(store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                ..Default::default()
            })
            .is_err());
        let history = store.history("d", &execution).unwrap().history.unwrap();
        let terminal = history.events.last().unwrap();
        assert_eq!(
            terminal.event_type,
            Some(EventType::WorkflowExecutionTimedOut)
        );
        assert_eq!(
            terminal
                .workflow_execution_timed_out_event_attributes
                .as_ref()
                .unwrap()
                .timeout_type,
            Some(TimeoutType::StartToClose)
        );
    }
}

#[cfg(test)]
mod namespace_tests {
    use super::*;
    fn start_request(domain: &str) -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            domain: domain.into(),
            workflow_id: "workflow".into(),
            request_id: "start-request".into(),
            workflow_type: Some(WorkflowType {
                name: "workflow-type".into(),
            }),
            task_list: Some(TaskList {
                name: "tasks".into(),
                ..Default::default()
            }),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        }
    }
    #[test]
    fn delayed_start_defers_the_first_decision_task() {
        let store = InMemoryExecutionStore::new();
        store.register_domain("domain", &uuid::Uuid::new_v4().to_string());
        let mut request = start_request("domain");
        request.workflow_id = "delayed".into();
        request.request_id = "delayed-request".into();
        // Staggered request IDs keep the idempotency keys separate.
        request.delay_start_seconds = Some(120);
        let started = store.start(&request).unwrap();
        // Go creates a WorkflowBackoffTimerTask instead of the decision task.
        assert!(store.list("domain", true).len() == 1);
        let inner = store.inner.read().unwrap();
        let domain = store.domain_id(&inner, "domain");
        let record = inner
            .by_run
            .get(&(domain, "delayed".to_owned(), started.run_id.clone()))
            .unwrap()
            .clone();
        drop(inner);
        assert_eq!(record.first_decision_task_backoff_seconds, 120);
        assert!(record.pending_decision.is_none());
        assert!(store.poll_decision("domain", "tasks", "worker").is_none());
        assert_eq!(
            store.fire_due_backoffs(record.start_time_millis + 119_000),
            0
        );
        assert!(store.poll_decision("domain", "tasks", "worker").is_none());
        assert_eq!(
            store.fire_due_backoffs(record.start_time_millis + 121_000),
            1
        );
        let task = store.poll_decision("domain", "tasks", "worker").unwrap();
        assert_eq!(
            task.workflow_execution.as_ref().unwrap().run_id,
            started.run_id
        );
    }

    #[test]
    fn cron_schedule_records_the_computed_backoff() {
        let store = InMemoryExecutionStore::new();
        store.register_domain("domain", &uuid::Uuid::new_v4().to_string());
        let mut request = start_request("domain");
        request.workflow_id = "cron".into();
        request.request_id = "cron-request".into();
        request.cron_schedule = "0 * * * *".into();
        request.cron_overlap_policy = Some(cadence_api::enums::CronOverlapPolicy::Skipped);
        let started = store.start(&request).unwrap();
        let inner = store.inner.read().unwrap();
        let domain = store.domain_id(&inner, "domain");
        let record = inner
            .by_run
            .get(&(domain, "cron".to_owned(), started.run_id.clone()))
            .unwrap();
        let attr = record
            .history
            .first()
            .unwrap()
            .workflow_execution_started_event_attributes
            .as_ref()
            .unwrap();
        // The backoff is the seconds until the next hour boundary.
        let backoff = attr.first_decision_task_backoff_seconds.unwrap();
        assert!(backoff > 0 && backoff <= 3600, "backoff was {backoff}");
        assert!(record.pending_decision.is_none());
        assert_eq!(attr.cron_schedule, "0 * * * *");
        assert_eq!(record.first_decision_task_backoff_seconds, backoff);
    }

    #[test]
    fn zero_backoff_still_schedules_immediately() {
        let store = InMemoryExecutionStore::new();
        store.register_domain("domain", &uuid::Uuid::new_v4().to_string());
        let started = store.start(&start_request("domain")).unwrap();
        let task = store.poll_decision("domain", "tasks", "worker").unwrap();
        assert_eq!(
            task.workflow_execution.as_ref().unwrap().run_id,
            started.run_id
        );
    }

    #[test]
    fn timed_out_cron_run_continues_as_new() {
        let store = InMemoryExecutionStore::new();
        store.register_domain("domain", &uuid::Uuid::new_v4().to_string());
        let mut request = start_request("domain");
        request.workflow_id = "cron-timeout".into();
        request.request_id = "cron-timeout-request".into();
        request.cron_schedule = "* * * * *".into();
        request.cron_overlap_policy = Some(cadence_api::enums::CronOverlapPolicy::Skipped);
        request.execution_start_to_close_timeout_seconds = Some(1);
        let started = store.start(&request).unwrap();
        // The cron backoff defers the first decision task.
        let inner = store.inner.read().unwrap();
        let domain = store.domain_id(&inner, "domain");
        assert!(inner
            .by_run
            .get(&(
                domain.clone(),
                "cron-timeout".to_owned(),
                started.run_id.clone()
            ))
            .unwrap()
            .pending_decision
            .is_none());
        drop(inner);
        // Past the execution timeout, the run is continued as new rather than
        // closed as timed out.
        let start_millis = store
            .inner
            .read()
            .unwrap()
            .by_run
            .get(&(
                domain.clone(),
                "cron-timeout".to_owned(),
                started.run_id.clone(),
            ))
            .unwrap()
            .start_time_millis;
        let closed = store.expire_due_workflows(start_millis + 61_000);
        assert_eq!(closed, 1);
        let inner = store.inner.read().unwrap();
        let runs = inner
            .by_workflow
            .get(&(domain, "cron-timeout".to_owned()))
            .unwrap();
        assert_eq!(runs.len(), 2, "expected a new continue-as-new run");
        let new_run = runs.last().unwrap();
        let new_record = inner
            .by_run
            .get(&(
                store.domain_id(&store.inner.read().unwrap(), "domain"),
                "cron-timeout".to_owned(),
                new_run.clone(),
            ))
            .unwrap();
        assert!(new_record.is_open());
        // The old run is closed as continued-as-new, not timed out.
        let old = inner
            .by_run
            .get(&(
                store.domain_id(&store.inner.read().unwrap(), "domain"),
                "cron-timeout".to_owned(),
                started.run_id.clone(),
            ))
            .unwrap();
        assert_eq!(
            old.close_status,
            Some(WorkflowExecutionCloseStatus::ContinuedAsNew)
        );
        assert_eq!(new_record.cron_schedule(), "* * * * *");
    }

    #[test]
    fn timed_out_run_without_retry_or_cron_closes_as_timed_out() {
        let store = InMemoryExecutionStore::new();
        store.register_domain("domain", &uuid::Uuid::new_v4().to_string());
        let mut request = start_request("domain");
        request.workflow_id = "plain-timeout".into();
        request.request_id = "plain-timeout-request".into();
        request.execution_start_to_close_timeout_seconds = Some(1);
        let started = store.start(&request).unwrap();
        let start_millis = store
            .inner
            .read()
            .unwrap()
            .by_run
            .get(&(
                store.domain_id(&store.inner.read().unwrap(), "domain"),
                "plain-timeout".to_owned(),
                started.run_id.clone(),
            ))
            .unwrap()
            .start_time_millis;
        assert_eq!(store.expire_due_workflows(start_millis + 61_000), 1);
        let inner = store.inner.read().unwrap();
        let record = inner
            .by_run
            .get(&(
                store.domain_id(&inner, "domain"),
                "plain-timeout".to_owned(),
                started.run_id,
            ))
            .unwrap();
        assert_eq!(
            record.close_status,
            Some(WorkflowExecutionCloseStatus::TimedOut)
        );
    }

    #[test]
    fn workflow_retry_can_back_off_longer_than_one_day() {
        assert_eq!(
            InMemoryExecutionStore::retry_backoff_seconds(
                0,
                172_800,
                0,
                2.0,
                0.0,
                3,
                0,
                None,
                0,
                &[],
                "boom",
            ),
            Some(172_800)
        );
    }

    #[test]
    fn failed_workflow_retries_and_preserves_failure_metadata() {
        use cadence_api::tasks::{Decision, FailWorkflowExecutionDecisionAttributes};
        let store = InMemoryExecutionStore::new();
        let mut request = start_request("domain");
        request.retry_policy = Some(RetryPolicy {
            initial_interval_in_seconds: 1,
            backoff_coefficient: 2.0,
            maximum_attempts: 2,
            ..Default::default()
        });
        let started = store.start(&request).unwrap();
        let task = store.poll_decision("domain", "tasks", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![Decision {
                    decision_type: Some(DecisionType::FailWorkflowExecution),
                    fail_workflow_execution_decision_attributes: Some(
                        FailWorkflowExecutionDecisionAttributes {
                            reason: Some("boom".into()),
                            details: b"failure".to_vec(),
                        },
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
        let inner = store.inner.read().unwrap();
        let runs = &inner.by_workflow[&("domain".to_owned(), request.workflow_id.clone())];
        assert_eq!(runs.len(), 2);
        let successor = &inner.by_run[&(
            "domain".to_owned(),
            request.workflow_id.clone(),
            runs[1].clone(),
        )];
        assert_eq!(successor.attempt, 1);
        let attrs = successor.started_attributes();
        assert_eq!(attrs.continued_execution_run_id, started.run_id);
        assert_eq!(attrs.continued_failure_reason, "boom");
        assert_eq!(attrs.continued_failure_details, b"failure");
        assert_eq!(
            attrs.initiator,
            Some(cadence_api::enums::ContinueAsNewInitiator::RetryPolicy)
        );
    }

    #[test]
    fn completed_cron_workflow_continues_with_last_completion_result() {
        use cadence_api::tasks::{CompleteWorkflowExecutionDecisionAttributes, Decision};
        let store = InMemoryExecutionStore::new();
        let mut request = start_request("domain");
        request.cron_schedule = "@every 1s".into();
        store.start(&request).unwrap();
        store.fire_due_backoffs(now_millis() + 2_000);
        let task = store.poll_decision("domain", "tasks", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![Decision {
                    decision_type: Some(DecisionType::CompleteWorkflowExecution),
                    complete_workflow_execution_decision_attributes: Some(
                        CompleteWorkflowExecutionDecisionAttributes {
                            result: b"cron-result".to_vec(),
                        },
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
        let inner = store.inner.read().unwrap();
        let runs = &inner.by_workflow[&("domain".to_owned(), request.workflow_id.clone())];
        assert_eq!(runs.len(), 2);
        let successor = &inner.by_run[&(
            "domain".to_owned(),
            request.workflow_id.clone(),
            runs[1].clone(),
        )];
        assert_eq!(successor.attempt, 0);
        assert_eq!(
            successor.started_attributes().last_completion_result,
            b"cron-result"
        );
        assert_eq!(
            successor.started_attributes().initiator,
            Some(cadence_api::enums::ContinueAsNewInitiator::CronSchedule)
        );
    }

    #[test]
    fn cron_recurrence_preserves_internal_jitter_metadata() {
        let store = InMemoryExecutionStore::new();
        let mut request = start_request("domain");
        request.cron_schedule = "@every 1s".into();
        request.jitter_start_seconds = Some(7);
        let started = store.start(&request).unwrap();
        let key = (
            "domain".to_owned(),
            request.workflow_id.clone(),
            started.run_id,
        );
        let started_at = store.inner.read().unwrap().by_run[&key].start_time_millis;
        assert_eq!(store.expire_due_workflows(started_at + 100_000), 1);
        let inner = store.inner.read().unwrap();
        let runs = &inner.by_workflow[&("domain".to_owned(), request.workflow_id.clone())];
        let next = &inner.by_run[&(
            "domain".to_owned(),
            request.workflow_id.clone(),
            runs[1].clone(),
        )];
        assert_eq!(next.jitter_start_seconds, 7);
        assert_eq!(
            InMemoryExecutionStore::start_attributes(next).jitter_start_seconds,
            Some(7)
        );
    }

    #[test]
    fn retry_policy_continues_as_new_attempts() {
        let store = InMemoryExecutionStore::new();
        store.register_domain("domain", &uuid::Uuid::new_v4().to_string());
        let mut request = start_request("domain");
        request.workflow_id = "retry-timeout".into();
        request.request_id = "retry-timeout-request".into();
        request.execution_start_to_close_timeout_seconds = Some(1);
        request.retry_policy = Some(cadence_api::types::RetryPolicy {
            initial_interval_in_seconds: 1,
            backoff_coefficient: 2.0,
            maximum_interval_in_seconds: 60,
            maximum_attempts: 5,
            expiration_interval_in_seconds: 3600,
            ..Default::default()
        });
        let started = store.start(&request).unwrap();
        let start_millis = store
            .inner
            .read()
            .unwrap()
            .by_run
            .get(&(
                store.domain_id(&store.inner.read().unwrap(), "domain"),
                "retry-timeout".to_owned(),
                started.run_id.clone(),
            ))
            .unwrap()
            .start_time_millis;
        assert_eq!(store.expire_due_workflows(start_millis + 61_000), 1);
        let inner = store.inner.read().unwrap();
        let domain = store.domain_id(&inner, "domain");
        let runs = inner
            .by_workflow
            .get(&(domain.clone(), "retry-timeout".to_owned()))
            .unwrap();
        assert_eq!(runs.len(), 2);
        let new_record = inner
            .by_run
            .get(&(domain, "retry-timeout".to_owned(), runs[1].clone()))
            .unwrap();
        let next_start = new_record.started_attributes();
        assert_eq!(next_start.continued_execution_run_id, started.run_id);
        assert_eq!(next_start.first_execution_run_id, started.run_id);
        assert_eq!(
            next_start.original_execution_run_id,
            new_record.execution.run_id
        );
        assert_eq!(
            next_start.initiator,
            Some(cadence_api::enums::ContinueAsNewInitiator::RetryPolicy)
        );
        assert_eq!(
            next_start.continued_failure_reason,
            "cadenceInternal:Timeout START_TO_CLOSE"
        );
        assert_eq!(
            next_start.expiration_timestamp,
            new_record.expiration_time_nanos
        );
        let old = &inner.by_run[&(
            store.domain_id(&inner, "domain"),
            "retry-timeout".to_owned(),
            started.run_id.clone(),
        )];
        let continued = old
            .history
            .last()
            .unwrap()
            .workflow_execution_continued_as_new_event_attributes
            .as_ref()
            .unwrap();
        assert_eq!(continued.initiator, next_start.initiator);
        assert_eq!(
            continued.failure_reason.as_deref(),
            Some("cadenceInternal:Timeout START_TO_CLOSE")
        );
        assert_eq!(continued.backoff_start_interval_in_seconds, Some(1));
        assert_eq!(new_record.attempt, 1);
        assert!(new_record.retry_policy().is_some());
        // The retry expiration timestamp is carried over to the retry run.
        assert!(new_record.expiration_time_nanos.is_some());
    }

    #[test]
    fn recreating_domain_isolates_runs_idempotency_and_task_tokens() {
        let store = InMemoryExecutionStore::new();
        let old = uuid::Uuid::new_v4().to_string();
        let new = uuid::Uuid::new_v4().to_string();
        store.register_domain("domain", &old);
        let first = store.start(&start_request("domain")).unwrap();
        let task = store.poll_decision("domain", "tasks", "worker").unwrap();
        assert_eq!(
            JsonTaskTokenSerializer::deserialize(&task.task_token)
                .unwrap()
                .domain_id,
            old
        );
        store.unregister_domain("domain");
        store.register_domain("domain", &new);
        assert!(store.list("domain", true).is_empty());
        assert!(store
            .describe("domain", &WorkflowExecution::new("workflow", &first.run_id))
            .is_err());
        let second = store.start(&start_request("domain")).unwrap();
        assert_ne!(first.run_id, second.run_id);
        let task = store.poll_decision("domain", "tasks", "worker").unwrap();
        assert_eq!(
            JsonTaskTokenSerializer::deserialize(&task.task_token)
                .unwrap()
                .domain_id,
            new
        );
    }
    #[test]
    fn uuid_shaped_domain_name_cannot_alias_another_domain_namespace() {
        let store = InMemoryExecutionStore::new();
        let first_id = uuid::Uuid::new_v4().to_string();
        let second_id = uuid::Uuid::new_v4().to_string();
        store.register_domain("first", &first_id);
        let first = store.start(&start_request("first")).unwrap();
        store.register_domain(&first_id, &second_id);
        let second = store.start(&start_request(&first_id)).unwrap();
        assert_eq!(store.list("first", true).len(), 1);
        assert_eq!(store.list(&first_id, true).len(), 1);
        let mut other_request = start_request("first");
        other_request.request_id = "another-request".into();
        match store.start(&other_request).unwrap_err() {
            StartError::AlreadyStarted(error) => assert_eq!(error.run_id, first.run_id),
            other => panic!("expected already-started, got {other:?}"),
        }
        store
            .terminate(&TerminateWorkflowExecutionRequest {
                domain: "first".into(),
                workflow_execution: Some(WorkflowExecution::new("workflow", first.run_id)),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            store
                .describe(
                    &first_id,
                    &WorkflowExecution::new("workflow", second.run_id)
                )
                .unwrap()
                .workflow_execution_info
                .unwrap()
                .close_status,
            None
        );
    }
    #[test]
    fn legacy_snapshot_namespace_and_signal_dedup_are_restored() {
        let store = InMemoryExecutionStore::new();
        store.register_domain_name("legacy");
        let run = store.start(&start_request("legacy")).unwrap();
        let execution = WorkflowExecution::new("workflow", run.run_id);
        let signal = SignalWorkflowExecutionRequest {
            domain: "legacy".into(),
            workflow_execution: Some(execution.clone()),
            signal_name: "signal".into(),
            request_id: "signal-request".into(),
            ..Default::default()
        };
        store.signal(&signal).unwrap();
        let mut snapshot = serde_json::to_value(store.snapshot()).unwrap();
        snapshot.as_object_mut().unwrap().remove("domain_ids");
        snapshot.as_object_mut().unwrap().remove("domain_names");
        for entry in snapshot["runs"].as_array_mut().unwrap() {
            entry[1]
                .as_object_mut()
                .unwrap()
                .remove("signal_request_ids");
        }
        let restored = InMemoryExecutionStore::restore(serde_json::from_value(snapshot).unwrap());
        let id = uuid::Uuid::new_v4().to_string();
        restored.sync_domains(&[("legacy".into(), id.clone())]);
        let task = restored.poll_decision("legacy", "tasks", "worker").unwrap();
        assert_eq!(
            JsonTaskTokenSerializer::deserialize(&task.task_token)
                .unwrap()
                .domain_id,
            id
        );
        let before = restored
            .history("legacy", &execution)
            .unwrap()
            .history
            .unwrap()
            .events
            .len();
        restored.signal(&signal).unwrap();
        assert_eq!(
            restored
                .history("legacy", &execution)
                .unwrap()
                .history
                .unwrap()
                .events
                .len(),
            before
        );
        assert_eq!(
            restored.start(&start_request("legacy")).unwrap().run_id,
            execution.run_id
        );
    }
    #[test]
    fn child_references_use_uuid_keys_and_domain_names_in_history() {
        let store = InMemoryExecutionStore::new();
        let parent_id = uuid::Uuid::new_v4().to_string();
        let child_id = uuid::Uuid::new_v4().to_string();
        store.register_domain("parent", &parent_id);
        store.register_domain("child", &child_id);
        let parent = store.start(&start_request("parent")).unwrap();
        let task = store.poll_decision("parent", "tasks", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![cadence_api::tasks::Decision {
                    decision_type: Some(DecisionType::StartChildWorkflowExecution),
                    start_child_workflow_execution_decision_attributes: Some(
                        cadence_api::tasks::StartChildWorkflowExecutionDecisionAttributes {
                            domain: "child".into(),
                            workflow_id: "child-workflow".into(),
                            workflow_type: Some(WorkflowType {
                                name: "child-type".into(),
                            }),
                            task_list: Some(TaskList {
                                name: "child-tasks".into(),
                                ..Default::default()
                            }),
                            execution_start_to_close_timeout_seconds: Some(60),
                            task_start_to_close_timeout_seconds: Some(10),
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
        let child = store
            .describe("child", &WorkflowExecution::new("child-workflow", ""))
            .unwrap();
        assert_eq!(
            child
                .workflow_execution_info
                .as_ref()
                .unwrap()
                .parent_domain
                .as_deref(),
            Some("parent")
        );
        assert_eq!(
            store
                .describe(
                    "parent",
                    &WorkflowExecution::new("workflow", &parent.run_id)
                )
                .unwrap()
                .pending_children[0]
                .domain,
            "child"
        );
        let task = store
            .poll_decision("child", "child-tasks", "worker")
            .unwrap();
        assert_eq!(
            JsonTaskTokenSerializer::deserialize(&task.task_token)
                .unwrap()
                .domain_id,
            child_id
        );
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                decisions: vec![cadence_api::tasks::Decision {
                    decision_type: Some(DecisionType::CompleteWorkflowExecution),
                    complete_workflow_execution_decision_attributes: Some(
                        cadence_api::tasks::CompleteWorkflowExecutionDecisionAttributes::default(),
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
        let history = store
            .history("parent", &WorkflowExecution::new("workflow", parent.run_id))
            .unwrap()
            .history
            .unwrap();
        assert_eq!(
            history
                .events
                .iter()
                .find_map(|e| e
                    .child_workflow_execution_completed_event_attributes
                    .as_ref())
                .unwrap()
                .domain,
            "child"
        );
        let started = history
            .events
            .iter()
            .find(|e| e.event_type == Some(EventType::ChildWorkflowExecutionStarted))
            .unwrap();
        let completed = history
            .events
            .iter()
            .find_map(|e| {
                e.child_workflow_execution_completed_event_attributes
                    .as_ref()
            })
            .unwrap();
        assert_eq!(completed.started_event_id, started.event_id);
    }
    #[test]
    fn signal_wakes_idle_workflow_and_duplicate_request_does_not_add_history() {
        let store = InMemoryExecutionStore::new();
        let run = store.start(&start_request("domain")).unwrap();
        let task = store.poll_decision("domain", "tasks", "worker").unwrap();
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                ..Default::default()
            })
            .unwrap();
        assert!(store.poll_decision("domain", "tasks", "worker").is_none());
        let signal = SignalWorkflowExecutionRequest {
            domain: "domain".into(),
            workflow_execution: Some(WorkflowExecution::new("workflow", run.run_id)),
            signal_name: "wake".into(),
            request_id: "same-request".into(),
            ..Default::default()
        };
        store.signal(&signal).unwrap();
        store.signal(&signal).unwrap();
        let task = store.poll_decision("domain", "tasks", "worker").unwrap();
        assert_eq!(
            task.history
                .unwrap()
                .events
                .iter()
                .filter(|e| e.event_type == Some(EventType::WorkflowExecutionSignaled))
                .count(),
            1
        );
    }
}

#[cfg(test)]
mod scoped_namespace_tests {
    use super::*;
    #[test]
    fn resolved_namespace_stays_pinned_across_recreation_and_uuid_shaped_alias() {
        let store = InMemoryExecutionStore::new();
        let old = uuid::Uuid::new_v4().to_string();
        let new = uuid::Uuid::new_v4().to_string();
        let alias = uuid::Uuid::new_v4().to_string();
        store.register_domain("name", &old);
        let scoped_old = store.with_domain_id(&old);
        let request = StartWorkflowExecutionRequest {
            domain: "name".into(),
            workflow_id: "workflow".into(),
            request_id: "request".into(),
            workflow_type: Some(WorkflowType {
                name: "type".into(),
            }),
            task_list: Some(TaskList {
                name: "tasks".into(),
                ..Default::default()
            }),
            execution_start_to_close_timeout_seconds: Some(60),
            task_start_to_close_timeout_seconds: Some(10),
            ..Default::default()
        };
        let first = scoped_old.start(&request).unwrap();
        store.unregister_domain("name");
        store.register_domain("name", &new);
        let scoped_new = store.with_domain_id(&new);
        let second = scoped_new.start(&request).unwrap();
        store.register_domain(&old, &alias);
        let old_task = scoped_old.poll_decision("name", "tasks", "worker").unwrap();
        assert_eq!(old_task.workflow_execution.unwrap().run_id, first.run_id);
        assert_eq!(
            JsonTaskTokenSerializer::deserialize(&old_task.task_token)
                .unwrap()
                .domain_id,
            old
        );
        assert!(scoped_old
            .poll_decision("name", "tasks", "worker")
            .is_none());
        let new_task = scoped_new.poll_decision("name", "tasks", "worker").unwrap();
        assert_eq!(new_task.workflow_execution.unwrap().run_id, second.run_id);
        assert_eq!(
            JsonTaskTokenSerializer::deserialize(&new_task.task_token)
                .unwrap()
                .domain_id,
            new
        );
    }
}

#[cfg(test)]
mod transient_decision_tests {
    use super::*;

    fn setup() -> (InMemoryExecutionStore, WorkflowExecution) {
        let store = InMemoryExecutionStore::new();
        let run = store
            .start(&StartWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_id: "workflow".into(),
                request_id: "start".into(),
                workflow_type: Some(WorkflowType {
                    name: "type".into(),
                }),
                task_list: Some(TaskList {
                    name: "tasks".into(),
                    ..Default::default()
                }),
                execution_start_to_close_timeout_seconds: Some(60),
                task_start_to_close_timeout_seconds: Some(1),
                ..Default::default()
            })
            .unwrap();
        (store, WorkflowExecution::new("workflow", run.run_id))
    }
    fn poll(store: &InMemoryExecutionStore) -> PollForDecisionTaskResponse {
        store.poll_decision("d", "tasks", "worker").unwrap()
    }
    fn fail(store: &InMemoryExecutionStore, task: &PollForDecisionTaskResponse) {
        store
            .fail_decision(&task.task_token, None, vec![], "worker", "")
            .unwrap();
    }
    fn events(store: &InMemoryExecutionStore, execution: &WorkflowExecution) -> Vec<HistoryEvent> {
        store
            .history("d", execution)
            .unwrap()
            .history
            .unwrap()
            .events
    }
    fn signal(store: &InMemoryExecutionStore, execution: &WorkflowExecution) {
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_execution: Some(execution.clone()),
                signal_name: "wake".into(),
                request_id: "signal".into(),
                ..Default::default()
            })
            .unwrap();
    }

    #[test]
    fn repeated_failures_materialize_only_the_successful_retry() {
        let (store, execution) = setup();
        let first = poll(&store);
        fail(&store, &first);
        let second = poll(&store);
        assert_eq!(second.attempt, 1);
        assert_eq!(events(&store, &execution).len(), 4);
        assert_eq!(second.history.as_ref().unwrap().events.len(), 6);
        assert_eq!(second.next_event_id, 5);
        fail(&store, &second);
        let third = poll(&store);
        assert_eq!(third.attempt, 2);
        assert_eq!(third.started_event_id, second.started_event_id);
        assert!(matches!(
            store.complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: second.task_token.clone(),
                ..Default::default()
            }),
            Err(TaskError::NotFound(_))
        ));
        assert!(matches!(
            store.fail_decision(&second.task_token, None, vec![], "worker", ""),
            Err(TaskError::NotFound(_))
        ));
        assert_eq!(events(&store, &execution).len(), 4);
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: third.task_token,
                identity: "worker".into(),
                ..Default::default()
            })
            .unwrap();
        let history = events(&store, &execution);
        assert_eq!(history.len(), 7);
        assert_eq!(
            history[4].event_type,
            Some(EventType::DecisionTaskScheduled)
        );
        assert_eq!(
            history[4]
                .decision_task_scheduled_event_attributes
                .as_ref()
                .unwrap()
                .attempt,
            2
        );
        assert_eq!(history[4].timestamp, third.scheduled_timestamp);
        assert_eq!(history[5].timestamp, third.started_timestamp);
        assert_eq!(
            history[6]
                .decision_task_completed_event_attributes
                .as_ref()
                .unwrap()
                .started_event_id,
            6
        );
        assert!(store.poll_decision("d", "tasks", "worker").is_none());
    }

    #[test]
    fn new_events_before_transient_poll_reset_attempt() {
        let (store, execution) = setup();
        fail(&store, &poll(&store));
        signal(&store, &execution);
        let next = poll(&store);
        assert_eq!(next.attempt, 0);
        assert_eq!(next.started_event_id, 7);
        assert_eq!(
            events(&store, &execution)[4].event_type,
            Some(EventType::WorkflowExecutionSignaled)
        );
        assert_eq!(events(&store, &execution).len(), 7);
    }

    #[test]
    fn buffered_events_during_transient_failure_reset_attempt() {
        let (store, execution) = setup();
        fail(&store, &poll(&store));
        let retry = poll(&store);
        signal(&store, &execution);
        fail(&store, &retry);
        let next = poll(&store);
        assert_eq!(next.attempt, 0);
        let history = events(&store, &execution);
        assert_eq!(history.len(), 7);
        assert_eq!(
            history
                .iter()
                .filter(|event| event.event_type == Some(EventType::DecisionTaskFailed))
                .count(),
            1
        );
        assert_eq!(
            history[4].event_type,
            Some(EventType::WorkflowExecutionSignaled)
        );
    }

    #[test]
    fn repeated_timeouts_are_transient_and_stale_tokens_are_not_found() {
        let (store, execution) = setup();
        let first = poll(&store);
        assert_eq!(
            store.expire_due_decisions(first.started_timestamp.unwrap() + 999),
            0
        );
        assert_eq!(
            store.expire_due_decisions(first.started_timestamp.unwrap() + 1000),
            1
        );
        assert!(matches!(
            store.complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: first.task_token,
                ..Default::default()
            }),
            Err(TaskError::NotFound(_))
        ));
        let retry = poll(&store);
        assert_eq!(retry.attempt, 1);
        assert_eq!(
            store.expire_due_decisions(retry.started_timestamp.unwrap() + 1000),
            1
        );
        assert_eq!(events(&store, &execution).len(), 4);
        let next = poll(&store);
        assert_eq!(next.attempt, 2);
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: next.task_token,
                ..Default::default()
            })
            .unwrap();
        let history = events(&store, &execution);
        assert_eq!(
            history
                .iter()
                .filter(|event| event.event_type == Some(EventType::DecisionTaskTimedOut))
                .count(),
            1
        );
        assert_eq!(history.len(), 7);
    }

    #[test]
    fn paged_worker_history_appends_frozen_transient_events_on_final_page() {
        let (store, execution) = setup();
        for index in 0..1001 {
            store
                .signal(&SignalWorkflowExecutionRequest {
                    domain: "d".into(),
                    workflow_execution: Some(execution.clone()),
                    signal_name: "history".into(),
                    request_id: format!("signal-{index}"),
                    ..Default::default()
                })
                .unwrap();
        }
        fail(&store, &poll(&store));
        let retry = poll(&store);
        assert_eq!(retry.history.as_ref().unwrap().events.len(), 1000);
        assert!(!retry.next_page_token.is_empty());
        fail(&store, &retry);
        let replacement = poll(&store);
        assert_eq!(replacement.attempt, 2);
        let page = store
            .history_page("d", &execution, 1000, &retry.next_page_token, None)
            .unwrap();
        let history = page.history.unwrap().events;
        assert_eq!(history.last().unwrap().event_id, retry.started_event_id);
        assert_eq!(
            history[history.len() - 2]
                .decision_task_scheduled_event_attributes
                .as_ref()
                .unwrap()
                .attempt,
            1
        );
        assert!(page.next_page_token.is_empty());
        assert_eq!(events(&store, &execution).len(), 1005);
    }

    #[test]
    fn transient_snapshot_restores_retry_and_timestamps() {
        let (store, execution) = setup();
        fail(&store, &poll(&store));
        let retry = poll(&store);
        let restored = InMemoryExecutionStore::restore(store.snapshot());
        restored
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: retry.task_token,
                ..Default::default()
            })
            .unwrap();
        let history = events(&restored, &execution);
        assert_eq!(history[4].timestamp, retry.scheduled_timestamp);
        assert_eq!(history[5].timestamp, retry.started_timestamp);
        assert_eq!(history.len(), 7);
    }
    #[test]
    fn inline_force_task_is_atomic_and_does_not_consume_another_workflow() {
        let (store, execution) = setup();
        store
            .start(&StartWorkflowExecutionRequest {
                domain: "d".into(),
                workflow_id: "other".into(),
                request_id: "other".into(),
                workflow_type: Some(WorkflowType {
                    name: "type".into(),
                }),
                task_list: Some(TaskList {
                    name: "tasks".into(),
                    ..Default::default()
                }),
                execution_start_to_close_timeout_seconds: Some(60),
                task_start_to_close_timeout_seconds: Some(1),
                ..Default::default()
            })
            .unwrap();
        let first = poll(&store);
        let response = store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: first.task_token,
                identity: "worker".into(),
                return_new_decision_task: true,
                force_create_new_decision_task: true,
                ..Default::default()
            })
            .unwrap();
        let inline = response.decision_task.unwrap();
        assert_eq!(inline.workflow_execution.as_ref(), Some(&execution));
        assert_eq!(
            inline.previous_started_event_id,
            Some(first.started_event_id)
        );
        let history = &inline.history.as_ref().unwrap().events;
        assert_eq!(
            history.first().unwrap().event_id,
            first.started_event_id + 1
        );
        assert_eq!(
            history
                .last()
                .unwrap()
                .decision_task_started_event_attributes
                .as_ref()
                .unwrap()
                .request_id,
            "request-from-RespondDecisionTaskCompleted"
        );
        let other = poll(&store);
        assert_eq!(other.workflow_execution.unwrap().workflow_id, "other");
        assert!(store.poll_decision("d", "tasks", "worker").is_none());
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: inline.task_token,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(events(&store, &execution).len(), 7);
    }

    #[test]
    fn return_new_task_flag_alone_does_not_schedule_an_idle_task() {
        let (store, _) = setup();
        let first = poll(&store);
        let response = store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: first.task_token,
                return_new_decision_task: true,
                ..Default::default()
            })
            .unwrap();
        assert!(response.decision_task.is_none());
        assert!(store.poll_decision("d", "tasks", "worker").is_none());
    }

    #[test]
    fn unhandled_closing_command_can_return_the_replay_task_inline() {
        let (store, execution) = setup();
        let first = poll(&store);
        signal(&store, &execution);
        let response = store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: first.task_token,
                return_new_decision_task: true,
                decisions: vec![cadence_api::tasks::Decision {
                    decision_type: Some(DecisionType::CompleteWorkflowExecution),
                    complete_workflow_execution_decision_attributes: Some(Default::default()),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
        let next = response.decision_task.unwrap();
        assert_eq!(next.started_event_id, 7);
        assert_eq!(next.history.unwrap().events.len(), 7);
        assert!(events(&store, &execution)
            .iter()
            .any(|event| event.event_type == Some(EventType::WorkflowExecutionSignaled)));
    }

    #[test]
    fn inline_history_pages_begin_after_the_previous_started_event() {
        let (store, execution) = setup();
        let first = poll(&store);
        for index in 0..1001 {
            store
                .signal(&SignalWorkflowExecutionRequest {
                    domain: "d".into(),
                    workflow_execution: Some(execution.clone()),
                    signal_name: "history".into(),
                    request_id: format!("buffer-{index}"),
                    ..Default::default()
                })
                .unwrap();
        }
        let response = store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: first.task_token,
                return_new_decision_task: true,
                ..Default::default()
            })
            .unwrap();
        let inline = response.decision_task.unwrap();
        let first_page = &inline.history.as_ref().unwrap().events;
        assert_eq!(first_page.len(), HISTORY_PAGE_SIZE);
        assert_eq!(
            first_page.first().unwrap().event_id,
            first.started_event_id + 1
        );
        let last_page = store
            .history_page("d", &execution, 1000, &inline.next_page_token, None)
            .unwrap();
        assert_eq!(
            last_page.history.unwrap().events.last().unwrap().event_id,
            inline.started_event_id
        );
        assert!(last_page.next_page_token.is_empty());
    }
    fn sticky_attributes() -> cadence_api::types::StickyExecutionAttributes {
        cadence_api::types::StickyExecutionAttributes {
            worker_task_list: Some(TaskList {
                name: "sticky-worker".into(),
                ..Default::default()
            }),
            schedule_to_start_timeout_seconds: Some(1),
        }
    }
    fn enable_sticky(store: &InMemoryExecutionStore) -> PollForDecisionTaskResponse {
        let first = poll(store);
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: first.task_token.clone(),
                sticky_attributes: Some(sticky_attributes()),
                ..Default::default()
            })
            .unwrap();
        first
    }

    #[test]
    fn sticky_poll_routes_to_owner_and_returns_incremental_history() {
        let (store, execution) = setup();
        let first = enable_sticky(&store);
        signal(&store, &execution);
        assert!(store.poll_decision("d", "tasks", "worker").is_none());
        let sticky = store.poll_decision("d", "sticky-worker", "worker").unwrap();
        assert_eq!(
            sticky.previous_started_event_id,
            Some(first.started_event_id)
        );
        assert_eq!(
            sticky.history.unwrap().events.first().unwrap().event_id,
            first.started_event_id + 1
        );
        assert_eq!(sticky.workflow_execution_task_list.unwrap().name, "tasks");
    }

    #[test]
    fn unpolled_sticky_task_times_out_and_falls_back_to_normal_queue() {
        let (store, execution) = setup();
        enable_sticky(&store);
        signal(&store, &execution);
        let key = (
            "d".to_owned(),
            "workflow".to_owned(),
            execution.run_id.clone(),
        );
        let scheduled = store.inner.read().unwrap().by_run[&key]
            .pending_decision
            .as_ref()
            .unwrap()
            .scheduled_time_millis
            .unwrap();
        assert_eq!(store.expire_due_decisions(scheduled + 999), 0);
        assert_eq!(store.expire_due_decisions(scheduled + 1000), 1);
        assert!(store
            .poll_decision("d", "sticky-worker", "worker")
            .is_none());
        let normal = poll(&store);
        assert_eq!(normal.attempt, 0);
        assert_eq!(normal.history.unwrap().events.first().unwrap().event_id, 1);
        let timeout = events(&store, &execution)
            .iter()
            .find_map(|event| event.decision_task_timed_out_event_attributes.clone())
            .unwrap();
        assert_eq!(timeout.timeout_type, Some(TimeoutType::ScheduleToStart));
        assert_eq!(timeout.started_event_id, 0);
    }

    #[test]
    fn failed_sticky_task_retries_on_normal_queue_with_full_history() {
        let (store, execution) = setup();
        enable_sticky(&store);
        signal(&store, &execution);
        let task = store.poll_decision("d", "sticky-worker", "worker").unwrap();
        fail(&store, &task);
        assert!(store
            .poll_decision("d", "sticky-worker", "worker")
            .is_none());
        let retry = poll(&store);
        assert_eq!(retry.attempt, 1);
        assert_eq!(retry.history.unwrap().events.first().unwrap().event_id, 1);
    }

    #[test]
    fn reset_sticky_preserves_pending_transfer_but_restores_full_history() {
        let (store, execution) = setup();
        enable_sticky(&store);
        signal(&store, &execution);
        store.reset_sticky_task_list("d", &execution).unwrap();
        let task = store.poll_decision("d", "sticky-worker", "worker").unwrap();
        assert_eq!(task.history.unwrap().events.first().unwrap().event_id, 1);
    }

    #[test]
    fn inline_sticky_task_removes_its_actual_sticky_queue_entry() {
        let (store, _) = setup();
        let first = poll(&store);
        let response = store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: first.task_token,
                sticky_attributes: Some(sticky_attributes()),
                return_new_decision_task: true,
                force_create_new_decision_task: true,
                ..Default::default()
            })
            .unwrap();
        assert!(response.decision_task.is_some());
        assert!(store
            .poll_decision("d", "sticky-worker", "worker")
            .is_none());
        assert!(store.poll_decision("d", "tasks", "worker").is_none());
    }

    #[test]
    fn expired_sticky_ttl_is_checked_before_new_event_refreshes_last_update() {
        let (store, execution) = setup();
        enable_sticky(&store);
        let key = (
            "d".to_owned(),
            "workflow".to_owned(),
            execution.run_id.clone(),
        );
        store
            .inner
            .write()
            .unwrap()
            .by_run
            .get_mut(&key)
            .unwrap()
            .last_updated_millis = now_millis() - 365 * 24 * 60 * 60 * 1000 - 1;
        signal(&store, &execution);
        assert!(store
            .poll_decision("d", "sticky-worker", "worker")
            .is_none());
        assert_eq!(
            poll(&store)
                .history
                .unwrap()
                .events
                .first()
                .unwrap()
                .event_id,
            1
        );
    }
}

#[cfg(test)]
mod version_state_tests {
    use super::*;
    use cadence_api::tasks::RespondDecisionTaskCompletedRequest;

    fn start(store: &InMemoryExecutionStore) -> WorkflowExecution {
        let response = store
            .start(&StartWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_id: "versioned".to_string(),
                workflow_type: Some(WorkflowType {
                    name: "test".to_string(),
                }),
                task_list: Some(TaskList {
                    name: "tl".to_string(),
                    ..Default::default()
                }),
                execution_start_to_close_timeout_seconds: Some(600),
                task_start_to_close_timeout_seconds: Some(10),
                request_id: "start".to_string(),
                ..Default::default()
            })
            .unwrap();
        WorkflowExecution::new("versioned", response.run_id)
    }

    fn consistent(
        store: &InMemoryExecutionStore,
        execution: &WorkflowExecution,
    ) -> cadence_api::types::VersionHistory {
        let inner = store.inner.read().unwrap();
        let r = &inner.by_run[&(
            "d".to_string(),
            execution.workflow_id.clone(),
            execution.run_id.clone(),
        )];
        let history = r
            .version_histories
            .as_ref()
            .unwrap()
            .current_history()
            .unwrap();
        assert_eq!(
            history.last_item().unwrap().event_id,
            r.history.last().unwrap().event_id
        );
        for event in &r.history {
            assert_eq!(
                history.event_version(event.event_id).unwrap(),
                event.version
            );
        }
        history.clone()
    }

    #[test]
    fn version_state_local_and_global_starts_have_committed_branch_tips() {
        for version in [EMPTY_VERSION, 42] {
            let store = InMemoryExecutionStore::new();
            store.set_domain_version("d", version);
            let execution = start(&store);
            let history = consistent(&store, &execution);
            assert_eq!(
                history.items,
                vec![crate::version_history::new_item(2, version)]
            );
            let branch = crate::history_branch::decode(&history.branch_token).unwrap();
            assert_eq!(branch.tree_id, execution.run_id);
            assert!(uuid::Uuid::parse_str(&branch.branch_id).is_ok());
            assert!(branch.ancestors.is_empty());
        }
    }

    #[test]
    fn version_state_buffer_flush_and_transient_decisions_advance_only_committed_tips() {
        let store = InMemoryExecutionStore::new();
        let execution = start(&store);
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        let tip = consistent(&store, &execution).last_item().unwrap().event_id;
        store
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(execution.clone()),
                signal_name: "buffered".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            consistent(&store, &execution).last_item().unwrap().event_id,
            tip
        );
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: task.task_token,
                ..Default::default()
            })
            .unwrap();
        let committed = consistent(&store, &execution).last_item().unwrap().event_id;
        assert!(committed > tip);
        let task = store.poll_decision("d", "tl", "worker").unwrap();
        store
            .fail_decision(&task.task_token, None, Vec::new(), "worker", "")
            .unwrap();
        consistent(&store, &execution);
        let transient = store.poll_decision("d", "tl", "retry").unwrap();
        consistent(&store, &execution);
        store
            .complete_decision(&RespondDecisionTaskCompletedRequest {
                task_token: transient.task_token,
                ..Default::default()
            })
            .unwrap();
        consistent(&store, &execution);
    }

    #[test]
    fn version_state_reset_forks_ancestry_and_snapshot_preserves_tokens() {
        let store = InMemoryExecutionStore::new();
        let execution = start(&store);
        store.poll_decision("d", "tl", "worker").unwrap();
        let original = consistent(&store, &execution);
        let reset = store
            .reset(&cadence_api::types::ResetWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(execution.clone()),
                decision_finish_event_id: 3,
                request_id: "reset".to_string(),
                ..Default::default()
            })
            .unwrap();
        let reset_execution = WorkflowExecution::new("versioned", reset.run_id);
        let forked = consistent(&store, &reset_execution);
        let source = crate::history_branch::decode(&original.branch_token).unwrap();
        let branch = crate::history_branch::decode(&forked.branch_token).unwrap();
        assert_eq!(branch.tree_id, source.tree_id);
        assert_ne!(branch.branch_id, source.branch_id);
        assert_eq!(
            branch.ancestors,
            vec![cadence_api::types::HistoryBranchRange {
                branch_id: source.branch_id,
                begin_node_id: 1,
                end_node_id: 4,
            }]
        );
        let path =
            std::env::temp_dir().join(format!("cadence-versions-{}.json", uuid::Uuid::new_v4()));
        store.save_to_file(&path).unwrap();
        let restored = InMemoryExecutionStore::load_from_file(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(consistent(&restored, &reset_execution), forked);
    }

    #[test]
    fn version_state_legacy_zero_version_snapshots_keep_their_recorded_stream() {
        let store = InMemoryExecutionStore::new();
        let execution = start(&store);
        let mut snapshot = serde_json::to_value(store.snapshot()).unwrap();
        let record = snapshot["runs"][0][1].as_object_mut().unwrap();
        record.remove("current_version");
        record.remove("version_histories");
        for event in record.get_mut("history").unwrap().as_array_mut().unwrap() {
            event["version"] = serde_json::json!(0);
        }
        let snapshot = serde_json::from_value(snapshot).unwrap();
        let restored = InMemoryExecutionStore::restore(snapshot);
        assert_eq!(
            consistent(&restored, &execution)
                .last_item()
                .unwrap()
                .version,
            0
        );
        restored
            .signal(&SignalWorkflowExecutionRequest {
                domain: "d".to_string(),
                workflow_execution: Some(execution.clone()),
                signal_name: "after-upgrade".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            consistent(&restored, &execution).items,
            vec![crate::version_history::new_item(3, 0)]
        );
    }
}
