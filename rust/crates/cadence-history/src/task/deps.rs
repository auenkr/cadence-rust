//! Minimal stand-ins for packages that other workers are porting concurrently
//! (persistence tasks, shard.Context, execution.*, cache, metrics, log, config).
//! Every item keeps the Go name so it can be swapped for the real port later.
//! `// TODO(link): replace by <pkg>::X` marks each replacement point.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

// ---------------------------------------------------------------------------
// time
// ---------------------------------------------------------------------------

/// Go `time.Time` as unix nanoseconds; `Time(0)` doubles as the zero value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub i64);

impl Time {
    pub const ZERO: Time = Time(0);
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
    pub fn unix_nano(&self) -> i64 {
        self.0
    }
    pub fn unix(&self) -> i64 {
        self.0.div_euclid(1_000_000_000)
    }
    pub fn from_unix_nano(n: i64) -> Time {
        Time(n)
    }
    pub fn add(&self, d: Duration) -> Time {
        Time(self.0.saturating_add(d.as_nanos() as i64))
    }
    /// Go `t.Sub(u)` clamped at zero (callers only compare against positive durations).
    pub fn sub(&self, other: Time) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(other.0).max(0) as u64)
    }
    pub fn sub_nanos(&self, other: Time) -> i64 {
        self.0.saturating_sub(other.0)
    }
    pub fn before(&self, other: Time) -> bool {
        self.0 < other.0
    }
    pub fn after(&self, other: Time) -> bool {
        self.0 > other.0
    }
}

impl Add<Duration> for Time {
    type Output = Time;
    fn add(self, rhs: Duration) -> Time {
        Time::add(&self, rhs)
    }
}

/// TODO(link): replace by clock::TimeSource
pub trait TimeSource: Send + Sync {
    fn now(&self) -> Time;
}

#[derive(Default)]
pub struct RealTimeSource;
impl TimeSource for RealTimeSource {
    fn now(&self) -> Time {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Time(d.as_nanos() as i64)
    }
}

/// Go `clock.TimerGate`: a resettable one-shot timer that only moves earlier.
pub struct TimerGate {
    time_source: Arc<dyn TimeSource>,
    state: std::sync::Mutex<TimerGateState>,
    notify: tokio::sync::Notify,
}

#[derive(Default)]
struct TimerGateState {
    next: Option<Time>,
    stopped: bool,
}

impl TimerGate {
    pub fn new(time_source: Arc<dyn TimeSource>) -> Self {
        TimerGate {
            time_source,
            state: std::sync::Mutex::new(TimerGateState::default()),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Go `Update`: arm the gate for `t` if no earlier fire time is pending.
    pub fn update(&self, t: Time) {
        let mut s = self.state.lock().unwrap();
        match s.next {
            Some(n) if n <= t => {}
            _ => {
                s.next = Some(t);
                self.notify.notify_one();
            }
        }
    }

    pub fn stop(&self) {
        self.state.lock().unwrap().stopped = true;
        self.notify.notify_one();
    }

    /// Go `<-gate.Chan()`; resolves when the gate fires. Pending forever after stop.
    pub async fn fired(&self) {
        loop {
            let (next, stopped) = {
                let s = self.state.lock().unwrap();
                (s.next, s.stopped)
            };
            if stopped {
                std::future::pending::<()>().await;
            }
            match next {
                None => self.notify.notified().await,
                Some(t) => {
                    let wait = t.sub(self.time_source.now());
                    if wait.is_zero() {
                        let mut s = self.state.lock().unwrap();
                        if s.next == Some(t) {
                            s.next = None;
                        }
                        return;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = self.notify.notified() => {}
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// errors (Go `types.*Error`, package-level sentinels and wrapped errors)
// ---------------------------------------------------------------------------

/// TODO(link): replace by types::*Error plus task package sentinels.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum TaskError {
    #[error("entity not exists: {0}")]
    EntityNotExists(String),
    #[error("workflow execution already completed: {0}")]
    WorkflowExecutionAlreadyCompleted(String),
    #[error("domain not active: {0}")]
    DomainNotActive(String),
    #[error("current workflow condition failed")]
    CurrentWorkflowConditionFailed,
    #[error("shard closed at {closed_at:?}")]
    ShardClosed { closed_at: Time },
    #[error("task list not owned by host")]
    TaskListNotOwnedByHost,
    #[error("Redispatch reason: {0:?}")]
    Redispatch(String),
    #[error("passive task pending for too long")]
    TaskDiscarded,
    #[error("redispatch the task while the domain is pending-active")]
    TaskPendingActive,
    #[error("workflow is busy")]
    WorkflowBusy,
    #[error("workflow rate limited")]
    WorkflowRateLimited,
    #[error("domain becomes active when processing task as standby")]
    DomainBecomesActive,
    #[error("missing workflow start event")]
    MissingWorkflowStartEvent,
    #[error("version histories are missing")]
    MissingVersionHistories,
    #[error("active cluster selection policy not found")]
    ActiveClusterSelectionPolicyNotFound,
    #[error("domain entry not found")]
    DomainEntryNotFound,
    #[error("queue task processor is not running")]
    TaskProcessorNotRunning,
    #[error("context deadline exceeded")]
    ContextDeadlineExceeded,
    #[error("context canceled")]
    ContextCanceled,
    #[error("sticky task condition: {0}")]
    StickyTaskCondition(String),
    #[error("service busy: {0}")]
    ServiceBusy(String),
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("shard ownership lost: {0}")]
    ShardOwnershipLost(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("internal service error: {0}")]
    InternalService(String),
    #[error("workflow execution already started: {0}")]
    WorkflowExecutionAlreadyStarted(String),
    #[error("cancellation already requested: {0}")]
    CancellationAlreadyRequested(String),
    #[error("sticky worker unavailable: {0}")]
    StickyWorkerUnavailable(String),
    #[error("retry task v2: {0}")]
    RetryTaskV2(String),
    #[error("unknown task: {0}")]
    UnknownTask(String),
    #[error("{0}")]
    Other(String),
    /// Go `errors.Join`.
    #[error("joined errors: {0:?}")]
    Joined(Vec<TaskError>),
}

pub type Result<T> = std::result::Result<T, TaskError>;

impl TaskError {
    /// Go `errors.Is` for sentinel values, looking through `Joined`.
    pub fn is(&self, target: &TaskError) -> bool {
        if self == target {
            return true;
        }
        match self {
            TaskError::Joined(v) => v.iter().any(|e| e.is(target)),
            _ => false,
        }
    }
    pub fn is_entity_not_exists(&self) -> bool {
        matches!(self, TaskError::EntityNotExists(_))
    }
    pub fn is_domain_not_active(&self) -> bool {
        match self {
            TaskError::DomainNotActive(_) => true,
            TaskError::Joined(v) => v.iter().any(|e| e.is_domain_not_active()),
            _ => false,
        }
    }
    pub fn is_shard_closed(&self) -> Option<Time> {
        match self {
            TaskError::ShardClosed { closed_at } => Some(*closed_at),
            TaskError::Joined(v) => v.iter().find_map(|e| e.is_shard_closed()),
            _ => None,
        }
    }
    pub fn is_task_list_not_owned_by_host(&self) -> bool {
        match self {
            TaskError::TaskListNotOwnedByHost => true,
            TaskError::Joined(v) => v.iter().any(|e| e.is_task_list_not_owned_by_host()),
            _ => false,
        }
    }
    pub fn is_redispatch(&self) -> bool {
        match self {
            TaskError::Redispatch(_) => true,
            TaskError::Joined(v) => v.iter().any(|e| e.is_redispatch()),
            _ => false,
        }
    }
    /// Go `common.IsContextTimeoutError`.
    pub fn is_context_timeout(&self) -> bool {
        matches!(self, TaskError::ContextDeadlineExceeded)
    }
    /// Go `common.IsServiceTransientError`.
    pub fn is_service_transient(&self) -> bool {
        matches!(
            self,
            TaskError::ServiceBusy(_)
                | TaskError::InternalService(_)
                | TaskError::ShardOwnershipLost(_)
                | TaskError::TaskListNotOwnedByHost
        )
    }
    /// Go `persistence.IsTransientError`.
    pub fn is_persistence_transient(&self) -> bool {
        matches!(self, TaskError::ServiceBusy(_) | TaskError::ContextDeadlineExceeded)
    }
}

/// Go `common.IsStickyTaskConditionError`.
pub fn is_sticky_task_condition_error(err: &TaskError) -> bool {
    matches!(err, TaskError::StickyTaskCondition(_))
}

// ---------------------------------------------------------------------------
// log + tags
// ---------------------------------------------------------------------------

/// TODO(link): replace by log/tag::Tag
#[derive(Clone, Debug)]
pub struct Tag {
    pub key: &'static str,
    pub value: String,
}

pub mod tag {
    use super::{Tag, Time};
    use std::fmt::Display;

    fn t(key: &'static str, v: impl Display) -> Tag {
        Tag { key, value: v.to_string() }
    }
    pub fn shard_id(v: i32) -> Tag { t("shard-id", v) }
    pub fn task_id(v: i64) -> Tag { t("queue-task-id", v) }
    pub fn task_visibility_timestamp(v: i64) -> Tag { t("queue-task-visibility-timestamp", v) }
    pub fn failover_version(v: i64) -> Tag { t("failover-version", v) }
    pub fn task_type(v: i32) -> Tag { t("queue-task-type", v) }
    pub fn workflow_domain_id(v: &str) -> Tag { t("wf-domain-id", v) }
    pub fn workflow_domain_name(v: &str) -> Tag { t("wf-domain-name", v) }
    pub fn workflow_id(v: &str) -> Tag { t("wf-id", v) }
    pub fn workflow_run_id(v: &str) -> Tag { t("wf-run-id", v) }
    pub fn workflow_event_id(v: i64) -> Tag { t("wf-event-id", v) }
    pub fn workflow_next_event_id(v: i64) -> Tag { t("wf-next-event-id", v) }
    pub fn workflow_schedule_id(v: i64) -> Tag { t("wf-schedule-id", v) }
    pub fn workflow_timer_id(v: &str) -> Tag { t("wf-timer-id", v) }
    pub fn workflow_activity_id(v: &str) -> Tag { t("wf-activity-id", v) }
    pub fn error(e: &dyn Display) -> Tag { t("error", e) }
    pub fn attempt_count(v: i64) -> Tag { t("attempt-count", v) }
    pub fn schedule_attempt(v: i64) -> Tag { t("attempt", v) }
    pub fn attempt(v: i64) -> Tag { t("attempt", v) }
    pub fn timestamp(v: Time) -> Tag { t("timestamp", v.0) }
    pub fn counter(v: usize) -> Tag { t("counter", v) }
    pub fn cluster_name(v: &str) -> Tag { t("cluster-name", v) }
    pub fn key(v: &str) -> Tag { t("key", v) }
    pub fn value(v: &str) -> Tag { t("value", v) }
    pub fn dynamic(k: &'static str, v: impl std::fmt::Debug) -> Tag { Tag { key: k, value: format!("{v:?}") } }
    pub fn timer_task_status(v: i32) -> Tag { t("timer-task-status", v) }
    pub fn activity_timeout_type(v: i32) -> Tag { t("timeout-type", v) }
    pub fn lifecycle(v: &'static str) -> Tag { Tag { key: "lifecycle", value: v.to_string() } }
    pub fn life_cycle_started() -> Tag { lifecycle("Started") }
    pub fn life_cycle_stopped() -> Tag { lifecycle("Stopped") }
    pub fn life_cycle_stop_timedout() -> Tag { lifecycle("StopTimedout") }
    pub fn life_cycle_processing_failed() -> Tag { lifecycle("ProcessingFailed") }
    pub fn operation_critical() -> Tag { Tag { key: "operation-result", value: "OperationCritical".into() } }
}

/// TODO(link): replace by log::Logger
#[derive(Clone, Default)]
pub struct Logger {
    tags: Vec<Tag>,
    pub debug_enabled: bool,
}

impl Logger {
    pub fn new() -> Self {
        Logger::default()
    }
    pub fn with_tags(&self, tags: Vec<Tag>) -> Logger {
        let mut l = self.clone();
        l.tags.extend(tags);
        l
    }
    pub fn debug_on(&self) -> bool {
        self.debug_enabled
    }
    fn fmt(&self, extra: &[Tag]) -> String {
        self.tags
            .iter()
            .chain(extra.iter())
            .map(|t| format!("{}={}", t.key, t.value))
            .collect::<Vec<_>>()
            .join(" ")
    }
    pub fn debug_tags(&self, msg: &str, tags: &[Tag]) {
        self.debug(msg, tags)
    }
    pub fn warn_tags(&self, msg: &str, tags: &[Tag]) {
        self.warn(msg, tags)
    }
    pub fn debug(&self, msg: &str, tags: &[Tag]) {
        tracing::debug!("{msg} {}", self.fmt(tags));
    }
    pub fn info(&self, msg: &str, tags: &[Tag]) {
        tracing::info!("{msg} {}", self.fmt(tags));
    }
    pub fn warn(&self, msg: &str, tags: &[Tag]) {
        tracing::warn!("{msg} {}", self.fmt(tags));
    }
    pub fn error(&self, msg: &str, tags: &[Tag]) {
        tracing::error!("{msg} {}", self.fmt(tags));
    }
    /// Go `Fatal` panics the process after logging.
    pub fn fatal(&self, msg: &str, tags: &[Tag]) -> ! {
        tracing::error!("{msg} {}", self.fmt(tags));
        panic!("{msg}");
    }
    pub fn helper(&self) -> Logger {
        self.clone()
    }
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

/// TODO(link): replace by metrics::ScopeIdx / metric defs.
pub type ScopeIdx = &'static str;
pub type MetricIdx = &'static str;

/// TODO(link): replace by metrics::Scope
pub trait MetricsScope: Send + Sync {
    fn inc_counter(&self, name: MetricIdx);
    fn add_counter(&self, name: MetricIdx, delta: i64);
    fn record_timer(&self, name: MetricIdx, d: Duration);
    fn exponential_histogram(&self, name: MetricIdx, d: Duration);
    fn int_exponential_histogram(&self, name: MetricIdx, v: usize);
    fn record_histogram_duration(&self, name: MetricIdx, d: Duration);
    fn record_histogram_value(&self, name: MetricIdx, v: f64);
    fn update_gauge(&self, name: MetricIdx, v: f64);
    fn tagged(&self, tags: Vec<Tag>) -> Arc<dyn MetricsScope>;
}

pub struct NoopScope;
impl MetricsScope for NoopScope {
    fn inc_counter(&self, _: MetricIdx) {}
    fn add_counter(&self, _: MetricIdx, _: i64) {}
    fn record_timer(&self, _: MetricIdx, _: Duration) {}
    fn exponential_histogram(&self, _: MetricIdx, _: Duration) {}
    fn int_exponential_histogram(&self, _: MetricIdx, _: usize) {}
    fn record_histogram_duration(&self, _: MetricIdx, _: Duration) {}
    fn record_histogram_value(&self, _: MetricIdx, _: f64) {}
    fn update_gauge(&self, _: MetricIdx, _: f64) {}
    fn tagged(&self, _: Vec<Tag>) -> Arc<dyn MetricsScope> {
        Arc::new(NoopScope)
    }
}

pub fn noop_scope() -> Arc<dyn MetricsScope> {
    Arc::new(NoopScope)
}

/// TODO(link): replace by metrics::Client
pub trait MetricsClient: Send + Sync {
    fn scope(&self, scope: ScopeIdx, tags: Vec<Tag>) -> Arc<dyn MetricsScope>;
    fn inc_counter(&self, scope: ScopeIdx, name: MetricIdx) {
        self.scope(scope, vec![]).inc_counter(name);
    }
    fn add_counter(&self, scope: ScopeIdx, name: MetricIdx, delta: i64) {
        self.scope(scope, vec![]).add_counter(name, delta);
    }
}

pub fn domain_tag(name: &str) -> Tag {
    Tag { key: "domain", value: name.to_string() }
}
pub fn domain_unknown_tag() -> Tag {
    domain_tag("_unknown_")
}
pub fn workflow_type_tag(name: &str) -> Tag {
    Tag { key: "workflowType", value: name.to_string() }
}
pub fn decision_type_tag(name: &str) -> Tag {
    Tag { key: "decisionType", value: name.to_string() }
}
pub fn get_task_list_tag(name: &str, kind: TaskListKind) -> Tag {
    // Go common.GetTaskListTag collapses sticky/ephemeral names to avoid cardinality blowup.
    let v = match kind {
        TaskListKind::Normal => name.to_string(),
        TaskListKind::Sticky => "__sticky__".to_string(),
        TaskListKind::Ephemeral => "__ephemeral__".to_string(),
    };
    Tag { key: "tasklist", value: v }
}

/// Cache of domain-tagged scopes (`shard.GetService().GetDomainMetricsScopeCache()`).
pub trait DomainMetricsScopeCache: Send + Sync {
    fn get(&self, domain_id: &str, scope: ScopeIdx) -> Option<Arc<dyn MetricsScope>>;
    fn put(&self, domain_id: &str, scope: ScopeIdx, s: Arc<dyn MetricsScope>);
}

// ---------------------------------------------------------------------------
// task list, domain cache, cluster metadata
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TaskListKind {
    #[default]
    Normal,
    Sticky,
    Ephemeral,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskList {
    pub name: String,
    pub kind: TaskListKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ArchivalStatus {
    #[default]
    Disabled,
    Enabled,
}

#[derive(Clone, Debug, Default)]
pub struct DomainInfo {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, Default)]
pub struct DomainConfig {
    pub history_archival_status: ArchivalStatus,
    pub history_archival_uri: String,
    pub visibility_archival_status: ArchivalStatus,
    pub visibility_archival_uri: String,
}

#[derive(Clone, Debug, Default)]
pub struct DomainReplicationConfig {
    pub active_cluster_name: String,
    pub active_active: bool,
    /// Go `ReplicationConfig.Clusters` (cluster names).
    pub clusters: Vec<String>,
}

impl DomainReplicationConfig {
    pub fn is_active_active(&self) -> bool {
        self.active_active
    }
}

/// TODO(link): replace by cache::DomainCacheEntry
#[derive(Clone, Debug, Default)]
pub struct DomainCacheEntry {
    pub info: DomainInfo,
    pub config: DomainConfig,
    pub replication_config: DomainReplicationConfig,
    pub is_global_domain: bool,
    pub failover_version: i64,
    pub retention_days: i32,
    pub sampled_for_longer_retention_enabled: bool,
    pub sampled_for_longer_retention: bool,
}

impl DomainCacheEntry {
    pub fn get_info(&self) -> &DomainInfo {
        &self.info
    }
    pub fn get_config(&self) -> &DomainConfig {
        &self.config
    }
    pub fn get_replication_config(&self) -> &DomainReplicationConfig {
        &self.replication_config
    }
    pub fn is_global_domain(&self) -> bool {
        self.is_global_domain
    }
    pub fn get_retention_days(&self, _workflow_id: &str) -> i32 {
        self.retention_days
    }
    pub fn is_sampled_for_longer_retention_enabled(&self, _workflow_id: &str) -> bool {
        self.sampled_for_longer_retention_enabled
    }
    pub fn is_sampled_for_longer_retention(&self, _workflow_id: &str) -> bool {
        self.sampled_for_longer_retention
    }
}

/// `cache.DomainCacheRefreshInterval`.
pub const DOMAIN_CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// TODO(link): replace by cache::DomainCache
pub trait DomainCache: Send + Sync {
    fn get_domain_by_id(&self, domain_id: &str) -> Result<Arc<DomainCacheEntry>>;
    fn get_domain(&self, name: &str) -> Result<Arc<DomainCacheEntry>>;
    fn get_domain_name(&self, domain_id: &str) -> Result<String>;
}

#[derive(Clone, Debug, Default)]
pub struct ActiveClusterInfo {
    pub active_cluster_name: String,
    pub failover_version: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterAttribute {
    pub scope: String,
    pub name: String,
}

#[derive(Clone, Debug, Default)]
pub struct ActiveClusterSelectionPolicy {
    pub cluster_attribute: Option<ClusterAttribute>,
}

/// TODO(link): replace by activecluster::Manager
#[async_trait]
pub trait ActiveClusterManager: Send + Sync {
    async fn get_active_cluster_info_by_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<ActiveClusterInfo>;
    async fn get_active_cluster_selection_policy_for_workflow(
        &self,
        domain_id: &str,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<Option<ActiveClusterSelectionPolicy>>;
}

/// TODO(link): replace by cluster::Metadata
pub trait ClusterMetadata: Send + Sync {
    fn get_current_cluster_name(&self) -> String;
}

// ---------------------------------------------------------------------------
// persistence tasks (persistence.Task and concrete task types)
// ---------------------------------------------------------------------------

pub mod ptask_type {
    // persistence.TransferTaskType*
    pub const TRANSFER_TASK_TYPE_DECISION_TASK: i32 = 0;
    pub const TRANSFER_TASK_TYPE_ACTIVITY_TASK: i32 = 1;
    pub const TRANSFER_TASK_TYPE_CLOSE_EXECUTION: i32 = 2;
    pub const TRANSFER_TASK_TYPE_CANCEL_EXECUTION: i32 = 3;
    pub const TRANSFER_TASK_TYPE_START_CHILD_EXECUTION: i32 = 4;
    pub const TRANSFER_TASK_TYPE_SIGNAL_EXECUTION: i32 = 5;
    pub const TRANSFER_TASK_TYPE_RECORD_WORKFLOW_STARTED: i32 = 6;
    pub const TRANSFER_TASK_TYPE_RESET_WORKFLOW: i32 = 7;
    pub const TRANSFER_TASK_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES: i32 = 8;
    pub const TRANSFER_TASK_TYPE_RECORD_WORKFLOW_CLOSED: i32 = 9;
    pub const TRANSFER_TASK_TYPE_RECORD_CHILD_EXECUTION_COMPLETED: i32 = 10;
    pub const TRANSFER_TASK_TYPE_APPLY_PARENT_CLOSE_POLICY: i32 = 11;
    // persistence.TaskType* (timer)
    pub const TASK_TYPE_DECISION_TIMEOUT: i32 = 0;
    pub const TASK_TYPE_ACTIVITY_TIMEOUT: i32 = 1;
    pub const TASK_TYPE_USER_TIMER: i32 = 2;
    pub const TASK_TYPE_WORKFLOW_TIMEOUT: i32 = 3;
    pub const TASK_TYPE_DELETE_HISTORY_EVENT: i32 = 4;
    pub const TASK_TYPE_ACTIVITY_RETRY_TIMER: i32 = 5;
    pub const TASK_TYPE_WORKFLOW_BACKOFF_TIMER: i32 = 6;
}

/// persistence.WorkflowBackoffTimeoutType*
pub const WORKFLOW_BACKOFF_TIMEOUT_TYPE_RETRY: i32 = 0;
pub const WORKFLOW_BACKOFF_TIMEOUT_TYPE_CRON: i32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryTaskCategory {
    Transfer,
    Timer,
    Replication,
    CrossCluster,
}

/// `persistence.HistoryTaskKey`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct HistoryTaskKey {
    pub scheduled_time: Time,
    pub task_id: i64,
}

/// `persistence.HistoryTaskCategoryIDTransfer` / `...IDTimer`
pub const HISTORY_TASK_CATEGORY_ID_TRANSFER: i32 = 1;
pub const HISTORY_TASK_CATEGORY_ID_TIMER: i32 = 2;
/// `shard.TimeBeforeShardClosedIsError`
pub const TIME_BEFORE_SHARD_CLOSED_IS_ERROR: Duration = Duration::from_secs(10);
/// `dynamicproperties.DurationPropertyFnWithShardIDFilter`
pub type DurationPropertyFnWithShardIDFilter = Arc<dyn Fn(i32) -> Duration + Send + Sync>;

impl HistoryTaskCategory {
    /// `HistoryTaskCategory.ID`
    pub fn id(&self) -> i32 {
        match self {
            HistoryTaskCategory::Transfer => 1,
            HistoryTaskCategory::Timer => 2,
            HistoryTaskCategory::Replication => 3,
            HistoryTaskCategory::CrossCluster => 4,
        }
    }
    /// `HistoryTaskCategory.Type() == HistoryTaskCategoryTypeImmediate`
    pub fn is_immediate(&self) -> bool {
        !matches!(self, HistoryTaskCategory::Timer)
    }
}

impl HistoryTaskKey {
    /// `NewHistoryTaskKey`
    pub fn new(scheduled_time: Time, task_id: i64) -> HistoryTaskKey {
        HistoryTaskKey { scheduled_time, task_id }
    }
    /// `NewImmediateTaskKey`
    pub fn new_immediate_task_key(task_id: i64) -> HistoryTaskKey {
        HistoryTaskKey { scheduled_time: Time(0), task_id }
    }
    /// `MaximumHistoryTaskKey`
    pub fn maximum() -> HistoryTaskKey {
        HistoryTaskKey { scheduled_time: Time(i64::MAX), task_id: i64::MAX }
    }
    /// `HistoryTaskKey.Compare`
    pub fn compare(&self, b: &HistoryTaskKey) -> i32 {
        match self.cmp(b) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Equal => 0,
        }
    }
    /// `HistoryTaskKey.Next`
    pub fn next(&self) -> HistoryTaskKey {
        if self.task_id == i64::MAX {
            return HistoryTaskKey { scheduled_time: Time(self.scheduled_time.0.saturating_add(1)), task_id: 0 };
        }
        HistoryTaskKey { scheduled_time: self.scheduled_time, task_id: self.task_id + 1 }
    }
    pub fn get_scheduled_time(&self) -> Time {
        self.scheduled_time
    }
    pub fn get_task_id(&self) -> i64 {
        self.task_id
    }
}

#[derive(Clone, Debug, Default)]
pub struct WorkflowIdentifier {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
}

#[derive(Clone, Debug, Default)]
pub struct TaskData {
    pub version: i64,
    pub task_id: i64,
    pub visibility_timestamp: Time,
}

/// TODO(link): replace by persistence::Task
pub trait PersistenceTask: Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;
    /// The wrapper object itself (differs from `as_any` only for queue-wiring adapters, whose
    /// `as_any` exposes the typed task the executors downcast to).
    fn wrapper_any(&self) -> &dyn Any {
        self.as_any()
    }
    fn get_task_category(&self) -> HistoryTaskCategory;
    fn get_task_type(&self) -> i32;
    fn get_domain_id(&self) -> &str;
    fn get_workflow_id(&self) -> &str;
    fn get_run_id(&self) -> &str;
    fn get_version(&self) -> i64;
    fn get_task_id(&self) -> i64;
    fn get_visibility_timestamp(&self) -> Time;
    fn get_task_key(&self) -> HistoryTaskKey {
        HistoryTaskKey { scheduled_time: self.get_visibility_timestamp(), task_id: self.get_task_id() }
    }
    /// Owned shared copy of the task (Go passes the interface value around).
    fn clone_arc(&self) -> Arc<dyn PersistenceTask>;
    /// Go `GetOriginalTaskList` / `GetOriginalTaskListKind` of the executed task (empty for non-matching tasks).
    fn get_original_task_list(&self) -> String {
        String::new()
    }
    fn get_original_task_list_kind(&self) -> TaskListKind {
        TaskListKind::Normal
    }
}

macro_rules! define_ptask {
    ($name:ident, $cat:expr, $ty:expr, { $($f:ident : $t:ty),* $(,)? }) => {
        define_ptask!($name, $cat, $ty, { $($f : $t),* }, {});
    };
    ($name:ident, $cat:expr, $ty:expr, { $($f:ident : $t:ty),* $(,)? }, { $($extra:tt)* }) => {
        #[derive(Clone, Debug, Default)]
        pub struct $name {
            pub workflow_identifier: WorkflowIdentifier,
            pub task_data: TaskData,
            $(pub $f: $t,)*
        }
        impl $name {
            pub fn domain_id(&self) -> &str { &self.workflow_identifier.domain_id }
            pub fn workflow_id(&self) -> &str { &self.workflow_identifier.workflow_id }
            pub fn run_id(&self) -> &str { &self.workflow_identifier.run_id }
        }
        impl PersistenceTask for $name {
            fn as_any(&self) -> &dyn Any { self }
            fn clone_arc(&self) -> Arc<dyn PersistenceTask> { Arc::new(self.clone()) }
            fn get_task_category(&self) -> HistoryTaskCategory { $cat }
            fn get_task_type(&self) -> i32 { $ty }
            fn get_domain_id(&self) -> &str { &self.workflow_identifier.domain_id }
            fn get_workflow_id(&self) -> &str { &self.workflow_identifier.workflow_id }
            fn get_run_id(&self) -> &str { &self.workflow_identifier.run_id }
            fn get_version(&self) -> i64 { self.task_data.version }
            fn get_task_id(&self) -> i64 { self.task_data.task_id }
            fn get_visibility_timestamp(&self) -> Time { self.task_data.visibility_timestamp }
            $($extra)*
        }
    };
}

use ptask_type::*;

define_ptask!(ActivityTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_ACTIVITY_TASK,
    { target_domain_id: String, task_list: String, schedule_id: i64,
      original_task_list: String, original_task_list_kind: TaskListKind }, { fn get_original_task_list(&self) -> String { self.original_task_list.clone() }
      fn get_original_task_list_kind(&self) -> TaskListKind { self.original_task_list_kind } });
define_ptask!(DecisionTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_DECISION_TASK,
    { target_domain_id: String, task_list: String, schedule_id: i64,
      original_task_list: String, original_task_list_kind: TaskListKind }, { fn get_original_task_list(&self) -> String { self.original_task_list.clone() }
      fn get_original_task_list_kind(&self) -> TaskListKind { self.original_task_list_kind } });
define_ptask!(CloseExecutionTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_CLOSE_EXECUTION, {});
define_ptask!(CancelExecutionTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_CANCEL_EXECUTION,
    { target_domain_id: String, target_workflow_id: String, target_run_id: String,
      target_child_workflow_only: bool, initiated_id: i64 });
define_ptask!(SignalExecutionTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_SIGNAL_EXECUTION,
    { target_domain_id: String, target_workflow_id: String, target_run_id: String,
      target_child_workflow_only: bool, initiated_id: i64 });
define_ptask!(StartChildExecutionTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_START_CHILD_EXECUTION,
    { target_domain_id: String, target_workflow_id: String, initiated_id: i64, task_list: String });
define_ptask!(RecordWorkflowStartedTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_RECORD_WORKFLOW_STARTED, {});
define_ptask!(ResetWorkflowTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_RESET_WORKFLOW, {});
define_ptask!(UpsertWorkflowSearchAttributesTask, HistoryTaskCategory::Transfer,
    TRANSFER_TASK_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, { task_list: String });
define_ptask!(RecordWorkflowClosedTask, HistoryTaskCategory::Transfer, TRANSFER_TASK_TYPE_RECORD_WORKFLOW_CLOSED, {});
define_ptask!(RecordChildExecutionCompletedTask, HistoryTaskCategory::Transfer,
    TRANSFER_TASK_TYPE_RECORD_CHILD_EXECUTION_COMPLETED,
    { target_domain_id: String, target_workflow_id: String, target_run_id: String });
define_ptask!(ApplyParentClosePolicyTask, HistoryTaskCategory::Transfer,
    TRANSFER_TASK_TYPE_APPLY_PARENT_CLOSE_POLICY, { target_domain_ids: Vec<String> });

define_ptask!(DecisionTimeoutTask, HistoryTaskCategory::Timer, TASK_TYPE_DECISION_TIMEOUT,
    { event_id: i64, schedule_attempt: i64, timeout_type: i32 });
define_ptask!(ActivityTimeoutTask, HistoryTaskCategory::Timer, TASK_TYPE_ACTIVITY_TIMEOUT,
    { event_id: i64, attempt: i64, timeout_type: i32 });
define_ptask!(UserTimerTask, HistoryTaskCategory::Timer, TASK_TYPE_USER_TIMER, { event_id: i64 });
define_ptask!(WorkflowTimeoutTask, HistoryTaskCategory::Timer, TASK_TYPE_WORKFLOW_TIMEOUT, {});
define_ptask!(DeleteHistoryEventTask, HistoryTaskCategory::Timer, TASK_TYPE_DELETE_HISTORY_EVENT, {});
define_ptask!(ActivityRetryTimerTask, HistoryTaskCategory::Timer, TASK_TYPE_ACTIVITY_RETRY_TIMER,
    { event_id: i64, attempt: i64 });
define_ptask!(WorkflowBackoffTimerTask, HistoryTaskCategory::Timer, TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
    { event_id: i64, timeout_type: i32 });

// ---------------------------------------------------------------------------
// misc constants shared with Go `common`
// ---------------------------------------------------------------------------

pub const EMPTY_EVENT_ID: i64 = -23;
pub const EMPTY_VERSION: i64 = -24;
pub const FIRST_EVENT_ID: i64 = 1;
/// `constants.MaxTaskTimeout`
pub const MAX_TASK_TIMEOUT: i32 = i32::MAX;

pub const DAEMON_STATUS_INITIALIZED: i32 = 0;
pub const DAEMON_STATUS_STARTED: i32 = 1;
pub const DAEMON_STATUS_STOPPED: i32 = 2;

/// TODO(link): replace by common::Daemon
#[async_trait]
pub trait Daemon: Send + Sync {
    async fn start(&self);
    async fn stop(&self);
}

/// Go `common.Int32Ptr` family is unnecessary in Rust; `Option` is used directly.
pub type StringMap = BTreeMap<String, String>;
pub type BytesMap = BTreeMap<String, Vec<u8>>;

// ---------------------------------------------------------------------------
// retry policy helpers (cadence_common::backoff)
// ---------------------------------------------------------------------------

pub use cadence_common::backoff::ExponentialRetryPolicy as RetryPolicy;
pub use cadence_common::backoff::NO_INTERVAL;

/// Go `policy.ComputeNextDelay(0, attempt)`; `done` (-1ns) collapses to zero.
pub fn compute_next_delay(policy: &RetryPolicy, elapsed: Duration, attempt: usize) -> Duration {
    let mut rng = rand::thread_rng();
    policy.compute_next_delay(&mut rng, elapsed, attempt).unwrap_or(Duration::ZERO)
}

/// Go `backoff.JitDuration`.
pub fn jit_duration(d: Duration, coefficient: f64) -> Duration {
    let mut rng = rand::thread_rng();
    cadence_common::jitter::jit_duration(&mut rng, d, coefficient)
}

/// Go `common.CreateTaskProcessingRetryPolicy`.
pub fn create_task_processing_retry_policy() -> RetryPolicy {
    RetryPolicy::new(Duration::from_millis(50))
        .with_maximum_interval(Duration::from_millis(100))
        .with_maximum_attempts(3)
}

/// Go `backoff.ThrottleRetry` with a retry policy and a retryable-error predicate.
pub struct ThrottleRetry {
    policy: RetryPolicy,
    is_retryable: fn(&TaskError) -> bool,
}

impl ThrottleRetry {
    pub fn new(policy: RetryPolicy, is_retryable: fn(&TaskError) -> bool) -> Self {
        ThrottleRetry { policy, is_retryable }
    }

    /// Go `ThrottleRetry.Do`.
    pub async fn do_op<F, Fut>(&self, mut op: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let start = std::time::Instant::now();
        let mut attempt = 0usize;
        loop {
            let err = match op().await {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            if !(self.is_retryable)(&err) {
                return Err(err);
            }
            let next = {
                let mut rng = rand::thread_rng();
                self.policy.compute_next_delay(&mut rng, start.elapsed(), attempt)
            };
            match next {
                None => return Err(err),
                Some(d) => tokio::time::sleep(d).await,
            }
            attempt += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// dynamic config property function types (dynamicproperties.*PropertyFn)
// ---------------------------------------------------------------------------

pub type IntPropertyFn = Arc<dyn Fn() -> i32 + Send + Sync>;
pub type BoolPropertyFn = Arc<dyn Fn() -> bool + Send + Sync>;
pub type DurationPropertyFn = Arc<dyn Fn() -> Duration + Send + Sync>;
pub type IntPropertyFnWithDomainFilter = Arc<dyn Fn(&str) -> i32 + Send + Sync>;
pub type BoolPropertyFnWithDomainFilter = Arc<dyn Fn(&str) -> bool + Send + Sync>;
pub type StringPropertyFnWithDomainFilter = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Go `constants.HistoryTaskDLQMode*` (service/history/constants).
pub const HISTORY_TASK_DLQ_MODE_ENABLED: &str = "enabled";
pub const HISTORY_TASK_DLQ_MODE_SHADOW: &str = "shadow";

/// TODO(link): replace by common::metrics typed identifiers.
pub mod metrics {
 pub const DATA_INCONSISTENT_COUNTER: &str = "DATA_INCONSISTENT_COUNTER";
 pub const HISTORY_TASK_DLQ_PAGE_SIZE_BYTES: &str = "HISTORY_TASK_DLQ_PAGE_SIZE_BYTES";
 pub const HISTORY_TASK_DLQ_PROCESSOR_SCOPE: &str = "HISTORY_TASK_DLQ_PROCESSOR_SCOPE";
 pub const HISTORY_TASK_DLQ_REINJECT_FAILURES_COUNTER: &str = "HISTORY_TASK_DLQ_REINJECT_FAILURES_COUNTER";
 pub const STALE_MUTABLE_STATE_COUNTER: &str = "STALE_MUTABLE_STATE_COUNTER";
 pub const TASK_ATTEMPT_PER_DOMAIN_COUNTS_HISTOGRAM: &str = "TASK_ATTEMPT_PER_DOMAIN_COUNTS_HISTOGRAM";
 pub const TASK_ATTEMPT_TIMER_PER_DOMAIN: &str = "TASK_ATTEMPT_TIMER_PER_DOMAIN";
 pub const TASK_DISCARDED_PER_DOMAIN: &str = "TASK_DISCARDED_PER_DOMAIN";
 pub const TASK_FAILURES_PER_DOMAIN: &str = "TASK_FAILURES_PER_DOMAIN";
 pub const TASK_LATENCY_PER_DOMAIN: &str = "TASK_LATENCY_PER_DOMAIN";
 pub const TASK_LATENCY_PER_DOMAIN_HISTOGRAM: &str = "TASK_LATENCY_PER_DOMAIN_HISTOGRAM";
 pub const TASK_LATENCY_PER_TASK_LIST_HISTOGRAM: &str = "TASK_LATENCY_PER_TASK_LIST_HISTOGRAM";
 pub const TASK_LIST_NOT_OWNED_BY_HOST_COUNTER_PER_DOMAIN: &str = "TASK_LIST_NOT_OWNED_BY_HOST_COUNTER_PER_DOMAIN";
 pub const TASK_NOT_ACTIVE_COUNTER_PER_DOMAIN: &str = "TASK_NOT_ACTIVE_COUNTER_PER_DOMAIN";
 pub const TASK_PENDING_ACTIVE_COUNTER_PER_DOMAIN: &str = "TASK_PENDING_ACTIVE_COUNTER_PER_DOMAIN";
 pub const TASK_PRIORITY_ASSIGNER_SCOPE: &str = "TASK_PRIORITY_ASSIGNER_SCOPE";
 pub const TASK_PROCESSING_LATENCY_PER_DOMAIN: &str = "TASK_PROCESSING_LATENCY_PER_DOMAIN";
 pub const TASK_PROCESSING_LATENCY_PER_DOMAIN_HISTOGRAM: &str = "TASK_PROCESSING_LATENCY_PER_DOMAIN_HISTOGRAM";
 pub const TASK_PROCESSING_LATENCY_PER_TASK_LIST_HISTOGRAM: &str = "TASK_PROCESSING_LATENCY_PER_TASK_LIST_HISTOGRAM";
 pub const TASK_QUEUE_LATENCY_PER_DOMAIN: &str = "TASK_QUEUE_LATENCY_PER_DOMAIN";
 pub const TASK_QUEUE_LATENCY_PER_DOMAIN_HISTOGRAM: &str = "TASK_QUEUE_LATENCY_PER_DOMAIN_HISTOGRAM";
 pub const TASK_QUEUE_LATENCY_PER_TASK_LIST_HISTOGRAM: &str = "TASK_QUEUE_LATENCY_PER_TASK_LIST_HISTOGRAM";
 pub const TASK_REDISPATCH_QUEUE_PENDING_TASKS_HISTOGRAM: &str = "TASK_REDISPATCH_QUEUE_PENDING_TASKS_HISTOGRAM";
 pub const TASK_REDISPATCH_QUEUE_PENDING_TASKS_TIMER: &str = "TASK_REDISPATCH_QUEUE_PENDING_TASKS_TIMER";
 pub const TASK_REQUESTS_PER_DOMAIN: &str = "TASK_REQUESTS_PER_DOMAIN";
 pub const TASK_REQUESTS_PER_TASK_LIST: &str = "TASK_REQUESTS_PER_TASK_LIST";
 pub const TASK_SCHEDULER_ALLOWED_COUNTER_PER_DOMAIN: &str = "TASK_SCHEDULER_ALLOWED_COUNTER_PER_DOMAIN";
 pub const TASK_SCHEDULER_RATE_LIMITER_SCOPE: &str = "TASK_SCHEDULER_RATE_LIMITER_SCOPE";
 pub const TASK_SCHEDULER_THROTTLED_COUNTER_PER_DOMAIN: &str = "TASK_SCHEDULER_THROTTLED_COUNTER_PER_DOMAIN";
 pub const TASK_SCHEDULE_LATENCY_PER_DOMAIN: &str = "TASK_SCHEDULE_LATENCY_PER_DOMAIN";
 pub const TASK_SCHEDULE_LATENCY_PER_TASK_LIST_HISTOGRAM: &str = "TASK_SCHEDULE_LATENCY_PER_TASK_LIST_HISTOGRAM";
 pub const TASK_STANDBY_RETRY_COUNTER_PER_DOMAIN: &str = "TASK_STANDBY_RETRY_COUNTER_PER_DOMAIN";
 pub const TASK_UNSUPPORTED_PER_DOMAIN: &str = "TASK_UNSUPPORTED_PER_DOMAIN";
 pub const TASK_WORKFLOW_BUSY_PER_DOMAIN: &str = "TASK_WORKFLOW_BUSY_PER_DOMAIN";
 pub const TIMER_TASK_THROTTLED_COUNTER: &str = "TIMER_TASK_THROTTLED_COUNTER";
 pub const TRANSFER_TASK_MISSING_EVENT_COUNTER_PER_DOMAIN: &str = "TRANSFER_TASK_MISSING_EVENT_COUNTER_PER_DOMAIN";
 pub const TRANSFER_TASK_THROTTLED_COUNTER: &str = "TRANSFER_TASK_THROTTLED_COUNTER";
 pub const TIMER_ACTIVE_QUEUE_PROCESSOR_SCOPE: &str = "TIMER_ACTIVE_QUEUE_PROCESSOR_SCOPE";
 pub const TIMER_ACTIVE_TASK_ACTIVITY_RETRY_TIMER_SCOPE: &str = "TIMER_ACTIVE_TASK_ACTIVITY_RETRY_TIMER_SCOPE";
 pub const TIMER_ACTIVE_TASK_ACTIVITY_TIMEOUT_SCOPE: &str = "TIMER_ACTIVE_TASK_ACTIVITY_TIMEOUT_SCOPE";
 pub const TIMER_ACTIVE_TASK_DECISION_TIMEOUT_SCOPE: &str = "TIMER_ACTIVE_TASK_DECISION_TIMEOUT_SCOPE";
 pub const TIMER_ACTIVE_TASK_DELETE_HISTORY_EVENT_SCOPE: &str = "TIMER_ACTIVE_TASK_DELETE_HISTORY_EVENT_SCOPE";
 pub const TIMER_ACTIVE_TASK_USER_TIMER_SCOPE: &str = "TIMER_ACTIVE_TASK_USER_TIMER_SCOPE";
 pub const TIMER_ACTIVE_TASK_WORKFLOW_BACKOFF_TIMER_SCOPE: &str = "TIMER_ACTIVE_TASK_WORKFLOW_BACKOFF_TIMER_SCOPE";
 pub const TIMER_ACTIVE_TASK_WORKFLOW_TIMEOUT_SCOPE: &str = "TIMER_ACTIVE_TASK_WORKFLOW_TIMEOUT_SCOPE";
 pub const TIMER_STANDBY_QUEUE_PROCESSOR_SCOPE: &str = "TIMER_STANDBY_QUEUE_PROCESSOR_SCOPE";
 pub const TIMER_STANDBY_TASK_ACTIVITY_RETRY_TIMER_SCOPE: &str = "TIMER_STANDBY_TASK_ACTIVITY_RETRY_TIMER_SCOPE";
 pub const TIMER_STANDBY_TASK_ACTIVITY_TIMEOUT_SCOPE: &str = "TIMER_STANDBY_TASK_ACTIVITY_TIMEOUT_SCOPE";
 pub const TIMER_STANDBY_TASK_DECISION_TIMEOUT_SCOPE: &str = "TIMER_STANDBY_TASK_DECISION_TIMEOUT_SCOPE";
 pub const TIMER_STANDBY_TASK_DELETE_HISTORY_EVENT_SCOPE: &str = "TIMER_STANDBY_TASK_DELETE_HISTORY_EVENT_SCOPE";
 pub const TIMER_STANDBY_TASK_USER_TIMER_SCOPE: &str = "TIMER_STANDBY_TASK_USER_TIMER_SCOPE";
 pub const TIMER_STANDBY_TASK_WORKFLOW_BACKOFF_TIMER_SCOPE: &str = "TIMER_STANDBY_TASK_WORKFLOW_BACKOFF_TIMER_SCOPE";
 pub const TIMER_STANDBY_TASK_WORKFLOW_TIMEOUT_SCOPE: &str = "TIMER_STANDBY_TASK_WORKFLOW_TIMEOUT_SCOPE";
 pub const TRANSFER_ACTIVE_QUEUE_PROCESSOR_SCOPE: &str = "TRANSFER_ACTIVE_QUEUE_PROCESSOR_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_ACTIVITY_SCOPE: &str = "TRANSFER_ACTIVE_TASK_ACTIVITY_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_APPLY_PARENT_CLOSE_POLICY_SCOPE: &str = "TRANSFER_ACTIVE_TASK_APPLY_PARENT_CLOSE_POLICY_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_CANCEL_EXECUTION_SCOPE: &str = "TRANSFER_ACTIVE_TASK_CANCEL_EXECUTION_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_CLOSE_EXECUTION_SCOPE: &str = "TRANSFER_ACTIVE_TASK_CLOSE_EXECUTION_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_DECISION_SCOPE: &str = "TRANSFER_ACTIVE_TASK_DECISION_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_RECORD_CHILD_EXECUTION_COMPLETED_SCOPE: &str = "TRANSFER_ACTIVE_TASK_RECORD_CHILD_EXECUTION_COMPLETED_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_RECORD_WORKFLOW_CLOSED_SCOPE: &str = "TRANSFER_ACTIVE_TASK_RECORD_WORKFLOW_CLOSED_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_RECORD_WORKFLOW_STARTED_SCOPE: &str = "TRANSFER_ACTIVE_TASK_RECORD_WORKFLOW_STARTED_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_RESET_WORKFLOW_SCOPE: &str = "TRANSFER_ACTIVE_TASK_RESET_WORKFLOW_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_SIGNAL_EXECUTION_SCOPE: &str = "TRANSFER_ACTIVE_TASK_SIGNAL_EXECUTION_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_START_CHILD_EXECUTION_SCOPE: &str = "TRANSFER_ACTIVE_TASK_START_CHILD_EXECUTION_SCOPE";
 pub const TRANSFER_ACTIVE_TASK_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES_SCOPE: &str = "TRANSFER_ACTIVE_TASK_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES_SCOPE";
 pub const TRANSFER_STANDBY_QUEUE_PROCESSOR_SCOPE: &str = "TRANSFER_STANDBY_QUEUE_PROCESSOR_SCOPE";
 pub const TRANSFER_STANDBY_TASK_ACTIVITY_SCOPE: &str = "TRANSFER_STANDBY_TASK_ACTIVITY_SCOPE";
 pub const TRANSFER_STANDBY_TASK_APPLY_PARENT_CLOSE_POLICY_SCOPE: &str = "TRANSFER_STANDBY_TASK_APPLY_PARENT_CLOSE_POLICY_SCOPE";
 pub const TRANSFER_STANDBY_TASK_CANCEL_EXECUTION_SCOPE: &str = "TRANSFER_STANDBY_TASK_CANCEL_EXECUTION_SCOPE";
 pub const TRANSFER_STANDBY_TASK_CLOSE_EXECUTION_SCOPE: &str = "TRANSFER_STANDBY_TASK_CLOSE_EXECUTION_SCOPE";
 pub const TRANSFER_STANDBY_TASK_DECISION_SCOPE: &str = "TRANSFER_STANDBY_TASK_DECISION_SCOPE";
 pub const TRANSFER_STANDBY_TASK_RECORD_CHILD_EXECUTION_COMPLETED_SCOPE: &str = "TRANSFER_STANDBY_TASK_RECORD_CHILD_EXECUTION_COMPLETED_SCOPE";
 pub const TRANSFER_STANDBY_TASK_RECORD_WORKFLOW_CLOSED_SCOPE: &str = "TRANSFER_STANDBY_TASK_RECORD_WORKFLOW_CLOSED_SCOPE";
 pub const TRANSFER_STANDBY_TASK_RECORD_WORKFLOW_STARTED_SCOPE: &str = "TRANSFER_STANDBY_TASK_RECORD_WORKFLOW_STARTED_SCOPE";
 pub const TRANSFER_STANDBY_TASK_RESET_WORKFLOW_SCOPE: &str = "TRANSFER_STANDBY_TASK_RESET_WORKFLOW_SCOPE";
 pub const TRANSFER_STANDBY_TASK_SIGNAL_EXECUTION_SCOPE: &str = "TRANSFER_STANDBY_TASK_SIGNAL_EXECUTION_SCOPE";
 pub const TRANSFER_STANDBY_TASK_START_CHILD_EXECUTION_SCOPE: &str = "TRANSFER_STANDBY_TASK_START_CHILD_EXECUTION_SCOPE";
 pub const TRANSFER_STANDBY_TASK_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES_SCOPE: &str = "TRANSFER_STANDBY_TASK_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES_SCOPE";
}

// TODO(link): replace by config::Config.
pub trait HistoryConfig: Send + Sync {
 fn enable_debug_mode(&self)->bool;
 fn enable_task_info_log_by_domain_id(&self,domain:&str)->bool;
 fn enable_drop_stuck_task_by_domain_id(&self,domain:&str)->bool;
 fn task_process_rps(&self)->i64;
 fn task_critical_retry_count(&self)->i32;
 fn number_of_shards(&self)->i32;
 fn task_scheduler_global_domain_rps(&self,domain:&str)->i32;
 fn task_scheduler_enable_rate_limiter(&self)->BoolPropertyFn;
 fn task_scheduler_enable_rate_limiter_shadow_mode(&self)->BoolPropertyFnWithDomainFilter;
}
// TODO(link): replace by shard::Controller.
pub trait ShardController: Send + Sync {fn num_shards(&self)->usize;}
// TODO(link): replace by resource::Resource.
pub trait Service: Send + Sync {
 fn get_domain_metrics_scope_cache(&self)->Arc<dyn DomainMetricsScopeCache>;
 fn get_history_task_dlq_manager(&self)->Arc<dyn crate::taskdlq::processor::HistoryTaskDLQManager>;
}
// TODO(link): replace by shard::Context.
pub trait ShardContext: Send + Sync {
 fn get_shard_id(&self)->i32;
 fn get_time_source(&self)->Arc<dyn TimeSource>;
 fn get_config(&self)->Arc<dyn HistoryConfig>;
 fn get_domain_cache(&self)->Arc<dyn DomainCache>;
 fn get_cluster_metadata(&self)->Arc<dyn ClusterMetadata>;
 fn get_active_cluster_manager(&self)->Arc<dyn ActiveClusterManager>;
 fn get_metrics_client(&self)->Arc<dyn MetricsClient>;
 fn get_service(&self)->Arc<dyn Service>;
 fn get_logger(&self)->Logger;
 fn update_if_needed_and_get_queue_max_read_level(&self,category:HistoryTaskCategory,cluster:&str)->HistoryTaskKey;
 /// Go `shard.Context` is itself a `TaskReinjector`.
 fn as_task_reinjector(&self)->Arc<dyn crate::taskdlq::interface::TaskReinjector>;
}
