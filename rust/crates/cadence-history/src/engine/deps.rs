//! Minimal stand-ins for Go packages that other workers port concurrently
//! (`execution`, `shard`, `decision`, `queue`, `ndc`, `replication`, `reset`,
//! `events`, `failover`, `taskdlq`, persistence managers, domain cache, ...).
//!
//! Each trait/struct carries the Go name and exposes only the methods used by
//! `engine` and `handler`. Receivers are `&self` (Go pointer semantics; the
//! implementation owns its interior mutability).
// TODO(link): replace every item in this module by the real port once the
// owning package lands (see the `TODO(link)` marker on each item).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cadence_api::errors::{
    AccessDeniedError, BadRequestError, CancellationAlreadyRequestedError,
    ClientVersionNotSupportedError, DomainNotActiveError, EntityNotExistsError,
    InternalServiceError, LimitExceededError, QueryFailedError, ServiceBusyError, EventAlreadyStartedError,
    ShardOwnershipLostError, WorkflowExecutionAlreadyCompletedError,
    WorkflowExecutionAlreadyStartedError,
};
use cadence_api::CadenceError;
use futures_util::future::BoxFuture;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Error type returned by every engine/handler method. Wraps the public
/// `types.*Error` family and adds the package-local sentinel errors that Go
/// compares by identity (`workflow.ErrStaleState`, `execution.ErrMissingWorkflowStartEvent`,
/// `persistence.*Error`).
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error(transparent)]
    Cadence(Box<CadenceError>),
    #[error(transparent)]
    EventAlreadyStarted(#[from] EventAlreadyStartedError),
    /// `workflow.ErrStaleState`
    #[error("cache mutable state could potentially be stale")]
    StaleState,
    /// `workflow.ErrMaxAttemptsExceeded`
    #[error("maximum attempts exceeded to update history")]
    MaxAttemptsExceeded,
    /// `execution.ErrMissingWorkflowStartEvent`
    #[error("unable to load start event for workflow")]
    MissingWorkflowStartEvent,
    /// `context.DeadlineExceeded`
    #[error("context deadline exceeded")]
    DeadlineExceeded,
    /// `context.Canceled`
    #[error("context canceled")]
    Canceled,
    /// `persistence.DuplicateRequestError`
    #[error("Request has already been applied to runID: {run_id}")]
    DuplicateRequest { request_type: WorkflowRequestType, run_id: String },
    /// `persistence.WorkflowExecutionAlreadyStartedError`
    #[error("{0}")]
    PersistenceAlreadyStarted(Box<PersistenceAlreadyStarted>),
    /// `persistence.CurrentWorkflowConditionFailedError`
    #[error("{0}")]
    CurrentWorkflowConditionFailed(String),
    /// `persistence.ConditionFailedError`
    #[error("{0}")]
    ConditionFailed(String),
    /// `persistence.ShardAlreadyExistError`
    #[error("{0}")]
    ShardAlreadyExist(String),
    /// `persistence.ShardOwnershipLostError` (`ShardID` when the persistence layer knows it).
    #[error("{message}")]
    PersistenceShardOwnershipLost { message: String, shard_id: Option<i32> },
    /// `persistence.TimeoutError`
    #[error("{0}")]
    PersistenceTimeout(String),
    #[error("{message}")]
    CurrentBranchChanged { message: String, current_branch_token: Vec<u8> },
    #[error("sticky worker unavailable")]
    StickyWorkerUnavailable,
    #[error("version histories do not exist")]
    MissingVersionHistories,
    /// `activecluster.ClusterAttributeNotFoundError`
    #[error("{0}")]
    ClusterAttributeNotFound(String),
    /// `persistence.TransactionSizeLimitError`
    #[error("{0}")]
    TransactionSizeLimit(String),
    /// Anything else (Go `errors.New` / `fmt.Errorf`).
    #[error("{0}")]
    Other(String),
}

pub type HResult<T> = Result<T, HistoryError>;

/// Go `activecluster.ClusterAttributeNotFoundError.Error()`: `%s` of a `*types.ClusterAttribute`
/// (no `String` method): `&{Scope Name}`, or `%!s(*types.ClusterAttribute=<nil>)` for nil.
pub fn cluster_attribute_not_found_message(domain_id: &str, attribute: Option<&cadence_api::types::ClusterAttribute>) -> String {
    let attr = match attribute {
        Some(a) => format!("&{{{} {}}}", a.scope, a.name),
        None => "%!s(*types.ClusterAttribute=<nil>)".to_owned(),
    };
    format!("could not find cluster attribute {attr} in the domain {domain_id}'s active cluster config")
}

macro_rules! history_error_from {
    ($($t:ty),* $(,)?) => { $(impl From<$t> for HistoryError {
        fn from(e: $t) -> Self { HistoryError::Cadence(Box::new(CadenceError::from(e))) }
    })* };
}
history_error_from!(
    AccessDeniedError,
    BadRequestError,
    CancellationAlreadyRequestedError,
    ClientVersionNotSupportedError,
    DomainNotActiveError,
    EntityNotExistsError,
    InternalServiceError,
    LimitExceededError,
    QueryFailedError,
    ServiceBusyError,
    ShardOwnershipLostError,
    WorkflowExecutionAlreadyCompletedError,
    WorkflowExecutionAlreadyStartedError,
);

impl HistoryError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        BadRequestError::new(msg).into()
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        InternalServiceError::new(msg).into()
    }
    pub fn entity_not_exists(msg: impl Into<String>) -> Self {
        EntityNotExistsError { message: msg.into(), ..Default::default() }.into()
    }
    pub fn is_entity_not_exists(&self) -> bool {
        matches!(self, HistoryError::Cadence(e) if matches!(&**e, CadenceError::EntityNotExists(_)))
    }
    pub fn is_workflow_already_completed(&self) -> bool {
        matches!(self, HistoryError::Cadence(e) if matches!(&**e, CadenceError::WorkflowAlreadyCompleted(_)))
    }
    pub fn is_service_busy(&self) -> bool {
        matches!(self, HistoryError::Cadence(e) if matches!(&**e, CadenceError::ServiceBusy(_)))
    }
    /// `persistence.IsTimeoutError`
    pub fn is_timeout(&self) -> bool {
        matches!(self, HistoryError::PersistenceTimeout(_) | HistoryError::DeadlineExceeded)
    }
    /// `execution.IsConflictError`
    pub fn is_conflict_error(&self) -> bool {
        matches!(self, HistoryError::ConditionFailed(_))
    }
}

/// Fields of `persistence.WorkflowExecutionAlreadyStartedError`.
#[derive(Debug, Clone, Default)]
pub struct PersistenceAlreadyStarted {
    pub msg: String,
    pub start_request_id: String,
    pub run_id: String,
    pub state: i32,
    pub close_status: i32,
    pub last_write_version: i64,
}
impl std::fmt::Display for PersistenceAlreadyStarted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

// ---------------------------------------------------------------------------
// persistence constants / small types (TODO(link): persistence package)
// ---------------------------------------------------------------------------

pub const WORKFLOW_STATE_CREATED: i32 = 0;
pub const WORKFLOW_STATE_RUNNING: i32 = 1;
pub const WORKFLOW_STATE_COMPLETED: i32 = 2;
pub const WORKFLOW_STATE_ZOMBIE: i32 = 3;
pub const WORKFLOW_STATE_VOID: i32 = 4;
pub const WORKFLOW_STATE_CORRUPTED: i32 = 5;

pub const WORKFLOW_CLOSE_STATUS_NONE: i32 = 0;
pub const WORKFLOW_CLOSE_STATUS_COMPLETED: i32 = 1;
pub const WORKFLOW_CLOSE_STATUS_FAILED: i32 = 2;
pub const WORKFLOW_CLOSE_STATUS_CANCELED: i32 = 3;
pub const WORKFLOW_CLOSE_STATUS_TERMINATED: i32 = 4;
pub const WORKFLOW_CLOSE_STATUS_CONTINUED_AS_NEW: i32 = 5;
pub const WORKFLOW_CLOSE_STATUS_TIMED_OUT: i32 = 6;

pub const DOMAIN_STATUS_REGISTERED: i32 = 0;
pub const DOMAIN_STATUS_DEPRECATED: i32 = 1;
pub const DOMAIN_STATUS_DELETED: i32 = 2;

pub const EMPTY_EVENT_ID: i64 = -23;
pub const FIRST_EVENT_ID: i64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HistoryTaskCategory {
    Transfer,
    Timer,
    Replication,
    CrossCluster,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowRequestType {
    Start,
    Signal,
    Cancel,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateWorkflowMode {
    BrandNew,
    WorkflowIdReuse,
    ContinueAsNew,
    Zombie,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateWorkflowRequestMode {
    New,
    Replicated,
}

/// `persistence.WorkflowExecutionInfo` (only the fields the engine reads).
#[derive(Debug, Clone, Default)]
pub struct WorkflowExecutionInfo {
    pub domain_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub first_execution_run_id: String,
    pub create_request_id: String,
    pub active_cluster_selection_policy: Option<cadence_api::types::ActiveClusterSelectionPolicy>,
    pub parent_workflow_id: String,
    pub parent_run_id: String,
    pub workflow_type_name: String,
    pub state: i32,
    pub close_status: i32,
    pub signal_count: i32,
    pub start_timestamp: i64,
    pub task_list: String,
    pub sticky_task_list: String,
    pub decision_schedule_id: i64,
    pub decision_started_id: i64,
    pub decision_attempt: i64,
    pub cron_schedule: String,
    pub has_retry_policy: bool,
    pub attempt: i32,
    pub client_library_version: String,
    pub client_feature_version: String,
    pub client_impl: String,
    pub sticky_schedule_to_start_timeout: i32,
    pub auto_reset_points: Option<cadence_api::types::ResetPoints>,
    pub workflow_timeout: i32,
    pub decision_start_to_close_timeout: i32,
    pub task_list_kind: Option<cadence_api::enums::TaskListKind>,
    pub memo: Option<std::collections::HashMap<String, Vec<u8>>>,
    pub search_attributes: Option<std::collections::HashMap<String, Vec<u8>>>,
    pub partition_config: Option<std::collections::HashMap<String, String>>,
    pub cron_overlap_policy: Option<cadence_api::enums::CronOverlapPolicy>,
    pub last_updated_timestamp: i64,
    pub parent_domain_id: String,
    pub initiated_id: i64,
}
impl WorkflowExecutionInfo {
    /// `IsRunning`
    pub fn is_running(&self) -> bool {
        self.state == WORKFLOW_STATE_CREATED || self.state == WORKFLOW_STATE_RUNNING
    }
}

/// `persistence.ActivityInfo` (fields used by the engine).
#[derive(Debug, Clone, Default)]
pub struct ActivityInfo {
    pub schedule_id: i64,
    pub started_id: i64,
    pub activity_id: String,
    pub request_id: String,
    pub details: Vec<u8>,
    pub attempt: i32,
    pub started_time: i64,
    pub scheduled_time: i64,
    pub schedule_to_close_timeout: i32,
    pub cancel_requested: bool,
    pub cancel_request_id: i64,
    pub has_retry_policy: bool,
    pub version: i64,
    pub started_identity: String,
    pub task_list: String,
    pub last_heart_beat_updated_time: i64,
    pub expiration_time: i64,
    pub maximum_attempts: i32,
    pub last_failure_reason: String,
    pub last_failure_details: Vec<u8>,
    pub last_failure_category: Option<cadence_api::enums::FailureCategory>,
    pub last_retry_interval_seconds: i32,
    pub last_worker_identity: String,
}

/// `cache.DomainCacheEntry` info (TODO(link): common/cache).
#[derive(Debug, Clone, Default)]
pub struct DomainInfo {
    pub id: String,
    pub name: String,
    pub status: i32,
}

#[derive(Debug, Clone, Default)]
pub struct DomainCacheEntry {
    pub info: DomainInfo,
    pub is_global_domain: bool,
    pub active_clusters: Vec<(Option<cadence_api::types::ClusterAttribute>, String)>,
    /// `GetFailoverVersion()`.
    pub failover_version: i64,
    /// Per cluster attribute `ActiveClusterInfo.FailoverVersion` of `ActiveClusters.AttributeScopes`.
    pub active_cluster_versions: Vec<(cadence_api::types::ClusterAttribute, i64)>,
    /// `failoverEndTime`; non-nil means the domain is pending active.
    pub failover_end_time: Option<i64>,
    /// `GetReplicationConfig().Clusters` names.
    pub clusters: Vec<String>,
    /// Checksums of `GetConfig().BadBinaries.Binaries`.
    pub bad_binaries: Vec<String>,
}

impl DomainCacheEntry {
    /// `DomainCacheEntry.GetActiveClusterInfoByClusterAttribute`: returns `(ActiveClusterName, FailoverVersion)`.
    /// A nil attribute yields the domain-level active cluster; an unknown attribute yields `None`.
    pub fn get_active_cluster_info_by_cluster_attribute(
        &self,
        attribute: Option<&cadence_api::types::ClusterAttribute>,
    ) -> Option<(String, i64)> {
        match attribute {
            None => self.active_clusters.iter().find(|(a, _)| a.is_none()).map(|(_, c)| (c.clone(), self.failover_version)),
            Some(attr) => {
                let (_, cluster) = self.active_clusters.iter().find(|(a, _)| a.as_ref() == Some(attr))?;
                let version = self.active_cluster_versions.iter().find(|(a, _)| a == attr).map(|(_, v)| *v).unwrap_or(self.failover_version);
                Some((cluster.clone(), version))
            }
        }
    }
}
impl DomainCacheEntry {
    /// `IsDomainPendingActive`
    pub fn is_domain_pending_active(&self) -> bool {
        self.failover_end_time.is_some()
    }
    pub fn get_info(&self) -> &DomainInfo {
        &self.info
    }
    /// `getActiveClusters` (Go `domainCache.go`): sorted attribute-scope cluster names, empty unless active-active.
    pub fn active_cluster_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.active_clusters.iter().filter(|(a, _)| a.is_some()).map(|(_, c)| c.clone()).collect();
        names.sort();
        names
    }
    /// `DomainReplicationConfiguration.IsActiveActive`: some attribute scope has a cluster attribute.
    pub fn is_active_active(&self) -> bool {
        self.active_clusters.iter().any(|(a, _)| a.is_some())
    }
    /// `NewDomainNotActiveError`
    pub fn new_domain_not_active_error(&self, current: &str, active: &str) -> HistoryError {
        if self.is_active_active() {
            let active_clusters = self.active_cluster_names();
            return DomainNotActiveError {
                message: format!(
                    "Domain: {} is active in cluster(s): [{}], while current cluster {} is a standby cluster. Operation active cluster: {}",
                    self.info.name, active_clusters.join(" "), current, active
                ),
                domain_name: self.info.name.clone(),
                current_cluster: current.to_string(),
                active_cluster: active.to_string(),
                active_clusters,
            }
            .into();
        }
        DomainNotActiveError {
            message: format!(
                "Domain: {} is active in cluster: {}, while current cluster {} is a standby cluster.",
                self.info.name, active, current
            ),
            domain_name: self.info.name.clone(),
            current_cluster: current.to_string(),
            active_cluster: active.to_string(),
            ..Default::default()
        }
        .into()
    }
}

#[async_trait]
pub trait DomainCache: Send + Sync {
    fn get_domain_by_id(&self, id: &str) -> HResult<Arc<DomainCacheEntry>>;
    fn get_domain(&self, name: &str) -> HResult<Arc<DomainCacheEntry>>;
    fn get_domain_name(&self, id: &str) -> HResult<String>;
    fn unregister_domain_change_callback(&self, shard_name: &str);
}

pub trait ClusterMetadata: Send + Sync {
    fn get_current_cluster_name(&self) -> String;
    fn cluster_name_for_failover_version(&self, version: i64) -> HResult<String>;
}

pub trait TimeSource: Send + Sync {
    /// Unix nanoseconds.
    fn now(&self) -> i64;
}

// ---------------------------------------------------------------------------
// execution package traits (TODO(link): execution::{Cache, Context, MutableState})
// ---------------------------------------------------------------------------

pub type ReleaseFunc = Arc<dyn Fn(Option<&HistoryError>) + Send + Sync>;
pub type ExecutionContextRef = Arc<dyn ExecutionContext>;
pub type MutableStateRef = Arc<dyn MutableState>;

pub use cadence_api::service_types::HistoryEvent;
pub use cadence_api::types::WorkflowExecution;

// MutableState and ExecutionContext are extended in `deps_ms.rs`.
pub use super::deps_ms::{ExecutionContext, MutableState};

/// `execution.Cache`
#[async_trait]
pub trait ExecutionCache: Send + Sync {
    async fn get_or_create_workflow_execution(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> HResult<(ExecutionContextRef, ReleaseFunc)>;
    async fn get_and_create_workflow_execution(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> HResult<(ExecutionContextRef, ExecutionContextRef, ReleaseFunc, bool)>;
    async fn get_or_create_current_workflow_execution(
        &self,
        domain_id: &str,
        workflow_id: &str,
    ) -> HResult<ReleaseFunc>;
}

/// Boxed future alias used by action callbacks.
pub type Fut<'a, T> = BoxFuture<'a, HResult<T>>;

pub type Map<K, V> = HashMap<K, V>;

impl From<CadenceError> for HistoryError { fn from(error: CadenceError) -> Self { Self::Cadence(Box::new(error)) } }

#[cfg(test)]
mod active_cluster_lookup_tests {
    use super::*;
    use cadence_api::types::ClusterAttribute;

    /// domainCache_test NewDomainNotActiveError: active-active form fills ActiveClusters (sorted) and uses the `%v` message.
    #[test]
    fn domain_not_active_error_active_active_form() {
        let mut entry = DomainCacheEntry { active_clusters: vec![(None, "c0".into()), (Some(attr("region", "us")), "c2".into()), (Some(attr("region", "eu")), "c1".into())], ..Default::default() };
        entry.info.name = "d".into();
        let HistoryError::Cadence(e) = entry.new_domain_not_active_error("c9", "c2") else { panic!() };
        let CadenceError::DomainNotActive(e) = *e else { panic!() };
        assert_eq!(e.active_clusters, vec!["c1".to_string(), "c2".to_string()]);
        assert_eq!(e.message, "Domain: d is active in cluster(s): [c1 c2], while current cluster c9 is a standby cluster. Operation active cluster: c2");
        assert_eq!((e.current_cluster.as_str(), e.active_cluster.as_str(), e.domain_name.as_str()), ("c9", "c2", "d"));
    }

    #[test]
    fn domain_not_active_error_plain_form() {
        let mut entry = DomainCacheEntry { active_clusters: vec![(None, "c0".into())], ..Default::default() };
        entry.info.name = "d".into();
        let HistoryError::Cadence(e) = entry.new_domain_not_active_error("c9", "c0") else { panic!() };
        let CadenceError::DomainNotActive(e) = *e else { panic!() };
        assert!(e.active_clusters.is_empty());
        assert_eq!(e.message, "Domain: d is active in cluster: c0, while current cluster c9 is a standby cluster.");
    }

    fn attr(scope: &str, name: &str) -> ClusterAttribute {
        ClusterAttribute { scope: scope.into(), name: name.into() }
    }

    /// activecluster manager_test: GetActiveClusterInfoByClusterAttribute table.
    #[test]
    fn get_active_cluster_info_by_cluster_attribute() {
        let entry = DomainCacheEntry {
            active_clusters: vec![(None, "c0".into()), (Some(attr("region", "us")), "c1".into()), (Some(attr("region", "eu")), "c2".into())],
            failover_version: 7,
            active_cluster_versions: vec![(attr("region", "us"), 11), (attr("region", "eu"), 12)],
            ..Default::default()
        };
        assert_eq!(entry.get_active_cluster_info_by_cluster_attribute(None), Some(("c0".into(), 7)));
        assert_eq!(entry.get_active_cluster_info_by_cluster_attribute(Some(&attr("region", "us"))), Some(("c1".into(), 11)));
        assert_eq!(entry.get_active_cluster_info_by_cluster_attribute(Some(&attr("region", "eu"))), Some(("c2".into(), 12)));
        assert_eq!(entry.get_active_cluster_info_by_cluster_attribute(Some(&attr("region", "asia"))), None);
        assert_eq!(entry.get_active_cluster_info_by_cluster_attribute(Some(&attr("zone", "us"))), None);
        let plain = DomainCacheEntry { active_clusters: vec![(None, "c0".into())], failover_version: 3, ..Default::default() };
        assert_eq!(plain.get_active_cluster_info_by_cluster_attribute(Some(&attr("region", "us"))), None);
        assert_eq!(plain.get_active_cluster_info_by_cluster_attribute(None), Some(("c0".into(), 3)));
    }

    #[test]
    fn duplicate_request_message_matches_go() {
        let e = HistoryError::DuplicateRequest { request_type: WorkflowRequestType::Signal, run_id: "r1".into() };
        assert_eq!(e.to_string(), "Request has already been applied to runID: r1");
    }
}
