//! Backend-neutral persistence boundary for durable Matching task queues.
//!
//! The synchronous methods mirror the existing SQLite queue operations and
//! their result contract. Implementations must preserve Cadence transaction,
//! paging, expiry and lease-fencing semantics at this boundary.

use crate::matching_blob::{TaskData, TaskListData};
use thiserror::Error;
use uuid::Uuid;

/// Driver-independent task-store errors. The SQLite-specific `Sql` and `Blob`
/// variants remain for source compatibility and preserve the old SQLite API;
/// new adapters normalize their driver failures into `Backend`.
#[derive(Debug, Error)]
pub enum MatchingTaskStoreError {
    #[error("unsupported SQLite task-store configuration: {0}")]
    UnsupportedConfiguration(String),
    #[error("incompatible Go SQLite Matching schema: {0}")]
    IncompatibleSchema(String),
    #[error("unsupported Matching task-store backend configuration: {0}")]
    BackendConfiguration(String),
    #[error("incompatible Matching task-store backend schema: {0}")]
    BackendSchema(String),
    #[error("{0}")]
    ConditionFailed(String),
    #[error("task list does not exist")]
    NotFound,
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Blob(#[from] cadence_thrift::Error),
    #[error("missing limit parameter")]
    MissingLimit,
    #[error("delete failed: {0} rows affected instead of 1")]
    DeleteCount(usize),
    #[error("task-store backend operation failed ({kind:?}): {message}")]
    Backend {
        kind: BackendErrorKind,
        message: String,
    },
}

/// Go's SQL adapter maps MySQL timeouts, throttling and other driver failures
/// to distinct service errors; all three are persistence-retryable there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendErrorKind {
    Timeout,
    Throttled,
    Internal,
    Permanent,
    /// Go `persistence.DBUnavailableError` (nosql `IsDBUnavailableError`).
    DbUnavailable,
    /// Go `types.BadRequestError`.
    BadRequest,
}

impl MatchingTaskStoreError {
    /// Matches Go persistence retry eligibility for SQLite today and explicit
    /// driver classification for network SQL adapters.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Sql(_)
                | Self::Backend {
                    kind: BackendErrorKind::Timeout
                        | BackendErrorKind::Throttled
                        | BackendErrorKind::Internal
                        | BackendErrorKind::DbUnavailable,
                    ..
                }
        )
    }
}

pub type StorageError = MatchingTaskStoreError;
pub type Result<T> = std::result::Result<T, MatchingTaskStoreError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListKey {
    pub domain_id: Uuid,
    pub name: String,
    /// Go persistence types: 0 decision, 1 activity (not protobuf enum values).
    pub task_type: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListRecord {
    pub range_id: i64,
    pub data: TaskListData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocatedTask {
    pub task_id: i64,
    pub data: TaskData,
}

/// Persistence operations consumed by `DurableTaskQueue`.
///
/// `create_tasks_with_domains` retains Go's cross-domain task row key behavior:
/// rows use each task's source domain while the lease fence applies to `key`.
pub trait MatchingTaskStore: Send + Sync {
    fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord>;

    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecord>;

    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        data: crate::matching_blob::TaskListData,
        now_nanos: i64,
    ) -> Result<()>;

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> Result<()>;

    /// Return tasks in the Go interval `(read_level, max_read_level]`, in
    /// ascending task ID order and bounded by `batch_size`. Expiry is handled
    /// by the queue reader, so implementations return expired rows too.
    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTask>>;

    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> Result<i64>;
    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()>;
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        limit: i64,
    ) -> Result<usize>;
    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()>;
}
