//! Backend-neutral persistence boundary for durable Matching task queues.
//!
//! The synchronous methods mirror the existing SQLite queue operations and
//! their result contract. Implementations must preserve Cadence transaction,
//! paging, expiry and lease-fencing semantics at this boundary.

use crate::matching_sqlite::{
    AllocatedTask, Result, TaskListKey, TaskListRecord,
};
use uuid::Uuid;

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
