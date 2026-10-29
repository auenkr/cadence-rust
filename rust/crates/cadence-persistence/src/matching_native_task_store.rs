//! Byte-preserving native persistence contract for Matching task queues.
//!
//! This mirrors [`crate::matching_task_store::MatchingTaskStore`] while keeping
//! Go strings as their original bytes throughout keys and blob models. This is
//! an interface only: no backend currently implements this trait.

use crate::matching_blob::{SqlRunIdBytesError, TaskDataBytes, TaskListDataBytes};
use cadence_common::go_string::GoString;
use thiserror::Error;
use uuid::Uuid;

use crate::matching_task_store::{AllocatedTask, BackendErrorKind, MatchingTaskStoreError};

/// A native queue key. Hashing and equality include the domain, raw name bytes,
/// and Go persistence task type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskListKeyBytes {
    pub domain_id: Uuid,
    pub name: GoString,
    /// Go persistence types: 0 decision, 1 activity (not protobuf enum values).
    pub task_type: i32,
}

impl From<crate::matching_task_store::TaskListKey> for TaskListKeyBytes {
    fn from(value: crate::matching_task_store::TaskListKey) -> Self {
        Self {
            domain_id: value.domain_id,
            name: value.name.into(),
            task_type: value.task_type,
        }
    }
}
impl TryFrom<&TaskListKeyBytes> for crate::matching_task_store::TaskListKey {
    type Error = std::string::FromUtf8Error;
    fn try_from(value: &TaskListKeyBytes) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            domain_id: value.domain_id,
            name: value.name.clone().into_utf8()?,
            task_type: value.task_type,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListRecordBytes {
    pub range_id: i64,
    pub data: TaskListDataBytes,
}
impl From<crate::matching_task_store::TaskListRecord> for TaskListRecordBytes {
    fn from(v: crate::matching_task_store::TaskListRecord) -> Self {
        Self {
            range_id: v.range_id,
            data: v.data.into(),
        }
    }
}
impl TryFrom<TaskListRecordBytes> for crate::matching_task_store::TaskListRecord {
    type Error = std::string::FromUtf8Error;
    fn try_from(v: TaskListRecordBytes) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            range_id: v.range_id,
            data: v.data.try_into()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocatedTaskBytes {
    pub task_id: i64,
    pub data: TaskDataBytes,
}
impl From<AllocatedTask> for AllocatedTaskBytes {
    fn from(v: AllocatedTask) -> Self {
        Self {
            task_id: v.task_id,
            data: v.data.into(),
        }
    }
}
impl TryFrom<AllocatedTaskBytes> for AllocatedTask {
    type Error = std::string::FromUtf8Error;
    fn try_from(v: AllocatedTaskBytes) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            task_id: v.task_id,
            data: v.data.try_into()?,
        })
    }
}

/// Error path retains malformed RunID bytes. Backend errors preserve the
/// legacy transient classification; no fatal raw identifier is stringified.
#[derive(Debug, Error)]
pub enum MatchingTaskStoreBytesError {
    #[error(transparent)]
    Legacy(#[from] MatchingTaskStoreError),
    #[error(transparent)]
    MalformedSqlRunId(#[from] SqlRunIdBytesError),
    /// Go error messages can themselves contain raw task-list bytes. Display
    /// is diagnostic; native transports must use the original message bytes.
    #[error("task-list condition failed: {message:?}")]
    ConditionFailed { message: GoString },
    #[error("task-store backend operation failed ({kind:?}): {message}")]
    Backend {
        kind: BackendErrorKind,
        message: String,
    },
}
impl MatchingTaskStoreBytesError {
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Legacy(e) => e.is_transient(),
            Self::MalformedSqlRunId(_) | Self::ConditionFailed { .. } => false,
            Self::Backend { kind, .. } => matches!(
                kind,
                BackendErrorKind::Timeout
                    | BackendErrorKind::Throttled
                    | BackendErrorKind::Internal
                    | BackendErrorKind::DbUnavailable
            ),
        }
    }

    pub fn lease_renewal(key: &TaskListKeyBytes, got_range: i64) -> Self {
        let mut message = b"leaseTaskList:renew failed:taskList:".to_vec();
        message.extend_from_slice(key.name.as_bytes());
        message.extend_from_slice(
            format!(
                ", taskListType:{}, haveRangeID:0, gotRangeID:{got_range}",
                key.task_type
            )
            .as_bytes(),
        );
        Self::ConditionFailed {
            message: GoString::from(message),
        }
    }
}
pub type Result<T> = std::result::Result<T, MatchingTaskStoreBytesError>;

/// Same transaction, paging, lease fencing, and cross-domain task semantics as
/// the legacy store, with original string bytes carried through every method.
pub trait MatchingTaskStoreBytes: Send + Sync {
    fn fetch_latest_config(
        &self,
        _row_type: i32,
    ) -> std::result::Result<Option<crate::config_store::ConfigStoreRow>, String> {
        Err("this task store has no config store".into())
    }
    fn get_task_list(&self, key: &TaskListKeyBytes) -> Result<TaskListRecordBytes>;
    fn lease(
        &self,
        key: &TaskListKeyBytes,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecordBytes>;
    fn update_task_list(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        data: TaskListDataBytes,
        now_nanos: i64,
    ) -> Result<()>;
    fn create_tasks_with_domains(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTaskBytes)],
    ) -> Result<()>;
    /// Returns `(read_level, max_read_level]` in ascending task ID order,
    /// bounded by `batch_size`; expired rows are included for queue-side handling.
    fn get_tasks(
        &self,
        key: &TaskListKeyBytes,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTaskBytes>>;
    fn task_list_size(&self, key: &TaskListKeyBytes, ack_level: i64) -> Result<i64>;
    fn complete_task(&self, key: &TaskListKeyBytes, task_id: i64) -> Result<()>;
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKeyBytes,
        inclusive_task_id: i64,
        limit: i64,
    ) -> Result<usize>;
    /// Signed Go TasksCompleted, including Cassandra's unknown count (-1).
    fn gc_complete_tasks_less_than(&self, key: &TaskListKeyBytes, ack: i64, limit: i64) -> Result<i64> {
        self.complete_tasks_less_than(key, ack, limit).map(|count| count as i64)
    }
    fn delete_task_list(&self, key: &TaskListKeyBytes, range: i64) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_blob::{PartitionConfig, TaskData, TaskListData};
    use std::collections::HashSet;

    fn key(domain: Uuid, name: &[u8], task_type: i32) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: domain,
            name: GoString::from(name),
            task_type,
        }
    }

    #[test]
    fn raw_invalid_and_replacement_rune_keys_are_isolated() {
        let domain = Uuid::nil();
        let keys = [
            key(domain, b"\xff", 0),
            key(domain, b"\xfe", 0),
            key(domain, "�".as_bytes(), 0),
        ];
        let set: HashSet<_> = keys.iter().cloned().collect();
        assert_eq!(set.len(), 3);
        assert_ne!(keys[0], keys[1]);
        assert_ne!(keys[0], keys[2]);
        assert_ne!(keys[1], keys[2]);
    }

    #[test]
    fn keys_separate_domain_and_task_type() {
        let domain = Uuid::nil();
        let other = Uuid::max();
        let base = key(domain, b"queue", 0);
        assert_ne!(base, key(other, b"queue", 0));
        assert_ne!(base, key(domain, b"queue", 1));
        assert_eq!(
            HashSet::from([
                base.clone(),
                key(other, b"queue", 0),
                key(domain, b"queue", 1)
            ])
            .len(),
            3
        );
    }

    #[test]
    fn valid_legacy_conversions_preserve_values_and_optional_containers() {
        let mut config = std::collections::BTreeMap::new();
        config.insert("k".to_owned(), "v".to_owned());
        let data = TaskData {
            workflow_id: "workflow".into(),
            run_id: Some(vec![1, 2]),
            raw_run_id: Some("raw".into()),
            schedule_id: 7,
            expiry_nanos: 8,
            created_nanos: 9,
            partition_config: Some(config),
        };
        let bytes = TaskDataBytes::from(data.clone());
        assert_eq!(TaskData::try_from(bytes).unwrap(), data);
        let absent = TaskListData::default();
        assert_eq!(
            TaskListData::try_from(TaskListDataBytes::from(absent.clone())).unwrap(),
            absent
        );
        let present_empty = TaskListData {
            adaptive_partition_config: Some(PartitionConfig {
                read_partitions: Some(Default::default()),
                write_partitions: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            TaskListData::try_from(TaskListDataBytes::from(present_empty.clone())).unwrap(),
            present_empty
        );
    }
}
