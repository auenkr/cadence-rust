//! Fatal conditions that Go terminates the Matching host for (`logger.Fatal`).
//!
//! One channel carries every cause so the launcher keeps a single graceful
//! fatal path: SQL writer RunID conversion failures and task-reader ackManager
//! invariant violations.
use cadence_persistence::matching_blob::{SqlRunIdBytesError, SqlRunIdError};

#[derive(Debug, Clone)]
pub enum MatchingFatal {
    /// Go `sqlplugin` RunID parse failure while appending tasks.
    SqlRunId(SqlRunIdError),
    /// Native writer failure, retaining arbitrary request RunID bytes.
    SqlRunIdBytes(SqlRunIdBytesError),
    /// Go `task_reader.go` `addSingleTaskToBuffer`: `ackManager.ReadItem`
    /// rejected a duplicate or non-increasing task id.
    ReaderInvariant(String),
}

impl std::fmt::Display for MatchingFatal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Preserve the pre-existing SQL writer fatal text exactly.
            Self::SqlRunId(cause) => write!(f, "{cause}"),
            Self::SqlRunIdBytes(cause) => write!(f, "{cause}"),
            Self::ReaderInvariant(detail) => {
                write!(f, "Matching task reader invariant violated: {detail}")
            }
        }
    }
}

impl std::error::Error for MatchingFatal {}

pub type MatchingFatalSender = tokio::sync::mpsc::UnboundedSender<MatchingFatal>;

/// Route both native actor fatal causes through the launcher's single host
/// shutdown channel. Sending happens in the actor's callback before it releases
/// its completion guard or replies to the failed write.
pub fn native_fatal_sinks(sender: MatchingFatalSender) -> crate::matching_registry_native::NativeFatalSinks {
    let writer = sender.clone();
    crate::matching_registry_native::NativeFatalSinks {
        writer: Some(std::sync::Arc::new(move |cause| {
            let _ = writer.send(MatchingFatal::SqlRunIdBytes(cause));
        })),
        reader: Some(std::sync::Arc::new(move |detail| {
            let _ = sender.send(MatchingFatal::ReaderInvariant(detail));
        })),
    }
}

#[cfg(test)]
mod native_tests {
    use super::*;

    #[test]
    fn native_actor_sinks_share_host_channel_and_retain_run_id_bytes() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let sinks = native_fatal_sinks(sender);
        let raw = cadence_common::go_string::GoString::from(&b"\xff\x00\xfe"[..]);
        sinks.writer.unwrap()(SqlRunIdBytesError { raw: raw.clone(), message: "bad UUID".into() });
        match receiver.try_recv().unwrap() {
            MatchingFatal::SqlRunIdBytes(cause) => assert_eq!(cause.raw, raw),
            cause => panic!("unexpected fatal cause: {cause:?}"),
        }
        sinks.reader.unwrap()("duplicate task ID".into());
        assert!(matches!(receiver.try_recv().unwrap(), MatchingFatal::ReaderInvariant(detail) if detail == "duplicate task ID"));
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use cadence_persistence::{
        matching_blob::{TaskData, TaskListData},
        matching_sqlite::{AllocatedTask, SqliteTaskStore, TaskListKey, TaskListRecord},
        matching_task_store::{MatchingTaskStore, Result as StoreResult},
    };
    use std::sync::{Arc, Mutex};

    /// Delegates to SQLite but, once, answers the first non-empty `get_tasks`
    /// scan with caller-chosen task ids (duplicate or out-of-order rows that
    /// the real primary key never produces).
    pub(crate) struct ScriptedReadStore {
        pub(crate) inner: Arc<SqliteTaskStore>,
        pub(crate) script: Mutex<Option<Vec<i64>>>,
    }

    impl MatchingTaskStore for ScriptedReadStore {
        fn get_task_list(&self, key: &TaskListKey) -> StoreResult<TaskListRecord> {
            self.inner.get_task_list(key)
        }
        fn lease(
            &self,
            key: &TaskListKey,
            kind: i16,
            previous_range: i64,
            now_nanos: i64,
        ) -> StoreResult<TaskListRecord> {
            self.inner.lease(key, kind, previous_range, now_nanos)
        }
        fn update_task_list(
            &self,
            key: &TaskListKey,
            range: i64,
            data: TaskListData,
            now_nanos: i64,
        ) -> StoreResult<()> {
            self.inner.update_task_list(key, range, data, now_nanos)
        }
        fn create_tasks_with_domains(
            &self,
            key: &TaskListKey,
            range: i64,
            now_nanos: i64,
            tasks: &[(uuid::Uuid, &AllocatedTask)],
        ) -> StoreResult<()> {
            let _ = now_nanos;
            self.inner.create_tasks_with_domains(
                key,
                range,
                tasks.iter().map(|(domain, task)| (*domain, *task)),
            )
        }
        fn get_tasks(
            &self,
            key: &TaskListKey,
            read_level: i64,
            max_read_level: i64,
            batch_size: i64,
        ) -> StoreResult<Vec<AllocatedTask>> {
            if let Some(ids) = self.script.lock().expect("script lock").take() {
                return Ok(ids
                    .into_iter()
                    .map(|task_id| AllocatedTask {
                        task_id,
                        data: TaskData::default(),
                    })
                    .collect());
            }
            self.inner
                .get_tasks(key, read_level, max_read_level, batch_size)
        }
        fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> StoreResult<i64> {
            self.inner.task_list_size(key, ack_level)
        }
        fn complete_task(&self, key: &TaskListKey, task_id: i64) -> StoreResult<()> {
            self.inner.complete_task(key, task_id)
        }
        fn complete_tasks_less_than(
            &self,
            key: &TaskListKey,
            inclusive_task_id: i64,
            limit: i64,
        ) -> StoreResult<usize> {
            self.inner
                .complete_tasks_less_than(key, inclusive_task_id, limit)
        }
        fn delete_task_list(&self, key: &TaskListKey, range: i64) -> StoreResult<()> {
            self.inner.delete_task_list(key, range)
        }
    }
}
