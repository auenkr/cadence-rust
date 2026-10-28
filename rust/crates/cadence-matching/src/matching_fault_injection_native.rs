//! Byte-preserving counterpart of Go's persistence task-manager error wrapper.
//! This is a private child of `matching_fault_injection` so its random draw,
//! grace period and fake-error mapping stay exactly shared with that wrapper.

use super::{draw, FakeError, STARTUP_GRACE};
use cadence_persistence::matching_blob::TaskListDataBytes;
use cadence_persistence::matching_native_task_store::{
    AllocatedTaskBytes, MatchingTaskStoreBytes, MatchingTaskStoreBytesError,
    Result as TaskBytesResult, TaskListKeyBytes, TaskListRecordBytes,
};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

type ErrorSink = Arc<dyn Fn(&str) + Send + Sync>;

/// Wrap a native task store with Go-compatible persistence error injection.
pub fn inject_task_store_bytes_errors(
    store: Arc<dyn MatchingTaskStoreBytes>,
    rate: f64,
) -> Arc<dyn MatchingTaskStoreBytes> {
    if rate == 0.0 {
        return store;
    }
    Arc::new(InjectingTaskStoreBytes {
        store,
        rate,
        started: Instant::now(),
        sink: Arc::new(|line| eprintln!("ERROR {line}")),
    })
}

struct InjectingTaskStoreBytes {
    store: Arc<dyn MatchingTaskStoreBytes>,
    rate: f64,
    started: Instant,
    sink: ErrorSink,
}

impl InjectingTaskStoreBytes {
    #[cfg(test)]
    fn with_start(
        store: Arc<dyn MatchingTaskStoreBytes>,
        rate: f64,
        started: Instant,
        sink: ErrorSink,
    ) -> Self {
        Self {
            store,
            rate,
            started,
            sink,
        }
    }

    fn run<T>(
        &self,
        operation: &'static str,
        call: impl FnOnce() -> TaskBytesResult<T>,
    ) -> TaskBytesResult<T> {
        let (fake, forward) = if self.started.elapsed() < STARTUP_GRACE {
            (None, true)
        } else {
            draw(self.rate)
        };
        self.run_choice(operation, fake, forward, call)
    }

    fn run_choice<T>(
        &self,
        operation: &'static str,
        fake: Option<FakeError>,
        forward: bool,
        call: impl FnOnce() -> TaskBytesResult<T>,
    ) -> TaskBytesResult<T> {
        let result = if forward { Some(call()) } else { None };
        let Some(fake) = fake else {
            return result.expect("forwarded call when no fake error");
        };
        let store_error = match &result {
            Some(Err(error)) => error.to_string(),
            _ => "<nil>".into(),
        };
        (self.sink)(&format!(
            "Injected fake persistence error store-operation={operation} error={:?} bool={forward} store-error={store_error}",
            fake.persistence_message()
        ));
        Err(MatchingTaskStoreBytesError::Legacy(
            fake.to_task_store_error(),
        ))
    }
}

impl MatchingTaskStoreBytes for InjectingTaskStoreBytes {
    fn fetch_latest_config(
        &self,
        row_type: i32,
    ) -> std::result::Result<Option<cadence_persistence::config_store::ConfigStoreRow>, String>
    {
        self.store.fetch_latest_config(row_type)
    }

    fn get_task_list(&self, key: &TaskListKeyBytes) -> TaskBytesResult<TaskListRecordBytes> {
        self.run("get-task-list", || self.store.get_task_list(key))
    }
    fn lease(
        &self,
        key: &TaskListKeyBytes,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> TaskBytesResult<TaskListRecordBytes> {
        self.run("lease-task-list", || {
            self.store.lease(key, kind, previous_range, now_nanos)
        })
    }
    fn update_task_list(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        data: TaskListDataBytes,
        now_nanos: i64,
    ) -> TaskBytesResult<()> {
        self.run("update-task-list", || {
            self.store.update_task_list(key, range, data, now_nanos)
        })
    }
    fn create_tasks_with_domains(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTaskBytes)],
    ) -> TaskBytesResult<()> {
        self.run("create-tasks", || {
            self.store
                .create_tasks_with_domains(key, range, now_nanos, tasks)
        })
    }
    fn get_tasks(
        &self,
        key: &TaskListKeyBytes,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> TaskBytesResult<Vec<AllocatedTaskBytes>> {
        self.run("get-tasks", || {
            self.store
                .get_tasks(key, read_level, max_read_level, batch_size)
        })
    }
    fn task_list_size(&self, key: &TaskListKeyBytes, ack_level: i64) -> TaskBytesResult<i64> {
        self.run("get-task-list-size", || {
            self.store.task_list_size(key, ack_level)
        })
    }
    fn complete_task(&self, key: &TaskListKeyBytes, task_id: i64) -> TaskBytesResult<()> {
        self.run("complete-task", || self.store.complete_task(key, task_id))
    }
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKeyBytes,
        inclusive_task_id: i64,
        limit: i64,
    ) -> TaskBytesResult<usize> {
        self.run("complete-tasks-less-than", || {
            self.store
                .complete_tasks_less_than(key, inclusive_task_id, limit)
        })
    }
    fn delete_task_list(&self, key: &TaskListKeyBytes, range: i64) -> TaskBytesResult<()> {
        self.run("delete-task-list", || {
            self.store.delete_task_list(key, range)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::go_string::GoString;
    use cadence_persistence::matching_blob::TaskListDataBytes;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default)]
    struct Recording(Mutex<Vec<Vec<u8>>>);

    impl MatchingTaskStoreBytes for Recording {
        fn get_task_list(&self, key: &TaskListKeyBytes) -> TaskBytesResult<TaskListRecordBytes> {
            self.0.lock().unwrap().push(key.name.as_bytes().to_vec());
            Ok(TaskListRecordBytes {
                range_id: 1,
                data: TaskListDataBytes::default(),
            })
        }
        fn lease(
            &self,
            _: &TaskListKeyBytes,
            _: i16,
            _: i64,
            _: i64,
        ) -> TaskBytesResult<TaskListRecordBytes> {
            unreachable!()
        }
        fn update_task_list(
            &self,
            _: &TaskListKeyBytes,
            _: i64,
            _: TaskListDataBytes,
            _: i64,
        ) -> TaskBytesResult<()> {
            unreachable!()
        }
        fn create_tasks_with_domains(
            &self,
            _: &TaskListKeyBytes,
            _: i64,
            _: i64,
            _: &[(Uuid, &AllocatedTaskBytes)],
        ) -> TaskBytesResult<()> {
            unreachable!()
        }
        fn get_tasks(
            &self,
            _: &TaskListKeyBytes,
            _: i64,
            _: i64,
            _: i64,
        ) -> TaskBytesResult<Vec<AllocatedTaskBytes>> {
            unreachable!()
        }
        fn task_list_size(&self, _: &TaskListKeyBytes, _: i64) -> TaskBytesResult<i64> {
            unreachable!()
        }
        fn complete_task(&self, _: &TaskListKeyBytes, _: i64) -> TaskBytesResult<()> {
            unreachable!()
        }
        fn complete_tasks_less_than(
            &self,
            _: &TaskListKeyBytes,
            _: i64,
            _: i64,
        ) -> TaskBytesResult<usize> {
            unreachable!()
        }
        fn delete_task_list(&self, _: &TaskListKeyBytes, _: i64) -> TaskBytesResult<()> {
            unreachable!()
        }
    }

    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::nil(),
            name: GoString::from(name),
            task_type: 0,
        }
    }

    #[test]
    fn startup_grace_forwards_arbitrary_key_bytes_unchanged() {
        let backend = Arc::new(Recording::default());
        let wrapped = InjectingTaskStoreBytes::with_start(
            backend.clone(),
            1.0,
            Instant::now(),
            Arc::new(|_| {}),
        );
        for bytes in [&b"\xff"[..], &b"\xfe"[..]] {
            wrapped.get_task_list(&key(bytes)).unwrap();
        }
        assert_eq!(
            *backend.0.lock().unwrap(),
            vec![b"\xff".to_vec(), b"\xfe".to_vec()]
        );
    }

    #[test]
    fn injected_errors_stop_or_forward_and_are_legacy_errors() {
        let backend = Arc::new(Recording::default());
        let logs = Arc::new(Mutex::new(Vec::new()));
        let log_sink: ErrorSink = {
            let logs = logs.clone();
            Arc::new(move |s| logs.lock().unwrap().push(s.to_owned()))
        };
        let wrapped = InjectingTaskStoreBytes::with_start(
            backend.clone(),
            1.0,
            Instant::now() - Duration::from_secs(31),
            log_sink,
        );
        let k = key(b"\xff");
        let err = wrapped
            .run_choice("get-task-list", Some(FakeError::ServiceBusy), false, || {
                wrapped.store.get_task_list(&k)
            })
            .unwrap_err();
        assert!(matches!(err, MatchingTaskStoreBytesError::Legacy(_)));
        assert!(backend.0.lock().unwrap().is_empty());
        let err = wrapped
            .run_choice("get-task-list", Some(FakeError::Timeout), true, || {
                wrapped.store.get_task_list(&k)
            })
            .unwrap_err();
        assert!(matches!(err, MatchingTaskStoreBytesError::Legacy(_)));
        assert_eq!(*backend.0.lock().unwrap(), vec![b"\xff".to_vec()]);
        let logs = logs.lock().unwrap();
        assert!(logs[0].contains("store-operation=get-task-list"));
        assert!(logs[1].contains("store-operation=get-task-list"));
    }
}
