//! Native Matching persistence rate limiting and metering; raw store values pass through unchanged.
use super::{MatchingPersistenceLimiter, REJECTED};
use cadence_persistence::{
    matching_blob::TaskListDataBytes,
    matching_native_task_store::{AllocatedTaskBytes, MatchingTaskStoreBytes,
        MatchingTaskStoreBytesError, Result as NativeResult, TaskListKeyBytes, TaskListRecordBytes},
};
use std::sync::Arc;
use uuid::Uuid;

pub fn limit_task_store_bytes(
    store: Arc<dyn MatchingTaskStoreBytes>,
    limiter: Arc<MatchingPersistenceLimiter>,
) -> Arc<dyn MatchingTaskStoreBytes> {
    Arc::new(RateLimitedTaskStoreBytes { store, limiter })
}

fn get_tasks_byte_size_bytes(tasks: &[AllocatedTaskBytes]) -> u64 {
    24 + tasks.iter().map(|task| {
        let partition = task.data.partition_config.as_ref().map_or(0, |map| {
            map.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
        });
        128 + 36 + task.data.workflow_id.len() as u64
            + task.data.run_id_bytes().len() as u64 + partition
    }).sum::<u64>()
}

struct RateLimitedTaskStoreBytes {
    store: Arc<dyn MatchingTaskStoreBytes>,
    limiter: Arc<MatchingPersistenceLimiter>,
}

impl RateLimitedTaskStoreBytes {
    fn permit(&self, operation: &'static str, domain_id: Option<Uuid>) -> NativeResult<()> {
        if self.limiter.allow() {
            Ok(())
        } else {
            self.limiter.record_task_rejection(operation, domain_id);
            Err(MatchingTaskStoreBytesError::Backend {
                kind: cadence_persistence::matching_task_store::BackendErrorKind::Throttled,
                message: REJECTED.into(),
            })
        }
    }
}

impl MatchingTaskStoreBytes for RateLimitedTaskStoreBytes {
    fn fetch_latest_config(&self, row_type: i32) -> Result<Option<cadence_persistence::config_store::ConfigStoreRow>, String> {
        self.store.fetch_latest_config(row_type)
    }

    fn get_task_list(&self, key: &TaskListKeyBytes) -> NativeResult<TaskListRecordBytes> {
        self.permit("GetTaskList", None)?;
        let started = std::time::Instant::now();
        let result = self.store.get_task_list(key);
        self.limiter
            .record_native_task_call("GetTaskList", None, started.elapsed(), &result);
        result
    }
    fn lease(
        &self,
        key: &TaskListKeyBytes,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> NativeResult<TaskListRecordBytes> {
        self.permit("LeaseTaskList", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.lease(key, kind, previous_range, now_nanos);
        self.limiter.record_native_task_call(
            "LeaseTaskList",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn update_task_list(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        data: TaskListDataBytes,
        now_nanos: i64,
    ) -> NativeResult<()> {
        self.permit("UpdateTaskList", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.update_task_list(key, range, data, now_nanos);
        self.limiter.record_native_task_call(
            "UpdateTaskList",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn create_tasks_with_domains(
        &self,
        key: &TaskListKeyBytes,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTaskBytes)],
    ) -> NativeResult<()> {
        self.permit("CreateTask", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self
            .store
            .create_tasks_with_domains(key, range, now_nanos, tasks);
        self.limiter.record_native_task_call(
            "CreateTask",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn get_tasks(
        &self,
        key: &TaskListKeyBytes,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> NativeResult<Vec<AllocatedTaskBytes>> {
        self.permit("GetTasks", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self
            .store
            .get_tasks(key, read_level, max_read_level, batch_size);
        self.limiter
            .record_native_task_call("GetTasks", Some(key.domain_id), started.elapsed(), &result);
        if let Ok(tasks) = &result {
            self.limiter.record_list_response(
                "GetTasks",
                tasks.len() as u64,
                get_tasks_byte_size_bytes(tasks),
            );
        }
        result
    }
    fn task_list_size(&self, key: &TaskListKeyBytes, ack_level: i64) -> NativeResult<i64> {
        self.permit("GetTaskListSize", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.task_list_size(key, ack_level);
        self.limiter.record_native_task_call(
            "GetTaskListSize",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn complete_task(&self, key: &TaskListKeyBytes, task_id: i64) -> NativeResult<()> {
        self.permit("CompleteTask", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.complete_task(key, task_id);
        self.limiter.record_native_task_call(
            "CompleteTask",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKeyBytes,
        inclusive_task_id: i64,
        limit: i64,
    ) -> NativeResult<usize> {
        self.permit("CompleteTasksLessThan", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self
            .store
            .complete_tasks_less_than(key, inclusive_task_id, limit);
        self.limiter.record_native_task_call(
            "CompleteTasksLessThan",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
    fn delete_task_list(&self, key: &TaskListKeyBytes, range: i64) -> NativeResult<()> {
        self.permit("DeleteTaskList", Some(key.domain_id))?;
        let started = std::time::Instant::now();
        let result = self.store.delete_task_list(key, range);
        self.limiter.record_native_task_call(
            "DeleteTaskList",
            Some(key.domain_id),
            started.elapsed(),
            &result,
        );
        result
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::go_string::GoString;
    use cadence_persistence::{matching_blob::{BlobEncoding, TaskDataBytes}, matching_sqlite::SqliteTaskStore};
    use crate::{admin::DynamicConfigOverlay, matching_metrics::RecordingMetricSink};
    use std::sync::{atomic::AtomicUsize, Mutex};

    #[test]
    fn native_metering_preserves_raw_rows_and_lease_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteTaskStore::create(dir.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap();
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        let store = limit_task_store_bytes(Arc::new(store), limiter);
        let key = TaskListKeyBytes { domain_id: Uuid::from_u128(1), name: GoString::from(&b"q\xff"[..]), task_type: 0 };
        let lease = store.lease(&key, 0, 0, 1).unwrap();
        let task = AllocatedTaskBytes { task_id: 1, data: TaskDataBytes {
            workflow_id: GoString::from(&b"wf\xfe"[..]),
            run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
            ..Default::default()
        }};
        store.create_tasks_with_domains(&key, lease.range_id, 2, &[(key.domain_id, &task)]).unwrap();
        let rows = store.get_tasks(&key, 0, 10, 10).unwrap();
        assert_eq!(rows, vec![task]);
        let error = store.lease(&key, 0, lease.range_id + 1, 3).unwrap_err();
        match error {
            MatchingTaskStoreBytesError::ConditionFailed { message } => {
                assert!(message.as_bytes().windows(2).any(|part| part == b"q\xff"));
            }
            other => panic!("expected native condition error, got {other:?}"),
        }
        assert!(sink.events().iter().any(|event| event.name == "persistence_errors_condition_failed_per_domain"));
        assert_eq!(get_tasks_byte_size_bytes(&rows), 24 + 128 + 36 + 3 + 36);
    }
}
