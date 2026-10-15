//! Durable single-owner Matching queue. Workflow state lives in History, not here.
use crate::{
    matching_blob::{TaskData, TaskListData},
    matching_sqlite::{
        AllocatedTask, Result as StorageResult, SqliteTaskStore, StorageError, TaskListKey,
    },
};
use cadence_common::ack_manager::AckManager;
use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::{Arc, Mutex},
};

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("matching task-list owner stopped")]
    Stopped,
    #[error("task is not in flight: {0}")]
    NotInFlight(i64),
    #[error("invalid matching reader state: {0}")]
    ReaderState(String),
}
pub type Result<T> = std::result::Result<T, QueueError>;
struct State {
    range_id: i64,
    next_id: i64,
    block_end: i64,
    max_read_level: i64,
    metadata: TaskListData,
    in_flight: HashMap<i64, AllocatedTask>,
    stopped: bool,
}
pub struct DurableTaskQueue {
    store: Arc<SqliteTaskStore>,
    key: TaskListKey,
    range_size: NonZeroU32,
    state: Mutex<State>,
    ack: AckManager,
}
#[derive(Debug)]
pub struct ReadBatch {
    pub tasks: Vec<AllocatedTask>,
    /// Reschedule the reader: a batch was loaded or the scan budget ran out.
    pub scan_pending: bool,
}
struct ScannedBatch {
    tasks: Vec<AllocatedTask>,
    read_level: i64,
    finished: bool,
}
fn scan_batch(
    store: &SqliteTaskStore,
    key: &TaskListKey,
    mut read: i64,
    max: i64,
    range: NonZeroU32,
    batch: NonZeroU32,
) -> StorageResult<ScannedBatch> {
    for _ in 0..10 {
        if read >= max {
            break;
        }
        let upper = read.wrapping_add(i64::from(range.get())).min(max);
        let tasks = store.get_tasks(key, read, upper, i64::from(batch.get()))?;
        if !tasks.is_empty() {
            return Ok(ScannedBatch {
                tasks,
                read_level: upper,
                finished: true,
            });
        }
        read = upper;
    }
    Ok(ScannedBatch {
        tasks: Vec::new(),
        read_level: read,
        finished: read == max,
    })
}
fn expired(expiry: i64, now: i64) -> bool {
    expiry > 0 && now > expiry
}

fn block(range: i64, size: NonZeroU32) -> (i64, i64) {
    let size = i64::from(size.get());
    (
        range.wrapping_sub(1).wrapping_mul(size).wrapping_add(1),
        range.wrapping_mul(size),
    )
}
impl DurableTaskQueue {
    pub fn open(
        store: Arc<SqliteTaskStore>,
        key: TaskListKey,
        kind: i16,
        range_size: NonZeroU32,
        now: i64,
    ) -> Result<Self> {
        let lease = store.lease(&key, kind, 0, now)?;
        let (next_id, block_end) = block(lease.range_id, range_size);
        let ack = AckManager::default();
        ack.set_ack_level(lease.data.ack_level);
        Ok(Self {
            store,
            key,
            range_size,
            ack,
            state: Mutex::new(State {
                range_id: lease.range_id,
                next_id,
                block_end,
                max_read_level: next_id.wrapping_sub(1),
                metadata: lease.data,
                in_flight: HashMap::new(),
                stopped: false,
            }),
        })
    }
    fn active(state: &State) -> Result<()> {
        if state.stopped {
            Err(QueueError::Stopped)
        } else {
            Ok(())
        }
    }
    fn check<T>(state: &mut State, result: StorageResult<T>) -> Result<T> {
        if matches!(result, Err(StorageError::ConditionFailed(_))) {
            state.stopped = true;
        }
        result.map_err(Into::into)
    }
    fn append_locked(
        &self,
        state: &mut State,
        data: &[TaskData],
        now: i64,
    ) -> Result<Vec<AllocatedTask>> {
        Self::active(state)?;
        let mut tasks = Vec::with_capacity(data.len());
        for data in data {
            if state.next_id > state.block_end {
                let result = self
                    .store
                    .lease(&self.key, state.metadata.kind, state.range_id, now);
                let lease = Self::check(state, result)?;
                state.range_id = lease.range_id;
                (state.next_id, state.block_end) = block(lease.range_id, self.range_size);
            }
            tasks.push(AllocatedTask {
                task_id: state.next_id,
                data: data.clone(),
            });
            state.next_id = state.next_id.wrapping_add(1);
        }
        let result = self.store.create_tasks(&self.key, state.range_id, &tasks);
        // Go advances the read boundary after the write returns, even on error.
        // Allocated IDs are never reused after an uncertain persistence outcome.
        if let Some(last) = tasks.last() {
            if last.task_id > 0 {
                state.max_read_level = last.task_id;
            }
        }
        Self::check(state, result)?;
        Ok(tasks)
    }
    pub fn append(&self, data: &[TaskData], now: i64) -> Result<Vec<AllocatedTask>> {
        self.append_locked(
            &mut self.state.lock().expect("matching queue lock"),
            data,
            now,
        )
    }
    /// Go scans at most ten bounded ID ranges per pump turn. Expired tasks
    /// advance the read level without becoming in-flight acknowledgements.
    pub fn read_batch(
        &self,
        read_range_size: NonZeroU32,
        batch_size: NonZeroU32,
        now: i64,
    ) -> Result<ReadBatch> {
        let mut state = self.state.lock().expect("matching queue lock");
        Self::active(&state)?;
        let (initial_read, initial_ack, _) = self.ack.levels();
        let scanned = scan_batch(
            &self.store,
            &self.key,
            initial_read,
            state.max_read_level,
            read_range_size,
            batch_size,
        )?;
        if scanned.tasks.is_empty() {
            self.ack.set_read_level(scanned.read_level);
            if initial_ack == initial_read {
                self.ack.set_ack_level(scanned.read_level);
            }
            return Ok(ReadBatch {
                tasks: Vec::new(),
                scan_pending: !scanned.finished,
            });
        }
        let mut ready = Vec::new();
        for task in scanned.tasks {
            if expired(task.data.expiry_nanos, now) {
                self.ack.set_read_level(task.task_id);
            } else {
                self.ack
                    .read_item(task.task_id)
                    .map_err(QueueError::ReaderState)?;
                state.in_flight.insert(task.task_id, task.clone());
                ready.push(task);
            }
        }
        Ok(ReadBatch {
            tasks: ready,
            scan_pending: true,
        })
    }

    /// Acknowledge only after History accepted start or Go's terminal-error
    /// classification explicitly permits dropping the queue entry.
    pub fn complete(&self, task_id: i64) -> Result<i64> {
        let mut state = self.state.lock().expect("matching queue lock");
        if state.in_flight.remove(&task_id).is_none() {
            return Err(QueueError::NotInFlight(task_id));
        }
        Ok(self.ack.ack_item(task_id))
    }
    /// Rewrite before acknowledging the original. Read-back Go TaskInfo does
    /// not retain ScheduleToStartTimeoutSeconds, so retry persistence has zero
    /// timeout and a fresh creation time, exactly like Go's completeTask path.
    pub fn rewrite_failed(&self, task_id: i64, now: i64) -> Result<AllocatedTask> {
        let mut state = self.state.lock().expect("matching queue lock");
        let original = state
            .in_flight
            .get(&task_id)
            .ok_or(QueueError::NotInFlight(task_id))?
            .clone();
        let data = original.data.at_creation(now, 0);
        let mut written = self.append_locked(&mut state, &[data], now)?;
        state.in_flight.remove(&task_id);
        self.ack.ack_item(task_id);
        Ok(written.remove(0))
    }
    pub fn persist_ack(&self, now: i64) -> Result<()> {
        let mut state = self.state.lock().expect("matching queue lock");
        Self::active(&state)?;
        let (_, ack, _) = self.ack.levels();
        if ack >= 0 {
            let mut metadata = state.metadata.clone();
            metadata.ack_level = ack;
            let result =
                self.store
                    .update_task_list(&self.key, state.range_id, metadata.clone(), now);
            Self::check(&mut state, result)?;
            state.metadata = metadata;
        }
        Ok(())
    }
    /// Queue controller schedules this separately from ack persistence, as Go's
    /// task GC does. Deletion itself intentionally does not fence on a lease.
    pub fn collect_garbage(&self, limit: i64) -> Result<usize> {
        let (_, ack, _) = self.ack.levels();
        Ok(self.store.complete_tasks_less_than(&self.key, ack, limit)?)
    }
    pub fn levels(&self) -> (i64, i64, i64) {
        self.ack.levels()
    }
    pub fn stop(&self) {
        self.state.lock().expect("matching queue lock").stopped = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_blob::BlobEncoding;
    use uuid::Uuid;
    fn n(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).unwrap()
    }
    fn key() -> TaskListKey {
        TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "queue".into(),
            task_type: 0,
        }
    }
    fn data() -> TaskData {
        TaskData {
            workflow_id: "workflow".into(),
            run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
            schedule_id: 2,
            ..Default::default()
        }
        .at_creation(100, 0)
    }
    #[test]
    fn matching_queue_original_go_reader_fixtures() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-queue-reader.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("tasks.db");
            let store = SqliteTaskStore::create(path, BlobEncoding::ThriftRW).unwrap();
            store.lease(&key(), 0, 0, 100).unwrap();
            let tasks: Vec<_> = case["ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|id| AllocatedTask {
                    task_id: id.as_i64().unwrap(),
                    data: data(),
                })
                .collect();
            store.create_tasks(&key(), 1, &tasks).unwrap();
            let actual = scan_batch(
                &store,
                &key(),
                case["read"].as_i64().unwrap(),
                case["max"].as_i64().unwrap(),
                n(case["range"].as_u64().unwrap() as u32),
                n(case["batch"].as_u64().unwrap() as u32),
            )
            .unwrap();
            assert_eq!(
                serde_json::json!(actual
                    .tasks
                    .iter()
                    .map(|task| task.task_id)
                    .collect::<Vec<_>>()),
                case["expected_ids"]
            );
            assert_eq!(serde_json::json!(actual.read_level), case["expected_read"]);
            assert_eq!(serde_json::json!(actual.finished), case["finished"]);
        }
    }
    #[test]
    fn matching_queue_original_go_expiry_fixtures() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-queue-expiry.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            assert_eq!(
                expired(
                    case["expiry"].as_i64().unwrap(),
                    case["now"].as_i64().unwrap()
                ),
                case["expired"].as_bool().unwrap()
            );
        }
    }

    #[test]
    fn matching_queue_ranges_out_of_order_ack_and_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(path, BlobEncoding::ThriftRWSnappy).unwrap());
        let queue = DurableTaskQueue::open(store.clone(), key(), 0, n(3), 100).unwrap();
        let tasks = queue.append(&vec![data(); 5], 100).unwrap();
        assert_eq!(
            tasks.iter().map(|task| task.task_id).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(store.get_task_list(&key()).unwrap().range_id, 2);
        drop(queue);
        let queue = DurableTaskQueue::open(store.clone(), key(), 0, n(3), 200).unwrap();
        let loaded = queue.read_batch(n(10), n(10), 200).unwrap();
        assert_eq!(loaded.tasks, tasks);
        assert_eq!(queue.complete(5).unwrap(), 0);
        for id in 1..=4 {
            queue.complete(id).unwrap();
        }
        assert_eq!(queue.levels(), (5, 5, 0));
        queue.persist_ack(300).unwrap();
        assert_eq!(queue.collect_garbage(100).unwrap(), 5);
        let next = queue.append(&[data()], 400).unwrap();
        assert_eq!(next[0].task_id, 7);
        assert_eq!(queue.read_batch(n(10), n(10), 400).unwrap().tasks, next);
        assert_eq!(queue.complete(7).unwrap(), 7);
        assert!(matches!(queue.complete(7), Err(QueueError::NotInFlight(7))));
    }
    #[test]
    fn matching_queue_failure_rewrite_is_durable_before_ack() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(path, BlobEncoding::ThriftRW).unwrap());
        let queue = DurableTaskQueue::open(store.clone(), key(), 0, n(100), 100).unwrap();
        queue.append(&[data()], 100).unwrap();
        queue.read_batch(n(100), n(10), 100).unwrap();
        let rewritten = queue.rewrite_failed(1, 200).unwrap();
        assert_eq!(rewritten.task_id, 2);
        assert_eq!(rewritten.data.created_nanos, 200);
        assert_eq!(
            rewritten.data.expiry_nanos,
            crate::matching_blob::GO_ZERO_TIME_NANOS
        );
        assert_eq!(queue.collect_garbage(100).unwrap(), 1);
        drop(queue);
        let recovered = DurableTaskQueue::open(store.clone(), key(), 0, n(100), 300).unwrap();
        let batch = recovered.read_batch(n(100), n(10), 300).unwrap();
        assert_eq!(batch.tasks, vec![rewritten]);
        // A second owner prevents rewriting. Original remains recoverable.
        let owner = DurableTaskQueue::open(store.clone(), key(), 0, n(100), 400).unwrap();
        assert!(matches!(
            recovered.rewrite_failed(2, 500),
            Err(QueueError::Storage(StorageError::ConditionFailed(_)))
        ));
        assert_eq!(recovered.levels().2, 1);
        assert_eq!(store.task_list_size(&key(), 0).unwrap(), 1);
        assert_eq!(
            owner.read_batch(n(1000), n(10), 600).unwrap().tasks[0].task_id,
            2
        );
        assert!(matches!(
            recovered.append(&[data()], 600),
            Err(QueueError::Stopped)
        ));
    }
    #[test]
    fn matching_queue_expiry_boundaries_and_scan_budget() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(path, BlobEncoding::ThriftRW).unwrap());
        let queue = DurableTaskQueue::open(store.clone(), key(), 0, n(100), 100).unwrap();
        let tasks = [-1, 0, 99, 100, 101].map(|expiry| TaskData {
            expiry_nanos: expiry,
            ..data()
        });
        queue.append(&tasks, 100).unwrap();
        let batch = queue.read_batch(n(100), n(10), 100).unwrap();
        assert_eq!(
            batch
                .tasks
                .iter()
                .map(|task| task.task_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 4, 5]
        );
        assert!(batch.scan_pending);
        for task in batch.tasks {
            queue.complete(task.task_id).unwrap();
        }
        assert_eq!(queue.levels(), (5, 5, 0));
        queue.persist_ack(100).unwrap();
        drop(queue);
        let queue = DurableTaskQueue::open(store, key(), 0, n(100), 200).unwrap();
        let batch = queue.read_batch(n(2), n(10), 200).unwrap();
        assert!(batch.tasks.is_empty());
        assert!(batch.scan_pending);
        assert_eq!(queue.levels(), (25, 25, 0));
    }
    #[test]
    fn matching_queue_concurrent_reads_claim_each_task_once() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(path, BlobEncoding::ThriftRW).unwrap());
        let queue = Arc::new(DurableTaskQueue::open(store, key(), 0, n(100), 100).unwrap());
        queue.append(&vec![data(); 16], 100).unwrap();
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let queue = queue.clone();
                std::thread::spawn(move || {
                    let task = queue.read_batch(n(100), n(1), 100).unwrap().tasks.remove(0);
                    queue.complete(task.task_id).unwrap();
                    task.task_id
                })
            })
            .collect();
        let mut ids: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        ids.sort();
        assert_eq!(ids, (1..=16).collect::<Vec<_>>());
        assert_eq!(queue.levels(), (16, 16, 0));
    }
}
