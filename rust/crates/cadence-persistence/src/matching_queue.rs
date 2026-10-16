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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
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
#[derive(Clone)]
pub struct PendingTask {
    pub data: TaskData,
    pub source_domain: uuid::Uuid,
    pub schedule_to_start_seconds: i32,
}
struct State {
    range_id: i64,
    next_id: i64,
    block_end: i64,
    max_read_level: i64,
    metadata: TaskListData,
    in_flight: HashMap<i64, AllocatedTask>,
}
pub struct DurableTaskQueue {
    store: Arc<SqliteTaskStore>,
    key: TaskListKey,
    range_size: NonZeroU32,
    state: Mutex<State>,
    reader: Mutex<()>,
    ack: AckManager,
    gc: crate::matching_gc::TaskGarbageCollector,
    stopped: AtomicBool,
    stop_waker: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
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
        // Go taskReader retries each individual bounded query, rather than
        // restarting the ten-range scan after a transient driver failure.
        let tasks = crate::matching_retry::persistence(|| {
            store.get_tasks(key, read, upper, i64::from(batch.get()))
        })?;
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
        let lease = crate::matching_retry::lease(now, |attempt_now| {
            store.lease(&key, kind, 0, attempt_now)
        })?;
        let (next_id, block_end) = block(lease.range_id, range_size);
        let ack = AckManager::default();
        ack.set_ack_level(lease.data.ack_level);
        Ok(Self {
            store,
            key,
            range_size,
            ack,
            gc: crate::matching_gc::TaskGarbageCollector::default(),
            stopped: AtomicBool::new(false),
            stop_waker: Mutex::new(None),
            reader: Mutex::new(()),
            state: Mutex::new(State {
                range_id: lease.range_id,
                next_id,
                block_end,
                max_read_level: next_id.wrapping_sub(1),
                metadata: lease.data,
                in_flight: HashMap::new(),
            }),
        })
    }
    fn active(&self) -> Result<()> {
        if self.is_stopped() {
            Err(QueueError::Stopped)
        } else {
            Ok(())
        }
    }
    fn check<T>(&self, result: StorageResult<T>) -> Result<T> {
        if matches!(result, Err(StorageError::ConditionFailed(_))) {
            self.stop();
        }
        result.map_err(Into::into)
    }
    fn append_locked(
        &self,
        state: &mut State,
        data: &[(uuid::Uuid, TaskData)],
        now: i64,
        timeouts: Option<&[i32]>,
    ) -> Result<Vec<AllocatedTask>> {
        self.active()?;
        let mut tasks = Vec::with_capacity(data.len());
        for (_, data) in data {
            if state.next_id > state.block_end {
                let result = crate::matching_retry::lease(now, |attempt_now| {
                    self.store
                        .lease(&self.key, state.metadata.kind, state.range_id, attempt_now)
                });
                // Go stops the writer on every terminal renewal failure,
                // including exhausted transient errors, not only lost leases.
                if result.is_err() {
                    self.stop();
                }
                let lease = result?;
                state.range_id = lease.range_id;
                (state.next_id, state.block_end) = block(lease.range_id, self.range_size);
            }
            tasks.push(AllocatedTask {
                task_id: state.next_id,
                data: data.clone(),
            });
            state.next_id = state.next_id.wrapping_add(1);
        }
        if let Some(timeouts) = timeouts {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|time| time.as_nanos() as i64)
                .unwrap_or_default();
            for (task, timeout) in tasks.iter_mut().zip(timeouts) {
                task.data = task.data.clone().at_creation(timestamp, *timeout);
            }
        }
        let result = self.store.create_tasks_with_domains(
            &self.key,
            state.range_id,
            tasks
                .iter()
                .zip(data)
                .map(|(task, (domain, _))| (*domain, task)),
        );
        // Go advances the read boundary after the write returns, even on error.
        // Allocated IDs are never reused after an uncertain persistence outcome.
        if let Some(last) = tasks.last() {
            if last.task_id > 0 {
                state.max_read_level = last.task_id;
            }
        }
        self.check(result)?;
        Ok(tasks)
    }
    pub fn append(&self, data: &[TaskData], now: i64) -> Result<Vec<AllocatedTask>> {
        self.append_locked(
            &mut self.state.lock().expect("matching queue lock"),
            &data
                .iter()
                .cloned()
                .map(|task| (self.key.domain_id, task))
                .collect::<Vec<_>>(),
            now,
            None,
        )
    }
    /// Preserve Go SQL's source-domain row keys for cross-domain activities.
    /// Reading this queue still filters on the target task-list domain.
    pub fn append_in_domain(
        &self,
        data: &[TaskData],
        source_domain: uuid::Uuid,
        now: i64,
    ) -> Result<Vec<AllocatedTask>> {
        self.append_locked(
            &mut self.state.lock().expect("matching queue lock"),
            &data
                .iter()
                .cloned()
                .map(|task| (source_domain, task))
                .collect::<Vec<_>>(),
            now,
            None,
        )
    }
    /// One Go CreateTasks transaction may contain source domains from multiple
    /// append requests while fencing the target task-list lease once.
    pub fn append_mixed(
        &self,
        data: &[(uuid::Uuid, TaskData)],
        now: i64,
    ) -> Result<Vec<AllocatedTask>> {
        self.append_locked(
            &mut self.state.lock().expect("matching queue lock"),
            data,
            now,
            None,
        )
    }
    pub fn append_pending(&self, pending: &[PendingTask]) -> Result<Vec<AllocatedTask>> {
        let data = pending
            .iter()
            .map(|task| (task.source_domain, task.data.clone()))
            .collect::<Vec<_>>();
        let timeouts = pending
            .iter()
            .map(|task| task.schedule_to_start_seconds)
            .collect::<Vec<_>>();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|time| time.as_nanos() as i64)
            .unwrap_or_default();
        self.append_locked(
            &mut self.state.lock().expect("matching queue lock"),
            &data,
            now,
            Some(&timeouts),
        )
    }
    pub fn task_list_kind(&self) -> i16 {
        self.state
            .lock()
            .expect("matching queue lock")
            .metadata
            .kind
    }
    pub fn retry_data(&self, task_id: i64) -> Result<TaskData> {
        self.state
            .lock()
            .expect("matching queue lock")
            .in_flight
            .get(&task_id)
            .map(|task| task.data.clone())
            .ok_or(QueueError::NotInFlight(task_id))
    }
    /// Go scans at most ten bounded ID ranges per pump turn. Expired tasks
    /// advance the read level without becoming in-flight acknowledgements.
    pub fn read_batch(
        &self,
        read_range_size: NonZeroU32,
        batch_size: NonZeroU32,
        now: i64,
    ) -> Result<ReadBatch> {
        self.read_batch_with_clock(read_range_size, batch_size, || now)
    }

    /// Read expiry time at admission, after persistence retries. The fixed-time
    /// wrapper remains useful for replaying deterministic Go scan fixtures.
    pub fn read_batch_with_clock(
        &self,
        read_range_size: NonZeroU32,
        batch_size: NonZeroU32,
        mut clock: impl FnMut() -> i64,
    ) -> Result<ReadBatch> {
        // Go has one reader pump, independent of the writer's lease mutex.
        // Serialize claims while allowing appends/completions during retries.
        let _reader = self.reader.lock().expect("matching reader lock");
        self.active()?;
        let max_read_level = self
            .state
            .lock()
            .expect("matching queue lock")
            .max_read_level;
        let (initial_read, _, _) = self.ack.levels();
        let scanned = scan_batch(
            &self.store,
            &self.key,
            initial_read,
            max_read_level,
            read_range_size,
            batch_size,
        )?;
        let mut state = self.state.lock().expect("matching queue lock");
        self.active()?;
        if scanned.tasks.is_empty() {
            self.ack.set_read_level(scanned.read_level);
            if self.ack.levels().1 == initial_read {
                self.ack.set_ack_level(scanned.read_level);
            }
            return Ok(ReadBatch {
                tasks: Vec::new(),
                scan_pending: !scanned.finished,
            });
        }
        let mut ready = Vec::new();
        for task in scanned.tasks {
            if expired(task.data.expiry_nanos, clock()) {
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
        let mut written =
            self.append_locked(&mut state, &[(self.key.domain_id, data)], now, None)?;
        state.in_flight.remove(&task_id);
        self.ack.ack_item(task_id);
        Ok(written.remove(0))
    }
    pub fn persist_ack(&self, now: i64) -> Result<()> {
        self.persist_ack_impl(now, false)
    }
    /// Go reader.Stop persists its final cursor even after the writer stopped.
    /// The database lease fence remains authoritative for this final write.
    pub fn persist_final_ack(&self, now: i64) -> Result<()> {
        self.persist_ack_impl(now, true)
    }
    fn persist_ack_impl(&self, now: i64, final_update: bool) -> Result<()> {
        let mut state = self.state.lock().expect("matching queue lock");
        if !final_update {
            self.active()?;
        }
        let (_, ack, _) = self.ack.levels();
        if ack >= 0 {
            let mut metadata = state.metadata.clone();
            metadata.ack_level = ack;
            let result =
                self.store
                    .update_task_list(&self.key, state.range_id, metadata.clone(), now);
            self.check(result)?;
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
    /// Go defaults: 100 rows per delete batch, one second between small drains.
    pub fn run_garbage_collection(&self, force: bool) -> Result<usize> {
        self.run_garbage_collection_with(
            force,
            NonZeroU32::new(100).unwrap(),
            std::time::Duration::from_secs(1),
        )
    }
    pub fn run_garbage_collection_with(
        &self,
        force: bool,
        batch: NonZeroU32,
        max_interval: std::time::Duration,
    ) -> Result<usize> {
        let (_, ack, _) = self.ack.levels();
        Ok(self.gc.run(
            ack,
            batch,
            max_interval,
            force,
            std::time::Instant::now(),
            |ack, limit| self.store.complete_tasks_less_than(&self.key, ack, limit),
        )?)
    }
    pub fn levels(&self) -> (i64, i64, i64) {
        self.ack.levels()
    }
    /// Queue status fields exposed by Go DescribeTaskList. Rate/isolation/QPS
    /// belong to the task-list controller rather than the persistence reader.
    pub fn status(&self) -> (i64, i64, i64, i64, i64, bool) {
        let state = self.state.lock().expect("matching queue lock");
        let (read, ack, backlog) = self.ack.levels();
        let (start, end) = block(state.range_id, self.range_size);
        (read, ack, backlog, start, end, ack == state.max_read_level)
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
    /// The wake callback must be nonblocking and idempotent. It can be invoked
    /// after registration races with a stop; it must not reenter queue methods.
    pub fn set_stop_waker(&self, wake: Arc<dyn Fn() + Send + Sync>) {
        *self.stop_waker.lock().expect("Matching stop waker lock") = Some(wake.clone());
        if self.is_stopped() {
            wake();
        }
    }
    pub fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            let wake = self
                .stop_waker
                .lock()
                .expect("Matching stop waker lock")
                .clone();
            if let Some(wake) = wake {
                wake();
            }
        }
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
    fn matching_queue_reader_retries_transient_query_without_losing_task() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).unwrap());
        let queue = Arc::new(DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap());
        let first = queue.append(&[data()], 2).unwrap();
        queue.read_batch(n(100), n(1), 3).unwrap();
        let written = queue.append(&[data()], 4).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch("ALTER TABLE tasks RENAME TO temporarily_unavailable_tasks")
            .unwrap();
        let copy = queue.clone();
        let reader = std::thread::spawn(move || copy.read_batch(n(100), n(1), 5));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while queue.reader.try_lock().is_ok() {
            assert!(std::time::Instant::now() < deadline, "reader did not start");
            std::thread::yield_now();
        }
        // Completion needs the state mutex: retrying reads must leave it free.
        let copy = queue.clone();
        let (sender, receiver) = std::sync::mpsc::channel();
        let completion = std::thread::spawn(move || {
            sender.send(copy.complete(first[0].task_id)).unwrap();
        });
        let completed = receiver.recv_timeout(std::time::Duration::from_secs(2));
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(!reader.is_finished(), "query must wait for table recovery");
        connection
            .execute_batch("ALTER TABLE temporarily_unavailable_tasks RENAME TO tasks")
            .unwrap();
        let loaded = reader.join().unwrap().unwrap();
        completion.join().unwrap();
        completed
            .expect("reader retries must not block completion")
            .unwrap();
        assert_eq!(loaded.tasks, written);
        assert!(queue.read_batch(n(100), n(1), 4).unwrap().tasks.is_empty());
        queue.complete(written[0].task_id).unwrap();
        assert!(!queue.is_stopped());
    }
    #[test]
    fn matching_queue_stop_does_not_wait_for_inflight_state_lock() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = Arc::new(DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap());
        let held = queue.state.lock().unwrap();
        let copy = queue.clone();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            copy.stop();
            let _ = sender.send(());
        });
        receiver
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("stop must not wait for database work holding queue state");
        assert!(queue.is_stopped());
        drop(held);
        worker.join().unwrap();
        assert!(queue.append(&[data()], 2).is_err());
    }
    #[test]
    fn matching_queue_lease_retries_sql_failure_and_stops_on_terminal_renewal() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).unwrap());
        let control = rusqlite::Connection::open(&path).unwrap();
        control.execute_batch("CREATE TRIGGER reject_lease BEFORE UPDATE ON task_lists BEGIN SELECT RAISE(FAIL, 'temporary lease failure'); END").unwrap();
        let release_path = path.clone();
        let release = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            rusqlite::Connection::open(release_path)
                .unwrap()
                .execute_batch("DROP TRIGGER reject_lease")
                .unwrap();
        });
        let queue = DurableTaskQueue::open(store.clone(), key(), 0, n(1), 1).unwrap();
        release.join().unwrap();
        let leased = store.get_task_list(&key()).unwrap();
        assert_eq!(leased.range_id, 1);
        assert!(leased.data.last_updated_nanos > 1);
        assert_eq!(queue.append(&[data()], 2).unwrap()[0].task_id, 1);
        // A corrupt persisted blob is a non-transient serializer error. Failed
        // renewal must stop admission even though this is not a condition error.
        control
            .execute_batch("UPDATE task_lists SET data=x'ff'")
            .unwrap();
        assert!(matches!(
            queue.append(&[data()], 3),
            Err(QueueError::Storage(StorageError::Blob(_)))
        ));
        assert!(queue.is_stopped());
        assert!(matches!(
            queue.append(&[data()], 4),
            Err(QueueError::Stopped)
        ));
    }

    #[test]
    fn matching_queue_cross_domain_rows_keep_source_key_and_target_fence() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let target = key();
        let queue = DurableTaskQueue::open(store.clone(), target.clone(), 0, n(100), 1).unwrap();
        let mut source = target.clone();
        source.domain_id = Uuid::from_u128(3);
        queue
            .append_in_domain(&[data()], source.domain_id, 2)
            .unwrap();
        assert!(queue.read_batch(n(100), n(10), 3).unwrap().tasks.is_empty());
        assert_eq!(store.get_tasks(&source, 0, 100, 10).unwrap().len(), 1);
        store.lease(&target, 0, 0, 4).unwrap();
        assert!(queue
            .append_in_domain(&[data()], source.domain_id, 5)
            .is_err());
        assert_eq!(store.get_tasks(&source, 0, 100, 10).unwrap().len(), 1);
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
    fn matching_queue_expiry_clock_advances_between_row_admissions() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap();
        let mut task = data();
        task.expiry_nanos = 100;
        let written = queue.append(&[task.clone(), task], 2).unwrap();
        let mut times = [100, 101].into_iter();
        let loaded = queue
            .read_batch_with_clock(n(100), n(10), || times.next().unwrap())
            .unwrap();
        // Equality is still deliverable; the next row expires at its own admission.
        assert_eq!(loaded.tasks, written[..1]);
        let (read, _, backlog) = queue.levels();
        assert_eq!(read, written[1].task_id);
        assert_eq!(backlog, 1);
        assert!(times.next().is_none());
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
