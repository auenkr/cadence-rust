//! Durable single-owner Matching queue. Workflow state lives in History, not here.
use crate::{
    matching_blob::{TaskData, TaskListData},
    matching_sqlite::{AllocatedTask, Result as StorageResult, StorageError, TaskListKey},
    matching_task_store::MatchingTaskStore,
};
use cadence_common::ack_manager::AckManager;
use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroU32,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
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
    store: Arc<dyn MatchingTaskStore>,
    key: TaskListKey,
    range_size: NonZeroU32,
    state: Mutex<State>,
    writer: Mutex<()>,
    database: Mutex<()>,
    reader: Arc<ReaderGate>,
    ack: AckManager,
    gc: crate::matching_gc::TaskGarbageCollector,
    gc_batch_size_provider: Mutex<Option<Arc<dyn Fn() -> i64 + Send + Sync>>>,
    stopped: AtomicBool,
    stop_waker: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    lease_observer: Mutex<Option<Arc<dyn Fn(bool) + Send + Sync>>>,
}
#[derive(Default)]
struct ReaderGate {
    held: Mutex<bool>,
    changed: Condvar,
}
impl ReaderGate {
    fn lock(self: &Arc<Self>) -> ReaderPermit {
        let mut held = self.held.lock().expect("matching reader gate lock");
        while *held {
            held = self.changed.wait(held).expect("matching reader gate lock");
        }
        *held = true;
        ReaderPermit(self.clone())
    }
    #[cfg(test)]
    fn try_lock(self: &Arc<Self>) -> std::result::Result<ReaderPermit, ()> {
        let mut held = self.held.lock().unwrap();
        if *held {
            return Err(());
        }
        *held = true;
        Ok(ReaderPermit(self.clone()))
    }
}
struct ReaderPermit(Arc<ReaderGate>);
impl Drop for ReaderPermit {
    fn drop(&mut self) {
        *self.0.held.lock().expect("matching reader gate lock") = false;
        self.0.changed.notify_one();
    }
}
/// One fetched batch with exclusive ownership of the read cursor. Dropping it
/// leaves rows not yet admitted unread, so a replacement pump can fetch them.
pub struct ReaderTurn {
    queue: Arc<DurableTaskQueue>,
    _permit: ReaderPermit,
    tasks: VecDeque<AllocatedTask>,
    pending: bool,
    expired: usize,
}
impl ReaderTurn {
    pub fn next(&mut self, mut clock: impl FnMut() -> i64) -> Result<Option<AllocatedTask>> {
        while let Some(task) = self.tasks.pop_front() {
            match self.queue.register_read(task, &mut clock)? {
                Some(task) => return Ok(Some(task)),
                None => self.expired += 1,
            }
        }
        Ok(None)
    }
    pub fn take_expired_count(&mut self) -> usize {
        std::mem::take(&mut self.expired)
    }
    pub fn scan_pending(&self) -> bool {
        self.pending
    }
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
    store: &dyn MatchingTaskStore,
    key: &TaskListKey,
    mut read: i64,
    max: i64,
    range: u32,
    batch: NonZeroU32,
) -> StorageResult<ScannedBatch> {
    for _ in 0..10 {
        if read >= max {
            break;
        }
        let upper = read.wrapping_add(i64::from(range)).min(max);
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
        store: Arc<dyn MatchingTaskStore>,
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
            gc_batch_size_provider: Mutex::new(None),
            stopped: AtomicBool::new(false),
            stop_waker: Mutex::new(None),
            lease_observer: Mutex::new(None),
            writer: Mutex::new(()),
            database: Mutex::new(()),
            reader: Arc::new(ReaderGate::default()),
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
    /// Install a service-owned callback for each task-list lease renewal.
    /// `true` means the retry operation obtained a lease; a failed renewal is
    /// reported once after its retry policy terminates.
    pub fn set_lease_observer(&self, observer: Arc<dyn Fn(bool) + Send + Sync>) {
        *self.lease_observer.lock().expect("matching lease observer lock") = Some(observer);
    }
    fn observe_lease(&self, success: bool) {
        if let Some(observer) = self
            .lease_observer
            .lock()
            .expect("matching lease observer lock")
            .clone()
        {
            observer(success);
        }
    }
    fn check<T>(&self, result: StorageResult<T>) -> Result<T> {
        if matches!(result, Err(StorageError::ConditionFailed(_))) {
            self.stop();
        }
        result.map_err(Into::into)
    }
    fn append_serialized(
        &self,
        data: &[(uuid::Uuid, TaskData)],
        now: i64,
        timeouts: Option<&[i32]>,
    ) -> Result<Vec<AllocatedTask>> {
        self.active()?;
        let mut tasks = Vec::with_capacity(data.len());
        for (_, data) in data {
            let mut state = self.state.lock().expect("matching queue lock");
            if state.next_id > state.block_end {
                drop(state);
                let result = crate::matching_retry::lease(now, |attempt_now| {
                    // Go taskListDB holds its mutex for each lease operation,
                    // not through the retry sleep. Reader state is independent.
                    let _database = self.database.lock().expect("matching database gate");
                    let (kind, range) = {
                        let state = self.state.lock().expect("matching queue lock");
                        (state.metadata.kind, state.range_id)
                    };
                    let lease = self.store.lease(&self.key, kind, range, attempt_now)?;
                    let mut state = self.state.lock().expect("matching queue lock");
                    state.range_id = lease.range_id;
                    (state.next_id, state.block_end) = block(lease.range_id, self.range_size);
                    Ok(())
                });
                self.observe_lease(result.is_ok());
                if result.is_err() {
                    self.stop();
                }
                result?;
                state = self.state.lock().expect("matching queue lock");
            }
            tasks.push(AllocatedTask {
                task_id: state.next_id,
                data: data.clone(),
            });
            state.next_id = state.next_id.wrapping_add(1);
        }
        let _database = self.database.lock().expect("matching database gate");
        let range_id = self.state.lock().expect("matching queue lock").range_id;
        if let Some(timeouts) = timeouts {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|time| time.as_nanos() as i64)
                .unwrap_or_default();
            for (task, timeout) in tasks.iter_mut().zip(timeouts) {
                task.data = task.data.clone().at_creation(timestamp, *timeout);
            }
        }
        let tasks_with_domains = tasks
            .iter()
            .zip(data)
            .map(|(task, (domain, _))| (*domain, task))
            .collect::<Vec<_>>();
        let result = self
            .store
            .create_tasks_with_domains(&self.key, range_id, &tasks_with_domains);
        // Go advances the read boundary after the write returns, even on error.
        // Allocated IDs are never reused after an uncertain persistence outcome.
        if let Some(last) = tasks.last() {
            if last.task_id > 0 {
                self.state
                    .lock()
                    .expect("matching queue lock")
                    .max_read_level = last.task_id;
            }
        }
        self.check(result)?;
        Ok(tasks)
    }
    pub fn append(&self, data: &[TaskData], now: i64) -> Result<Vec<AllocatedTask>> {
        self.append_in_domain(data, self.key.domain_id, now)
    }
    /// Preserve Go SQL's source-domain row keys for cross-domain activities.
    /// Reading this queue still filters on the target task-list domain.
    pub fn append_in_domain(
        &self,
        data: &[TaskData],
        source_domain: uuid::Uuid,
        now: i64,
    ) -> Result<Vec<AllocatedTask>> {
        let _writer = self.writer.lock().expect("matching writer gate");
        self.append_serialized(
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
        let _writer = self.writer.lock().expect("matching writer gate");
        self.append_serialized(data, now, None)
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
        let _writer = self.writer.lock().expect("matching writer gate");
        self.append_serialized(&data, now, Some(&timeouts))
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
        read_range_size: u32,
        batch_size: NonZeroU32,
        now: i64,
    ) -> Result<ReadBatch> {
        self.read_batch_with_clock(read_range_size, batch_size, || now)
    }

    /// Read expiry time at admission, after persistence retries. The fixed-time
    /// wrapper remains useful for replaying deterministic Go scan fixtures.
    pub fn read_batch_with_clock(
        &self,
        read_range_size: u32,
        batch_size: NonZeroU32,
        clock: impl FnMut() -> i64,
    ) -> Result<ReadBatch> {
        let mut tasks = Vec::new();
        let scan_pending = self.read_batch_into(read_range_size, batch_size, clock, |task| {
            tasks.push(task);
            true
        })?;
        Ok(ReadBatch {
            tasks,
            scan_pending,
        })
    }

    /// Admit one row at a time. The callback may block without holding queue
    /// state; false retains the current in-flight row and leaves later rows unread.
    pub fn read_batch_into(
        &self,
        read_range_size: u32,
        batch_size: NonZeroU32,
        mut clock: impl FnMut() -> i64,
        mut admit: impl FnMut(AllocatedTask) -> bool,
    ) -> Result<bool> {
        let (_permit, scanned) = self.load_reader_turn(read_range_size, batch_size)?;
        let pending = !scanned.tasks.is_empty() || !scanned.finished;
        for task in scanned.tasks {
            if let Some(task) = self.register_read(task, &mut clock)? {
                if !admit(task) {
                    break;
                }
            }
        }
        Ok(pending)
    }

    /// SQL-only phase for an asynchronous pump. The owned permit keeps the
    /// cursor exclusive across async buffer waits without holding a thread/lock.
    pub fn reader_turn(
        self: &Arc<Self>,
        read_range_size: u32,
        batch_size: NonZeroU32,
    ) -> Result<ReaderTurn> {
        let (permit, scanned) = self.load_reader_turn(read_range_size, batch_size)?;
        Ok(ReaderTurn {
            queue: self.clone(),
            _permit: permit,
            pending: !scanned.tasks.is_empty() || !scanned.finished,
            tasks: scanned.tasks.into(),
            expired: 0,
        })
    }
    fn load_reader_turn(
        &self,
        range: u32,
        batch: NonZeroU32,
    ) -> Result<(ReaderPermit, ScannedBatch)> {
        let permit = self.reader.lock();
        self.active()?;
        let max = self
            .state
            .lock()
            .expect("matching queue lock")
            .max_read_level;
        let (initial_read, _, _) = self.ack.levels();
        let scanned = scan_batch(
            self.store.as_ref(),
            &self.key,
            initial_read,
            max,
            range,
            batch,
        )?;
        let _state = self.state.lock().expect("matching queue lock");
        self.active()?;
        if scanned.tasks.is_empty() {
            self.ack.set_read_level(scanned.read_level);
            if self.ack.levels().1 == initial_read {
                self.ack.set_ack_level(scanned.read_level);
            }
        }
        Ok((permit, scanned))
    }
    fn register_read(
        &self,
        task: AllocatedTask,
        clock: impl FnOnce() -> i64,
    ) -> Result<Option<AllocatedTask>> {
        let mut state = self.state.lock().expect("matching queue lock");
        self.active()?;
        if expired(task.data.expiry_nanos, clock()) {
            self.ack.set_read_level(task.task_id);
            return Ok(None);
        }
        self.ack
            .read_item(task.task_id)
            .map_err(QueueError::ReaderState)?;
        state.in_flight.insert(task.task_id, task.clone());
        Ok(Some(task))
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
        let _writer = self.writer.lock().expect("matching writer gate");
        let data = self.retry_data(task_id)?.at_creation(now, 0);
        let mut written = self.append_serialized(&[(self.key.domain_id, data)], now, None)?;
        self.state
            .lock()
            .expect("matching queue lock")
            .in_flight
            .remove(&task_id);
        self.ack.ack_item(task_id);
        Ok(written.remove(0))
    }
    pub fn persist_ack(&self, now: i64) -> Result<()> {
        self.persist_ack_impl(now, false)
    }
    /// The owner's last successfully persisted partition configuration. Like
    /// Go taskListDB, readers never observe an update that failed persistence.
    pub fn partition_config(&self) -> Option<crate::matching_blob::PartitionConfig> {
        self.state
            .lock()
            .expect("matching queue lock")
            .metadata
            .adaptive_partition_config
            .clone()
    }
    /// Persist native task-list partition metadata under the current lease.
    /// Version validation and assignment belong to the manager. Preserve the
    /// persisted ack level, rather than publishing unflushed reader progress.
    pub fn update_partition_config(
        &self,
        config: Option<crate::matching_blob::PartitionConfig>,
        now: i64,
    ) -> Result<()> {
        let _database = self.database.lock().expect("matching database gate");
        self.active()?;
        let (range, mut metadata) = {
            let state = self.state.lock().expect("matching queue lock");
            (state.range_id, state.metadata.clone())
        };
        metadata.adaptive_partition_config = config;
        self.check(
            self.store
                .update_task_list(&self.key, range, metadata.clone(), now),
        )?;
        self.state.lock().expect("matching queue lock").metadata = metadata;
        Ok(())
    }
    /// Go reader.Stop persists its final cursor even after the writer stopped.
    /// The database lease fence remains authoritative for this final write.
    pub fn persist_final_ack(&self, now: i64) -> Result<()> {
        self.persist_ack_impl(now, true)
    }
    fn persist_ack_impl(&self, now: i64, final_update: bool) -> Result<()> {
        // Fence the metadata snapshot against this owner's own lease renewal.
        let _database = self.database.lock().expect("matching database gate");
        if !final_update {
            self.active()?;
        }
        let (_, ack, _) = self.ack.levels();
        if ack >= 0 {
            let (range, mut metadata) = {
                let state = self.state.lock().expect("matching queue lock");
                (state.range_id, state.metadata.clone())
            };
            metadata.ack_level = ack;
            let result = self
                .store
                .update_task_list(&self.key, range, metadata.clone(), now);
            self.check(result)?;
            self.state.lock().expect("matching queue lock").metadata = metadata;
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
        let provider = self
            .gc_batch_size_provider
            .lock()
            .expect("matching GC config lock")
            .clone();
        match provider {
            Some(provider) => self.run_garbage_collection_with_batch_provider(
                force,
                move || provider(),
                std::time::Duration::from_secs(1),
            ),
            None => self.run_garbage_collection_with(
                force,
                NonZeroU32::new(100).unwrap(),
                std::time::Duration::from_secs(1),
            ),
        }
    }
    pub fn set_gc_batch_size_provider(&self, provider: Arc<dyn Fn() -> i64 + Send + Sync>) {
        *self
            .gc_batch_size_provider
            .lock()
            .expect("matching GC config lock") = Some(provider);
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
    /// Live signed batch provider sampled after the GC lock is acquired.
    pub fn run_garbage_collection_with_batch_provider(
        &self,
        force: bool,
        batch_provider: impl FnOnce() -> i64,
        max_interval: std::time::Duration,
    ) -> Result<usize> {
        let (_, ack, _) = self.ack.levels();
        Ok(self.gc.run_with_batch_provider(
            ack,
            batch_provider,
            max_interval,
            force,
            std::time::Instant::now(),
            |ack, limit| self.store.complete_tasks_less_than(&self.key, ack, limit),
        )?)
    }
    pub fn levels(&self) -> (i64, i64, i64) {
        self.ack.levels()
    }
    /// Go task-list reader's task count query, excluding rows at/below the ack watermark.
    pub fn task_list_size(&self) -> Result<i64> {
        let (_, ack, _) = self.ack.levels();
        Ok(self.store.task_list_size(&self.key, ack)?)
    }
    /// Estimated queue lag emitted by Go as maxReadLevel minus ackLevel.
    pub fn task_lag(&self) -> i64 {
        let state = self.state.lock().expect("matching queue lock");
        let (_, ack, _) = self.ack.levels();
        state.max_read_level.saturating_sub(ack)
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
    use crate::matching_sqlite::SqliteTaskStore;
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
    fn matching_queue_partition_metadata_survives_ack_rollover_and_reopen() {
        use crate::matching_blob::{Partition, PartitionConfig};
        for encoding in [BlobEncoding::ThriftRW, BlobEncoding::ThriftRWSnappy] {
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(
                SqliteTaskStore::create(directory.path().join("tasks.db"), encoding).unwrap(),
            );
            let queue = DurableTaskQueue::open(store.clone(), key(), 0, n(1), 1).unwrap();
            let config = PartitionConfig {
                version: 7,
                num_read_partitions: 1,
                num_write_partitions: 1,
                read_partitions: Some(std::collections::BTreeMap::from([(
                    0,
                    Partition {
                        isolation_groups: Some(vec!["A".into()]),
                    },
                )])),
                write_partitions: Some(std::collections::BTreeMap::new()),
            };
            queue
                .update_partition_config(Some(config.clone()), 2)
                .unwrap();
            let tasks = queue.append(&[data(), data()], 3).unwrap();
            queue.read_batch(100, n(10), 4).unwrap();
            for task in tasks {
                queue.complete(task.task_id).unwrap();
            }
            // A config write must retain the DB cursor until ack persistence.
            let old_ack = store.get_task_list(&key()).unwrap().data.ack_level;
            queue
                .update_partition_config(Some(config.clone()), 5)
                .unwrap();
            assert_eq!(store.get_task_list(&key()).unwrap().data.ack_level, old_ack);
            queue.persist_ack(6).unwrap();
            assert_eq!(queue.partition_config(), Some(config.clone()));
            queue.stop();
            let reopened = DurableTaskQueue::open(store, key(), 0, n(1), 7).unwrap();
            assert_eq!(reopened.partition_config(), Some(config));
            reopened.update_partition_config(None, 8).unwrap();
            assert_eq!(reopened.partition_config(), None);
        }
    }
    #[test]
    fn matching_queue_partition_storage_failure_keeps_previous_metadata() {
        use crate::matching_blob::PartitionConfig;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).unwrap());
        let queue = DurableTaskQueue::open(store.clone(), key(), 0, n(100), 1).unwrap();
        let original = PartitionConfig {
            version: 1,
            ..Default::default()
        };
        queue
            .update_partition_config(Some(original.clone()), 2)
            .unwrap();
        let connection = rusqlite::Connection::open(path).unwrap();
        connection.execute_batch("CREATE TRIGGER reject_config_update BEFORE UPDATE ON task_lists BEGIN SELECT RAISE(FAIL, 'injected write failure'); END").unwrap();
        assert!(queue.update_partition_config(None, 3).is_err());
        assert!(!queue.is_stopped());
        assert_eq!(queue.partition_config(), Some(original.clone()));
        assert_eq!(
            store
                .get_task_list(&key())
                .unwrap()
                .data
                .adaptive_partition_config,
            Some(original)
        );
        connection
            .execute_batch("DROP TRIGGER reject_config_update")
            .unwrap();
        queue.update_partition_config(None, 4).unwrap();
        queue.persist_ack(5).unwrap();
        assert_eq!(
            store
                .get_task_list(&key())
                .unwrap()
                .data
                .adaptive_partition_config,
            None
        );
    }
    #[test]
    fn matching_queue_live_gc_batch_provider_is_used_for_each_attempt() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("gc.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap();
        let configured = Arc::new(std::sync::atomic::AtomicI64::new(1));
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let value = configured.clone();
        let count = reads.clone();
        queue.set_gc_batch_size_provider(Arc::new(move || {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            value.load(std::sync::atomic::Ordering::SeqCst)
        }));
        let written = queue.append(&[data(), data()], 2).unwrap();
        let batch = queue.read_batch(10, n(10), 3).unwrap();
        for task in batch.tasks {
            queue.complete(task.task_id).unwrap();
        }
        assert_eq!(queue.run_garbage_collection(true).unwrap(), 2);
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(written.len(), 2);

        configured.store(2, std::sync::atomic::Ordering::SeqCst);
        let later = queue.append(&[data()], 4).unwrap();
        let batch = queue.read_batch(10, n(10), 5).unwrap();
        for task in batch.tasks {
            queue.complete(task.task_id).unwrap();
        }
        assert_eq!(queue.run_garbage_collection(true).unwrap(), 1);
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(later.len(), 1);
    }
    #[test]
    fn matching_queue_partition_update_fences_stale_owner_without_publishing() {
        use crate::matching_blob::PartitionConfig;
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let stale = DurableTaskQueue::open(store.clone(), key(), 0, n(100), 1).unwrap();
        let original = PartitionConfig {
            version: 1,
            ..Default::default()
        };
        stale
            .update_partition_config(Some(original.clone()), 2)
            .unwrap();
        let current = DurableTaskQueue::open(store.clone(), key(), 0, n(100), 3).unwrap();
        let replacement = PartitionConfig {
            version: 2,
            ..Default::default()
        };
        assert!(matches!(
            stale.update_partition_config(Some(replacement), 4),
            Err(QueueError::Storage(StorageError::ConditionFailed(_)))
        ));
        assert!(stale.is_stopped());
        assert_eq!(stale.partition_config(), Some(original.clone()));
        assert_eq!(current.partition_config(), Some(original.clone()));
        assert_eq!(
            store
                .get_task_list(&key())
                .unwrap()
                .data
                .adaptive_partition_config,
            Some(original)
        );
        assert!(matches!(
            stale.update_partition_config(None, 5),
            Err(QueueError::Stopped)
        ));
    }
    #[test]
    fn matching_queue_reader_retries_transient_query_without_losing_task() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).unwrap());
        let queue = Arc::new(DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap());
        let first = queue.append(&[data()], 2).unwrap();
        queue.read_batch(100, n(1), 3).unwrap();
        let written = queue.append(&[data()], 4).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch("ALTER TABLE tasks RENAME TO temporarily_unavailable_tasks")
            .unwrap();
        let copy = queue.clone();
        let reader = std::thread::spawn(move || copy.read_batch(100, n(1), 5));
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
        assert!(queue.read_batch(100, n(1), 4).unwrap().tasks.is_empty());
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
    fn matching_queue_lease_backoff_allows_durable_reads_and_ack_updates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tasks.db");
        let store = Arc::new(SqliteTaskStore::create(&path, BlobEncoding::ThriftRW).unwrap());
        let queue = Arc::new(DurableTaskQueue::open(store.clone(), key(), 0, n(1), 1).unwrap());
        queue.append(&[data()], 2).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("CREATE TRIGGER reject_renewal BEFORE UPDATE ON task_lists WHEN NEW.range_id > OLD.range_id BEGIN SELECT RAISE(FAIL, 'temporary lease outage'); END;").unwrap();
        let copy = queue.clone();
        let writing = std::thread::spawn(move || copy.append(&[data()], 3));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while queue.writer.try_lock().is_ok() {
            assert!(std::time::Instant::now() < deadline, "writer did not enter");
            std::thread::yield_now();
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(!writing.is_finished(), "renewal must wait for recovery");
        let copy = queue.clone();
        let (sender, receiver) = std::sync::mpsc::channel();
        let reading = std::thread::spawn(move || {
            let result = (|| -> Result<()> {
                let rows = copy.read_batch(100, n(1), 4)?;
                assert_eq!(rows.tasks[0].task_id, 1);
                copy.complete(1)?;
                copy.persist_ack(5)
            })();
            sender.send(result).unwrap();
        });
        let progress = receiver.recv_timeout(std::time::Duration::from_millis(500));
        // Always release the lease worker before asserting the concurrency
        // result, so a failing regression does not leave a30sec retry thread.
        connection
            .execute_batch("DROP TRIGGER reject_renewal")
            .unwrap();
        let written = writing.join().unwrap().unwrap();
        reading.join().unwrap();
        progress
            .expect("lease backoff blocked existing task delivery/ack")
            .unwrap();
        assert_eq!(written[0].task_id, 2);
        let persisted = store.get_task_list(&key()).unwrap();
        assert_eq!(persisted.range_id, 2);
        assert_eq!(persisted.data.ack_level, 1);
        assert_eq!(queue.read_batch(100, n(1), 6).unwrap().tasks, written);
        assert!(!queue.is_stopped());
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
        assert!(queue.read_batch(100, n(10), 3).unwrap().tasks.is_empty());
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
                case["range"].as_u64().unwrap() as u32,
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
    fn matching_queue_original_go_blocked_admission_fixtures() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-queue-admission.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(
                SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                    .unwrap(),
            );
            let queue = DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap();
            queue.append(&vec![data(); 6], 2).unwrap();
            let mut remaining =
                case["consumed"].as_u64().unwrap() + case["capacity"].as_u64().unwrap();
            let mut completed = true;
            queue
                .read_batch_into(
                    100,
                    n(10),
                    || 3,
                    |_| {
                        if remaining == 0 {
                            completed = false;
                            false
                        } else {
                            remaining -= 1;
                            true
                        }
                    },
                )
                .unwrap();
            let (read, _, backlog) = queue.levels();
            assert_eq!(read, case["read"].as_i64().unwrap());
            assert_eq!(backlog, case["backlog"].as_i64().unwrap());
            assert_eq!(completed, case["completed"].as_bool().unwrap());
        }
    }
    #[test]
    fn matching_queue_owned_turn_retains_unadmitted_rows_across_threads() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = Arc::new(DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap());
        let written = queue.append(&vec![data(); 3], 2).unwrap();
        let turn = queue.reader_turn(100, n(10)).unwrap();
        let mut turn = std::thread::spawn(move || turn).join().unwrap();
        assert_eq!(queue.levels().2, 0, "fetching is not admission");
        assert_eq!(turn.next(|| 3).unwrap(), Some(written[0].clone()));
        queue.complete(written[0].task_id).unwrap();
        drop(turn);
        assert_eq!(queue.read_batch(100, n(10), 4).unwrap().tasks, written[1..]);
    }
    #[test]
    fn matching_queue_cancelled_admission_leaves_later_batch_rows_unread() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap();
        let written = queue.append(&vec![data(); 3], 2).unwrap();
        let initial_ack = queue.levels().1;
        let pending = queue
            .read_batch_into(
                100,
                n(10),
                || 3,
                |task| {
                    assert_eq!(task, written[0]);
                    // Registered before the send; inspecting state also checks
                    // that the callback does not hold the producer state mutex.
                    assert_eq!(queue.status().0, task.task_id);
                    assert_eq!(queue.levels().2, 1);
                    false
                },
            )
            .unwrap();
        assert!(pending);
        assert_eq!(queue.levels(), (written[0].task_id, initial_ack, 1));
        assert_eq!(
            queue.retry_data(written[0].task_id).unwrap(),
            written[0].data
        );
        let rest = queue.read_batch(100, n(10), 4).unwrap();
        assert_eq!(rest.tasks, written[1..]);
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
            .read_batch_with_clock(100, n(10), || times.next().unwrap())
            .unwrap();
        // Equality is still deliverable; the next row expires at its own admission.
        assert_eq!(loaded.tasks, written[..1]);
        let (read, _, backlog) = queue.levels();
        assert_eq!(read, written[1].task_id);
        assert_eq!(backlog, 1);
        assert!(times.next().is_none());
    }
    #[test]
    fn matching_reader_turn_reports_initially_expired_rows() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = Arc::new(DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap());
        let mut task = data();
        task.expiry_nanos = 100;
        queue.append(&[task], 2).unwrap();
        let mut turn = queue.reader_turn(100, n(10)).unwrap();
        assert!(turn.next(|| 101).unwrap().is_none());
        assert_eq!(turn.take_expired_count(), 1);
        assert_eq!(turn.take_expired_count(), 0);
        assert_eq!(queue.levels().2, 0);
    }
    #[test]
    fn matching_queue_reader_size_and_lag_gauges_follow_ack_levels() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(directory.path().join("tasks.db"), BlobEncoding::ThriftRW)
                .unwrap(),
        );
        let queue = DurableTaskQueue::open(store, key(), 0, n(100), 1).unwrap();
        queue.append(&[data(), data()], 2).unwrap();
        assert_eq!(queue.task_list_size().unwrap(), 2);
        let batch = queue.read_batch(100, n(10), 3).unwrap();
        queue.complete(batch.tasks[0].task_id).unwrap();
        assert_eq!(queue.task_list_size().unwrap(), 1);
        assert_eq!(queue.task_lag(), 1);
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
        let loaded = queue.read_batch(10, n(10), 200).unwrap();
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
        assert_eq!(queue.read_batch(10, n(10), 400).unwrap().tasks, next);
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
        queue.read_batch(100, n(10), 100).unwrap();
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
        let batch = recovered.read_batch(100, n(10), 300).unwrap();
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
            owner.read_batch(1000, n(10), 600).unwrap().tasks[0].task_id,
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
        let batch = queue.read_batch(100, n(10), 100).unwrap();
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
        let batch = queue.read_batch(2, n(10), 200).unwrap();
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
                    let task = queue.read_batch(100, n(1), 100).unwrap().tasks.remove(0);
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
