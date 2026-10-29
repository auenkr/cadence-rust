//! Byte-preserving durable Matching queue over `MatchingTaskStoreBytes`.
//!
//! Mirrors `matching_queue.rs` (range leasing, writer transaction ordering,
//! reader cursor, ack, GC, partition metadata, stop) but carries
//! `TaskListKeyBytes`/`TaskDataBytes`/`TaskListDataBytes`/`AllocatedTaskBytes`
//! end to end, never converting through the UTF-8 legacy model.
use crate::{
    matching_blob::{PartitionConfigBytes, TaskDataBytes, TaskListDataBytes, GO_ZERO_TIME_NANOS},
    matching_native_task_store::{
        AllocatedTaskBytes, MatchingTaskStoreBytes, MatchingTaskStoreBytesError as StoreError,
        TaskListKeyBytes,
    },
    matching_task_store::MatchingTaskStoreError as LegacyError,
};
use cadence_common::ack_manager::AckManager;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    num::NonZeroU32,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum NativeQueueError {
    #[error(transparent)]
    Storage(#[from] StoreError),
    #[error("matching task-list owner stopped")]
    Stopped,
    #[error("task is not in flight: {0}")]
    NotInFlight(i64),
    #[error("invalid matching reader state: {0}")]
    ReaderState(String),
}
pub type Result<T> = std::result::Result<T, NativeQueueError>;
type StoreResult<T> = std::result::Result<T, StoreError>;
pub type LeaseObserver = Arc<dyn Fn(bool) + Send + Sync>;
pub type ConditionFailedObserver = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone)]
pub struct PendingTaskBytes {
    pub data: TaskDataBytes,
    pub source_domain: uuid::Uuid,
    pub schedule_to_start_seconds: i32,
}

/// Go SQL CreateTasks expiry: nonpositive timeout means Go zero time.
fn at_creation(mut data: TaskDataBytes, now: i64, schedule_to_start_seconds: i32) -> TaskDataBytes {
    data.created_nanos = now;
    data.expiry_nanos = if schedule_to_start_seconds > 0 {
        now.wrapping_add(i64::from(schedule_to_start_seconds) * 1_000_000_000)
    } else {
        GO_ZERO_TIME_NANOS
    };
    data
}

fn is_condition_failed(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::ConditionFailed { .. } | StoreError::Legacy(LegacyError::ConditionFailed(_))
    )
}

const STASHED: &str = "\0native-store-error-stashed";

/// The shared retry policy is typed on the legacy error. Legacy and backend
/// errors pass through (keeping transient classification); other native errors
/// are non-transient, so they are stashed and restored unchanged.
fn adapt<T>(
    stash: &RefCell<Option<StoreError>>,
    result: StoreResult<T>,
) -> std::result::Result<T, LegacyError> {
    result.map_err(|error| match error {
        StoreError::Legacy(e) => e,
        StoreError::Backend { kind, message } => LegacyError::Backend { kind, message },
        other => {
            *stash.borrow_mut() = Some(other);
            LegacyError::ConditionFailed(STASHED.into())
        }
    })
}
fn restore<T>(
    stash: &RefCell<Option<StoreError>>,
    result: std::result::Result<T, LegacyError>,
) -> StoreResult<T> {
    result.map_err(|error| match error {
        LegacyError::ConditionFailed(m) if m == STASHED => stash
            .borrow_mut()
            .take()
            .unwrap_or(StoreError::Legacy(LegacyError::ConditionFailed(m))),
        other => StoreError::Legacy(other),
    })
}
fn retry<T>(mut operation: impl FnMut() -> StoreResult<T>) -> StoreResult<T> {
    let stash = RefCell::new(None);
    let result = crate::matching_retry::persistence(|| adapt(&stash, operation()));
    restore(&stash, result)
}
fn retry_lease<T>(now: i64, mut operation: impl FnMut(i64) -> StoreResult<T>) -> StoreResult<T> {
    let stash = RefCell::new(None);
    let result = crate::matching_retry::lease(now, |at| adapt(&stash, operation(at)));
    restore(&stash, result)
}

struct State {
    range_id: i64,
    next_id: i64,
    block_end: i64,
    max_read_level: i64,
    metadata: TaskListDataBytes,
    in_flight: HashMap<i64, AllocatedTaskBytes>,
}

pub struct DurableTaskQueueBytes {
    store: Arc<dyn MatchingTaskStoreBytes>,
    key: TaskListKeyBytes,
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
    lease_observer: Mutex<Option<LeaseObserver>>,
    condition_failed_observer: Mutex<Option<ConditionFailedObserver>>,
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
}
struct ReaderPermit(Arc<ReaderGate>);
impl Drop for ReaderPermit {
    fn drop(&mut self) {
        *self.0.held.lock().expect("matching reader gate lock") = false;
        self.0.changed.notify_one();
    }
}

/// One fetched batch with exclusive ownership of the read cursor.
pub struct ReaderTurnBytes {
    queue: Arc<DurableTaskQueueBytes>,
    _permit: ReaderPermit,
    tasks: VecDeque<AllocatedTaskBytes>,
    pending: bool,
    expired: usize,
}
impl ReaderTurnBytes {
    pub fn next(&mut self, mut clock: impl FnMut() -> i64) -> Result<Option<AllocatedTaskBytes>> {
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
pub struct ReadBatchBytes {
    pub tasks: Vec<AllocatedTaskBytes>,
    pub scan_pending: bool,
}
struct ScannedBatch {
    tasks: Vec<AllocatedTaskBytes>,
    read_level: i64,
    finished: bool,
}
fn scan_batch(
    store: &dyn MatchingTaskStoreBytes,
    key: &TaskListKeyBytes,
    mut read: i64,
    max: i64,
    mut range: impl FnMut() -> i64,
    mut batch: impl FnMut() -> i64,
) -> StoreResult<ScannedBatch> {
    for _ in 0..10 {
        if read >= max {
            break;
        }
        let upper = read.wrapping_add(range()).min(max);
        let batch = batch();
        let tasks = retry(|| store.get_tasks(key, read, upper, batch))?;
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

impl DurableTaskQueueBytes {
    pub fn open(
        store: Arc<dyn MatchingTaskStoreBytes>,
        key: TaskListKeyBytes,
        kind: i16,
        range_size: NonZeroU32,
        now: i64,
    ) -> Result<Self> {
        let lease = retry_lease(now, |at| store.lease(&key, kind, 0, at))?;
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
            condition_failed_observer: Mutex::new(None),
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
    pub fn key(&self) -> &TaskListKeyBytes {
        &self.key
    }
    fn active(&self) -> Result<()> {
        if self.is_stopped() {
            Err(NativeQueueError::Stopped)
        } else {
            Ok(())
        }
    }
    pub fn set_lease_observer(&self, observer: LeaseObserver) {
        *self
            .lease_observer
            .lock()
            .expect("matching lease observer lock") = Some(observer);
    }
    fn observe_lease(&self, success: bool) {
        let observer = self
            .lease_observer
            .lock()
            .expect("matching lease observer lock")
            .clone();
        if let Some(observer) = observer {
            observer(success);
        }
    }
    pub fn set_condition_failed_observer(&self, observer: ConditionFailedObserver) {
        *self
            .condition_failed_observer
            .lock()
            .expect("matching condition observer lock") = Some(observer);
    }
    fn check<T>(&self, result: StoreResult<T>) -> Result<T> {
        self.check_with(result, true)
    }
    /// Go reports a lost lease only from the task writer and periodic ack
    /// persistence, not partition-config updates or the final ack.
    fn check_with<T>(&self, result: StoreResult<T>, report: bool) -> Result<T> {
        if matches!(&result, Err(e) if is_condition_failed(e)) {
            if report {
                let observer = self
                    .condition_failed_observer
                    .lock()
                    .expect("matching condition observer lock")
                    .clone();
                if let Some(observer) = observer {
                    observer();
                }
            }
            self.stop();
        }
        result.map_err(Into::into)
    }
    fn append_serialized(
        &self,
        data: &[(uuid::Uuid, TaskDataBytes)],
        now: i64,
        timeouts: Option<&[i32]>,
    ) -> Result<Vec<AllocatedTaskBytes>> {
        self.active()?;
        let mut tasks = Vec::with_capacity(data.len());
        for (_, data) in data {
            let mut state = self.state.lock().expect("matching queue lock");
            if state.next_id > state.block_end {
                drop(state);
                let result = retry_lease(now, |attempt_now| {
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
            tasks.push(AllocatedTaskBytes {
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
                task.data = at_creation(task.data.clone(), timestamp, *timeout);
            }
        }
        let tasks_with_domains = tasks
            .iter()
            .zip(data)
            .map(|(task, (domain, _))| (*domain, task))
            .collect::<Vec<_>>();
        let result =
            self.store
                .create_tasks_with_domains(&self.key, range_id, now, &tasks_with_domains);
        // Go advances the read boundary after the write returns, even on error.
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
    pub fn append(&self, data: &[TaskDataBytes], now: i64) -> Result<Vec<AllocatedTaskBytes>> {
        self.append_in_domain(data, self.key.domain_id, now)
    }
    pub fn append_in_domain(
        &self,
        data: &[TaskDataBytes],
        source_domain: uuid::Uuid,
        now: i64,
    ) -> Result<Vec<AllocatedTaskBytes>> {
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
    pub fn append_mixed(
        &self,
        data: &[(uuid::Uuid, TaskDataBytes)],
        now: i64,
    ) -> Result<Vec<AllocatedTaskBytes>> {
        let _writer = self.writer.lock().expect("matching writer gate");
        self.append_serialized(data, now, None)
    }
    pub fn append_pending(
        &self,
        pending: &[PendingTaskBytes],
        now_nanos: i64,
    ) -> Result<Vec<AllocatedTaskBytes>> {
        let data = pending
            .iter()
            .map(|t| (t.source_domain, t.data.clone()))
            .collect::<Vec<_>>();
        let timeouts = pending
            .iter()
            .map(|t| t.schedule_to_start_seconds)
            .collect::<Vec<_>>();
        let _writer = self.writer.lock().expect("matching writer gate");
        self.append_serialized(&data, now_nanos, Some(&timeouts))
    }
    pub fn task_list_kind(&self) -> i16 {
        self.state
            .lock()
            .expect("matching queue lock")
            .metadata
            .kind
    }
    pub fn retry_data(&self, task_id: i64) -> Result<TaskDataBytes> {
        self.state
            .lock()
            .expect("matching queue lock")
            .in_flight
            .get(&task_id)
            .map(|task| task.data.clone())
            .ok_or(NativeQueueError::NotInFlight(task_id))
    }
    pub fn read_batch(
        &self,
        read_range_size: u32,
        batch_size: NonZeroU32,
        now: i64,
    ) -> Result<ReadBatchBytes> {
        self.read_batch_with_clock(read_range_size, batch_size, || now)
    }
    pub fn read_batch_with_clock(
        &self,
        read_range_size: u32,
        batch_size: NonZeroU32,
        clock: impl FnMut() -> i64,
    ) -> Result<ReadBatchBytes> {
        let mut tasks = Vec::new();
        let scan_pending = self.read_batch_into(read_range_size, batch_size, clock, |task| {
            tasks.push(task);
            true
        })?;
        Ok(ReadBatchBytes {
            tasks,
            scan_pending,
        })
    }
    pub fn read_batch_into(
        &self,
        read_range_size: u32,
        batch_size: NonZeroU32,
        mut clock: impl FnMut() -> i64,
        mut admit: impl FnMut(AllocatedTaskBytes) -> bool,
    ) -> Result<bool> {
        let (_permit, scanned) = self.load_reader_turn(
            || i64::from(read_range_size),
            || i64::from(batch_size.get()),
        )?;
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
    pub fn reader_turn(
        self: &Arc<Self>,
        read_range_size: u32,
        batch_size: NonZeroU32,
    ) -> Result<ReaderTurnBytes> {
        self.reader_turn_live(|| i64::from(read_range_size), || i64::from(batch_size.get()))
    }
    /// Sample dynamic properties per scanned range, like Go getTaskBatch.
    pub fn reader_turn_live(
        self: &Arc<Self>,
        range: impl FnMut() -> i64,
        batch: impl FnMut() -> i64,
    ) -> Result<ReaderTurnBytes> {
        let (permit, scanned) = self.load_reader_turn(range, batch)?;
        Ok(ReaderTurnBytes {
            queue: self.clone(),
            _permit: permit,
            pending: !scanned.tasks.is_empty() || !scanned.finished,
            tasks: scanned.tasks.into(),
            expired: 0,
        })
    }
    fn load_reader_turn(
        &self,
        range: impl FnMut() -> i64,
        batch: impl FnMut() -> i64,
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
        task: AllocatedTaskBytes,
        clock: impl FnOnce() -> i64,
    ) -> Result<Option<AllocatedTaskBytes>> {
        let mut state = self.state.lock().expect("matching queue lock");
        self.active()?;
        if expired(task.data.expiry_nanos, clock()) {
            self.ack.set_read_level(task.task_id);
            return Ok(None);
        }
        self.ack
            .read_item(task.task_id)
            .map_err(NativeQueueError::ReaderState)?;
        state.in_flight.insert(task.task_id, task.clone());
        Ok(Some(task))
    }
    pub fn complete(&self, task_id: i64) -> Result<i64> {
        let mut state = self.state.lock().expect("matching queue lock");
        if state.in_flight.remove(&task_id).is_none() {
            return Err(NativeQueueError::NotInFlight(task_id));
        }
        Ok(self.ack.ack_item(task_id))
    }
    /// Rewrite before acknowledging the original; zero timeout, fresh creation time.
    pub fn rewrite_failed(&self, task_id: i64, now: i64) -> Result<AllocatedTaskBytes> {
        let _writer = self.writer.lock().expect("matching writer gate");
        let data = at_creation(self.retry_data(task_id)?, now, 0);
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
    pub fn partition_config(&self) -> Option<PartitionConfigBytes> {
        self.state
            .lock()
            .expect("matching queue lock")
            .metadata
            .adaptive_partition_config
            .clone()
    }
    pub fn update_partition_config(
        &self,
        config: Option<PartitionConfigBytes>,
        now: i64,
    ) -> Result<()> {
        let _database = self.database.lock().expect("matching database gate");
        self.active()?;
        let (range, mut metadata) = {
            let state = self.state.lock().expect("matching queue lock");
            (state.range_id, state.metadata.clone())
        };
        metadata.adaptive_partition_config = config;
        self.check_with(
            self.store
                .update_task_list(&self.key, range, metadata.clone(), now),
            false,
        )?;
        self.state.lock().expect("matching queue lock").metadata = metadata;
        Ok(())
    }
    pub fn persist_final_ack(&self, now: i64) -> Result<()> {
        self.persist_ack_impl(now, true)
    }
    fn persist_ack_impl(&self, now: i64, final_update: bool) -> Result<()> {
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
            self.check_with(result, !final_update)?;
            self.state.lock().expect("matching queue lock").metadata = metadata;
        }
        Ok(())
    }
    pub fn collect_garbage(&self, limit: i64) -> Result<usize> {
        let (_, ack, _) = self.ack.levels();
        Ok(self.store.complete_tasks_less_than(&self.key, ack, limit)?)
    }
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
        self.run_garbage_collection_with_batch_provider(
            force,
            || i64::from(batch.get()),
            max_interval,
        )
    }
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
    pub fn task_list_size(&self) -> Result<i64> {
        let (_, ack, _) = self.ack.levels();
        Ok(self.store.task_list_size(&self.key, ack)?)
    }
    pub fn task_lag(&self) -> i64 {
        let state = self.state.lock().expect("matching queue lock");
        let (_, ack, _) = self.ack.levels();
        state.max_read_level.saturating_sub(ack)
    }
    pub fn status(&self) -> (i64, i64, i64, i64, i64, bool) {
        let state = self.state.lock().expect("matching queue lock");
        let (read, ack, backlog) = self.ack.levels();
        let (start, end) = block(state.range_id, self.range_size);
        (read, ack, backlog, start, end, ack == state.max_read_level)
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
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
    use cadence_common::go_string::GoString;
    use uuid::Uuid;

    fn n(v: u32) -> NonZeroU32 {
        NonZeroU32::new(v).unwrap()
    }
    fn key(name: &[u8]) -> TaskListKeyBytes {
        TaskListKeyBytes {
            domain_id: Uuid::from_u128(1),
            name: GoString::from(name),
            task_type: 0,
        }
    }
    fn data(workflow: &[u8]) -> TaskDataBytes {
        at_creation(
            TaskDataBytes {
                workflow_id: GoString::from(workflow),
                run_id: Some(Uuid::from_u128(2).as_bytes().to_vec()),
                schedule_id: 2,
                ..Default::default()
            },
            100,
            0,
        )
    }
    fn store(dir: &tempfile::TempDir) -> Arc<SqliteTaskStore> {
        Arc::new(SqliteTaskStore::create(dir.path().join("t.db"), BlobEncoding::ThriftRW).unwrap())
    }
    fn rows(s: &SqliteTaskStore, k: &TaskListKeyBytes) -> usize {
        MatchingTaskStoreBytes::get_tasks(s, k, 0, i64::MAX, 1000)
            .unwrap()
            .len()
    }

    // Go task_reader.go getTaskBatch samples each property on every range.
    #[test]
    fn scan_samples_live_properties_per_range_and_keeps_signed_width() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let key = key(b"live-scan");
        let mut ranges = [2, 3, 9].into_iter();
        let mut batches = [1, 2, 3].into_iter();
        let scan = scan_batch(
            store.as_ref(),
            &key,
            0,
            10,
            || ranges.next().unwrap(),
            || batches.next().unwrap(),
        )
        .unwrap();
        assert_eq!(scan.read_level, 10);
        assert!(scan.finished);
        assert!(ranges.next().is_none());
        assert!(batches.next().is_none());

        let scan = scan_batch(store.as_ref(), &key, 10, 20, || -1, || 1).unwrap();
        assert_eq!(scan.read_level, 0);
        assert!(!scan.finished);
        let scan = scan_batch(
            store.as_ref(),
            &key,
            0,
            10,
            || i64::from(u32::MAX) + 1,
            || 1,
        )
        .unwrap();
        assert_eq!(scan.read_level, 10);
        assert!(scan.finished);
    }

    #[test]
    fn raw_identifier_queues_are_isolated_and_bytes_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let names: [&[u8]; 3] = [b"\xff", b"\xfe", "\u{fffd}".as_bytes()];
        let queues: Vec<_> = names
            .iter()
            .map(|nm| DurableTaskQueueBytes::open(s.clone(), key(nm), 0, n(100), 1).unwrap())
            .collect();
        for (i, q) in queues.iter().enumerate() {
            let wf = [b'w', 0xff, i as u8];
            q.append(&[data(&wf)], 2).unwrap();
        }
        for (i, q) in queues.iter().enumerate() {
            let batch = q.read_batch(100, n(10), 3).unwrap();
            assert_eq!(batch.tasks.len(), 1);
            assert_eq!(
                batch.tasks[0].data.workflow_id.as_bytes(),
                &[b'w', 0xff, i as u8]
            );
            assert_eq!(
                batch.tasks[0].data.run_id,
                Some(Uuid::from_u128(2).as_bytes().to_vec())
            );
            q.complete(batch.tasks[0].task_id).unwrap();
        }
    }

    #[test]
    fn stale_lease_fences_writer_and_reports_condition_failure() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let first = DurableTaskQueueBytes::open(s.clone(), key(b"q\xff"), 0, n(100), 1).unwrap();
        let reports = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = reports.clone();
        first.set_condition_failed_observer(Arc::new(move || {
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        let second = DurableTaskQueueBytes::open(s.clone(), key(b"q\xff"), 0, n(100), 2).unwrap();
        assert_eq!(second.status().3, 101);
        let err = first.append(&[data(b"wf")], 3).unwrap_err();
        assert!(matches!(err, NativeQueueError::Storage(ref e) if is_condition_failed(e)));
        assert!(first.is_stopped());
        assert_eq!(reports.load(Ordering::SeqCst), 1);
        assert!(matches!(
            first.append(&[data(b"wf")], 4),
            Err(NativeQueueError::Stopped)
        ));
        // The new owner is unaffected and no row from the fenced writer exists.
        assert_eq!(rows(&s, &key(b"q\xff")), 0);
        second.append(&[data(b"wf")], 5).unwrap();
        assert_eq!(rows(&s, &key(b"q\xff")), 1);
    }

    #[test]
    fn range_rollover_renews_lease_and_never_reuses_ids() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let q = DurableTaskQueueBytes::open(s.clone(), key(b"\xfe"), 0, n(2), 1).unwrap();
        let ids: Vec<_> = q
            .append(&[data(b"a"), data(b"b"), data(b"c")], 2)
            .unwrap()
            .iter()
            .map(|t| t.task_id)
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
        let record = MatchingTaskStoreBytes::get_task_list(&*s, &key(b"\xfe")).unwrap();
        assert_eq!(record.range_id, 2);
    }

    #[test]
    fn ack_cursor_persists_and_reopen_resumes_after_ack_level() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let k = key(b"\xff\xfe");
        let q = DurableTaskQueueBytes::open(s.clone(), k.clone(), 0, n(100), 1).unwrap();
        q.append(&[data(b"a"), data(b"b"), data(b"c")], 2).unwrap();
        let batch = q.read_batch(100, n(10), 3).unwrap();
        assert_eq!(batch.tasks.len(), 3);
        q.complete(batch.tasks[0].task_id).unwrap();
        q.complete(batch.tasks[1].task_id).unwrap();
        assert!(matches!(
            q.complete(999),
            Err(NativeQueueError::NotInFlight(999))
        ));
        q.persist_ack(4).unwrap();
        assert_eq!(
            MatchingTaskStoreBytes::get_task_list(&*s, &k)
                .unwrap()
                .data
                .ack_level,
            2
        );
        q.stop();
        assert!(matches!(q.persist_ack(5), Err(NativeQueueError::Stopped)));
        q.persist_final_ack(5).unwrap();

        let reopened = DurableTaskQueueBytes::open(s.clone(), k.clone(), 0, n(100), 6).unwrap();
        let resumed = reopened.read_batch(100, n(10), 7).unwrap();
        assert_eq!(resumed.tasks.len(), 1);
        assert_eq!(resumed.tasks[0].task_id, 3);
        assert_eq!(resumed.tasks[0].data.workflow_id.as_bytes(), b"c");
    }

    #[test]
    fn garbage_collection_deletes_only_acked_raw_keyed_rows() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let k = key(b"gc\xff");
        let other = key(b"gc\xfe");
        let q = DurableTaskQueueBytes::open(s.clone(), k.clone(), 0, n(100), 1).unwrap();
        let o = DurableTaskQueueBytes::open(s.clone(), other.clone(), 0, n(100), 1).unwrap();
        o.append(&[data(b"o")], 2).unwrap();
        q.append(&[data(b"a"), data(b"b")], 2).unwrap();
        let batch = q.read_batch(100, n(10), 3).unwrap();
        q.complete(batch.tasks[0].task_id).unwrap();
        assert_eq!(q.run_garbage_collection(true).unwrap(), 1);
        assert_eq!(rows(&s, &k), 1);
        assert_eq!(rows(&s, &other), 1);
        assert_eq!(q.task_list_size().unwrap(), 1);
    }

    #[test]
    fn expired_tasks_advance_cursor_without_entering_flight() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let q = DurableTaskQueueBytes::open(s.clone(), key(b"exp"), 0, n(100), 1).unwrap();
        let mut stale = data(b"old");
        stale.expiry_nanos = 10;
        q.append(&[stale, data(b"live")], 2).unwrap();
        let batch = q.read_batch(100, n(10), 1_000).unwrap();
        assert_eq!(batch.tasks.len(), 1);
        assert_eq!(batch.tasks[0].data.workflow_id.as_bytes(), b"live");
    }

    #[test]
    fn cross_domain_append_keeps_source_domain_row_key() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir);
        let k = key(b"xd");
        let q = DurableTaskQueueBytes::open(s.clone(), k.clone(), 1, n(100), 1).unwrap();
        // Rows are stored under the source domain, so the target-domain reader sees none.
        q.append_in_domain(&[data(b"x")], Uuid::from_u128(9), 2)
            .unwrap();
        assert_eq!(rows(&s, &k), 0);
        let source = TaskListKeyBytes {
            domain_id: Uuid::from_u128(9),
            ..k
        };
        let stored = MatchingTaskStoreBytes::get_tasks(&*s, &source, 0, i64::MAX, 10).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].data.workflow_id.as_bytes(), b"x");
        assert_eq!(q.task_list_kind(), 1);
    }
}
