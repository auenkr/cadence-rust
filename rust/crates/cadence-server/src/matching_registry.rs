//! Matching-owned queue lifecycle, independent of workflow execution state.
use crate::{matching_dispatch::TaskDispatcher, matching_history::MatchingHistoryClient};
use cadence_persistence::{
    matching_blob::TaskData,
    matching_queue::DurableTaskQueue,
    matching_sqlite::{AllocatedTask, SqliteTaskStore, TaskListKey},
};
use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::{Arc, Mutex, Weak},
};
use tonic::Status;
use uuid::Uuid;

fn registry_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_nanos() as i64)
        .unwrap_or_default()
}
type Key = (Uuid, String, i32);
type StartupGates = HashMap<Key, Weak<Mutex<()>>>;
struct Owner {
    queue: Arc<DurableTaskQueue>,
    dispatcher: TaskDispatcher,
    writer: crate::matching_writer::TaskWriter,
    reader: crate::matching_reader::TaskReader,
    limiter: Arc<crate::matching_limiter::TaskListLimiter>,
    pollers: Arc<Mutex<cadence_common::poller::Manager>>,
    last_update: Mutex<tokio::time::Instant>,
    next_idle_check: Mutex<tokio::time::Instant>,
    liveness: Arc<cadence_common::liveness::Liveness>,
}
impl Owner {
    fn stop(&self) {
        self.writer.stop();
        self.reader.stop();
        self.queue.stop();
        self.dispatcher.notify_new_tasks();
    }    fn finish(
        &self,
        now: i64,
        readers: &mut Vec<crate::matching_reader::TaskReader>,
    ) -> Result<(), Status> {
        self.stop();
        readers.push(self.reader.clone());
        let ack = self.queue.persist_final_ack(now);
        let garbage = self.queue.run_garbage_collection(true);
        ack.and(garbage.map(|_| ()))
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}
#[derive(Default)]
struct State {
    owners: HashMap<Key, Arc<Owner>>,
    stopped: bool,
}
/// Queue creation and insertion run in the same blocking worker, so cancelling
/// an RPC while its lease is acquired cannot orphan or duplicate that lease.
#[derive(Clone)]
pub struct QueueRegistry {
    store: Arc<SqliteTaskStore>,
    history: MatchingHistoryClient,
    range_size: NonZeroU32,
    state: Arc<Mutex<State>>,
    changed: Arc<tokio::sync::Notify>,
    startups: Arc<Mutex<StartupGates>>,
    domain_metadata: Option<(cadence_persistence::InMemoryDomainStore, String)>,
    /// Go `cfg.NumTasklistWritePartitions(domain, taskList, type)`, clamped to
    /// >= 1 by the same caller. `None` keeps the single-partition default.
    partition_count: Option<PartitionCountProvider>,
}
/// Live write-partition-count provider used to size the dispatch limiter and to
/// enumerate partitions, mirroring Go's `NumTasklistWritePartitions`.
pub type PartitionCountProvider = Arc<dyn Fn(&str, &str, i32) -> i64 + Send + Sync>;
#[derive(Clone)]
pub struct QueueHandle {
    pub queue: Arc<DurableTaskQueue>,
    pub dispatcher: TaskDispatcher,
    pub pollers: Arc<Mutex<cadence_common::poller::Manager>>,
    pub limiter: Arc<crate::matching_limiter::TaskListLimiter>,
    liveness: Arc<cadence_common::liveness::Liveness>,
    writer: crate::matching_writer::TaskWriter,
}
impl QueueHandle {
    pub async fn enqueue(
        &self,
        data: TaskData,
        source_domain: Uuid,
        schedule_to_start_seconds: i32,
    ) -> Result<AllocatedTask, Status> {
        self.liveness.mark_alive(registry_now());
        self.writer
            .append(crate::matching_writer::WriteTask {
                data,
                source_domain,
                schedule_to_start_seconds,
            })
            .await
    }
    /// Notification remains in the persistence worker if the AddTask RPC is
    /// cancelled while SQLite is writing. History may safely retry the request.
    pub async fn append(
        &self,
        tasks: Vec<TaskData>,
        now: i64,
    ) -> Result<Vec<AllocatedTask>, Status> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            handle.liveness.mark_alive(registry_now());
            let tasks = handle
                .queue
                .append(&tasks, now)
                .map_err(|error| Status::unavailable(error.to_string()))?;
            handle.dispatcher.notify_new_tasks();
            Ok(tasks)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }
    pub async fn append_in_domain(
        &self,
        tasks: Vec<TaskData>,
        source_domain: Uuid,
        now: i64,
    ) -> Result<Vec<AllocatedTask>, Status> {
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            handle.liveness.mark_alive(registry_now());
            let tasks = handle
                .queue
                .append_in_domain(&tasks, source_domain, now)
                .map_err(|error| Status::unavailable(error.to_string()))?;
            handle.dispatcher.notify_new_tasks();
            Ok(tasks)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }
}
// SQL cleanup runs on the blocking pool; joining async loops must release that
// worker first, so simultaneous owner unloads cannot starve their own readers.
async fn owner_work<T: Send + 'static>(
    operation: impl FnOnce(&mut Vec<crate::matching_reader::TaskReader>) -> Result<T, Status>
        + Send
        + 'static,
) -> Result<T, Status> {
    let (result, readers) = tokio::task::spawn_blocking(move || {
        let mut readers = Vec::new();
        let result = operation(&mut readers);
        (result, readers)
    })
    .await
    .map_err(|error| Status::internal(error.to_string()))?;
    for reader in readers {
        reader.wait_stopped().await;
    }
    result
}
impl QueueRegistry {
    pub fn new(
        store: Arc<SqliteTaskStore>,
        history: MatchingHistoryClient,
        range_size: NonZeroU32,
    ) -> Self {
        Self {
            store,
            history,
            range_size,
            state: Arc::new(Mutex::new(State::default())),
            changed: Arc::new(tokio::sync::Notify::new()),
            startups: Arc::new(Mutex::new(HashMap::new())),
            domain_metadata: None,
            partition_count: None,
        }
    }
    /// Install Go's `NumTasklistWritePartitions` provider. The provider reads
    /// `matching.numTasklistWritePartitions`; the registry clamps to >= 1.
    pub fn with_partition_count(mut self, provider: PartitionCountProvider) -> Self {
        self.partition_count = Some(provider);
        self
    }
    pub fn with_domain_metadata(
        mut self,
        domains: cadence_persistence::InMemoryDomainStore,
        cluster: String,
    ) -> Self {
        self.domain_metadata = Some((domains, cluster));
        self
    }
    /// task_type is the persistence enum (decision 0, activity 1).
    pub async fn get(&self, key: TaskListKey, kind: i16, now: i64) -> Result<QueueHandle, Status> {
        if !(0..=1).contains(&key.task_type) {
            return Err(Status::invalid_argument("Unknown persistence task type."));
        }
        let registry = self.clone();
        owner_work(move |readers| {
            let lookup = (key.domain_id, key.name.clone(), key.task_type);
            // Go registers a manager before Start performs lease I/O. Serialize
            // startup by queue identity while keeping unrelated owners usable.
            // Weak entries cannot retain every task-list name ever encountered.
            let startup = {
                let mut gates = registry
                    .startups
                    .lock()
                    .expect("Matching startup gates lock");
                gates.retain(|_, gate| gate.strong_count() > 0);
                match gates.get(&lookup).and_then(Weak::upgrade) {
                    Some(gate) => gate,
                    None => {
                        let gate = Arc::new(Mutex::new(()));
                        gates.insert(lookup.clone(), Arc::downgrade(&gate));
                        gate
                    }
                }
            };
            let _starting = startup.lock().expect("Matching queue startup lock");
            let mut state = registry.state.lock().expect("Matching registry lock");
            if state.stopped {
                return Err(Status::unavailable("Matching registry stopped."));
            }
            if state
                .owners
                .get(&lookup)
                .is_some_and(|owner| owner.queue.is_stopped())
            {
                if let Some(owner) = state.owners.remove(&lookup) {
                    drop(state);
                    if let Err(error) = owner.finish(now, readers) {
                        eprintln!("Matching stopped owner cleanup failed: {error}");
                    }
                    state = registry.state.lock().expect("Matching registry lock");
                    if state.stopped {
                        return Err(Status::unavailable("Matching registry stopped."));
                    }
                }
            }
            let owner = match state.owners.get(&lookup) {
                Some(owner) => owner.clone(),
                None => {
                    drop(state);
                    let domain = key.domain_id.to_string();
                    let count_partitions: crate::matching_limiter::PartitionCount = {
                        let provider = registry.partition_count.clone();
                        let domain_id = key.domain_id.to_string();
                        let task_list = key.name.clone();
                        let task_type = key.task_type;
                        Arc::new(move || {
                            provider
                                .as_ref()
                                .map(|provider| provider(&domain_id, &task_list, task_type))
                                .unwrap_or(1)
                                .max(1)
                        })
                    };
                    let queue = Arc::new(
                        DurableTaskQueue::open(
                            registry.store.clone(),
                            key,
                            kind,
                            registry.range_size,
                            now,
                        )
                        .map_err(|error| Status::unavailable(error.to_string()))?,
                    );
                    let liveness = Arc::new(cadence_common::liveness::Liveness::new(
                        registry_now(),
                        300_000_000_000,
                    ));
                    let activity_domain = domain.clone();
                    let metadata = registry.domain_metadata.clone();
                    let activity = Arc::new(move || {
                        metadata.as_ref().is_none_or(|(domains, cluster)| {
                            domains
                                .is_active_in(&activity_domain, cluster)
                                .unwrap_or(true)
                        })
                    });
                    let dispatcher =
                        TaskDispatcher::new(queue.clone(), registry.history.clone(), domain)
                            .with_liveness(liveness.clone());
                    let writer = crate::matching_writer::TaskWriter::new(
                        queue.clone(),
                        dispatcher.task_notification(),
                        crate::matching_writer::WriterConfig::default(),
                    );
                    let limiter = Arc::new(
                        crate::matching_limiter::TaskListLimiter::with_partitions(count_partitions),
                    );
                    let reader = crate::matching_reader::TaskReader::with_domain_activity_and_limiter(
                        queue.clone(),
                        dispatcher.task_notification(),
                        crate::matching_reader::ReaderConfig::default(),
                        activity,
                        limiter.clone(),
                    );
                    let read_stop = reader.stop_signal();
                    let wake = registry.changed.clone();
                    let stopped = writer.stop_signal();
                    queue.set_stop_waker(Arc::new(move || {
                        let _ = stopped.send(true);
                        read_stop.send_replace(true);
                        wake.notify_waiters();
                    }));
                    let owner = Arc::new(Owner {
                        dispatcher: dispatcher
                            .with_writer(writer.clone())
                            .with_reader(reader.clone()),
                        writer,
                        reader,
                        limiter,
                        queue,
                        pollers: Arc::new(Mutex::new(cadence_common::poller::Manager::default())),
                        last_update: Mutex::new(tokio::time::Instant::now()),
                        next_idle_check: Mutex::new(
                            tokio::time::Instant::now() + std::time::Duration::from_secs(150),
                        ),
                        liveness,
                    });
                    state = registry.state.lock().expect("Matching registry lock");
                    if state.stopped {
                        drop(state);
                        if let Err(error) = owner.finish(now, readers) {
                            eprintln!("Matching startup shutdown cleanup failed: {error}");
                        }
                        return Err(Status::unavailable("Matching registry stopped."));
                    }
                    state.owners.insert(lookup, owner.clone());
                    registry.changed.notify_waiters();
                    owner
                }
            };
            Ok(QueueHandle {
                queue: owner.queue.clone(),
                dispatcher: owner.dispatcher.clone(),
                pollers: owner.pollers.clone(),
                limiter: owner.limiter.clone(),
                liveness: owner.liveness.clone(),
                writer: owner.writer.clone(),
            })
        })
        .await
    }
    /// An old manager's teardown must never unregister its replacement.
    pub async fn unload(
        &self,
        key: TaskListKey,
        handle: QueueHandle,
        now: i64,
    ) -> Result<bool, Status> {
        let registry = self.clone();
        owner_work(move |readers| {
            let mut state = registry.state.lock().expect("Matching registry lock");
            let lookup = (key.domain_id, key.name, key.task_type);
            let Some(owner) = state.owners.get(&lookup) else {
                return Ok(false);
            };
            if !Arc::ptr_eq(&owner.queue, &handle.queue) {
                return Ok(false);
            }
            let owner = state.owners.remove(&lookup).expect("checked owner");
            drop(state);
            owner.finish(now, readers)?;
            Ok(true)
        })
        .await
    }
    /// Called by the service's configured ack timer. Fenced owners stop in the
    /// queue layer; report errors rather than silently accepting stale ownership.
    pub async fn persist_acks(&self, now: i64) -> Result<(), Status> {
        self.flush(now, false, None).await
    }
    /// All handles become unusable after shutdown. Persist every owner even if
    /// another owner fails, and return the first failure for service diagnostics.
    pub async fn shutdown(&self, now: i64) -> Result<(), Status> {
        self.flush(now, true, None).await
    }
    /// Fixed-interval controller. Reset after each completed update, as Go's
    /// task reader does; a transient persistence error does not kill the pump.
    /// The caller supplies the configured interval and owns the shutdown signal.
    pub async fn maintain_until<F>(
        &self,
        interval: std::time::Duration,
        shutdown: F,
    ) -> Result<(), Status>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Err(error) = self.reap_stopped(registry_now()).await {
                eprintln!("Matching fatal-owner cleanup failed: {error}");
            }
            let deadline = self
                .state
                .lock()
                .expect("Matching registry lock")
                .owners
                .values()
                .map(|owner| {
                    (*owner.last_update.lock().expect("Matching ack timer lock") + interval).min(
                        *owner
                            .next_idle_check
                            .lock()
                            .expect("Matching idle timer lock"),
                    )
                })
                .min()
                .unwrap_or_else(|| tokio::time::Instant::now() + interval);
            tokio::select! {
                biased;
                _ = &mut shutdown => return self.shutdown(registry_now()).await,
                _ = &mut changed => continue,
                _ = tokio::time::sleep_until(deadline) => {
                    if let Err(error) = self.sweep_idle(registry_now(), true).await { eprintln!("Matching idle cleanup failed: {error}"); }
                    if let Err(error) = self.flush(registry_now(), false, Some(interval)).await {
                        eprintln!("Matching acknowledgement update failed: {error}");
                    }
                }
            }
        }
    }
    async fn reap_stopped(&self, now: i64) -> Result<(), Status> {
        let registry = self.clone();
        owner_work(move |readers| {
            let candidates = registry
                .state
                .lock()
                .expect("Matching registry lock")
                .owners
                .iter()
                .filter(|(_, owner)| owner.queue.is_stopped())
                .map(|(key, owner)| (key.clone(), owner.clone()))
                .collect::<Vec<_>>();
            let mut failure = None;
            for (key, owner) in candidates {
                if registry.detach(&key, &owner) {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(()), Err)
        })
        .await
    }
    /// Check idleness using the caller's wall-clock time source. Scheduled
    /// ticks use due_only; an explicit controller scan may check every owner.
    pub async fn sweep_idle(&self, now: i64, due_only: bool) -> Result<(), Status> {
        let registry = self.clone();
        owner_work(move |readers| {
            let candidates = {
                let state = registry.state.lock().expect("Matching registry lock");
                state
                    .owners
                    .iter()
                    .filter_map(|(key, owner)| {
                        let mut next = owner
                            .next_idle_check
                            .lock()
                            .expect("Matching idle timer lock");
                        if due_only && *next > tokio::time::Instant::now() {
                            return None;
                        }
                        // Go uses a ticker at TTL/2; retain its cadence after delayed ticks.
                        let interval = std::time::Duration::from_secs(150);
                        let lag = tokio::time::Instant::now().saturating_duration_since(*next);
                        *next += interval * ((lag.as_nanos() / interval.as_nanos() + 1) as u32);
                        (!owner.liveness.is_alive(now)).then(|| (key.clone(), owner.clone()))
                    })
                    .collect::<Vec<_>>()
            };
            let mut failure = None;
            for (key, owner) in candidates {
                if registry.detach(&key, &owner) {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(()), Err)
        })
        .await
    }
    fn detach(&self, key: &Key, expected: &Arc<Owner>) -> bool {
        let mut state = self.state.lock().expect("Matching registry lock");
        if state
            .owners
            .get(key)
            .is_some_and(|owner| Arc::ptr_eq(owner, expected))
        {
            state.owners.remove(key);
            true
        } else {
            false
        }
    }
    async fn flush(
        &self,
        now: i64,
        stop: bool,
        due: Option<std::time::Duration>,
    ) -> Result<(), Status> {
        let registry = self.clone();
        owner_work(move |readers| {
            let owners = {
                let mut state = registry.state.lock().expect("Matching registry lock");
                if state.stopped {
                    return Ok(());
                }
                if stop {
                    state.stopped = true;
                    state.owners.drain().collect::<Vec<_>>()
                } else {
                    state
                        .owners
                        .iter()
                        .map(|(key, owner)| (key.clone(), owner.clone()))
                        .collect()
                }
            };
            // Unregister all owners and stop admission before doing any database
            // cleanup. A slow owner must not hold the registry lock or leave
            // other shutdown handles accepting new work.
            if stop {
                for (_, owner) in &owners {
                    owner.stop();
                }
            }
            let mut failure = None;
            for (key, owner) in owners {
                if stop {
                    if let Err(error) = owner.finish(now, readers) {
                        failure.get_or_insert(error);
                    }
                    continue;
                }
                if !owner.queue.is_stopped()
                    && due.is_some_and(|interval| {
                        owner
                            .last_update
                            .lock()
                            .expect("Matching ack timer lock")
                            .elapsed()
                            < interval
                    })
                {
                    continue;
                }
                let update = owner.queue.persist_ack(now);
                if let Err(error) = update {
                    failure.get_or_insert_with(|| Status::unavailable(error.to_string()));
                }
                *owner.last_update.lock().expect("Matching ack timer lock") =
                    tokio::time::Instant::now();
                if owner.queue.is_stopped() {
                    let detached = registry.detach(&key, &owner);
                    if detached {
                        if let Err(error) = owner.finish(now, readers) {
                            failure.get_or_insert(error);
                        }
                    }
                }
                owner.dispatcher.notify_new_tasks();
            }
            failure.map_or(Ok(()), Err)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::matching_blob::BlobEncoding;
    use std::time::Duration;
    async fn take(registry: &QueueRegistry, handle: &QueueHandle) -> AllocatedTask {
        let reader = registry
            .state
            .lock()
            .unwrap()
            .owners
            .values()
            .find(|owner| Arc::ptr_eq(&owner.queue, &handle.queue))
            .unwrap()
            .reader
            .clone();
        tokio::time::timeout(Duration::from_secs(2), reader.poll())
            .await
            .unwrap()
            .unwrap()
    }
    fn fixture() -> (
        tempfile::TempDir,
        Arc<SqliteTaskStore>,
        QueueRegistry,
        TaskListKey,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteTaskStore::create(dir.path().join("tasks.db"), BlobEncoding::ThriftRW).unwrap(),
        );
        let history = MatchingHistoryClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            Duration::from_secs(1),
            32 * 1024 * 1024,
        );
        let registry = QueueRegistry::new(store.clone(), history, NonZeroU32::new(100).unwrap());
        let key = TaskListKey {
            domain_id: Uuid::from_u128(1),
            name: "queue".into(),
            task_type: 0,
        };
        (dir, store, registry, key)
    }
    #[tokio::test]
    async fn slow_startup_does_not_block_loaded_owner_or_shutdown() {
        let (directory, _store, registry, key) = fixture();
        let loaded = registry.get(key.clone(), 0, 1).await.unwrap();
        let connection = rusqlite::Connection::open(directory.path().join("tasks.db")).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut new_key = key.clone();
        new_key.name = "slow-startup".into();
        let lookup = (new_key.domain_id, new_key.name.clone(), new_key.task_type);
        let worker = registry.clone();
        let loading = tokio::spawn(async move { worker.get(new_key, 0, 2).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let gate = registry
                    .startups
                    .lock()
                    .unwrap()
                    .get(&lookup)
                    .and_then(Weak::upgrade);
                if gate.is_some_and(|gate| gate.try_lock().is_err()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Let the lease call enter SQLite's busy wait behind our transaction.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let again = tokio::time::timeout(Duration::from_millis(500), registry.get(key, 0, 3))
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&loaded.queue, &again.queue));
        // Shutdown stops admission without waiting for a lease that has not yet
        // been registered. The loader must fence its eventual publication.
        let shutdown = registry.clone();
        let stopped = tokio::spawn(async move { shutdown.shutdown(4).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if registry.state.lock().unwrap().stopped {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        connection.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            loading.await.unwrap().err().unwrap().code(),
            tonic::Code::Unavailable
        );
        stopped.await.unwrap().unwrap();
        assert!(registry.state.lock().unwrap().owners.is_empty());
    }

    #[tokio::test]
    async fn concurrent_load_acquires_one_lease_and_isolates_task_types() {
        let (_dir, store, registry, key) = fixture();
        let mut requests = Vec::new();
        for _ in 0..20 {
            let registry = registry.clone();
            let key = key.clone();
            requests.push(tokio::spawn(async move {
                registry.get(key, 0, 1).await.unwrap()
            }));
        }
        let first = requests.remove(0).await.unwrap();
        for request in requests {
            assert!(Arc::ptr_eq(&first.queue, &request.await.unwrap().queue));
        }
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 1);
        let mut activity = key.clone();
        activity.task_type = 1;
        let other = registry.get(activity, 0, 1).await.unwrap();
        assert!(!Arc::ptr_eq(&first.queue, &other.queue));
    }
    #[test]
    fn shutdown_joins_readers_with_one_blocking_worker() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (_directory, _store, registry, key) = fixture();
            let mut handles = Vec::new();
            for index in 0..10 {
                let mut key = key.clone();
                key.name = format!("shutdown-{index}");
                let handle = registry.get(key, 0, 1).await.unwrap();
                handle
                    .append(vec![TaskData::default(); 3], 1)
                    .await
                    .unwrap();
                handles.push(handle);
            }
            tokio::time::timeout(Duration::from_secs(2), registry.shutdown(2))
                .await
                .unwrap()
                .unwrap();
            assert!(handles.iter().all(|handle| handle.queue.is_stopped()));
        });
    }
    #[tokio::test]
    async fn shutdown_persists_ack_and_stops_previously_returned_handles() {
        let (_dir, store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &handle).await;
        handle.queue.complete(task.task_id).unwrap();
        registry.shutdown(2).await.unwrap();
        assert_eq!(
            store.get_task_list(&key).unwrap().data.ack_level,
            task.task_id
        );
        assert!(handle.queue.append(&[TaskData::default()], 3).is_err());
        assert!(registry.get(key, 0, 3).await.is_err());
        registry.shutdown(3).await.unwrap();
    }
    #[tokio::test]
    async fn fenced_owner_does_not_prevent_other_queues_from_flushing() {
        let (_dir, store, registry, key) = fixture();
        let stale = registry.get(key.clone(), 0, 1).await.unwrap();
        let mut other_key = key.clone();
        other_key.name = "other".into();
        let other = registry.get(other_key.clone(), 0, 1).await.unwrap();
        other.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &other).await;
        other.queue.complete(task.task_id).unwrap();
        store.lease(&key, 0, 0, 2).unwrap();
        assert!(registry.persist_acks(3).await.is_err());
        assert!(stale.append(vec![TaskData::default()], 3).await.is_err());
        assert_eq!(
            store.get_task_list(&other_key).unwrap().data.ack_level,
            task.task_id
        );
        assert!(!registry.state.lock().unwrap().owners.contains_key(&(
            key.domain_id,
            key.name.clone(),
            key.task_type
        )));
        registry.shutdown(4).await.unwrap();
        assert!(other.append(vec![TaskData::default()], 4).await.is_err());
    }
    #[tokio::test]
    async fn stale_teardown_cannot_remove_replacement_owner() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.append(vec![TaskData::default()], 1).await.unwrap();
        assert!(registry.unload(key.clone(), old.clone(), 2).await.unwrap());
        let new = registry.get(key.clone(), 0, 3).await.unwrap();
        assert!(!registry.unload(key.clone(), old, 4).await.unwrap());
        let same = registry.get(key.clone(), 0, 4).await.unwrap();
        assert!(Arc::ptr_eq(&new.queue, &same.queue));
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
        let recovered = take(&registry, &new).await;
        assert_eq!(recovered.task_id, 1);
    }
    #[tokio::test]
    async fn maintenance_persists_live_ack_and_shutdown_stops_handles() {
        let (_dir, store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &handle).await;
        handle.queue.complete(task.task_id).unwrap();
        assert_eq!(store.get_task_list(&key).unwrap().data.ack_level, 0);
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let worker = registry.clone();
        let pump = tokio::spawn(async move {
            worker
                .maintain_until(Duration::from_millis(10), async {
                    let _ = shutdown.await;
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.get_task_list(&key).unwrap().data.ack_level != task.task_id {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(store.get_task_list(&key).unwrap().data.last_updated_nanos > 1);
        stop.send(()).unwrap();
        pump.await.unwrap().unwrap();
        assert!(handle.queue.is_stopped());
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
        assert!(handle.append(vec![TaskData::default()], 2).await.is_err());
    }
    #[tokio::test]
    async fn stopped_writer_is_replaced_without_waiting_for_ack_timer() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &old).await;
        old.queue.complete(task.task_id).unwrap();
        old.queue.stop();
        let new = registry.get(key.clone(), 0, 2).await.unwrap();
        assert!(!Arc::ptr_eq(&old.queue, &new.queue));
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
        assert_eq!(
            store.get_task_list(&key).unwrap().data.ack_level,
            task.task_id
        );
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 0);
        assert!(!registry.unload(key, old, 3).await.unwrap());
    }
    #[tokio::test]
    async fn shutdown_wakes_blocked_dispatcher() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key, 0, 1).await.unwrap();
        let poll = tokio::spawn(async move {
            handle
                .dispatcher
                .poll_decision(
                    cadence_proto::api::PollForDecisionTaskRequest::default(),
                    Duration::from_secs(10),
                )
                .await
        });
        tokio::task::yield_now().await;
        registry.shutdown(2).await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(2), poll)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(response.task_token.is_empty());
    }
    #[tokio::test]
    async fn maintenance_deadlines_are_per_owner_and_reset_after_update() {
        let (_dir, store, registry, first) = fixture();
        registry.get(first.clone(), 0, 1).await.unwrap();
        let mut second = first.clone();
        second.name = "later".into();
        registry.get(second.clone(), 0, 1).await.unwrap();
        {
            let state = registry.state.lock().unwrap();
            let owner = &state.owners[&(first.domain_id, first.name.clone(), first.task_type)];
            *owner.last_update.lock().unwrap() =
                tokio::time::Instant::now() - Duration::from_secs(120);
        }
        registry
            .flush(100, false, Some(Duration::from_secs(60)))
            .await
            .unwrap();
        assert_eq!(
            store.get_task_list(&first).unwrap().data.last_updated_nanos,
            100
        );
        assert_eq!(
            store
                .get_task_list(&second)
                .unwrap()
                .data
                .last_updated_nanos,
            1
        );
        registry
            .flush(101, false, Some(Duration::from_secs(60)))
            .await
            .unwrap();
        assert_eq!(
            store.get_task_list(&first).unwrap().data.last_updated_nanos,
            100
        );
        assert_eq!(
            store
                .get_task_list(&second)
                .unwrap()
                .data
                .last_updated_nanos,
            1
        );
        registry.shutdown(102).await.unwrap();
    }
    #[tokio::test]
    async fn replacement_can_load_before_old_cleanup_and_keeps_its_lease() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.append(vec![TaskData::default()], 1).await.unwrap();
        let task = take(&registry, &old).await;
        old.queue.complete(task.task_id).unwrap();
        let lookup = (key.domain_id, key.name.clone(), key.task_type);
        let owner = registry.state.lock().unwrap().owners[&lookup].clone();
        assert!(registry.detach(&lookup, &owner));
        owner.stop();
        let new = registry.get(key.clone(), 0, 2).await.unwrap();
        let added = new.append(vec![TaskData::default()], 2).await.unwrap();
        assert_eq!(added[0].task_id, 101);
        let finishing = owner.clone();
        assert!(
            owner_work(move |readers| finishing.finish(3, readers))
                .await
                .is_err(),
            "old final ack must be fenced"
        );
        assert!(!registry.detach(&lookup, &owner));
        assert!(Arc::ptr_eq(
            &new.queue,
            &registry.get(key.clone(), 0, 4).await.unwrap().queue
        ));
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
        assert_eq!(store.get_task_list(&key).unwrap().data.ack_level, 0);
        assert_eq!(
            store
                .get_tasks(&key, 0, 200, 10)
                .unwrap()
                .iter()
                .map(|task| task.task_id)
                .collect::<Vec<_>>(),
            vec![101]
        );
    }
    #[tokio::test]
    async fn idle_unload_retains_unacknowledged_tasks_for_recovery() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.append(vec![TaskData::default()], 1).await.unwrap();
        old.liveness.mark_alive(0);
        registry.sweep_idle(300_000_000_000, false).await.unwrap();
        assert!(old.queue.is_stopped());
        assert!(registry.state.lock().unwrap().owners.is_empty());
        assert_eq!(store.task_list_size(&key, 0).unwrap(), 1);
        let new = registry.get(key.clone(), 0, 300_000_000_001).await.unwrap();
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
        let recovered = take(&registry, &new).await;
        assert_eq!(recovered.task_id, 1);
    }
    #[tokio::test]
    async fn describe_lookup_does_not_refresh_liveness_but_append_does() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.liveness.mark_alive(0);
        let described = registry.get(key.clone(), 0, 2).await.unwrap();
        assert!(!described.liveness.is_alive(registry_now()));
        handle.append(vec![TaskData::default()], 2).await.unwrap();
        assert!(described.liveness.is_alive(registry_now()));
        registry.sweep_idle(registry_now(), false).await.unwrap();
        assert!(!handle.queue.is_stopped());
    }
    #[tokio::test]
    async fn idle_tick_runs_without_waiting_for_ack_deadline() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.liveness.mark_alive(0);
        {
            let state = registry.state.lock().unwrap();
            *state.owners[&(key.domain_id, key.name.clone(), key.task_type)]
                .next_idle_check
                .lock()
                .unwrap() = tokio::time::Instant::now();
        }
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let worker = registry.clone();
        let pump = tokio::spawn(async move {
            worker
                .maintain_until(Duration::from_secs(60), async {
                    let _ = shutdown.await;
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !handle.queue.is_stopped() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send(()).unwrap();
        pump.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn fatal_owner_wakeup_does_not_wait_for_ack_interval() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let worker = registry.clone();
        let pump = tokio::spawn(async move {
            worker
                .maintain_until(Duration::from_secs(3600), async {
                    let _ = shutdown.await;
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        handle.queue.stop();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !registry.state.lock().unwrap().owners.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send(()).unwrap();
        pump.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn registration_notification_reschedules_empty_controller() {
        let (_dir, _store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        let lookup = (key.domain_id, key.name.clone(), key.task_type);
        let owner = registry
            .state
            .lock()
            .unwrap()
            .owners
            .remove(&lookup)
            .unwrap();
        owner.liveness.mark_alive(0);
        *owner.next_idle_check.lock().unwrap() = tokio::time::Instant::now();
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let worker = registry.clone();
        let pump = tokio::spawn(async move {
            worker
                .maintain_until(Duration::from_secs(3600), async {
                    let _ = shutdown.await;
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Controlled registration after the pump has begun waiting with no owners.
        registry.state.lock().unwrap().owners.insert(lookup, owner);
        registry.changed.notify_waiters();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !handle.queue.is_stopped() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send(()).unwrap();
        pump.await.unwrap().unwrap();
    }
}
