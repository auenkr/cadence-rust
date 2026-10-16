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
    sync::{Arc, Mutex},
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
struct Owner {
    queue: Arc<DurableTaskQueue>,
    dispatcher: TaskDispatcher,
    pollers: Arc<Mutex<cadence_common::poller::Manager>>,
    last_update: Mutex<tokio::time::Instant>,
    next_idle_check: Mutex<tokio::time::Instant>,
    liveness: Arc<cadence_common::liveness::Liveness>,
}
impl Owner {
    fn stop(&self) {
        self.queue.stop();
        self.dispatcher.notify_new_tasks();
    }
    fn finish(&self, now: i64) -> Result<(), Status> {
        self.stop();
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
}
#[derive(Clone)]
pub struct QueueHandle {
    pub queue: Arc<DurableTaskQueue>,
    pub dispatcher: TaskDispatcher,
    pub pollers: Arc<Mutex<cadence_common::poller::Manager>>,
    liveness: Arc<cadence_common::liveness::Liveness>,
}
impl QueueHandle {
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
        }
    }
    /// task_type is the persistence enum (decision 0, activity 1).
    pub async fn get(&self, key: TaskListKey, kind: i16, now: i64) -> Result<QueueHandle, Status> {
        if !(0..=1).contains(&key.task_type) {
            return Err(Status::invalid_argument("Unknown persistence task type."));
        }
        let registry = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = registry.state.lock().expect("Matching registry lock");
            if state.stopped {
                return Err(Status::unavailable("Matching registry stopped."));
            }
            let lookup = (key.domain_id, key.name.clone(), key.task_type);
            if state
                .owners
                .get(&lookup)
                .is_some_and(|owner| owner.queue.is_stopped())
            {
                if let Some(owner) = state.owners.remove(&lookup) {
                    drop(state);
                    if let Err(error) = owner.finish(now) {
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
                    let domain = key.domain_id.to_string();
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
                    let wake = registry.changed.clone();
                    queue.set_stop_waker(Arc::new(move || wake.notify_waiters()));
                    let liveness = Arc::new(cadence_common::liveness::Liveness::new(
                        registry_now(),
                        300_000_000_000,
                    ));
                    let owner = Arc::new(Owner {
                        dispatcher: TaskDispatcher::new(
                            queue.clone(),
                            registry.history.clone(),
                            domain,
                        )
                        .with_liveness(liveness.clone()),
                        queue,
                        pollers: Arc::new(Mutex::new(cadence_common::poller::Manager::default())),
                        last_update: Mutex::new(tokio::time::Instant::now()),
                        next_idle_check: Mutex::new(
                            tokio::time::Instant::now() + std::time::Duration::from_secs(150),
                        ),
                        liveness,
                    });
                    state.owners.insert(lookup, owner.clone());
                    registry.changed.notify_waiters();
                    owner
                }
            };
            Ok(QueueHandle {
                queue: owner.queue.clone(),
                dispatcher: owner.dispatcher.clone(),
                pollers: owner.pollers.clone(),
                liveness: owner.liveness.clone(),
            })
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }
    /// An old manager's teardown must never unregister its replacement.
    pub async fn unload(
        &self,
        key: TaskListKey,
        handle: QueueHandle,
        now: i64,
    ) -> Result<bool, Status> {
        let registry = self.clone();
        tokio::task::spawn_blocking(move || {
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
            owner.finish(now)?;
            Ok(true)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
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
        tokio::task::spawn_blocking(move || {
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
                    if let Err(error) = owner.finish(now) {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(()), Err)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }
    /// Check idleness using the caller's wall-clock time source. Scheduled
    /// ticks use due_only; an explicit controller scan may check every owner.
    pub async fn sweep_idle(&self, now: i64, due_only: bool) -> Result<(), Status> {
        let registry = self.clone();
        tokio::task::spawn_blocking(move || {
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
                    if let Err(error) = owner.finish(now) {
                        failure.get_or_insert(error);
                    }
                }
            }
            failure.map_or(Ok(()), Err)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
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
        tokio::task::spawn_blocking(move || {
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
                    if let Err(error) = owner.finish(now) {
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
                        if let Err(error) = owner.finish(now) {
                            failure.get_or_insert(error);
                        }
                    }
                }
                owner.dispatcher.notify_new_tasks();
            }
            failure.map_or(Ok(()), Err)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_persistence::matching_blob::BlobEncoding;
    use std::time::Duration;
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
    #[tokio::test]
    async fn shutdown_persists_ack_and_stops_previously_returned_handles() {
        let (_dir, store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.queue.append(&[TaskData::default()], 1).unwrap();
        let task = handle
            .queue
            .read_batch(
                NonZeroU32::new(100).unwrap(),
                NonZeroU32::new(1).unwrap(),
                1,
            )
            .unwrap()
            .tasks
            .remove(0);
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
        let task = other
            .queue
            .read_batch(
                NonZeroU32::new(100).unwrap(),
                NonZeroU32::new(1).unwrap(),
                1,
            )
            .unwrap()
            .tasks
            .remove(0);
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
        let recovered = new
            .queue
            .read_batch(
                NonZeroU32::new(100).unwrap(),
                NonZeroU32::new(1).unwrap(),
                4,
            )
            .unwrap();
        assert_eq!(recovered.tasks.len(), 1);
        assert_eq!(recovered.tasks[0].task_id, 1);
    }
    #[tokio::test]
    async fn maintenance_persists_live_ack_and_shutdown_stops_handles() {
        let (_dir, store, registry, key) = fixture();
        let handle = registry.get(key.clone(), 0, 1).await.unwrap();
        handle.append(vec![TaskData::default()], 1).await.unwrap();
        let task = handle
            .queue
            .read_batch(
                NonZeroU32::new(100).unwrap(),
                NonZeroU32::new(1).unwrap(),
                1,
            )
            .unwrap()
            .tasks
            .remove(0);
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
        let task = old
            .queue
            .read_batch(
                NonZeroU32::new(100).unwrap(),
                NonZeroU32::new(1).unwrap(),
                1,
            )
            .unwrap()
            .tasks
            .remove(0);
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
        let task = old
            .queue
            .read_batch(
                NonZeroU32::new(100).unwrap(),
                NonZeroU32::new(1).unwrap(),
                1,
            )
            .unwrap()
            .tasks
            .remove(0);
        old.queue.complete(task.task_id).unwrap();
        let lookup = (key.domain_id, key.name.clone(), key.task_type);
        let owner = registry.state.lock().unwrap().owners[&lookup].clone();
        assert!(registry.detach(&lookup, &owner));
        owner.stop();
        let new = registry.get(key.clone(), 0, 2).await.unwrap();
        let added = new.append(vec![TaskData::default()], 2).await.unwrap();
        assert_eq!(added[0].task_id, 101);
        assert!(owner.finish(3).is_err(), "old final ack must be fenced");
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
        let recovered = new
            .queue
            .read_batch(
                NonZeroU32::new(100).unwrap(),
                NonZeroU32::new(1).unwrap(),
                300_000_000_001,
            )
            .unwrap();
        assert_eq!(recovered.tasks[0].task_id, 1);
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
