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
}
#[derive(Clone)]
pub struct QueueHandle {
    pub queue: Arc<DurableTaskQueue>,
    pub dispatcher: TaskDispatcher,
    pub pollers: Arc<Mutex<cadence_common::poller::Manager>>,
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
                    owner.dispatcher.notify_new_tasks();
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
                    let owner = Arc::new(Owner {
                        dispatcher: TaskDispatcher::new(
                            queue.clone(),
                            registry.history.clone(),
                            domain,
                        ),
                        queue,
                        pollers: Arc::new(Mutex::new(cadence_common::poller::Manager::default())),
                        last_update: Mutex::new(tokio::time::Instant::now()),
                    });
                    state.owners.insert(lookup, owner.clone());
                    owner
                }
            };
            Ok(QueueHandle {
                queue: owner.queue.clone(),
                dispatcher: owner.dispatcher.clone(),
                pollers: owner.pollers.clone(),
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
            let result = owner.queue.persist_ack(now);
            owner.queue.stop();
            owner.dispatcher.notify_new_tasks();
            state.owners.remove(&lookup);
            result.map_err(|error| Status::unavailable(error.to_string()))?;
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
            let deadline = self
                .state
                .lock()
                .expect("Matching registry lock")
                .owners
                .values()
                .map(|owner| *owner.last_update.lock().expect("Matching ack timer lock") + interval)
                .min()
                .unwrap_or_else(|| tokio::time::Instant::now() + interval);
            tokio::select! {
                biased;
                _ = &mut shutdown => return self.shutdown(registry_now()).await,
                _ = tokio::time::sleep_until(deadline) => {
                    if let Err(error) = self.flush(registry_now(), false, Some(interval)).await {
                        eprintln!("Matching acknowledgement update failed: {error}");
                    }
                }
            }
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
            let mut state = registry.state.lock().expect("Matching registry lock");
            if state.stopped {
                return Ok(());
            }
            let mut failure = None;
            for owner in state.owners.values() {
                if due.is_some_and(|interval| {
                    owner
                        .last_update
                        .lock()
                        .expect("Matching ack timer lock")
                        .elapsed()
                        < interval
                }) {
                    continue;
                }
                if let Err(error) = owner.queue.persist_ack(now) {
                    failure.get_or_insert_with(|| Status::unavailable(error.to_string()));
                }
                *owner.last_update.lock().expect("Matching ack timer lock") =
                    tokio::time::Instant::now();
                if stop {
                    owner.queue.stop();
                }
                owner.dispatcher.notify_new_tasks();
            }
            // Go's fatal reader/writer watcher unregisters stopped managers.
            // Keep healthy queues alive when another owner loses its lease.
            state.owners.retain(|_, owner| !owner.queue.is_stopped());
            if stop {
                state.stopped = true;
                state.owners.clear();
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
        assert!(handle.append(vec![TaskData::default()], 2).await.is_err());
    }
    #[tokio::test]
    async fn stopped_writer_is_replaced_without_waiting_for_ack_timer() {
        let (_dir, store, registry, key) = fixture();
        let old = registry.get(key.clone(), 0, 1).await.unwrap();
        old.queue.stop();
        let new = registry.get(key.clone(), 0, 2).await.unwrap();
        assert!(!Arc::ptr_eq(&old.queue, &new.queue));
        assert_eq!(store.get_task_list(&key).unwrap().range_id, 2);
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
        let error = tokio::time::timeout(Duration::from_secs(2), poll)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
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
}
