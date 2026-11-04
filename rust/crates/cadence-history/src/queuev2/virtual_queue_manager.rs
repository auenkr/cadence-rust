//! Port of queuev2/virtual_queue_manager.go

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::deps::*;
use super::interface::Daemon;
use super::monitor::Monitor;
use super::pending_task_tracker::new_pending_task_tracker;
use super::queue_reader::QueueReader;
use super::queue_state::VirtualSliceState;
use super::virtual_queue::*;
use super::virtual_slice::{new_virtual_slice, VirtualSlice};

pub const ROOT_QUEUE_ID: i64 = 0;

#[derive(Clone)]
pub struct VirtualQueueManagerOptions {
    pub root_queue_options: VirtualQueueOptions,
    pub non_root_queue_options: VirtualQueueOptions,
    pub virtual_slice_force_append_interval: DurationFn,
}

#[async_trait]
pub trait VirtualQueueManager: Daemon {
    async fn virtual_queues(&self) -> HashMap<i64, Arc<dyn VirtualQueue>>;
    async fn get_or_create_virtual_queue(&self, queue_id: i64) -> Arc<dyn VirtualQueue>;
    async fn update_and_get_state(&self) -> HashMap<i64, Vec<VirtualSliceState>>;
    async fn add_new_virtual_slice_to_root_queue(&self, s: Arc<dyn VirtualSlice>);
    async fn get_min_read_level(&self) -> HistoryTaskKey;
}

pub struct VirtualQueueManagerImpl {
    processor: Arc<dyn Processor>,
    rescheduler: Arc<dyn Rescheduler>,
    logger: Arc<dyn Logger>,
    metrics_scope: Arc<dyn MetricsScope>,
    time_source: Arc<dyn TimeSource>,
    task_load_rate_limiter: Arc<dyn Limiter>,
    monitor: Arc<dyn Monitor>,
    queue_manager_options: VirtualQueueManagerOptions,

    status: AtomicI32,
    virtual_queues: RwLock<HashMap<i64, Arc<dyn VirtualQueue>>>,
    next_force_new_slice_time: StdMutex<Time>,
}

#[allow(clippy::too_many_arguments)]
pub fn new_virtual_queue_manager(
    processor: Arc<dyn Processor>,
    rescheduler: Arc<dyn Rescheduler>,
    task_initializer: TaskInitializer,
    queue_reader: Arc<dyn QueueReader>,
    logger: Arc<dyn Logger>,
    metrics_scope: Arc<dyn MetricsScope>,
    time_source: Arc<dyn TimeSource>,
    task_load_rate_limiter: Arc<dyn Limiter>,
    monitor: Arc<dyn Monitor>,
    queue_manager_options: VirtualQueueManagerOptions,
    virtual_queue_states: HashMap<i64, Vec<VirtualSliceState>>,
) -> Arc<VirtualQueueManagerImpl> {
    let mut virtual_queues: HashMap<i64, Arc<dyn VirtualQueue>> = HashMap::new();
    for (queue_id, states) in virtual_queue_states {
        let virtual_slices: SliceVec = states
            .into_iter()
            .map(|state| {
                new_virtual_slice(
                    state,
                    task_initializer.clone(),
                    queue_reader.clone(),
                    new_pending_task_tracker(),
                    logger.clone(),
                )
            })
            .collect();
        let opts = if queue_id == ROOT_QUEUE_ID {
            queue_manager_options.root_queue_options.clone()
        } else {
            queue_manager_options.non_root_queue_options.clone()
        };
        virtual_queues.insert(
            queue_id,
            new_virtual_queue(
                processor.clone(),
                rescheduler.clone(),
                logger.with_tags(&[("virtual-queue-id", queue_id.to_string())]),
                metrics_scope.clone(),
                time_source.clone(),
                task_load_rate_limiter.clone(),
                monitor.clone(),
                virtual_slices,
                opts,
            ),
        );
    }
    let create_state = CreateQueueCtx {
        task_initializer,
        queue_reader,
    };
    Arc::new(VirtualQueueManagerImpl {
        processor,
        rescheduler,
        logger,
        metrics_scope,
        time_source,
        task_load_rate_limiter,
        monitor,
        queue_manager_options,
        status: AtomicI32::new(DAEMON_STATUS_INITIALIZED),
        virtual_queues: RwLock::new(virtual_queues),
        next_force_new_slice_time: StdMutex::new(zero_time()),
    })
    .with_ctx(create_state)
}

/// Kept for parity with Go's `createVirtualQueueFn` closure (which captures only the
/// processor etc.; the task initializer / reader are not needed to create empty queues).
struct CreateQueueCtx {
    #[allow(dead_code)]
    task_initializer: TaskInitializer,
    #[allow(dead_code)]
    queue_reader: Arc<dyn QueueReader>,
}

trait WithCtx {
    fn with_ctx(self, c: CreateQueueCtx) -> Self;
}
impl WithCtx for Arc<VirtualQueueManagerImpl> {
    fn with_ctx(self, _c: CreateQueueCtx) -> Self {
        self
    }
}

impl VirtualQueueManagerImpl {
    /// Go `createVirtualQueueFn`.
    fn create_virtual_queue(&self, queue_id: i64, slices: SliceVec) -> Arc<dyn VirtualQueue> {
        let opts = if queue_id == ROOT_QUEUE_ID {
            self.queue_manager_options.root_queue_options.clone()
        } else {
            self.queue_manager_options.non_root_queue_options.clone()
        };
        new_virtual_queue(
            self.processor.clone(),
            self.rescheduler.clone(),
            self.logger
                .with_tags(&[("virtual-queue-id", queue_id.to_string())]),
            self.metrics_scope.clone(),
            self.time_source.clone(),
            self.task_load_rate_limiter.clone(),
            self.monitor.clone(),
            slices,
            opts,
        )
    }

    async fn append_or_merge_slice(&self, vq: &Arc<dyn VirtualQueue>, s: Arc<dyn VirtualSlice>) {
        let now = self.time_source.now();
        let new_state = s.get_state();
        let next_force = *self.next_force_new_slice_time.lock().unwrap();
        if now > next_force {
            self.logger.debug(&format!(
                "append new slice to virtual queue currentTime={now} nextForceNewSliceTime={next_force} inclusiveMinTaskKey={:?} exclusiveMaxTaskKey={:?}",
                new_state.range.inclusive_min_task_key, new_state.range.exclusive_max_task_key
            ));
            vq.append_slices(vec![s]).await;
            *self.next_force_new_slice_time.lock().unwrap() = now
                + chrono::Duration::from_std((self
                    .queue_manager_options
                    .virtual_slice_force_append_interval)())
                .unwrap_or(chrono::Duration::zero());
            return;
        }
        self.logger.debug(&format!(
            "merge slice to virtual queue currentTime={now} nextForceNewSliceTime={next_force} inclusiveMinTaskKey={:?} exclusiveMaxTaskKey={:?}",
            new_state.range.inclusive_min_task_key, new_state.range.exclusive_max_task_key
        ));
        vq.merge_with_last_slice(s).await;
    }
}

#[async_trait]
impl Daemon for VirtualQueueManagerImpl {
    async fn start(&self) {
        if self
            .status
            .compare_exchange(
                DAEMON_STATUS_INITIALIZED,
                DAEMON_STATUS_STARTED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        let queues = self.virtual_queues.read().await;
        for vq in queues.values() {
            vq.start().await;
        }
    }

    async fn stop(&self) {
        if self
            .status
            .compare_exchange(
                DAEMON_STATUS_STARTED,
                DAEMON_STATUS_STOPPED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        let queues = self.virtual_queues.read().await;
        for vq in queues.values() {
            vq.stop().await;
        }
    }
}

#[async_trait]
impl VirtualQueueManager for VirtualQueueManagerImpl {
    async fn virtual_queues(&self) -> HashMap<i64, Arc<dyn VirtualQueue>> {
        self.virtual_queues.read().await.clone()
    }

    async fn get_or_create_virtual_queue(&self, queue_id: i64) -> Arc<dyn VirtualQueue> {
        if let Some(vq) = self.virtual_queues.read().await.get(&queue_id) {
            return vq.clone();
        }
        let mut queues = self.virtual_queues.write().await;
        if let Some(vq) = queues.get(&queue_id) {
            return vq.clone();
        }
        let vq = self.create_virtual_queue(queue_id, Vec::new());
        queues.insert(queue_id, vq.clone());
        vq.start().await;
        vq
    }

    async fn update_and_get_state(&self) -> HashMap<i64, Vec<VirtualSliceState>> {
        let mut queues = self.virtual_queues.write().await;
        let mut states = HashMap::new();
        let keys: Vec<i64> = queues.keys().copied().collect();
        for key in keys {
            let vq = queues.get(&key).unwrap().clone();
            let state = vq.update_and_get_state().await;
            if !state.is_empty() {
                states.insert(key, state);
            } else if key != ROOT_QUEUE_ID {
                vq.stop().await;
                queues.remove(&key);
            }
        }
        states
    }

    async fn add_new_virtual_slice_to_root_queue(&self, s: Arc<dyn VirtualSlice>) {
        if let Some(vq) = self.virtual_queues.read().await.get(&ROOT_QUEUE_ID).cloned() {
            self.append_or_merge_slice(&vq, s).await;
            return;
        }
        let mut queues = self.virtual_queues.write().await;
        if let Some(vq) = queues.get(&ROOT_QUEUE_ID).cloned() {
            self.append_or_merge_slice(&vq, s).await;
            return;
        }
        let vq = self.create_virtual_queue(ROOT_QUEUE_ID, vec![s]);
        queues.insert(ROOT_QUEUE_ID, vq.clone());
        vq.start().await;
    }

    async fn get_min_read_level(&self) -> HistoryTaskKey {
        let queues = self.virtual_queues.read().await;
        let mut min_read_level = HistoryTaskKey::maximum();
        for vq in queues.values() {
            vq.iterate_slices(&mut |s| {
                let read_level = s.get_read_level();
                if read_level.compare(&min_read_level) < 0 {
                    min_read_level = read_level;
                }
            })
            .await;
        }
        min_read_level
    }
}
