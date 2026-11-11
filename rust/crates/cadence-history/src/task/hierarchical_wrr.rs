//! Port of `go/common/task/{ttl_channel,iwrr_schedule,iwrr_node,
//! hierarchical_weighted_round_robin_task_pool,hierarchical_weighted_round_robin_task_scheduler}.go`.
//!
//! Metrics scopes of the Go scheduler are not ported (same as the flat WRR scheduler in
//! `parallel_processor.rs`). The Go `scheduler_options.go` only selects between the FIFO and flat
//! WRR schedulers (neither is used by history), so it is not ported here.

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::sync::{mpsc, Notify, RwLock as AsyncRwLock};
use tokio_util::sync::CancellationToken;

use super::deps::{Daemon, TaskError, Time, TimeSource};
use super::parallel_processor::{CommonProcessor, TaskRef};

const STATUS_INITIALIZED: i32 = 0;
const STATUS_STARTED: i32 = 1;
const STATUS_STOPPED: i32 = 2;
const SHUTDOWN_WAIT: Duration = Duration::from_secs(60);
/// Go `defaultIdleChannelTTLInSeconds`.
pub const DEFAULT_IDLE_CHANNEL_TTL_IN_SECONDS: i64 = 3600;

/// Errors of the hierarchical pool / scheduler.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HierarchicalTaskError {
    /// Go `errWeightedKeyWeightMustBeGreaterThanZero`.
    #[error("weight must be greater than 0")]
    WeightMustBeGreaterThanZero,
    /// Go `ctx.Err()` of the pool context.
    #[error("context canceled")]
    Canceled,
    /// Go `ErrTaskSchedulerClosed`.
    #[error("task scheduler has already shutdown")]
    SchedulerClosed,
}

// ---------------------------------------------------------------------------
// TTLChannel
// ---------------------------------------------------------------------------

/// Go `TTLChannel`: a bounded channel that expires when idle.
pub struct TtlChannel<V> {
    tx: mpsc::Sender<V>,
    rx: Mutex<mpsc::Receiver<V>>,
    last_write_secs: AtomicI64,
    ref_count: AtomicI32,
}

impl<V> TtlChannel<V> {
    pub fn new(buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size.max(1));
        Self { tx, rx: Mutex::new(rx), last_write_secs: AtomicI64::new(0), ref_count: AtomicI32::new(0) }
    }
    pub fn inc_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
    }
    pub fn dec_ref(&self) {
        self.ref_count.fetch_sub(1, Ordering::SeqCst);
    }
    pub fn ref_count(&self) -> i32 {
        self.ref_count.load(Ordering::SeqCst)
    }
    pub fn last_write_time(&self) -> Time {
        Time(self.last_write_secs.load(Ordering::SeqCst).saturating_mul(1_000_000_000))
    }
    pub fn update_last_write_time(&self, now: Time) {
        self.last_write_secs.store(now.0.div_euclid(1_000_000_000), Ordering::SeqCst);
    }
    pub fn sender(&self) -> &mpsc::Sender<V> {
        &self.tx
    }
    /// Non-blocking receive (Go `select { case <-c.Chan(): default: }`).
    pub fn try_recv(&self) -> Option<V> {
        self.rx.lock().unwrap().try_recv().ok()
    }
    pub fn len(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn cap(&self) -> usize {
        self.tx.max_capacity()
    }
    pub fn should_cleanup(&self, now: Time, ttl: Duration) -> bool {
        now.0 - self.last_write_time().0 > ttl.as_nanos() as i64 && self.len() == 0 && self.ref_count() == 0
    }
}

// ---------------------------------------------------------------------------
// IWRR schedule
// ---------------------------------------------------------------------------

/// Go `iwrrSchedule`: snapshot of weighted items sorted ascending by weight.
pub struct IwrrSchedule<V> {
    items: Vec<(V, i64)>,
    max_weight: i64,
    total_len: i64,
}

impl<V: Clone> IwrrSchedule<V> {
    /// Go `newIWRRSchedule`: items with weight <= 0 are ignored.
    pub fn new(items: impl IntoIterator<Item = (V, i64)>) -> Self {
        let mut items: Vec<(V, i64)> = items.into_iter().filter(|(_, w)| *w > 0).collect();
        if items.is_empty() {
            return Self { items, max_weight: 0, total_len: 0 };
        }
        let total_len = items.iter().map(|(_, w)| *w).sum();
        items.sort_by_key(|(_, w)| *w);
        let max_weight = items.last().map(|(_, w)| *w).unwrap_or(0);
        Self { items, max_weight, total_len }
    }

    pub fn iter(self: &Arc<Self>) -> IwrrIterator<V> {
        if self.items.is_empty() {
            return IwrrIterator { schedule: self.clone(), round: 0, index: 0 };
        }
        IwrrIterator { schedule: self.clone(), round: self.max_weight - 1, index: self.items.len() as i64 - 1 }
    }

    /// Total virtual length (sum of weights).
    pub fn len(&self) -> i64 {
        self.total_len
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Go `iwrrIterator`.
pub struct IwrrIterator<V> {
    schedule: Arc<IwrrSchedule<V>>,
    round: i64,
    index: i64,
}

impl<V: Clone> IwrrIterator<V> {
    pub fn try_next(&mut self) -> Option<V> {
        let items = &self.schedule.items;
        if items.is_empty() {
            return None;
        }
        while self.round >= 0 {
            if self.index >= 0 && items[self.index as usize].1 > self.round {
                let item = items[self.index as usize].0.clone();
                self.index -= 1;
                return Some(item);
            }
            self.round -= 1;
            self.index = items.len() as i64 - 1;
        }
        None
    }
}

// ---------------------------------------------------------------------------
// IWRR node
// ---------------------------------------------------------------------------

/// Go `WeightedKey`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeightedKey<K> {
    pub key: K,
    pub weight: i64,
}

type Children<K, V> = HashMap<K, (Arc<IwrrNode<K, V>>, i64)>;

/// Go `iwrrNode`: node of the hierarchical IWRR tree.
pub struct IwrrNode<K, V> {
    children: RwLock<Children<K, V>>,
    c: TtlChannel<V>,
    children_item_count: AtomicI64,
    drain_self_first: AtomicBool,
    schedule: Mutex<Arc<IwrrSchedule<Arc<IwrrNode<K, V>>>>>,
    iter: Mutex<IwrrIterator<Arc<IwrrNode<K, V>>>>,
}

/// Nodes visited by `acquire_path` (root first, leaf last). Child channel refs are released on drop,
/// matching the Go `defer container.item.c.DecRef()`.
pub struct PathGuard<K, V> {
    nodes: Vec<Arc<IwrrNode<K, V>>>,
}

impl<K, V> PathGuard<K, V> {
    pub fn leaf(&self) -> &Arc<IwrrNode<K, V>> {
        self.nodes.last().expect("path guard always holds the root")
    }

    /// Go post-callback bookkeeping: each ancestor drains children first and accounts the delta.
    pub fn finish(&self, delta: i64) {
        for i in (0..self.nodes.len() - 1).rev() {
            self.nodes[i].drain_self_first.store(true, Ordering::SeqCst);
            self.nodes[i].children_item_count.fetch_add(delta, Ordering::SeqCst);
        }
    }
}

impl<K, V> Drop for PathGuard<K, V> {
    fn drop(&mut self) {
        for n in self.nodes.iter().skip(1) {
            n.c.dec_ref();
        }
    }
}

impl<K: Hash + Eq + Clone, V> IwrrNode<K, V> {
    /// Go `newiwrrNode`.
    pub fn new(buffer_size: usize) -> Arc<Self> {
        let schedule = Arc::new(IwrrSchedule::new(Vec::new()));
        let iter = schedule.iter();
        Arc::new(Self {
            children: RwLock::new(HashMap::new()),
            c: TtlChannel::new(buffer_size),
            children_item_count: AtomicI64::new(0),
            drain_self_first: AtomicBool::new(false),
            schedule: Mutex::new(schedule),
            iter: Mutex::new(iter),
        })
    }

    pub fn channel(&self) -> &TtlChannel<V> {
        &self.c
    }

    pub fn children_item_count(&self) -> i64 {
        self.children_item_count.load(Ordering::SeqCst)
    }

    pub fn drain_self_first(&self) -> bool {
        self.drain_self_first.load(Ordering::SeqCst)
    }

    pub fn child_count(&self) -> usize {
        self.children.read().unwrap().len()
    }

    /// Navigates / creates the nodes along `path` (Go `executeAtPath` without the callback). The
    /// leaf's `drainSelfFirst` is cleared as in Go; the caller runs its operation on `leaf().channel()`
    /// and then calls `finish(delta)`.
    pub fn acquire_path(self: &Arc<Self>, path: &[WeightedKey<K>], buffer_size: usize) -> PathGuard<K, V> {
        let mut guard = PathGuard { nodes: vec![self.clone()] };
        let mut cur = self.clone();
        for wk in path {
            let fast = {
                let ch = cur.children.read().unwrap();
                match ch.get(&wk.key) {
                    Some((n, w)) if *w == wk.weight => {
                        n.c.inc_ref();
                        Some(n.clone())
                    }
                    _ => None,
                }
            };
            let child = match fast {
                Some(c) => c,
                None => {
                    let mut ch = cur.children.write().unwrap();
                    let existing = ch.get(&wk.key).map(|(n, w)| (n.clone(), *w));
                    let mut needs_update = false;
                    let node = match existing {
                        None => {
                            let n = Self::new(buffer_size);
                            ch.insert(wk.key.clone(), (n.clone(), wk.weight));
                            needs_update = true;
                            n
                        }
                        Some((n, w)) => {
                            if w != wk.weight {
                                ch.insert(wk.key.clone(), (n.clone(), wk.weight));
                                needs_update = true;
                            }
                            n
                        }
                    };
                    if needs_update {
                        cur.update_schedule_locked(&ch);
                    }
                    node.c.inc_ref();
                    node
                }
            };
            guard.nodes.push(child.clone());
            cur = child;
        }
        guard.leaf().drain_self_first.store(false, Ordering::SeqCst);
        guard
    }

    /// Go `executeAtPath` with a synchronous callback returning the item count delta.
    pub fn execute_at_path(
        self: &Arc<Self>,
        path: &[WeightedKey<K>],
        buffer_size: usize,
        callback: impl FnOnce(&TtlChannel<V>) -> i64,
    ) -> i64 {
        let guard = self.acquire_path(path, buffer_size);
        let delta = callback(&guard.leaf().c);
        guard.finish(delta);
        delta
    }

    /// Go `updateScheduleLocked` (caller holds the children write lock).
    fn update_schedule_locked(&self, children: &Children<K, V>) {
        let schedule = Arc::new(IwrrSchedule::new(children.values().map(|(n, w)| (n.clone(), *w))));
        *self.schedule.lock().unwrap() = schedule;
    }

    /// Go `tryGetNextItem`. Single consumer only.
    pub fn try_get_next_item(&self) -> Option<V> {
        let drain_self = self.drain_self_first.load(Ordering::SeqCst);
        if drain_self {
            if let Some(item) = self.c.try_recv() {
                return Some(item);
            }
        }
        if let Some(item) = self.try_children() {
            return Some(item);
        }
        if !drain_self {
            if let Some(item) = self.c.try_recv() {
                return Some(item);
            }
        }
        None
    }

    fn try_children(&self) -> Option<V> {
        while self.children_item_count.load(Ordering::SeqCst) > 0 {
            let next = self.iter.lock().unwrap().try_next();
            match next {
                None => {
                    if self.children_item_count.load(Ordering::SeqCst) == 0 {
                        return None;
                    }
                    let schedule = self.schedule.lock().unwrap().clone();
                    *self.iter.lock().unwrap() = schedule.iter();
                }
                Some(child) => {
                    if let Some(item) = child.try_get_next_item() {
                        self.children_item_count.fetch_sub(1, Ordering::SeqCst);
                        return Some(item);
                    }
                }
            }
        }
        None
    }

    /// Go `cleanup`: removes idle descendants; returns true if this node should be removed by its parent.
    pub fn cleanup(&self, now: Time, ttl: Duration) -> bool {
        let snapshot: HashMap<K, Arc<Self>> = {
            let ch = self.children.read().unwrap();
            if ch.is_empty() {
                return self.c.should_cleanup(now, ttl);
            }
            ch.iter().map(|(k, (n, _))| (k.clone(), n.clone())).collect()
        };

        let keys_to_remove: Vec<K> =
            snapshot.iter().filter(|(_, child)| child.cleanup(now, ttl)).map(|(k, _)| k.clone()).collect();
        if keys_to_remove.is_empty() {
            return false;
        }

        let mut ch = self.children.write().unwrap();
        let mut needs_update = false;
        for key in keys_to_remove {
            let node = match ch.get(&key) {
                Some((n, _)) if Arc::ptr_eq(n, &snapshot[&key]) => n.clone(),
                _ => continue,
            };
            if !node.c.should_cleanup(now, ttl) {
                continue;
            }
            let has_no_children = node.children.read().unwrap().is_empty();
            if has_no_children {
                ch.remove(&key);
                needs_update = true;
            }
        }
        if needs_update {
            self.update_schedule_locked(&ch);
        }
        if ch.is_empty() {
            return self.c.should_cleanup(now, ttl);
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

/// Go `HierarchicalWeightedRoundRobinTaskPoolOptions`.
pub struct HierarchicalWeightedRoundRobinTaskPoolOptions<K> {
    pub buffer_size: usize,
    pub task_to_weighted_keys_fn: Arc<dyn Fn(&TaskRef) -> Vec<WeightedKey<K>> + Send + Sync>,
}

struct PoolInner<K> {
    status: AtomicI32,
    root: Arc<IwrrNode<K, TaskRef>>,
    ctx: CancellationToken,
    options: HierarchicalWeightedRoundRobinTaskPoolOptions<K>,
    time_source: Arc<dyn TimeSource>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Go `hierarchicalWeightedRoundRobinTaskPoolImpl`.
pub struct HierarchicalWeightedRoundRobinTaskPool<K> {
    inner: Arc<PoolInner<K>>,
}

fn verify_weighted_keys<K>(keys: &[WeightedKey<K>]) -> Result<(), HierarchicalTaskError> {
    if keys.iter().any(|k| k.weight <= 0) {
        return Err(HierarchicalTaskError::WeightMustBeGreaterThanZero);
    }
    Ok(())
}

struct RootRef<K: Hash + Eq + Clone>(Arc<IwrrNode<K, TaskRef>>);
impl<K: Hash + Eq + Clone> RootRef<K> {
    fn new(root: &Arc<IwrrNode<K, TaskRef>>) -> Self {
        root.c.inc_ref();
        Self(root.clone())
    }
}
impl<K: Hash + Eq + Clone> Drop for RootRef<K> {
    fn drop(&mut self) {
        self.0.c.dec_ref();
    }
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> HierarchicalWeightedRoundRobinTaskPool<K> {
    pub fn new(time_source: Arc<dyn TimeSource>, options: HierarchicalWeightedRoundRobinTaskPoolOptions<K>) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                status: AtomicI32::new(STATUS_INITIALIZED),
                root: IwrrNode::new(options.buffer_size),
                ctx: CancellationToken::new(),
                options,
                time_source,
                handle: Mutex::new(None),
            }),
        }
    }

    pub fn start(&self) {
        let i = &self.inner;
        if i.status.compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        let me = i.clone();
        *i.handle.lock().unwrap() = Some(tokio::spawn(async move { me.cleanup_loop().await }));
        tracing::info!("Hierarchical weighted round robin task pool started.");
    }

    pub async fn stop(&self) {
        let i = &self.inner;
        if i.status.compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        i.ctx.cancel();
        let handle = i.handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.await;
        }
        tracing::info!("Hierarchical weighted round robin task pool stopped.");
    }

    /// Go `doCleanup`.
    pub fn do_cleanup(&self, now: Time, ttl: Duration) {
        self.inner.root.cleanup(now, ttl);
    }

    /// Go `Enqueue`: blocks while the target channel is full.
    pub async fn enqueue(&self, task: TaskRef) -> Result<(), HierarchicalTaskError> {
        let i = &self.inner;
        let keys = (i.options.task_to_weighted_keys_fn)(&task);
        verify_weighted_keys(&keys)?;
        let _root_ref = RootRef::new(&i.root);
        let guard = i.root.acquire_path(&keys, i.options.buffer_size);
        let leaf = guard.leaf();
        let permit = tokio::select! {
            p = leaf.c.sender().reserve() => p.map_err(|_| HierarchicalTaskError::Canceled),
            _ = i.ctx.cancelled() => Err(HierarchicalTaskError::Canceled),
        };
        match permit {
            Ok(p) => {
                p.send(task);
                leaf.c.update_last_write_time(i.time_source.now());
                guard.finish(1);
                Ok(())
            }
            Err(e) => {
                guard.finish(0);
                Err(e)
            }
        }
    }

    /// Go `TryEnqueue`: Ok(false) if the target channel is full.
    pub fn try_enqueue(&self, task: TaskRef) -> Result<bool, HierarchicalTaskError> {
        let i = &self.inner;
        let keys = (i.options.task_to_weighted_keys_fn)(&task);
        verify_weighted_keys(&keys)?;
        let _root_ref = RootRef::new(&i.root);
        let mut err = None;
        let delta = i.root.execute_at_path(&keys, i.options.buffer_size, |c| {
            if i.ctx.is_cancelled() {
                err = Some(HierarchicalTaskError::Canceled);
                return 0;
            }
            match c.sender().try_send(task) {
                Ok(()) => {
                    c.update_last_write_time(i.time_source.now());
                    1
                }
                Err(_) => 0,
            }
        });
        match err {
            Some(e) => Err(e),
            None => Ok(delta > 0),
        }
    }

    /// Go `TryDequeue` (single consumer).
    pub fn try_dequeue(&self) -> Option<TaskRef> {
        self.inner.root.try_get_next_item()
    }

    pub fn root(&self) -> &Arc<IwrrNode<K, TaskRef>> {
        &self.inner.root
    }
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> PoolInner<K> {
    async fn cleanup_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_secs((DEFAULT_IDLE_CHANNEL_TTL_IN_SECONDS / 2) as u64));
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let now = self.time_source.now();
                    self.root.cleanup(now, Duration::from_secs(DEFAULT_IDLE_CHANNEL_TTL_IN_SECONDS as u64));
                }
                _ = self.ctx.cancelled() => return,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

struct SchedInner<K> {
    status: AtomicI32,
    pool: HierarchicalWeightedRoundRobinTaskPool<K>,
    cancel: CancellationToken,
    notify: Notify,
    lifecycle: AsyncRwLock<()>,
    processor: Arc<dyn CommonProcessor>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Go `hierarchicalWeightedRoundRobinTaskSchedulerImpl`.
pub struct HierarchicalWeightedRoundRobinTaskScheduler<K> {
    inner: Arc<SchedInner<K>>,
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> HierarchicalWeightedRoundRobinTaskScheduler<K> {
    pub fn new(
        time_source: Arc<dyn TimeSource>,
        processor: Arc<dyn CommonProcessor>,
        options: HierarchicalWeightedRoundRobinTaskPoolOptions<K>,
    ) -> Self {
        Self {
            inner: Arc::new(SchedInner {
                status: AtomicI32::new(STATUS_INITIALIZED),
                pool: HierarchicalWeightedRoundRobinTaskPool::new(time_source, options),
                cancel: CancellationToken::new(),
                notify: Notify::new(),
                lifecycle: AsyncRwLock::new(()),
                processor,
                handle: Mutex::new(None),
            }),
        }
    }

    fn is_stopped(&self) -> bool {
        self.inner.status.load(Ordering::SeqCst) == STATUS_STOPPED
    }

    pub fn pool(&self) -> &HierarchicalWeightedRoundRobinTaskPool<K> {
        &self.inner.pool
    }

    /// Go `Submit`.
    pub async fn submit(&self, task: TaskRef) -> Result<(), HierarchicalTaskError> {
        let _g = self.inner.lifecycle.read().await;
        if self.is_stopped() {
            return Err(HierarchicalTaskError::SchedulerClosed);
        }
        self.inner.pool.enqueue(task).await?;
        self.inner.notify.notify_one();
        Ok(())
    }

    /// Go `TrySubmit`.
    pub async fn try_submit(&self, task: TaskRef) -> Result<bool, HierarchicalTaskError> {
        let _g = self.inner.lifecycle.read().await;
        if self.is_stopped() {
            return Err(HierarchicalTaskError::SchedulerClosed);
        }
        let ok = self.inner.pool.try_enqueue(task)?;
        if ok {
            self.inner.notify.notify_one();
        }
        Ok(ok)
    }
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static> SchedInner<K> {
    fn is_stopped(&self) -> bool {
        self.status.load(Ordering::SeqCst) == STATUS_STOPPED
    }

    async fn dispatcher(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = self.notify.notified() => self.dispatch_tasks().await,
                _ = self.cancel.cancelled() => return,
            }
        }
    }

    async fn dispatch_tasks(&self) {
        loop {
            if self.is_stopped() {
                return;
            }
            let Some(task) = self.pool.try_dequeue() else { return };
            if let Err(e) = self.processor.submit(task.clone()).await {
                tracing::error!("fail to submit task to processor: {e}");
                task.nack(Some(TaskError::Other(e.to_string())));
            }
        }
    }

    fn drain_and_nack_tasks(&self) {
        while let Some(task) = self.pool.try_dequeue() {
            task.nack(None);
        }
    }
}

#[async_trait]
impl<K: Hash + Eq + Clone + Send + Sync + 'static> Daemon for HierarchicalWeightedRoundRobinTaskScheduler<K> {
    async fn start(&self) {
        let i = &self.inner;
        if i.status.compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        i.pool.start();
        let me = i.clone();
        *i.handle.lock().unwrap() = Some(tokio::spawn(async move { me.dispatcher().await }));
        tracing::info!("Hierarchical weighted round robin task scheduler started.");
    }

    async fn stop(&self) {
        let i = &self.inner;
        if i.status.compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        i.cancel.cancel();
        i.pool.stop().await;
        let handle = i.handle.lock().unwrap().take();
        if let Some(h) = handle {
            if tokio::time::timeout(SHUTDOWN_WAIT, h).await.is_err() {
                tracing::warn!("Hierarchical weighted round robin task scheduler timedout on shutdown.");
            }
        }
        // Wait for in-flight Submit/TrySubmit calls that passed the stopped check before draining.
        drop(i.lifecycle.write().await);
        i.drain_and_nack_tasks();
        tracing::info!("Hierarchical weighted round robin task scheduler stopped.");
    }
}

#[cfg(test)]
mod tests {
    use super::super::deps::RealTimeSource;
    use super::super::parallel_processor::SubmitError;
    use super::*;
    use futures_util::future::BoxFuture;
    use std::sync::atomic::AtomicUsize;

    struct T {
        id: u32,
        acked: AtomicBool,
        nacked: AtomicBool,
    }
    impl T {
        fn new(id: u32) -> Arc<T> {
            Arc::new(T { id, acked: AtomicBool::new(false), nacked: AtomicBool::new(false) })
        }
    }
    impl super::super::parallel_processor::CommonTask for T {
        fn execute(&self) -> BoxFuture<'_, Result<(), TaskError>> {
            Box::pin(async { Ok(()) })
        }
        fn handle_err(&self, err: TaskError) -> Option<TaskError> {
            Some(err)
        }
        fn retry_err(&self, _err: &TaskError) -> bool {
            false
        }
        fn ack(&self) {
            self.acked.store(true, Ordering::SeqCst);
        }
        fn nack(&self, _err: Option<TaskError>) {
            self.nacked.store(true, Ordering::SeqCst);
        }
        fn as_any(&self) -> Option<&dyn std::any::Any> {
            Some(self)
        }
    }

    fn id_of(t: &TaskRef) -> u32 {
        t.as_any().and_then(|a| a.downcast_ref::<T>()).unwrap().id
    }

    fn wk(key: &str, weight: i64) -> WeightedKey<String> {
        WeightedKey { key: key.to_string(), weight }
    }

    #[test]
    fn schedule_iterates_interleaved_by_weight() {
        let s = Arc::new(IwrrSchedule::new(vec![("a", 3), ("b", 1), ("c", 0)]));
        assert_eq!(s.len(), 4);
        let mut it = s.iter();
        let got: Vec<_> = std::iter::from_fn(|| it.try_next()).collect();
        assert_eq!(got, vec!["a", "a", "a", "b"]);
        assert!(Arc::new(IwrrSchedule::<u8>::new(Vec::new())).iter().try_next().is_none());
    }

    #[test]
    fn ttl_channel_cleanup_rules() {
        let c: TtlChannel<u32> = TtlChannel::new(2);
        let ttl = Duration::from_secs(10);
        c.update_last_write_time(Time(100 * 1_000_000_000));
        assert!(!c.should_cleanup(Time(105 * 1_000_000_000), ttl));
        assert!(c.should_cleanup(Time(111 * 1_000_000_000), ttl));
        c.inc_ref();
        assert!(!c.should_cleanup(Time(111 * 1_000_000_000), ttl));
        c.dec_ref();
        c.sender().try_send(1).unwrap();
        assert_eq!(c.len(), 1);
        assert!(!c.should_cleanup(Time(111 * 1_000_000_000), ttl));
        assert_eq!(c.try_recv(), Some(1));
        assert_eq!(c.cap(), 2);
    }

    #[test]
    fn node_dequeues_children_by_weight_then_own_channel() {
        let root: Arc<IwrrNode<String, u32>> = IwrrNode::new(8);
        let push = |path: &[WeightedKey<String>], v: u32| {
            root.execute_at_path(path, 8, |c| {
                c.sender().try_send(v).unwrap();
                1
            })
        };
        for v in [1, 2, 3] {
            push(&[wk("a", 2)], v);
        }
        push(&[wk("b", 1)], 10);
        assert_eq!(root.children_item_count(), 4);
        assert!(root.drain_self_first());
        let got: Vec<_> = std::iter::from_fn(|| root.try_get_next_item()).collect();
        assert_eq!(got, vec![1, 2, 10, 3]);
        assert_eq!(root.children_item_count(), 0);
    }

    #[test]
    fn node_weight_change_updates_schedule_and_cleanup_removes_idle_children() {
        let root: Arc<IwrrNode<String, u32>> = IwrrNode::new(4);
        root.execute_at_path(&[wk("a", 1)], 4, |c| {
            c.update_last_write_time(Time(1_000_000_000));
            0
        });
        root.execute_at_path(&[wk("a", 5)], 4, |_| 0);
        assert_eq!(root.child_count(), 1);
        let ttl = Duration::from_secs(10);
        assert!(!root.cleanup(Time(5 * 1_000_000_000), ttl));
        assert_eq!(root.child_count(), 1);
        // a nested leaf with a pending item is kept
        root.execute_at_path(&[wk("b", 1), wk("c", 1)], 4, |c| {
            c.sender().try_send(7).unwrap();
            1
        });
        root.cleanup(Time(1_000 * 1_000_000_000), ttl);
        assert_eq!(root.child_count(), 1);
        assert_eq!(root.try_get_next_item(), Some(7));
        assert!(root.cleanup(Time(2_000 * 1_000_000_000), ttl));
        assert_eq!(root.child_count(), 0);
    }

    fn pool(buffer: usize) -> HierarchicalWeightedRoundRobinTaskPool<String> {
        HierarchicalWeightedRoundRobinTaskPool::new(
            Arc::new(RealTimeSource),
            HierarchicalWeightedRoundRobinTaskPoolOptions {
                buffer_size: buffer,
                task_to_weighted_keys_fn: Arc::new(|t: &TaskRef| {
                    let id = id_of(t);
                    if id >= 100 {
                        vec![wk("bad", 0)]
                    } else {
                        vec![wk(if id % 2 == 0 { "even" } else { "odd" }, 1)]
                    }
                }),
            },
        )
    }

    #[tokio::test]
    async fn pool_enqueue_dequeue_and_validation() {
        let p = pool(1);
        assert_eq!(p.enqueue(T::new(100)).await, Err(HierarchicalTaskError::WeightMustBeGreaterThanZero));
        assert_eq!(p.try_enqueue(T::new(100)), Err(HierarchicalTaskError::WeightMustBeGreaterThanZero));
        assert_eq!(p.try_enqueue(T::new(2)), Ok(true));
        assert_eq!(p.try_enqueue(T::new(4)), Ok(false));
        p.enqueue(T::new(1)).await.unwrap();
        let mut got: Vec<u32> = std::iter::from_fn(|| p.try_dequeue()).map(|t| id_of(&t)).collect();
        got.sort();
        assert_eq!(got, vec![1, 2]);
        assert!(p.try_dequeue().is_none());
        assert_eq!(p.root().channel().ref_count(), 0);
    }

    #[tokio::test]
    async fn pool_blocked_enqueue_is_cancelled_by_stop() {
        let p = Arc::new(pool(1));
        p.start();
        p.enqueue(T::new(2)).await.unwrap();
        let p2 = p.clone();
        let h = tokio::spawn(async move { p2.enqueue(T::new(4)).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        p.stop().await;
        assert_eq!(h.await.unwrap(), Err(HierarchicalTaskError::Canceled));
    }

    #[derive(Default)]
    struct Collect {
        got: Mutex<Vec<u32>>,
        count: AtomicUsize,
    }
    #[async_trait]
    impl Daemon for Collect {
        async fn start(&self) {}
        async fn stop(&self) {}
    }
    #[async_trait]
    impl CommonProcessor for Collect {
        async fn submit(&self, task: TaskRef) -> Result<(), SubmitError> {
            self.got.lock().unwrap().push(id_of(&task));
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn scheduler(proc: Arc<Collect>) -> HierarchicalWeightedRoundRobinTaskScheduler<String> {
        HierarchicalWeightedRoundRobinTaskScheduler::new(
            Arc::new(RealTimeSource),
            proc,
            HierarchicalWeightedRoundRobinTaskPoolOptions {
                buffer_size: 4,
                task_to_weighted_keys_fn: Arc::new(|t: &TaskRef| vec![wk(&format!("k{}", id_of(t) % 3), 1)]),
            },
        )
    }

    #[tokio::test]
    async fn scheduler_dispatches_all_tasks_and_rejects_after_stop() {
        let proc = Arc::new(Collect::default());
        let s = scheduler(proc.clone());
        s.start().await;
        s.start().await;
        for id in 0..9 {
            s.submit(T::new(id)).await.unwrap();
        }
        assert_eq!(s.try_submit(T::new(9)).await, Ok(true));
        for _ in 0..100 {
            if proc.count.load(Ordering::SeqCst) == 10 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut got = proc.got.lock().unwrap().clone();
        got.sort();
        assert_eq!(got, (0..10).collect::<Vec<_>>());
        s.stop().await;
        s.stop().await;
        assert_eq!(s.submit(T::new(1)).await, Err(HierarchicalTaskError::SchedulerClosed));
        assert_eq!(s.try_submit(T::new(1)).await, Err(HierarchicalTaskError::SchedulerClosed));
    }

    #[tokio::test]
    async fn scheduler_stop_nacks_undispatched_tasks() {
        let proc = Arc::new(Collect::default());
        let s = scheduler(proc.clone());
        let t = T::new(1);
        // enqueue directly into the pool so that the dispatcher never sees a notification
        s.start().await;
        s.pool().try_enqueue(t.clone()).unwrap();
        s.stop().await;
        let dispatched = proc.count.load(Ordering::SeqCst) == 1;
        assert!(dispatched != t.nacked.load(Ordering::SeqCst));
    }
}
