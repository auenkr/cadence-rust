//! group: assembly/controller
//!
//! `ShardRegistry`: lazy per-shard context + engine creation with ownership
//! decided by the membership ring (Go `shard.Controller.GetEngineForShard`).
//!
//! The shard row is acquired through `ShardFactory` (range-id steal inside
//! `ShardContext::acquire`), so `shard::ShardController::acquire` is deliberately
//! not used (it would bump the range twice). Only its hashing helpers are.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use cadence_common::membership::Host;
use tokio::sync::OnceCell;

use crate::engine::deps::{HResult, HistoryError};
use crate::engine::runtime::HistoryEngine;
use crate::shard::ShardController;
use crate::shard_context::ShardContext;
use crate::task::deps::MetricsScope as _;
use crate::wiring::resources::metrics::Scope;
use crate::wiring::assembly::engine_factory::build_engine;
use crate::wiring::assembly::env::{HistoryEnv, QueueFactory, ReplicationFactory, ShardBuildCtx};
use crate::wiring::assembly::shard_factory::{ShardFactory, TaskShardContext};

/// Everything created for one owned shard.
pub struct ShardEntry {
    pub shard_id: i32,
    pub shard: Arc<ShardContext>,
    pub task_context: Arc<TaskShardContext>,
    pub engine: Arc<HistoryEngine>,
}

/// Pure ownership decision. `owner` is the ring lookup of the shard id.
/// Returns `ShardOwnershipLost` carrying the owner address when another host owns it.
pub fn check_ownership(shard_id: u32, who_am_i: &str, owner: Option<&Host>) -> HResult<()> {
    match owner {
        None => Err(HistoryError::internal(format!(
            "no history hosts in the membership ring to own shard {shard_id}"
        ))),
        Some(h) if h.identity() == who_am_i => Ok(()),
        Some(h) => Err(cadence_api::errors::ShardOwnershipLostError::with_owner(
            format!("Shard {shard_id} is not owned by this host, owner: {}", h.address),
            h.address.clone(),
        )
        .into()),
    }
}

/// Go `handlerImpl.convertError` for `*persistence.ShardOwnershipLostError`: `CreateShardOwnershipLostError(self, owner)`.
/// A failed ring lookup yields an empty owner (Go passes an empty `HostInfo`).
pub fn shard_ownership_lost_error(who_am_i: &str, owner: Option<&Host>) -> cadence_api::errors::ShardOwnershipLostError {
    cadence_api::errors::ShardOwnershipLostError::with_owner(
        format!("Shard is not owned by host: {who_am_i}"),
        owner.map(|h| h.address.clone()).unwrap_or_default(),
    )
}

/// Pure part of `convertError`: rewrites a persistence ShardOwnershipLost using `lookup(shard_id)`.
pub fn convert_shard_ownership_lost(
    e: HistoryError,
    who_am_i: &str,
    lookup: impl Fn(i32) -> Option<Host>,
) -> HistoryError {
    match e {
        HistoryError::PersistenceShardOwnershipLost { shard_id: Some(shard_id), .. } => {
            shard_ownership_lost_error(who_am_i, lookup(shard_id).as_ref()).into()
        }
        other => other,
    }
}

type Cell = Arc<OnceCell<Arc<ShardEntry>>>;

/// Go `removeHistoryShardItem` identity compare: only the item registered for this acquisition
/// (`mine`) is removed; a replaced shard's late callback never removes its successor. A shard that
/// closes while still initializing is removed too (its cell is not yet published).
fn remove_matches(current: Option<&Cell>, mine: &Cell) -> bool {
    current.is_some_and(|c| Arc::ptr_eq(c, mine))
}

pub struct ShardRegistry {
    env: Arc<HistoryEnv>,
    factory: Arc<ShardFactory>,
    queues: Arc<dyn QueueFactory>,
    replication: Arc<dyn ReplicationFactory>,
    entries: Arc<Mutex<HashMap<u32, Cell>>>,
    num_shards: u32,
    runtime: OnceLock<tokio::runtime::Handle>,
    /// Go `metrics.HistoryShardControllerScope`.
    metrics_scope: Scope,
}

impl ShardRegistry {
    pub fn new(
        env: Arc<HistoryEnv>,
        queues: Arc<dyn QueueFactory>,
        replication: Arc<dyn ReplicationFactory>,
    ) -> Arc<Self> {
        let num_shards = env.config.number_of_shards.max(1) as u32;
        Arc::new(Self {
            factory: Arc::new(ShardFactory::new(env.clone())),
            queues,
            replication,
            entries: Arc::new(Mutex::new(HashMap::new())),
            num_shards,
            runtime: OnceLock::new(),
            metrics_scope: Scope::new(env.metrics_registry.clone(), "HistoryShardController", vec![]),
            env,
        })
    }

    pub fn env(&self) -> &Arc<HistoryEnv> {
        &self.env
    }

    pub fn shard_id_for_workflow(&self, workflow_id: &str) -> u32 {
        ShardController::workflow_id_to_shard(workflow_id, self.num_shards)
    }

    /// Engine for the shard owning `workflow_id`, creating the shard lazily.
    pub async fn engine_for_workflow(&self, workflow_id: &str) -> Result<Arc<HistoryEngine>, HistoryError> {
        self.engine_for_shard(self.shard_id_for_workflow(workflow_id)).await
    }

    pub async fn engine_for_shard(&self, shard_id: u32) -> Result<Arc<HistoryEngine>, HistoryError> {
        // Go `GetEngineForShard`: latency timer + exponential histogram.
        let start = std::time::Instant::now();
        let res = self.entry_for_shard(shard_id).await;
        let elapsed = start.elapsed();
        self.metrics_scope.record_timer("GetEngineForShardLatency", elapsed);
        self.metrics_scope.exponential_histogram("GetEngineForShardLatencyHistogram", elapsed);
        if res.is_err() {
            self.metrics_scope.inc_counter("GetEngineForShardErrorCounter");
        }
        Ok(res?.engine.clone())
    }

    /// Go `handlerImpl.convertError` (ShardOwnershipLost branch).
    pub fn convert_error(&self, e: HistoryError) -> HistoryError {
        convert_shard_ownership_lost(e, self.env.membership.who_am_i(), |id| {
            self.env.membership.lookup(&crate::lookup::shard_key(id))
        })
    }

    pub async fn entry_for_shard(&self, shard_id: u32) -> Result<Arc<ShardEntry>, HistoryError> {
        if shard_id >= self.num_shards {
            return Err(HistoryError::bad_request(format!(
                "shard id {shard_id} exceeds shard count {}",
                self.num_shards
            )));
        }
        let owner = self.env.membership.lookup(&crate::lookup::shard_key(shard_id as i32));
        if let Err(e) = check_ownership(shard_id, self.env.membership.who_am_i(), owner.as_ref()) {
            // Not (or no longer) ours: drop any stale local state.
            self.close_shard(shard_id);
            return Err(e);
        }
        let cell = {
            let mut guard = self.entries.lock().unwrap();
            guard.entry(shard_id).or_default().clone()
        };
        let entry = cell.get_or_try_init(|| self.create_entry(shard_id, cell.clone())).await?;
        self.metrics_scope.update_gauge("NumShardsGauge", self.owned_shard_ids().len() as f64);
        Ok(entry.clone())
    }

    async fn create_entry(&self, shard_id: u32, my_cell: Cell) -> HResult<Arc<ShardEntry>> {
        let env = self.env.clone();
        let factory = self.factory.clone();
        let queues = self.queues.clone();
        let replication = self.replication.clone();
        let entries = self.entries.clone();
        let handle = tokio::runtime::Handle::current();
        let _ = self.runtime.set(handle.clone());
        let started = std::time::Instant::now();
        let scope = self.metrics_scope.clone();
        tracing::info!(shard_id, "Creating new history shard item");
        let closed_early = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closed_early_cb = closed_early.clone();
        let entry = tokio::task::spawn_blocking(move || -> HResult<Arc<ShardEntry>> {
            let id = shard_id as i32;
            let on_close = entries.clone();
            // Go `removeEngineForShard`: the engine is stopped when the shard goes away.
            let closed_scope = scope.clone();
            let close_callback: Arc<dyn Fn(i32) + Send + Sync> = Arc::new(move |closed: i32| {
                // Go `shardClosedCallback`.
                closed_scope.inc_counter("ShardClosedCounter");
                closed_scope.inc_counter("ShardItemRemovedCounter");
                tracing::info!(shard_id = closed, reason = "shardClosedCallback", "Shard controller state changed: stopping shard");
                // Go `removeEngineForShard(shardID, shardItem)` only removes the item that owns the closing
                // context: a late callback of a replaced shard must not tear down its healthy successor.
                let cell = {
                    let mut map = on_close.lock().unwrap();
                    if remove_matches(map.get(&(closed as u32)), &my_cell) {
                        map.remove(&(closed as u32))
                    } else {
                        None
                    }
                };
                match cell.map(|c| c.get().cloned()) {
                    Some(Some(entry)) => {
                        let engine = entry.engine.clone();
                        handle.spawn(async move { engine.stop().await });
                    }
                    // Closed while initializing: the engine is stopped once creation finishes.
                    Some(None) => closed_early_cb.store(true, std::sync::atomic::Ordering::SeqCst),
                    None => {}
                }
            });
            let shard = factory.new_shard_context(id, close_callback)?;
            let (core, cache) = super::engine_factory::build_core(&env, &shard);
            let ctx = ShardBuildCtx {
                env: env.clone(),
                shard: shard.clone(),
                engine_slot: core.engine_slot.clone(),
                core: core.clone(),
                cache: cache.clone(),
            };
            let queue_processors = queues.build_queue_processors(&ctx).map_err(|e| HistoryError::internal(e.to_string()))?;
            let parts = replication.build_replication(&ctx).map_err(|e| HistoryError::internal(e.to_string()))?;
            let engine = build_engine(&env, &shard, core, cache, queue_processors, parts);
            let _ = ctx.engine_slot.set(Arc::downgrade(&engine));
            Ok(Arc::new(ShardEntry {
                shard_id: id,
                task_context: TaskShardContext::new(env, shard.clone()),
                shard,
                engine,
            }))
        })
        .await
        .map_err(|e| HistoryError::internal(format!("shard creation task failed: {e}")))??;
        // Go `newShardItem` -> `engine.Start()` after the engine is created.
        entry.engine.start().await;
        if closed_early.load(std::sync::atomic::Ordering::SeqCst) {
            // Go: the removed item's engine is stopped after its start (item lock serializes).
            self.stop_engine(&entry);
        }
        let acquisition = started.elapsed();
        self.metrics_scope.inc_counter("ShardItemCreatedCounter");
        {
            // Go `ShardItemAcquisitionLatency` (ShardInfoScope).
            let shard_info = Scope::new(self.env.metrics_registry.clone(), "ShardInfo", vec![]);
            shard_info.record_timer("ShardItemAcquisitionLatency", acquisition);
            shard_info.exponential_histogram("ShardItemAcquisitionLatencyHistogram", acquisition);
        }
        tracing::info!(shard_id, "Created new history shard item");
        Ok(entry)
    }

    /// Stops the engine of a shard entry (Go `Engine.Stop`) on the runtime.
    fn stop_engine(&self, entry: &Arc<ShardEntry>) {
        let engine = entry.engine.clone();
        match tokio::runtime::Handle::try_current().ok().or_else(|| self.runtime.get().cloned()) {
            Some(h) => {
                h.spawn(async move { engine.stop().await });
            }
            None => tracing::warn!(shard_id = entry.shard_id, "no tokio runtime: engine not stopped"),
        }
    }

    pub fn owned_shard_ids(&self) -> Vec<u32> {
        let guard = self.entries.lock().unwrap();
        let mut ids: Vec<u32> = guard.iter().filter(|(_, c)| c.get().is_some()).map(|(k, _)| *k).collect();
        ids.sort_unstable();
        ids
    }

    /// Closes one shard (ownership lost / membership change).
    pub fn close_shard(&self, shard_id: u32) {
        let cell = self.entries.lock().unwrap().remove(&shard_id);
        if let Some(entry) = cell.and_then(|c| c.get().cloned()) {
            // Go `removeEngineForShard`.
            let start = std::time::Instant::now();
            tracing::info!(shard_id, "removeEngineForShard called");
            self.stop_engine(&entry);
            let elapsed = start.elapsed();
            self.metrics_scope.record_timer("RemoveEngineForShardLatency", elapsed);
            self.metrics_scope.exponential_histogram("RemoveEngineForShardLatencyHistogram", elapsed);
            tracing::info!(shard_id, "removeEngineForShard completed");
            entry.shard.close_shard(None);
        }
    }

    /// Call after the membership ring changed: closes shards this host no longer owns.
    pub fn on_membership_change(&self) {
        self.metrics_scope.inc_counter("MembershipChangedCounter");
        tracing::info!("Ring membership changed");
        // Go `acquireShards`.
        let acquire_start = std::time::Instant::now();
        self.metrics_scope.inc_counter("AcquireShardsCounter");
        let _acquire = scopeguard_timer(self.metrics_scope.clone(), acquire_start);
        let who = self.env.membership.who_am_i().to_string();
        let ids: Vec<u32> = self.entries.lock().unwrap().keys().copied().collect();
        for id in ids {
            let owner = self.env.membership.lookup(&crate::lookup::shard_key(id as i32));
            if check_ownership(id, &who, owner.as_ref()).is_err() {
                self.close_shard(id);
            }
        }
    }

    /// Closes every shard (service shutdown).
    pub fn close_all(&self) {
        let drained: Vec<Cell> = self.entries.lock().unwrap().drain().map(|(_, c)| c).collect();
        for cell in drained {
            if let Some(entry) = cell.get() {
                self.stop_engine(entry);
                entry.shard.close_shard(None);
            }
        }
    }
}

struct AcquireTimer(Scope, std::time::Instant);
impl Drop for AcquireTimer {
    fn drop(&mut self) {
        let d = self.1.elapsed();
        self.0.record_timer("AcquireShardsLatency", d);
        self.0.exponential_histogram("AcquireShardsLatencyHistogram", d);
    }
}
fn scopeguard_timer(s: Scope, start: std::time::Instant) -> AcquireTimer {
    AcquireTimer(s, start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_shard_ownership_lost_fills_owner() {
        use crate::engine::deps::HistoryError;
        let lost = |id| HistoryError::PersistenceShardOwnershipLost { message: "x".into(), shard_id: id };
        let host = Host { address: "10.0.0.2:7934".into(), identity: String::new(), grpc_address: String::new() };
        let HistoryError::Cadence(c) = convert_shard_ownership_lost(lost(Some(7)), "me:7934", |id| { assert_eq!(id, 7); Some(host.clone()) }) else { panic!() };
        let cadence_api::CadenceError::ShardOwnershipLost(e) = *c else { panic!() };
        assert_eq!((e.message.as_str(), e.owner.as_str()), ("Shard is not owned by host: me:7934", "10.0.0.2:7934"));
        // lookup failure: Go passes an empty HostInfo -> empty owner
        let HistoryError::Cadence(c) = convert_shard_ownership_lost(lost(Some(7)), "me:7934", |_| None) else { panic!() };
        let cadence_api::CadenceError::ShardOwnershipLost(e) = *c else { panic!() };
        assert_eq!(e.owner, "");
        // other errors and unknown shard ids pass through
        assert!(matches!(convert_shard_ownership_lost(lost(None), "me", |_| None), HistoryError::PersistenceShardOwnershipLost { .. }));
        assert!(matches!(convert_shard_ownership_lost(HistoryError::internal("i"), "me", |_| None), HistoryError::Cadence(_)));
    }

    fn host(identity: &str, address: &str) -> Host {
        Host { address: address.into(), identity: identity.into(), grpc_address: String::new() }
    }

    #[test]
    fn close_callback_removes_only_own_cell() {
        let mine: Cell = Arc::new(OnceCell::new());
        let other: Cell = Arc::new(OnceCell::new());
        assert!(!remove_matches(None, &mine));
        assert!(!remove_matches(Some(&other), &mine));
        // uninitialized own cell (closed during init) is removed
        assert!(remove_matches(Some(&mine), &mine));
    }

    #[test]
    fn owned_shard_is_ok() {
        assert!(check_ownership(3, "a", Some(&host("a", "a:7934"))).is_ok());
    }

    #[test]
    fn foreign_shard_reports_owner_address() {
        let err = check_ownership(3, "a", Some(&host("b", "b:7934"))).unwrap_err();
        assert!(err.to_string().contains("b:7934"), "{err}");
        let HistoryError::Cadence(c) = &err else { panic!("expected cadence error") };
        let cadence_api::CadenceError::ShardOwnershipLost(x) = &**c else { panic!("expected ownership lost") };
        assert_eq!(x.owner, "b:7934");
    }

    #[test]
    fn empty_ring_is_internal_error() {
        assert!(check_ownership(3, "a", None).is_err());
    }

    #[test]
    fn workflow_hash_is_stable_and_in_range() {
        let a = ShardController::workflow_id_to_shard("wf", 4);
        assert_eq!(a, ShardController::workflow_id_to_shard("wf", 4));
        assert!(a < 4);
    }
}
