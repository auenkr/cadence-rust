//! group: assembly/engine_cache
//!
//! Concrete `ExecutionContext`, `ExecutionCache` and `WorkflowUpdater` over the
//! SQL execution store. Port of `execution/context.go` (load path),
//! `execution/cache.go` (per-workflow lock, release/clear semantics) and
//! `workflow/util.go` (`Load`, `UpdateWithActionFunc`,
//! `UpdateCurrentWithActionFunc`, `updateHelper`).
//!
//! Persisting a modified mutable state (`UpdateWorkflowExecutionAsActive` /
//! `UpdateWorkflowExecutionTasks`) closes the transaction (`engine_close`),
//! appends the history batch, allocates task ids and writes the mutation.
//! Replication tasks and new-run (continue-as-new / reset) writes are not ported.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cadence_persistence::history_contract::{self as pc, GoExecutionStore, GoHistoryV2Store};
use cadence_persistence::sqlblobs::{timer_task_to_struct, transfer_task_to_struct, Encoding};
use tokio::sync::OwnedMutexGuard;

use crate::engine::deps::{
    DomainCache, ExecutionCache, ExecutionContext, ExecutionContextRef, HResult, HistoryError,
    MutableStateRef, ReleaseFunc, TimeSource, WorkflowExecution,
};
use crate::engine::runtime::{UpdateAction, UpdateFn, WorkflowUpdater};
use crate::shard_context::{ShardContext, ShardContextError};
use crate::wiring::assembly::engine_state::{EngineMutableState, StateParams};
use crate::wiring::persistence::engine_ndc::{to_history_error, SqlHistoryManager};

/// Go `workflow.ConditionalRetryCount`.
const CONDITIONAL_RETRY_COUNT: usize = 5;

/// Shared collaborators of every context created for one shard.
pub struct EngineCore {
    pub shard: Arc<ShardContext>,
    pub executions: Arc<dyn GoExecutionStore>,
    pub domains: Arc<dyn DomainCache>,
    pub clock: Arc<dyn TimeSource>,
    /// `config.StickyTTL(domainName)` in nanoseconds.
    pub sticky_ttl_nanos: Arc<dyn Fn(&str) -> i64 + Send + Sync>,
    /// Current cluster name (timer id allocation cursor).
    pub current_cluster: String,
    /// History branch store (`AppendHistoryNodes`).
    pub history_v2: Arc<dyn GoHistoryV2Store>,
    /// Per-domain task generation parameters (retention, failover version, config).
    pub state_params: Arc<dyn Fn(&str) -> StateParams + Send + Sync>,
    /// Filled once the shard's `HistoryEngine` exists; used to notify the queue
    /// processors of new tasks (Go `shard.taskNotifier.notifyTasks`).
    pub engine_slot: Arc<std::sync::OnceLock<std::sync::Weak<crate::engine::runtime::HistoryEngine>>>,
    /// `config.EnableStrongIdempotencySanityCheck(domainName)`.
    pub strong_idempotency_sanity_check: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    /// Go `config.IsAdvancedVisibilityWritingEnabled()` (evaluated per use).
    pub advanced_visibility_writing: Arc<dyn Fn() -> bool + Send + Sync>,
    /// Go `shard.GetActiveClusterManager()`.
    pub active_cluster_manager: Arc<dyn crate::engine::runtime::ActiveClusterManager>,
    /// Go `clientBean.GetRemoteAdminClient`.
    pub remote_admin_clients: Arc<crate::wiring::resources::replication_clients::RemoteAdminClients>,
    /// Go `semaphoreMetadataManager`.
    pub semaphore_metadata: Arc<dyn cadence_persistence::SemaphorePersistence>,
}

impl EngineCore {
    /// Go `shard.GetEngine().NotifyNewHistoryEvent`. No engine yet: nothing to wake.
    pub(super) fn notify_history_event(&self, event: crate::engine::runtime::HistoryNotification) {
        if let Some(engine) = self.engine_slot.get().and_then(std::sync::Weak::upgrade) {
            engine.notify_new_history_event(event);
        }
    }

    /// Go `taskNotifier.notifyTasks`: wakes the transfer / timer queue
    /// processors after a successful write of `transfer` / `timer` tasks.
    pub(super) fn notify_tasks(&self, transfer: &[pc::TransferTask], timer: &[pc::TimerTask]) {
        use crate::engine::notify_tasks::{NotifiedTask, NotifiedTaskKind, NotifyTaskInfo};
        tracing::debug!(transfer = transfer.len(), timer = timer.len(), has_engine = self.engine_slot.get().is_some(), "notify queues of new tasks");
        let Some(engine) = self.engine_slot.get().and_then(std::sync::Weak::upgrade) else { return };
        let info_of = |d: &str, w: &str, r: &str, tasks: Vec<NotifiedTask>| {
            let mut info = NotifyTaskInfo { tasks, ..Default::default() };
            info.execution_info.domain_id = d.to_string();
            info.execution_info.workflow_id = w.to_string();
            info.execution_info.run_id = r.to_string();
            info
        };
        if let Some(first) = transfer.first() {
            let tasks = transfer
                .iter()
                .map(|t| NotifiedTask {
                    version: t.version,
                    task_id: t.task_id,
                    task_type: t.task_type as i16,
                    visibility_timestamp: t.visibility_time_unix_nanos,
                    kind: NotifiedTaskKind::Generic,
                })
                .collect();
            engine.notify_new_transfer_tasks(&info_of(&first.domain_id, &first.workflow_id, &first.run_id, tasks));
        }
        if let Some(first) = timer.first() {
            let tasks = timer
                .iter()
                .map(|t| NotifiedTask {
                    version: t.version,
                    task_id: t.task_id,
                    task_type: t.task_type as i16,
                    visibility_timestamp: t.visibility_timestamp_unix_nanos,
                    kind: NotifiedTaskKind::Generic,
                })
                .collect();
            engine.notify_new_timer_tasks(&info_of(&first.domain_id, &first.workflow_id, &first.run_id, tasks));
        }
    }
}

pub(super) fn shard_err(e: ShardContextError) -> HistoryError {
    match e {
        ShardContextError::ShardOwnershipLost(m) => HistoryError::PersistenceShardOwnershipLost(m),
        other => HistoryError::internal(other.to_string()),
    }
}

pub(super) fn task_blob(data: Result<Vec<u8>, impl std::fmt::Display>) -> HResult<pc::DataBlob> {
    data.map(|data| pc::DataBlob { encoding: "thriftrw".to_string(), data })
        .map_err(|e| HistoryError::internal(format!("encode task: {e}")))
}

/// Go `execution.contextImpl` (read side).
pub struct EngineExecutionContext {
    domain_id: String,
    execution: WorkflowExecution,
    core: Arc<EngineCore>,
    state: Mutex<Option<Arc<EngineMutableState>>>,
}

impl EngineExecutionContext {
    pub(super) fn new(core: Arc<EngineCore>, domain_id: &str, execution: &WorkflowExecution) -> Arc<Self> {
        Arc::new(Self {
            domain_id: domain_id.to_string(),
            execution: execution.clone(),
            core,
            state: Mutex::new(None),
        })
    }

    /// Installs a freshly created state (after the create-workflow write).
    pub(super) fn set_state(&self, state: Arc<EngineMutableState>) {
        *self.state.lock().unwrap() = Some(state);
    }

    /// Go `Context.Clear`.
    pub fn clear(&self) {
        *self.state.lock().unwrap() = None;
    }

    /// Go `Context.LoadWorkflowExecution` returning the concrete state.
    pub async fn load_state(&self) -> HResult<Arc<EngineMutableState>> {
        if let Some(state) = self.state.lock().unwrap().clone() {
            return Ok(state);
        }
        let core = &self.core;
        let snapshot = core
            .executions
            .get_workflow_execution(
                core.shard.shard_id(),
                self.domain_id.clone(),
                self.execution.workflow_id.clone(),
                self.execution.run_id.clone(),
                core.shard.range_id(),
            )
            .await
            .map_err(to_history_error)?;
        let domain = core.domains.get_domain_by_id(&self.domain_id)?;
        let ttl = (core.sticky_ttl_nanos)(&domain.info.name);
        let clock = core.clock.clone();
        let state = Arc::new(EngineMutableState::from_snapshot(
            snapshot,
            (*domain).clone(),
            ttl,
            Arc::new(move || clock.now()),
        )?
        .with_params((core.state_params)(&self.domain_id)));
        *self.state.lock().unwrap() = Some(state.clone());
        Ok(state)
    }

    /// NDC checkout must happen while the caller owns this context's cache
    /// lock. A dirty engine transaction cannot be represented by the persisted
    /// snapshot yet, so reject it instead of exporting stale data.
    pub(super) async fn clean_snapshot_for_ndc(&self) -> HResult<pc::WorkflowSnapshot> {
        let state = self.load_state().await?;
        if state.is_dirty() {
            return Err(HistoryError::internal(
                "cannot check out NDC state while engine mutable state is dirty",
            ));
        }
        Ok(state.snapshot.clone())
    }

    /// Append an incoming NDC history batch without rewriting its source task
    /// IDs. Go `PersistNonStartWorkflowBatchEvents` assigns only a shard
    /// transaction ID; event task IDs belong to the replicating cluster.
    pub(super) async fn persist_ndc_history_batch(
        &self,
        events: &crate::execution::context::WorkflowEvents,
        new_branch: bool,
    ) -> HResult<crate::execution::context::PersistedBlob> {
        use crate::execution::context::PersistedBlob;
        if events.events.is_empty() {
            return if new_branch {
                Err(HistoryError::internal("cannot persist first workflow events with empty events"))
            } else {
                Ok(PersistedBlob::default())
            };
        }
        self.core.domains.get_domain_name(&events.key.domain_id)?;
        let transaction_id = self.core.shard.generate_task_id().map_err(shard_err)?;
        let manager = SqlHistoryManager::new(self.core.history_v2.clone(), self.core.shard.shard_id());
        let cleanup = if new_branch {
            format!("{}:{}:{}", events.key.domain_id, events.key.workflow_id, events.key.run_id)
        } else {
            String::new()
        };
        let data_blob = manager
            .append_history_nodes(&events.branch_token, &events.events, transaction_id, new_branch, &cleanup)
            .await
            .map_err(to_history_error)?;
        Ok(PersistedBlob {
            data_blob,
            branch_token: events.branch_token.clone(),
            first_event_id: events.events[0].event_id,
        })
    }

    /// Go `UpdateWorkflowExecutionAsActive`.
    pub async fn update_as_active(&self, now: i64) -> HResult<()> {
        let state = self.load_state().await?;
        if !state.is_dirty() {
            return Ok(());
        }
        self.persist(&state, now, true, None, None).await
    }

    /// Go `UpdateWorkflowExecutionAsPassive`.
    pub async fn update_as_passive(&self, now: i64) -> HResult<()> {
        let state = self.load_state().await?;
        self.persist(&state, now, false, None, None).await
    }

    /// Go `UpdateWorkflowExecutionWithNewAsActive`: persists this (terminated)
    /// run together with the snapshot of the new run.
    pub(super) async fn update_with_new_as_active(
        &self,
        now: i64,
        new_snapshot: pc::WorkflowSnapshot,
        held_gate: &tokio::sync::OwnedMutexGuard<()>,
    ) -> HResult<()> {
        let state = self.load_state().await?;
        self.persist(&state, now, true, Some(new_snapshot), Some(held_gate)).await
    }

    /// Go `UpdateWorkflowExecutionWithNewAsActive` with the new run's mutable state:
    /// the closed current run and the new run are written in one update (shared by the
    /// ContinueAsNew decision and the timer-driven workflow retry / cron).
    pub(super) async fn update_with_new_run(&self, now: i64, new_run: &Arc<EngineMutableState>) -> HResult<()> {
        let core = &self.core;
        let info = {
            let g = new_run.inner.lock().unwrap();
            format!("{}:{}", g.pc_exec.workflow_id, g.pc_exec.run_id)
        };
        let mut txn = new_run.close_transaction(now, true)?;
        let (size, last, _blob) = crate::wiring::assembly::engine_start::append_new_branch(core, &mut txn, &info).await?;
        // Go holds the shard lock from task-id allocation through the write.
        let gate = core.shard.lock_task_writes().await;
        let (snapshot, max_read_level, transfer, timer) = crate::wiring::assembly::engine_start::snapshot_of(core, txn)?;
        self.update_with_new_as_active(now, snapshot, &gate).await?;
        core.shard.update_max_read_level(max_read_level);
        drop(gate);
        core.notify_tasks(&transfer, &timer);
        new_run.finish_transaction(size, last);
        Ok(())
    }

    /// Go `UpdateWorkflowExecutionTasks`.
    async fn update_tasks(&self, now: i64) -> HResult<()> {
        let state = self.load_state().await?;
        if !state.is_dirty() {
            return Ok(());
        }
        self.persist(&state, now, false, None, None).await
    }

    /// Close the transaction, assign task ids, append the history batch and
    /// write the mutation (Go `updateWorkflowExecutionWithNew` without a new run).
    async fn persist(
        &self,
        state: &Arc<EngineMutableState>,
        now: i64,
        active: bool,
        new_snapshot: Option<pc::WorkflowSnapshot>,
        held_gate: Option<&tokio::sync::OwnedMutexGuard<()>>,
    ) -> HResult<()> {
        let core = &self.core;
        let shard = &core.shard;
        let mut txn = state.close_transaction(now, active)?;

        // History batch.
        let mut history_size = state.history_size();
        let mut last_event_task_id = None;
        if !txn.events.is_empty() {
            let ids = shard.generate_task_ids(txn.events.len()).map_err(shard_err)?;
            for (ev, id) in txn.events.iter_mut().zip(ids.iter()) {
                ev.task_id = *id;
            }
            last_event_task_id = ids.last().copied();
            let transaction_id = shard.generate_task_id().map_err(shard_err)?;
            let manager = SqlHistoryManager::new(core.history_v2.clone(), shard.shard_id());
            let blob = manager
                .append_history_nodes(&txn.branch_token, &txn.events, transaction_id, false, "")
                .await
                .map_err(to_history_error)?;
            history_size += blob.data.len() as i64;
            txn.mutation.execution_info.history_size = history_size;
            if let Some(id) = last_event_task_id {
                txn.mutation.execution_info.last_event_task_id = id;
            }
            txn.mutation.last_write_version =
                txn.events.last().map(|e| e.version).unwrap_or(txn.mutation.last_write_version);
        }

        // Task ids. The gate stays held until the write finished (see ShardContext::lock_task_writes).
        // When the caller already holds it (task ids allocated for a new run), do not lock again.
        let _task_write_gate = match held_gate {
            Some(_) => None,
            None => Some(shard.lock_task_writes().await),
        };
        let mut max_read_level = 0i64;
        let mut transfer_slots: Vec<(i64, i64)> = txn.transfer.iter().map(|_| (0, 0)).collect();
        shard.allocate_transfer_ids(&mut transfer_slots, &mut max_read_level).map_err(shard_err)?;
        let mut timer_slots: Vec<(i64, i64)> =
            txn.timer.iter().map(|t| (0, t.visibility_timestamp_unix_nanos)).collect();
        shard.allocate_timer_ids(&core.current_cluster, &mut timer_slots).map_err(shard_err)?;

        let mut transfer_tasks = Vec::new();
        for (t, slot) in txn.transfer.iter_mut().zip(transfer_slots.iter()) {
            t.task_id = slot.0;
            t.visibility_time_unix_nanos = slot.1;
            let s = transfer_task_to_struct(t).map_err(|e| HistoryError::internal(e.to_string()))?;
            transfer_tasks.push(pc::Task {
                visibility_timestamp_unix_nanos: slot.1,
                task_type: t.task_type,
                task_id: slot.0,
                data: task_blob(Encoding::ThriftRW.encode(&s))?,
            });
        }
        let mut timer_tasks = Vec::new();
        for (t, slot) in txn.timer.iter_mut().zip(timer_slots.iter()) {
            t.task_id = slot.0;
            t.visibility_timestamp_unix_nanos = slot.1;
            let s = timer_task_to_struct(t).map_err(|e| HistoryError::internal(e.to_string()))?;
            timer_tasks.push(pc::Task {
                visibility_timestamp_unix_nanos: slot.1,
                task_type: t.task_type,
                task_id: slot.0,
                data: task_blob(Encoding::ThriftRW.encode(&s))?,
            });
        }
        if !transfer_tasks.is_empty() {
            txn.mutation
                .tasks_by_category
                .insert(pc::HistoryTaskCategory { category_type: 1, category_id: 1 }, transfer_tasks);
        }
        if !timer_tasks.is_empty() {
            txn.mutation
                .tasks_by_category
                .insert(pc::HistoryTaskCategory { category_type: 2, category_id: 2 }, timer_tasks);
        }

        let request = pc::UpdateWorkflowExecutionRequest {
            shard_id: shard.shard_id(),
            range_id: shard.range_id(),
            mode: if active { pc::UpdateWorkflowMode::UpdateCurrent } else { pc::UpdateWorkflowMode::IgnoreCurrent },
            update_workflow_mutation: txn.mutation,
            new_workflow_snapshot: new_snapshot,
            workflow_request_mode: pc::WorkflowRequestMode::New,
            current_timestamp_unix_nanos: now,
        };
        match core.executions.update_workflow_execution(request).await {
            Ok(()) => {
                shard.update_max_read_level(max_read_level);
                core.notify_tasks(&txn.transfer, &txn.timer);
                state.finish_transaction(history_size, last_event_task_id);
                Ok(())
            }
            Err(e) => {
                if matches!(e, cadence_persistence::history_contract::PersistenceError::ShardOwnershipLost { .. }) {
                    shard.close_shard(None);
                }
                Err(to_history_error(e))
            }
        }
    }
}

#[async_trait]
impl ExecutionContext for EngineExecutionContext {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }
    fn get_execution(&self) -> WorkflowExecution {
        self.execution.clone()
    }
    fn get_workflow_execution(&self) -> Option<MutableStateRef> {
        self.state.lock().unwrap().clone().map(|s| s as MutableStateRef)
    }
    async fn load_workflow_execution(&self) -> HResult<MutableStateRef> {
        Ok(self.load_state().await? as MutableStateRef)
    }
    async fn update_workflow_execution_tasks(&self, now: i64) -> HResult<()> {
        self.update_tasks(now).await
    }
    async fn update_workflow_execution_as_active(&self, now: i64) -> HResult<()> {
        self.update_as_active(now).await
    }
    fn clear(&self) {
        EngineExecutionContext::clear(self)
    }
}

struct Entry {
    lock: Arc<tokio::sync::Mutex<()>>,
    context: Arc<EngineExecutionContext>,
}

type Key = (String, String, String);

/// Go `execution.cacheImpl`: contexts keyed by domain/workflow/run, each guarded
/// by a lock held until the returned `ReleaseFunc` is called.
pub struct EngineExecutionCache {
    core: Arc<EngineCore>,
    entries: Mutex<HashMap<Key, Arc<Entry>>>,
    max_size: usize,
}

impl EngineExecutionCache {
    pub(super) fn core(&self) -> Arc<EngineCore> {
        self.core.clone()
    }

    /// Test-only: drops every cached context so the next load reads persistence (Go cache miss).
    #[cfg(test)]
    pub fn clear_all_for_test(&self) {
        self.entries.lock().unwrap().clear();
    }

    pub fn new(core: Arc<EngineCore>) -> Arc<Self> {
        Arc::new(Self { core, entries: Mutex::new(HashMap::new()), max_size: 10_000 })
    }

    /// Go `validateWorkflowExecutionInfo`: fills the current run id when empty.
    pub(super) async fn validate(&self, domain_id: &str, execution: &WorkflowExecution) -> HResult<WorkflowExecution> {
        if execution.workflow_id.is_empty() {
            return Err(HistoryError::bad_request("Can't load workflow execution.  WorkflowId not set."));
        }
        let mut execution = execution.clone();
        if execution.run_id.is_empty() {
            let current = self
                .core
                .executions
                .get_current_execution(self.core.shard.shard_id(), domain_id.to_string(), execution.workflow_id.clone())
                .await
                .map_err(to_history_error)?;
            execution.run_id = current.run_id;
        } else if uuid::Uuid::parse_str(&execution.run_id).is_err() {
            return Err(HistoryError::bad_request("RunId is not valid UUID."));
        }
        Ok(execution)
    }

    fn entry(&self, key: &Key, domain_id: &str, execution: &WorkflowExecution) -> Arc<Entry> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(e) = entries.get(key) {
            return e.clone();
        }
        if entries.len() >= self.max_size {
            // Evict entries nobody holds (a held lock owns a clone of the mutex Arc).
            entries.retain(|_, e| Arc::strong_count(&e.lock) > 1);
        }
        let entry = Arc::new(Entry {
            lock: Arc::new(tokio::sync::Mutex::new(())),
            context: EngineExecutionContext::new(self.core.clone(), domain_id, execution),
        });
        entries.insert(key.clone(), entry.clone());
        entry
    }

    /// Go `getOrCreateWorkflowExecutionInternal` (execution already validated).
    pub async fn acquire(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
        force_clear: bool,
    ) -> HResult<(Arc<EngineExecutionContext>, ReleaseFunc)> {
        let key = (domain_id.to_string(), execution.workflow_id.clone(), execution.run_id.clone());
        let entry = self.entry(&key, domain_id, execution);
        let guard: OwnedMutexGuard<()> = entry.lock.clone().lock_owned().await;
        let slot = Mutex::new(Some(guard));
        let context = entry.context.clone();
        let release_context = context.clone();
        let release: ReleaseFunc = Arc::new(move |err: Option<&HistoryError>| {
            if err.is_some() || force_clear {
                release_context.clear();
            }
            // Dropping the guard unlocks the workflow.
            drop(slot.lock().unwrap().take());
        });
        Ok((context, release))
    }

    /// Go `GetOrCreateWorkflowExecution` with the concrete context type.
    pub async fn get_or_create(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> HResult<(Arc<EngineExecutionContext>, ReleaseFunc)> {
        let execution = self.validate(domain_id, execution).await?;
        self.acquire(domain_id, &execution, false).await
    }
}

#[async_trait]
impl ExecutionCache for EngineExecutionCache {
    async fn get_or_create_workflow_execution(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> HResult<(ExecutionContextRef, ReleaseFunc)> {
        let (context, release) = self.get_or_create(domain_id, execution).await?;
        Ok((context, release))
    }

    async fn get_and_create_workflow_execution(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
    ) -> HResult<(ExecutionContextRef, ExecutionContextRef, ReleaseFunc, bool)> {
        let execution = self.validate(domain_id, execution).await?;
        let key = (domain_id.to_string(), execution.workflow_id.clone(), execution.run_id.clone());
        let hit = self.entries.lock().unwrap().contains_key(&key);
        // The context loaded from the DB is never cached (Go comment).
        let from_db: ExecutionContextRef = EngineExecutionContext::new(self.core.clone(), domain_id, &execution);
        if hit {
            let (cached, release) = self.acquire(domain_id, &execution, false).await?;
            Ok((cached, from_db, release, true))
        } else {
            let noop: ReleaseFunc = Arc::new(|_| {});
            Ok((from_db.clone(), from_db, noop, false))
        }
    }

    async fn get_or_create_current_workflow_execution(
        &self,
        domain_id: &str,
        workflow_id: &str,
    ) -> HResult<ReleaseFunc> {
        // Empty run id is the key of the "current workflow" lock; always cleared on release.
        let execution = WorkflowExecution { workflow_id: workflow_id.to_string(), run_id: String::new() };
        let (_, release) = self.acquire(domain_id, &execution, true).await?;
        Ok(release)
    }
}

/// Go `workflow.UpdateWithActionFunc` / `UpdateCurrentWithActionFunc`.
pub struct EngineWorkflowUpdater {
    cache: Arc<EngineExecutionCache>,
    core: Arc<EngineCore>,
}

impl EngineWorkflowUpdater {
    pub fn new(cache: Arc<EngineExecutionCache>, core: Arc<EngineCore>) -> Self {
        Self { cache, core }
    }

    /// Go `workflow.Load`: for an empty run id, follow the current-run pointer
    /// until the loaded run is running or current.
    async fn load_current(
        &self,
        domain_id: &str,
        workflow_id: &str,
    ) -> HResult<(Arc<EngineExecutionContext>, Arc<EngineMutableState>, ReleaseFunc)> {
        for _ in 0..CONDITIONAL_RETRY_COUNT {
            let (context, release) =
                self.cache.get_or_create(domain_id, &WorkflowExecution { workflow_id: workflow_id.to_string(), ..Default::default() }).await?;
            let state = match context.load_state().await {
                Ok(s) => s,
                Err(e) => {
                    release(Some(&e));
                    return Err(e);
                }
            };
            use crate::engine::deps::MutableState;
            if state.is_workflow_execution_running() {
                return Ok((context, state, release));
            }
            let current = self
                .core
                .executions
                .get_current_execution(self.core.shard.shard_id(), domain_id.to_string(), workflow_id.to_string())
                .await;
            match current {
                Err(e) => {
                    let e = to_history_error(e);
                    release(Some(&e));
                    return Err(e);
                }
                Ok(c) if c.run_id == context.get_execution().run_id => return Ok((context, state, release)),
                Ok(_) => release(None),
            }
        }
        Err(HistoryError::internal("unable to locate current workflow execution"))
    }

    /// Go `updateHelper`.
    async fn update_helper<'a>(
        &self,
        context: &Arc<EngineExecutionContext>,
        now: i64,
        action: &UpdateFn<'a>,
    ) -> HResult<()> {
        use crate::engine::deps::MutableState;
        for attempt in 0..CONDITIONAL_RETRY_COUNT {
            let state = context.load_state().await?;
            let ctx_ref: ExecutionContextRef = context.clone();
            let post = match action(ctx_ref, state.clone() as MutableStateRef).await {
                Ok(p) => p,
                Err(HistoryError::StaleState) => {
                    context.clear();
                    if attempt != CONDITIONAL_RETRY_COUNT - 1 {
                        context.load_state().await?;
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let UpdateAction { noop, create_decision } = post;
            if noop {
                return Ok(());
            }
            if create_decision && !state.has_pending_decision() {
                state.add_decision_task_scheduled(false)?;
            }
            match context.update_as_active(now).await {
                Err(HistoryError::DuplicateRequest { .. }) => return Ok(()),
                Err(e) if e.is_conflict_error() => {
                    if attempt != CONDITIONAL_RETRY_COUNT - 1 {
                        context.clear();
                        context.load_state().await?;
                    }
                    continue;
                }
                other => return other,
            }
        }
        Err(HistoryError::MaxAttemptsExceeded)
    }
}

#[async_trait]
impl WorkflowUpdater for EngineWorkflowUpdater {
    async fn update<'a>(
        &self,
        domain_id: &str,
        execution: &WorkflowExecution,
        current: bool,
        now: i64,
        action: UpdateFn<'a>,
    ) -> HResult<()> {
        let (context, release) = if current {
            // Go returns nil when the domain name cannot be resolved.
            if self.core.domains.get_domain_name(domain_id).is_err() {
                return Ok(());
            }
            if execution.run_id.is_empty() {
                let (c, _s, r) = self.load_current(domain_id, &execution.workflow_id).await?;
                (c, r)
            } else {
                let (c, r) = self.cache.get_or_create(domain_id, execution).await?;
                if let Err(e) = c.load_state().await {
                    r(Some(&e));
                    return Err(e);
                }
                (c, r)
            }
        } else {
            let (c, r) = self.cache.get_or_create(domain_id, execution).await?;
            if let Err(e) = c.load_state().await {
                r(Some(&e));
                return Err(e);
            }
            (c, r)
        };
        let result = self.update_helper(&context, now, &action).await;
        release(result.as_ref().err());
        result
    }
}
