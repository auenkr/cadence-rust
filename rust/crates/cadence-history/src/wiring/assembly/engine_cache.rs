//! group: assembly/engine_cache
//!
//! Concrete `ExecutionContext`, `ExecutionCache` and `WorkflowUpdater` over the
//! SQL execution store. Port of `execution/context.go` (load path),
//! `execution/cache.go` (per-workflow lock, release/clear semantics) and
//! `workflow/util.go` (`Load`, `UpdateWithActionFunc`,
//! `UpdateCurrentWithActionFunc`, `updateHelper`).
//!
//! Not ported (explicit errors): persisting a modified mutable state
//! (`UpdateWorkflowExecutionAsActive` / `UpdateWorkflowExecutionTasks` close
//! transaction) and `AddDecisionTaskScheduledEvent`. See memory "Unit D5".

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cadence_persistence::history_contract::GoExecutionStore;
use tokio::sync::OwnedMutexGuard;

use crate::engine::deps::{
    DomainCache, ExecutionCache, ExecutionContext, ExecutionContextRef, HResult, HistoryError,
    MutableStateRef, ReleaseFunc, TimeSource, WorkflowExecution,
};
use crate::engine::runtime::{UpdateAction, UpdateFn, WorkflowUpdater};
use crate::shard_context::ShardContext;
use crate::wiring::assembly::engine_state::EngineMutableState;
use crate::wiring::persistence::engine_ndc::to_history_error;

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
}

/// Go `execution.contextImpl` (read side).
pub struct EngineExecutionContext {
    domain_id: String,
    execution: WorkflowExecution,
    core: Arc<EngineCore>,
    state: Mutex<Option<Arc<EngineMutableState>>>,
}

impl EngineExecutionContext {
    fn new(core: Arc<EngineCore>, domain_id: &str, execution: &WorkflowExecution) -> Arc<Self> {
        Arc::new(Self {
            domain_id: domain_id.to_string(),
            execution: execution.clone(),
            core,
            state: Mutex::new(None),
        })
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
        )?);
        *self.state.lock().unwrap() = Some(state.clone());
        Ok(state)
    }

    /// Go `UpdateWorkflowExecutionAsActive`. Only the no-op case (nothing was
    /// modified in memory) is supported; persisting a modified state needs the
    /// close-transaction port.
    pub async fn update_as_active(&self, _now: i64) -> HResult<()> {
        let state = self.load_state().await?;
        if state.is_dirty() {
            return Err(HistoryError::internal(
                "UpdateWorkflowExecutionAsActive is not ported yet (close transaction over EngineMutableState)",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ExecutionContext for EngineExecutionContext {
    fn get_execution(&self) -> WorkflowExecution {
        self.execution.clone()
    }
    fn get_workflow_execution(&self) -> Option<MutableStateRef> {
        self.state.lock().unwrap().clone().map(|s| s as MutableStateRef)
    }
    async fn load_workflow_execution(&self) -> HResult<MutableStateRef> {
        Ok(self.load_state().await? as MutableStateRef)
    }
    async fn update_workflow_execution_tasks(&self, _now: i64) -> HResult<()> {
        Err(HistoryError::internal(
            "UpdateWorkflowExecutionTasks is not ported yet (close transaction over EngineMutableState)",
        ))
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
    pub fn new(core: Arc<EngineCore>) -> Arc<Self> {
        Arc::new(Self { core, entries: Mutex::new(HashMap::new()), max_size: 10_000 })
    }

    /// Go `validateWorkflowExecutionInfo`: fills the current run id when empty.
    async fn validate(&self, domain_id: &str, execution: &WorkflowExecution) -> HResult<WorkflowExecution> {
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
                return Err(HistoryError::internal(
                    "Failed to add decision scheduled event. (AddDecisionTaskScheduledEvent is not ported yet)",
                ));
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
