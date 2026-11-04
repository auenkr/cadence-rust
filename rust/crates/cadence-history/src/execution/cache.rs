//! Pinned workflow context cache orchestration from Go `execution/cache.go`.
use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};
use cadence_api::types::WorkflowExecution;
use super::context::WorkflowExecutionKey;

/// Backend supplies the common pinned TTL/size cache and context mutex. A get
/// or put pins the entry; every successful pin is paired with release, even
/// when locking fails. Current-execution reads use the persistence transient
/// retry policy, not history event retries.
pub trait ExecutionCacheBackend {
    type Context: Clone;
    type Error;
    fn get(&mut self, key: &WorkflowExecutionKey) -> Option<Self::Context>;
    fn put_if_not_exist(&mut self, key: &WorkflowExecutionKey, context: Self::Context) -> Result<Self::Context, Self::Error>;
    fn new_context(&mut self, key: &WorkflowExecutionKey) -> Self::Context;
    fn lock(&mut self, context: &Self::Context, timeout: Option<Duration>) -> Result<(), Self::Error>;
    fn unlock(&mut self, context: &Self::Context);
    fn clear(&mut self, context: &Self::Context);
    fn release(&mut self, key: &WorkflowExecutionKey);
    fn domain_name(&self, id: &str) -> Result<String, Self::Error>;
    fn current_execution_with_retry(&mut self, domain_id: &str, domain_name: &str, workflow_id: &str) -> Result<String, Self::Error>;
    fn valid_run_id(&self, id: &str) -> bool;
    fn bad_request(&self, message: &'static str) -> Self::Error;
    fn metric(&mut self, scope: &'static str, name: &'static str);
}

/// Cloneable release callback state, matching Go's atomic compare-and-swap:
/// only the first invocation clears/unlocks/unpins the context.
pub struct ReleaseHandle<C> {
    key: WorkflowExecutionKey, context: C, released: Arc<AtomicBool>, force_clear: bool, noop: bool,
}
impl<C: Clone> Clone for ReleaseHandle<C> {
    fn clone(&self) -> Self { Self { key: self.key.clone(), context: self.context.clone(), released: self.released.clone(), force_clear: self.force_clear, noop: self.noop } }
}
impl<C> ReleaseHandle<C> {
    fn new(key: WorkflowExecutionKey, context: C, force_clear: bool, noop: bool) -> Self { Self { key, context, released: Arc::new(AtomicBool::new(false)), force_clear, noop } }
    pub fn release<B: ExecutionCacheBackend<Context = C>>(&self, backend: &mut B, had_error: bool) {
        if self.noop || self.released.swap(true, Ordering::AcqRel) { return; }
        if had_error || self.force_clear || std::thread::panicking() { backend.clear(&self.context); }
        backend.unlock(&self.context);
        backend.release(&self.key);
    }
}
pub struct ExecutionCache<B> { pub backend: B, pub disabled: bool }
pub type CachedExecution<C> = (C, ReleaseHandle<C>);
pub type InspectedExecution<C> = (Option<C>, C, Option<ReleaseHandle<C>>, bool);
impl<B: ExecutionCacheBackend> ExecutionCache<B> {
    pub fn get_or_create_current_workflow_execution(&mut self, domain: &str, workflow: &str, timeout: Option<Duration>) -> Result<CachedExecution<B::Context>, B::Error> {
        const SCOPE: &str = "HistoryCacheGetOrCreateCurrentScope";
        self.backend.metric(SCOPE, "CacheRequests");
        let key = WorkflowExecutionKey { domain_id: domain.into(), workflow_id: workflow.into(), run_id: String::new() };
        self.get_or_create_internal(key, timeout, SCOPE, true)
    }
    pub fn get_and_create_workflow_execution(&mut self, domain: &str, execution: WorkflowExecution, timeout: Option<Duration>) -> Result<InspectedExecution<B::Context>, B::Error> {
        const SCOPE: &str = "HistoryCacheGetAndCreateScope";
        self.backend.metric(SCOPE, "CacheRequests");
        let key = match self.validate_workflow_execution_info(domain, execution) { Ok(key) => key, Err(err) => { self.backend.metric(SCOPE, "CacheFailures"); return Err(err); } };
        let cached = self.backend.get(&key);
        let release = if let Some(context) = &cached {
            if let Err(err) = self.backend.lock(context, timeout) {
                self.backend.release(&key); self.backend.metric(SCOPE, "CacheFailures"); self.backend.metric(SCOPE, "AcquireLockFailedCounter"); return Err(err);
            }
            Some(ReleaseHandle::new(key.clone(), context.clone(), false, false))
        } else { self.backend.metric(SCOPE, "CacheMissCounter"); None };
        let database = self.backend.new_context(&key);
        let hit = cached.is_some();
        Ok((cached, database, release, hit))
    }
    pub fn get_or_create_workflow_execution(&mut self, domain: &str, execution: WorkflowExecution, timeout: Option<Duration>) -> Result<CachedExecution<B::Context>, B::Error> {
        const SCOPE: &str = "HistoryCacheGetOrCreateScope";
        self.backend.metric(SCOPE, "CacheRequests");
        let key = match self.validate_workflow_execution_info(domain, execution) { Ok(key) => key, Err(err) => { self.backend.metric(SCOPE, "CacheFailures"); return Err(err); } };
        self.get_or_create_internal(key, timeout, SCOPE, false)
    }
    pub fn get_or_create_workflow_execution_for_background(&mut self, domain: &str, execution: WorkflowExecution) -> Result<CachedExecution<B::Context>, B::Error> { self.get_or_create_workflow_execution(domain, execution, None) }
    pub fn get_or_create_workflow_execution_with_timeout(&mut self, domain: &str, execution: WorkflowExecution, timeout: Duration) -> Result<CachedExecution<B::Context>, B::Error> { self.get_or_create_workflow_execution(domain, execution, Some(timeout)) }
    fn get_or_create_internal(&mut self, key: WorkflowExecutionKey, timeout: Option<Duration>, scope: &'static str, force_clear: bool) -> Result<CachedExecution<B::Context>, B::Error> {
        if self.disabled { let context = self.backend.new_context(&key); let release = ReleaseHandle::new(key, context.clone(), false, true); return Ok((context, release)); }
        let context = if let Some(context) = self.backend.get(&key) { context } else {
            self.backend.metric(scope, "CacheMissCounter");
            let context = self.backend.new_context(&key);
            match self.backend.put_if_not_exist(&key, context) { Ok(context) => context, Err(err) => { self.backend.metric(scope, "CacheFailures"); return Err(err); } }
        };
        let release = ReleaseHandle::new(key.clone(), context.clone(), force_clear, false);
        if let Err(err) = self.backend.lock(&context, timeout) {
            self.backend.release(&key); self.backend.metric(scope, "CacheFailures"); self.backend.metric(scope, "AcquireLockFailedCounter"); return Err(err);
        }
        Ok((context, release))
    }
    fn validate_workflow_execution_info(&mut self, domain: &str, mut execution: WorkflowExecution) -> Result<WorkflowExecutionKey, B::Error> {
        if execution.workflow_id.is_empty() { return Err(self.backend.bad_request("Can't load workflow execution.  WorkflowId not set.")); }
        let name = self.backend.domain_name(domain)?;
        if execution.run_id.is_empty() { execution.run_id = self.backend.current_execution_with_retry(domain, &name, &execution.workflow_id)?; }
        else if !self.backend.valid_run_id(&execution.run_id) { return Err(self.backend.bad_request("RunID is not valid UUID.")); }
        Ok(WorkflowExecutionKey::new(domain, &execution))
    }
}
