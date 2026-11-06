//! Minimal stand-ins for the `execution` package surface used by task executors.
//! TODO(link): replace by `crate::execution::{Context, MutableState}`.

use std::sync::Arc;

use async_trait::async_trait;

use super::deps::*;

#[derive(Clone, Debug, Default)]
pub struct WorkflowExecutionInfo {
    pub decision_schedule_id: i64,
    pub decision_attempt: i64,
}

#[derive(Clone, Debug, Default)]
pub struct DecisionInfo {
    pub schedule_id: i64,
    pub started_id: i64,
}

#[derive(Clone, Debug, Default)]
pub struct ContinueAsNewWorkflowExecutionDecisionAttributes;

pub const DECISION_TASK_FAILED_CAUSE_FORCE_CLOSE_DECISION: &str = "ForceCloseDecision";

/// Go `execution.MutableState` (subset used by task_util; extended by executors).
#[async_trait]
pub trait MutableState: Send + Sync {
    fn get_execution_info(&self) -> WorkflowExecutionInfo;
    fn get_next_event_id(&self) -> i64;
    fn get_domain_entry(&self) -> Arc<DomainCacheEntry>;
    fn get_in_flight_decision(&self) -> Option<DecisionInfo>;
    fn add_timeout_workflow_event(&self, first_event_id: i64) -> Result<()>;
    async fn add_continue_as_new_event(
        &self,
        first_event_id: i64,
        decision_finished_event_id: i64,
        parent_domain_name: &str,
        attrs: &ContinueAsNewWorkflowExecutionDecisionAttributes,
    ) -> Result<Arc<dyn MutableState>>;
}

/// Go `execution.Context`.
#[async_trait]
pub trait ExecutionContext: Send + Sync {
    async fn load_workflow_execution(&self) -> Result<Arc<dyn MutableState>>;
    fn clear(&self);
}

/// Go `execution.FailDecision`.
/// TODO(link): replace by execution::fail_decision
pub fn fail_decision(_ms: &dyn MutableState, _decision: &DecisionInfo, _cause: &str) -> Result<()> {
    Err(TaskError::Other("fail_decision not linked".into()))
}

// ---- persistence DLQ request shims (TODO(link): replace by cadence-persistence types) ----
#[derive(Clone, Debug)]
pub struct CreateHistoryDLQTaskRequest {
    pub shard_id: i32,
    pub domain_id: String,
    pub domain_name: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
    pub task: Arc<dyn PersistenceTask>,
}

#[derive(Clone, Debug)]
pub struct CreateHistoryDLQAckLevelRequest {
    pub shard_id: i32,
    pub domain_id: String,
    pub cluster_attribute_scope: String,
    pub cluster_attribute_name: String,
    pub task_category: HistoryTaskCategory,
}

// ---- version histories shim ----
#[derive(Clone, Copy, Debug, Default)]
pub struct VersionHistoryItem {
    pub event_id: i64,
    pub version: i64,
}

pub trait VersionHistory: Send + Sync {
    fn get_last_item(&self) -> Result<VersionHistoryItem>;
}

pub trait VersionHistories: Send + Sync {
    fn get_current_version_history(&self) -> Result<Arc<dyn VersionHistory>>;
}

/// Extension used by standby executors; TODO(link): merge into `MutableState` when ported.
pub trait MutableStateVersionHistories {
    fn get_version_histories(&self) -> Option<Arc<dyn VersionHistories>>;
}

// ---- quotas shim. TODO(link): replace by cadence_common::quotas Collection / Limiter ----
#[async_trait]
pub trait Limiter: Send + Sync {
    fn allow(&self) -> bool;
    async fn wait(&self, ctx: &tokio_util::sync::CancellationToken) -> Result<()>;
}

/// Go `quotas.ICollection[string]`.
pub trait LimiterCollection: Send + Sync {
    fn for_key(&self, key: &str) -> Arc<dyn Limiter>;
}

fn unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Go `quotas.DynamicRateLimiter` (ttl 5s, minBurst 1).
struct DynamicRateLimiter {
    rps: Box<dyn Fn() -> f64 + Send + Sync>,
    rl: std::sync::Mutex<cadence_common::rate_limiter::RateLimiter>,
    last_update: std::sync::atomic::AtomicI64,
}

const DYNAMIC_LIMITER_TTL_NANOS: i64 = 5_000_000_000;
const DYNAMIC_LIMITER_MIN_BURST: i64 = 1;

fn limit_and_burst(rps: f64) -> (f64, i64) {
    let burst = (rps.ceil() as i64).max(DYNAMIC_LIMITER_MIN_BURST);
    // 0 rps zeroes the burst to immediately cut off new permits.
    (rps, if rps == 0.0 { 0 } else { burst })
}

impl DynamicRateLimiter {
    fn new(rps: Box<dyn Fn() -> f64 + Send + Sync>) -> Self {
        let (l, b) = limit_and_burst(rps());
        Self {
            rps,
            rl: std::sync::Mutex::new(cadence_common::rate_limiter::RateLimiter::new(l, b)),
            last_update: std::sync::atomic::AtomicI64::new(unix_nanos()),
        }
    }

    fn maybe_refresh(&self) {
        use std::sync::atomic::Ordering;
        let now = unix_nanos();
        let last = self.last_update.load(Ordering::SeqCst);
        if now > last + DYNAMIC_LIMITER_TTL_NANOS - 1
            && self
                .last_update
                .compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let (l, b) = limit_and_burst((self.rps)());
            self.rl.lock().unwrap().set_limit_and_burst(l, b);
        }
    }
}

#[async_trait]
impl Limiter for DynamicRateLimiter {
    fn allow(&self) -> bool {
        self.maybe_refresh();
        self.rl.lock().unwrap().allow(unix_nanos())
    }

    async fn wait(&self, ctx: &tokio_util::sync::CancellationToken) -> Result<()> {
        self.maybe_refresh();
        if ctx.is_cancelled() {
            return Err(TaskError::Other("context canceled".into()));
        }
        let reservation = {
            let now = unix_nanos();
            self.rl
                .lock()
                .unwrap()
                .reserve_wait(now, None)
                .map_err(|e| TaskError::Other(format!("rate: Wait: {e:?}")))?
        };
        let delay = reservation.delay_nanos();
        if delay > 0 {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_nanos(delay as u64)) => {}
                _ = ctx.cancelled() => {
                    self.rl.lock().unwrap().cancel(&reservation);
                    return Err(TaskError::Other("context canceled".into()));
                }
            }
        }
        Ok(())
    }
}

/// Go `quotas.Collection`: lazily creates one limiter per key.
struct SimpleCollection {
    rps: Arc<dyn Fn(&str) -> i64 + Send + Sync>,
    limiters: std::sync::Mutex<std::collections::HashMap<String, Arc<dyn Limiter>>>,
}

impl LimiterCollection for SimpleCollection {
    fn for_key(&self, key: &str) -> Arc<dyn Limiter> {
        let mut m = self.limiters.lock().unwrap();
        if let Some(l) = m.get(key) {
            return l.clone();
        }
        let rps = self.rps.clone();
        let k = key.to_string();
        let l: Arc<dyn Limiter> = Arc::new(DynamicRateLimiter::new(Box::new(move || rps(&k) as f64)));
        m.insert(key.to_string(), l.clone());
        l
    }
}

/// Go `quotas.NewCollection(dynamicquotas.NewSimpleDynamicRateLimiterFactory(rps))`.
pub fn new_simple_dynamic_collection(
    rps: Arc<dyn Fn(&str) -> i64 + Send + Sync>,
) -> Arc<dyn LimiterCollection> {
    Arc::new(SimpleCollection {
        rps,
        limiters: std::sync::Mutex::new(std::collections::HashMap::new()),
    })
}
