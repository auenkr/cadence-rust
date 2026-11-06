//! Port of go/service/history/service.go: the History `Service` (handler chain
//! construction, start, and the graceful `Stop` sequence).
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cadence_service_core::ringpop_tchannel::RingpopHandler;
use tokio::sync::Notify;

use crate::config_collection::PropDomain;
use crate::handler::{Handler, HandlerImpl};
use crate::wiring::assembly::controller::ShardRegistry;
use crate::wiring::assembly::env::HistoryEnv;
use crate::wiring::server::{HistoryServer, RunningServer, ServerConfig};
use crate::workflowcache::{DomainNames, Limiter, LimiterFactory, WorkflowCache};
use crate::wrappers::ratelimited::{HistoryHandler, WorkflowIdCache};

/// Go `workflowIDCacheTTL`.
pub const WORKFLOW_ID_CACHE_TTL: Duration = Duration::from_secs(1);
/// Go `workflowIDCacheMaxCount`.
pub const WORKFLOW_ID_CACHE_MAX_COUNT: usize = 10_000;

/// Go `common.DaemonStatus*`.
const STATUS_INITIALIZED: i32 = 0;
const STATUS_STARTED: i32 = 1;
const STATUS_STOPPED: i32 = 2;

/// Go `gossipPropagationDelay` / `gracePeriod` in `Service.Stop`.
const GOSSIP_PROPAGATION_DELAY: Duration = Duration::from_millis(400);
const GRACE_PERIOD: Duration = Duration::from_secs(2);

/// Go `common.SleepWithMinDuration(desired, available)`: sleeps `min(desired, available)`
/// and returns what is left of `available`.
pub async fn sleep_with_min_duration(desired: Duration, available: Duration) -> Duration {
    let d = desired.min(available);
    tokio::time::sleep(d).await;
    available - d
}

/// Membership-ring operations the service lifecycle needs (Go `membership.Resolver.EvictSelf`).
#[async_trait]
pub trait MembershipControl: Send + Sync {
    async fn evict_self(&self);
}

/// No-op membership (single host / no Ringpop).
pub struct NoMembership;

#[async_trait]
impl MembershipControl for NoMembership {
    async fn evict_self(&self) {}
}

/// Dynamic-rate limiter used by the workflow-ID cache (Go `quotas.NewSimpleDynamicRateLimiter`):
/// token bucket with burst `max(rps, 1)` whose rate is re-read on every call.
struct DynamicBucket {
    rps: Arc<dyn Fn() -> i64 + Send + Sync>,
    state: Mutex<(f64, Instant)>,
}

impl Limiter for DynamicBucket {
    fn allow(&self) -> bool {
        let rps = (self.rps)();
        let burst = rps.max(1) as f64;
        let mut st = self.state.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(st.1).as_secs_f64();
        st.1 = now;
        st.0 = (st.0 + elapsed * rps.max(0) as f64).min(burst);
        if st.0 >= 1.0 {
            st.0 -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Go `quotas.NewSimpleDynamicRateLimiterFactory(rps func(domain) int)`.
pub struct DynamicLimiterFactory(pub PropDomain<i64>);

impl LimiterFactory for DynamicLimiterFactory {
    fn limiter(&self, domain_name: &str) -> Arc<dyn Limiter> {
        let prop = self.0.clone();
        let domain = domain_name.to_string();
        let rps: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || prop(&domain));
        let burst = rps().max(1) as f64;
        Arc::new(DynamicBucket { rps, state: Mutex::new((burst, Instant::now())) })
    }
}

struct CacheDomainNames(Arc<dyn crate::failover::DomainCache>);

impl DomainNames for CacheDomainNames {
    fn domain_name(&self, domain_id: &str) -> Result<String, String> {
        self.0.get_domain_name(domain_id)
    }
}

struct CacheAdapter(Arc<WorkflowCache>);

impl WorkflowIdCache for CacheAdapter {
    fn allow_external(&self, domain: &str, workflow_id: &str) -> bool {
        self.0.allow_external(domain, workflow_id)
    }
}

/// The cadence-history service.
pub struct Service {
    status: AtomicI32,
    env: Arc<HistoryEnv>,
    handler: Arc<dyn Handler>,
    membership: Arc<dyn MembershipControl>,
    server: Mutex<Option<RunningServer>>,
    stop_c: Notify,
}

impl Service {
    /// Go `NewService` + the handler construction of `Start`.
    pub fn new(env: Arc<HistoryEnv>, registry: Arc<ShardRegistry>, membership: Arc<dyn MembershipControl>) -> Self {
        let domains: Arc<dyn crate::failover::DomainCache> = env.domain_cache.clone();
        let wf_cache = Arc::new(WorkflowCache::new(
            Arc::new(CacheDomainNames(domains)),
            Arc::new(DynamicLimiterFactory(env.config.workflow_id_external_rps.clone())),
            Arc::new(DynamicLimiterFactory(env.config.workflow_id_internal_rps.clone())),
            WORKFLOW_ID_CACHE_TTL,
            WORKFLOW_ID_CACHE_MAX_COUNT,
        ));
        let raw = Arc::new(HandlerImpl::new(registry, (env.config.rps)() as f64));
        let handler: Arc<dyn Handler> =
            Arc::new(HistoryHandler { wrapped: raw, workflow_id_cache: Arc::new(CacheAdapter(wf_cache)) });
        Self {
            status: AtomicI32::new(STATUS_INITIALIZED),
            env,
            handler,
            membership,
            server: Mutex::new(None),
            stop_c: Notify::new(),
        }
    }

    /// Go `Service.Start`: registers the Thrift and gRPC handlers on the listeners and
    /// starts the handler. Returns once started; use [`wait_stopped`](Self::wait_stopped)
    /// for Go's `<-s.stopC`.
    pub async fn start(
        &self,
        thrift: tokio::net::TcpListener,
        grpc: tokio::net::TcpListener,
        ringpop: Option<RingpopHandler>,
        max_message_size: usize,
    ) -> anyhow::Result<()> {
        if self.status.compare_exchange(STATUS_INITIALIZED, STATUS_STARTED, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Ok(());
        }
        tracing::info!("history starting");
        let thrift_addr = thrift.local_addr()?;
        let grpc_addr = grpc.local_addr()?;
        let mut cfg = ServerConfig::new(thrift_addr, grpc_addr);
        cfg.max_message_size = max_message_size;
        // Host-level rate limiting is done by HandlerImpl (Go handler.go), not the transport.
        cfg.rps = 0.0;
        cfg.ringpop = ringpop;
        let running = HistoryServer::start_with_listeners(cfg, self.handler.clone(), thrift, grpc).await?;
        *self.server.lock().unwrap() = Some(running);
        tracing::info!("history started");
        Ok(())
    }

    /// Go `<-s.stopC`.
    pub async fn wait_stopped(&self) {
        if self.status.load(Ordering::SeqCst) == STATUS_STOPPED {
            return;
        }
        self.stop_c.notified().await;
    }

    /// Go `Service.Stop` (graceful shutdown sequence).
    pub async fn stop(&self) {
        if self.status.compare_exchange(STATUS_STARTED, STATUS_STOPPED, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        // 1. remove self from the membership ring
        // 2. wait for other members to discover we are going down
        // 3-5. stop acquiring shards, wait for ownership transfer, reject new requests
        // 6. wait for grace period
        // 7. force stop the whole world and return
        let mut remaining = (self.env.config.shutdown_drain_duration)();
        tracing::info!("ShutdownHandler: Evicting self from membership ring");
        self.membership.evict_self().await;
        tracing::info!("ShutdownHandler: Waiting for others to discover I am unhealthy");
        remaining = sleep_with_min_duration(GOSSIP_PROPAGATION_DELAY, remaining).await;
        remaining = self.handler.prepare_to_stop(remaining).await;
        let _ = sleep_with_min_duration(GRACE_PERIOD, remaining).await;
        self.stop_c.notify_waiters();
        let running = self.server.lock().unwrap().take();
        if let Some(running) = running {
            running.stop_now().await;
        }
        tracing::info!("history stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn sleep_with_min_duration_returns_remainder() {
        let left = sleep_with_min_duration(Duration::from_millis(400), Duration::from_secs(1)).await;
        assert_eq!(left, Duration::from_millis(600));
        let left = sleep_with_min_duration(Duration::from_secs(2), Duration::from_secs(1)).await;
        assert_eq!(left, Duration::ZERO);
    }

    #[test]
    fn dynamic_bucket_honours_burst_and_dynamic_rate() {
        let f = DynamicLimiterFactory(Arc::new(|_d: &str| 2));
        let l = f.limiter("d");
        assert!(l.allow());
        assert!(l.allow());
        assert!(!l.allow());
        let off = DynamicLimiterFactory(Arc::new(|_d: &str| 0)).limiter("d");
        assert!(off.allow()); // burst 1 as in rate.NewLimiter(0, 1)
        assert!(!off.allow());
    }
}
