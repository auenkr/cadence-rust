//! Matching persistence host and cluster QPS enforcement.
//!
//! Go constructs one dynamic limiter per configured datastore and shares it
//! among that datastore's task, domain, and semaphore managers. The effective
//! rate is refreshed every five seconds from matching.persistenceMaxQPS and
//! matching.persistenceGlobalMaxQPS.

use crate::admin::DynamicConfigOverlay;
use cadence_common::rate_limiter::RateLimiter;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const HOST_QPS_DEFAULT: i64 = 3000;
const GLOBAL_QPS_DEFAULT: i64 = 0;
const REFRESH_NANOS: i64 = 5_000_000_000;

type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PersistenceRateSnapshot {
    pub host_qps: f64,
    pub global_qps: f64,
    pub matching_members: usize,
    pub effective_qps: f64,
}

struct LimiterState {
    limiter: RateLimiter,
    last_refresh_nanos: i64,
    snapshot: PersistenceRateSnapshot,
}

/// A per-process limiter shared by all Matching persistence clients using the
/// configured default datastore. Rejections are counted for observability.
pub struct MatchingPersistenceLimiter {
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    matching_members: Arc<AtomicUsize>,
    clock: Clock,
    state: Mutex<LimiterState>,
    enabled: bool,
    rejected: AtomicU64,
}

impl std::fmt::Debug for MatchingPersistenceLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchingPersistenceLimiter")
            .field("rejected", &self.rejected())
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl MatchingPersistenceLimiter {
    pub fn new(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        matching_members: Arc<AtomicUsize>,
    ) -> Arc<Self> {
        Self::with_clock(overlay, matching_members, Arc::new(monotonic_nanos))
    }

    fn with_clock(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        matching_members: Arc<AtomicUsize>,
        clock: Clock,
    ) -> Arc<Self> {
        let now = clock();
        let snapshot = read_rate_snapshot(&overlay, matching_members.load(Ordering::Relaxed));
        let enabled = snapshot.effective_qps > 0.0;
        let mut limiter = RateLimiter::new(snapshot.effective_qps, burst(snapshot.effective_qps));
        limiter.tokens(now);
        Arc::new(Self {
            overlay,
            matching_members,
            clock,
            state: Mutex::new(LimiterState {
                limiter,
                last_refresh_nanos: now,
                snapshot,
            }),
            enabled,
            rejected: AtomicU64::new(0),
        })
    }

    /// Consume one backend-operation permit without waiting, as Go's
    /// persistence wrappers do. Rejected calls must not reach the backend.
    pub fn allow(&self) -> bool {
        // Go's persistence factory omits the wrapper if the initial dynamic
        // rate is zero. A later config update does not install one in place.
        if !self.enabled {
            return true;
        }
        let now = (self.clock)();
        let mut state = self
            .state
            .lock()
            .expect("Matching persistence limiter lock");
        if now.saturating_sub(state.last_refresh_nanos) >= REFRESH_NANOS {
            let snapshot =
                read_rate_snapshot(&self.overlay, self.matching_members.load(Ordering::Relaxed));
            state
                .limiter
                .set_limit_and_burst(snapshot.effective_qps, burst(snapshot.effective_qps));
            state.snapshot = snapshot;
            state.last_refresh_nanos = now;
        }
        let allowed = state.limiter.allow(now);
        if !allowed {
            self.rejected.fetch_add(1, Ordering::Relaxed);
        }
        allowed
    }

    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> PersistenceRateSnapshot {
        self.state
            .lock()
            .expect("Matching persistence limiter lock")
            .snapshot
    }
}

fn burst(qps: f64) -> i64 {
    if qps == 0.0 {
        0
    } else {
        qps.ceil().max(1.0).min(i64::MAX as f64) as i64
    }
}

fn read_rate_snapshot(
    overlay: &Arc<Mutex<DynamicConfigOverlay>>,
    matching_members: usize,
) -> PersistenceRateSnapshot {
    let overlay = overlay.lock().expect("admin dynamic config lock");
    let host_values = overlay.property_values("matching.persistenceMaxQPS");
    let host_qps = if host_values.is_empty() {
        HOST_QPS_DEFAULT
    } else {
        cadence_persistence::dynamic_config::integer(host_values, None, HOST_QPS_DEFAULT)
    }
    .max(0) as f64;
    let global_values = overlay.property_values("matching.persistenceGlobalMaxQPS");
    let global_qps = if global_values.is_empty() {
        GLOBAL_QPS_DEFAULT
    } else {
        cadence_persistence::dynamic_config::integer(global_values, None, GLOBAL_QPS_DEFAULT)
    }
    .max(0) as f64;
    let effective_qps = if global_qps > 0.0 && matching_members > 0 {
        (global_qps / matching_members as f64)
            .max(1.0)
            .min(host_qps)
    } else {
        host_qps
    };
    PersistenceRateSnapshot {
        host_qps,
        global_qps,
        matching_members,
        effective_qps,
    }
}

fn monotonic_nanos() -> i64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
    use std::sync::atomic::AtomicI64;

    fn set(overlay: &Arc<Mutex<DynamicConfigOverlay>>, key: &str, value: i64) {
        overlay.lock().unwrap().update(
            false,
            key,
            vec![DynamicConfigValue {
                filters: vec![],
                value: Some(DataBlob {
                    encoding_type: Some(EncodingType::JSON),
                    data: value.to_string().into_bytes(),
                }),
            }],
        );
    }

    #[test]
    fn persistence_rate_is_shared_live_and_uses_per_member_global_cap() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let members = Arc::new(AtomicUsize::new(2));
        set(&overlay, "matching.persistenceMaxQPS", 10);
        set(&overlay, "matching.persistenceGlobalMaxQPS", 6);
        let now = Arc::new(AtomicI64::new(0));
        let clock_now = now.clone();
        let limiter = MatchingPersistenceLimiter::with_clock(
            overlay.clone(),
            members.clone(),
            Arc::new(move || clock_now.load(Ordering::SeqCst)),
        );

        let initial = limiter.snapshot();
        assert_eq!(initial.effective_qps, 3.0);
        assert!(limiter.allow());
        assert!(limiter.allow());
        assert!(limiter.allow());
        assert!(!limiter.allow());
        assert_eq!(limiter.rejected(), 1);

        set(&overlay, "matching.persistenceMaxQPS", 20);
        set(&overlay, "matching.persistenceGlobalMaxQPS", 0);
        members.store(1, Ordering::SeqCst);
        now.store(REFRESH_NANOS, Ordering::SeqCst);
        assert!(limiter.allow());
        assert_eq!(limiter.snapshot().effective_qps, 20.0);
    }

    #[test]
    fn zero_global_qps_falls_back_to_go_host_default() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let limiter = MatchingPersistenceLimiter::with_clock(
            overlay,
            Arc::new(AtomicUsize::new(3)),
            Arc::new(|| 0),
        );
        assert_eq!(limiter.snapshot().host_qps, 3000.0);
        assert_eq!(limiter.snapshot().effective_qps, 3000.0);
    }

    #[test]
    fn zero_rate_at_resource_construction_leaves_the_store_unwrapped() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(&overlay, "matching.persistenceMaxQPS", 0);
        let limiter = MatchingPersistenceLimiter::with_clock(
            overlay.clone(),
            Arc::new(AtomicUsize::new(1)),
            Arc::new(|| 0),
        );
        set(&overlay, "matching.persistenceMaxQPS", 1);
        assert!(limiter.allow());
        assert_eq!(limiter.rejected(), 0);
    }
}
