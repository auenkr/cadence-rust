//! Go `service/matching/handler/handler.go` host rate limiters: a
//! `MultiStageRateLimiter` of a dynamic global limiter and per-domain dynamic
//! limiters that fall back to the global rps when the domain rps is not
//! positive (`dynamicquotas.NewFallbackDynamicRateLimiterFactory`).
use crate::matching_limiter::MonotonicClock;
use cadence_common::rate_limiter::RateLimiter;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Go `errMatchingHostThrottle` message.
pub const HOST_THROTTLE_MESSAGE: &str = "Matching host rps exceeded";
/// Go `quotas` dynamic limiter refresh TTL (5s) and minimum burst.
const TTL_NANOS: i64 = 5_000_000_000;
const MIN_BURST: i64 = 1;

pub type RpsFn = Arc<dyn Fn() -> f64 + Send + Sync>;
pub type DomainRpsFn = Arc<dyn Fn(&str) -> i64 + Send + Sync>;
pub type NowFn = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Go `quotas.DynamicRateLimiter`.
struct DynamicLimiter {
    rps: RpsFn,
    now: NowFn,
    state: Mutex<(RateLimiter, i64)>,
}

fn limit_and_burst(rps: f64) -> (f64, i64) {
    let burst = (rps.ceil() as i64).max(MIN_BURST);
    // Zero rps zeroes the burst to cut off new permits immediately.
    (rps, if rps == 0.0 { 0 } else { burst })
}

impl DynamicLimiter {
    fn new(rps: RpsFn, now: NowFn) -> Self {
        let (limit, burst) = limit_and_burst(rps());
        let started = now();
        Self {
            rps,
            now,
            state: Mutex::new((RateLimiter::new(limit, burst), started)),
        }
    }

    fn refresh(&self, state: &mut (RateLimiter, i64), now: i64) {
        // Go: `now.After(lastUpdated.Add(ttl-1))`.
        if now > state.1 + TTL_NANOS - 1 {
            state.1 = now;
            let (limit, burst) = limit_and_burst((self.rps)());
            state.0.set_limit_and_burst(limit, burst);
        }
    }

    /// Go `Reserve()` followed by `Allow()`: a token taken only when it needs
    /// no wait. The caller returns it with `release` if the request is denied.
    fn reserve(&self) -> Option<cadence_common::rate_limiter::Reservation> {
        let now = (self.now)();
        let mut state = self.state.lock().expect("host limiter lock");
        self.refresh(&mut state, now);
        let reservation = state.0.reserve(now);
        reservation.allowed().then_some(reservation)
    }

    fn release(&self, reservation: &cadence_common::rate_limiter::Reservation) {
        self.state
            .lock()
            .expect("host limiter lock")
            .0
            .used(reservation, false);
    }

    fn allow(&self) -> bool {
        let now = (self.now)();
        let mut state = self.state.lock().expect("host limiter lock");
        self.refresh(&mut state, now);
        state.0.allow(now)
    }
}

/// Go `quotas.NewMultiStageRateLimiter(global, domainCollection, nil)`.
pub struct HostRateLimiter {
    global: DynamicLimiter,
    domains: Mutex<HashMap<String, Arc<DynamicLimiter>>>,
    domain_rps: DomainRpsFn,
    fallback_rps: RpsFn,
    now: NowFn,
}

impl HostRateLimiter {
    /// `domain_rps` is the domain-filtered property; `global_rps` is both the
    /// global limit and the fallback when the domain property is not positive.
    pub fn new(global_rps: RpsFn, domain_rps: DomainRpsFn) -> Self {
        let clock = MonotonicClock::new();
        Self::with_clock(global_rps, domain_rps, Arc::new(move || clock.now()))
    }

    pub fn with_clock(global_rps: RpsFn, domain_rps: DomainRpsFn, now: NowFn) -> Self {
        Self {
            global: DynamicLimiter::new(global_rps.clone(), now.clone()),
            domains: Mutex::new(HashMap::new()),
            domain_rps,
            fallback_rps: global_rps,
            now,
        }
    }

    fn domain_limiter(&self, domain: &str) -> Arc<DynamicLimiter> {
        let mut domains = self.domains.lock().expect("host limiter domains lock");
        domains
            .entry(domain.to_owned())
            .or_insert_with(|| {
                let primary = self.domain_rps.clone();
                let fallback = self.fallback_rps.clone();
                let name = domain.to_owned();
                Arc::new(DynamicLimiter::new(
                    Arc::new(move || {
                        let limit = primary(&name);
                        if limit > 0 {
                            limit as f64
                        } else {
                            fallback()
                        }
                    }),
                    self.now.clone(),
                ))
            })
            .clone()
    }

    /// Go `MultiStageRateLimiter.Allow(quotas.Info{Domain: domain})`.
    pub fn allow(&self, domain: &str) -> bool {
        let held = if domain.is_empty() {
            None
        } else {
            let limiter = self.domain_limiter(domain);
            match limiter.reserve() {
                Some(reservation) => Some((limiter, reservation)),
                None => return false,
            }
        };
        let allowed = self.global.allow();
        if !allowed {
            if let Some((limiter, reservation)) = &held {
                limiter.release(reservation);
            }
        }
        allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn clock() -> (Arc<AtomicI64>, NowFn) {
        let time = Arc::new(AtomicI64::new(1_000_000_000_000));
        let reader = time.clone();
        (time, Arc::new(move || reader.load(Ordering::SeqCst)))
    }

    fn fixed(rps: f64) -> RpsFn {
        Arc::new(move || rps)
    }

    #[test]
    fn global_burst_is_ceil_rps_then_denies() {
        let (_, now) = clock();
        let limiter = HostRateLimiter::with_clock(fixed(3.0), Arc::new(|_| 0), now);
        assert!(limiter.allow(""));
        assert!(limiter.allow(""));
        assert!(limiter.allow(""));
        assert!(!limiter.allow(""));
    }

    #[test]
    fn tokens_refill_with_time() {
        let (time, now) = clock();
        let limiter = HostRateLimiter::with_clock(fixed(1.0), Arc::new(|_| 0), now);
        assert!(limiter.allow(""));
        assert!(!limiter.allow(""));
        time.fetch_add(1_000_000_000, Ordering::SeqCst);
        assert!(limiter.allow(""));
    }

    #[test]
    fn zero_rps_denies_everything() {
        let (_, now) = clock();
        let limiter = HostRateLimiter::with_clock(fixed(0.0), Arc::new(|_| 0), now);
        assert!(!limiter.allow(""));
        assert!(!limiter.allow("domain"));
    }

    #[test]
    fn domain_limit_falls_back_to_global_when_not_positive() {
        let (_, now) = clock();
        let limiter = HostRateLimiter::with_clock(fixed(2.0), Arc::new(|_| 0), now);
        assert!(limiter.allow("a"));
        assert!(limiter.allow("a"));
        assert!(!limiter.allow("a"));
    }

    #[test]
    fn positive_domain_limit_is_enforced_per_domain() {
        let (_, now) = clock();
        let domain_rps: DomainRpsFn = Arc::new(|domain| if domain == "a" { 1 } else { 0 });
        let limiter = HostRateLimiter::with_clock(fixed(100.0), domain_rps, now);
        assert!(limiter.allow("a"));
        assert!(!limiter.allow("a"));
        // Domain "b" falls back to the global rps and is unaffected by "a".
        assert!(limiter.allow("b"));
    }

    #[test]
    fn domain_token_is_returned_when_global_denies() {
        let (time, now) = clock();
        let domain_rps: DomainRpsFn = Arc::new(|_| 1);
        let limiter = HostRateLimiter::with_clock(fixed(1.0), domain_rps, now.clone());
        assert!(limiter.allow(""));
        // Global is exhausted: "b" passes its domain stage, then is denied.
        assert!(!limiter.allow("b"));
        let tokens = limiter
            .domain_limiter("b")
            .state
            .lock()
            .unwrap()
            .0
            .tokens(now());
        assert_eq!(tokens, 1.0);
        time.fetch_add(1_000_000_000, Ordering::SeqCst);
        assert!(limiter.allow("b"));
    }

    #[test]
    fn rps_change_applies_after_ttl() {
        let (time, now) = clock();
        let rps = Arc::new(AtomicI64::new(1));
        let source = rps.clone();
        let limiter = HostRateLimiter::with_clock(
            Arc::new(move || source.load(Ordering::SeqCst) as f64),
            Arc::new(|_| 0),
            now,
        );
        assert!(limiter.allow(""));
        assert!(!limiter.allow(""));
        rps.store(1000, Ordering::SeqCst);
        // Before the 5s TTL the old limit still applies.
        time.fetch_add(1_000_000_000, Ordering::SeqCst);
        assert!(limiter.allow(""));
        assert!(!limiter.allow(""));
        time.fetch_add(5_000_000_000, Ordering::SeqCst);
        assert!(limiter.allow(""));
        time.fetch_add(10_000_000, Ordering::SeqCst);
        assert!(limiter.allow(""));
        assert!(limiter.allow(""));
    }
}
