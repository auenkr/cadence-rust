//! Go `common/quotas.DynamicRateLimiter` (`NewDynamicRateLimiter`): an
//! `x/time/rate` token bucket (starts full) whose rps is re-read at most every
//! 5s, with burst `max(ceil(rps), 1)` and burst 0 when rps is 0.
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::rate_limiter::RateLimiter;

/// Go `_defaultOpts.TTL`.
pub const DYNAMIC_LIMITER_TTL_NANOS: i64 = 5_000_000_000;
/// Go `_defaultOpts.MinBurst`.
pub const DYNAMIC_LIMITER_MIN_BURST: i64 = 1;

pub type RpsFn = Box<dyn Fn() -> f64 + Send + Sync>;

/// Go `getLimitAndBurst`.
pub fn limit_and_burst(rps: f64) -> (f64, i64) {
    let burst = (rps.ceil() as i64).max(DYNAMIC_LIMITER_MIN_BURST);
    (rps, if rps == 0.0 { 0 } else { burst })
}

fn unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub struct DynamicRateLimiter {
    rps: RpsFn,
    rl: Mutex<RateLimiter>,
    last_update: AtomicI64,
}

impl DynamicRateLimiter {
    pub fn new(rps: RpsFn) -> Self {
        let (l, b) = limit_and_burst(rps());
        Self {
            rps,
            rl: Mutex::new(RateLimiter::new(l, b)),
            last_update: AtomicI64::new(unix_nanos()),
        }
    }

    fn maybe_refresh(&self, now: i64) {
        let last = self.last_update.load(Ordering::SeqCst);
        if now > last + DYNAMIC_LIMITER_TTL_NANOS - 1
            && self
                .last_update
                .compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let (l, b) = limit_and_burst((self.rps)());
            self.rl
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .set_limit_and_burst(l, b);
        }
    }

    /// Go `Allow()`.
    pub fn allow(&self) -> bool {
        self.allow_at(unix_nanos())
    }

    fn allow_at(&self, now: i64) -> bool {
        self.maybe_refresh(now);
        self.rl.lock().unwrap_or_else(|e| e.into_inner()).allow(now)
    }

    /// Go `Wait(ctx)` without a deadline: reserves one token and returns how
    /// long to sleep, or `None` when Go's `Wait` errors (burst 0).
    pub fn reserve_delay(&self) -> Option<Duration> {
        self.reserve_delay_at(unix_nanos())
    }

    fn reserve_delay_at(&self, now: i64) -> Option<Duration> {
        self.maybe_refresh(now);
        let r = self
            .rl
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reserve_wait(now, None)
            .ok()?;
        Some(Duration::from_nanos(r.delay_nanos().max(0) as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_full_and_bursts_ceil_rps() {
        let l = DynamicRateLimiter::new(Box::new(|| 2.5));
        let now = unix_nanos();
        assert!(l.allow_at(now) && l.allow_at(now) && l.allow_at(now));
        assert!(!l.allow_at(now));
    }

    #[test]
    fn wait_bursts_without_spacing() {
        let l = DynamicRateLimiter::new(Box::new(|| 10.0));
        let now = unix_nanos();
        for _ in 0..10 {
            assert_eq!(l.reserve_delay_at(now), Some(Duration::ZERO));
        }
        let d = l.reserve_delay_at(now).unwrap();
        assert!(d > Duration::ZERO && d <= Duration::from_millis(100));
    }

    #[test]
    fn zero_rps_cuts_off_permits() {
        let l = DynamicRateLimiter::new(Box::new(|| 0.0));
        let now = unix_nanos();
        assert!(!l.allow_at(now));
        assert_eq!(l.reserve_delay_at(now), None);
    }

    #[test]
    fn rps_refreshes_after_ttl() {
        let rps = std::sync::Arc::new(AtomicI64::new(1));
        let r2 = rps.clone();
        let l = DynamicRateLimiter::new(Box::new(move || r2.load(Ordering::SeqCst) as f64));
        let now = unix_nanos();
        assert!(l.allow_at(now) && !l.allow_at(now));
        rps.store(0, Ordering::SeqCst);
        let later = now + DYNAMIC_LIMITER_TTL_NANOS + 1;
        assert!(!l.allow_at(later));
    }
}
