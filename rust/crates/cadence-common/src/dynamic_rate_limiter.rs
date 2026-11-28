//! Go `common/quotas.DynamicRateLimiter` (`NewDynamicRateLimiter`): an
//! `x/time/rate` token bucket (starts full) whose rps is re-read at most every
//! 5s, with burst `max(ceil(rps), 1)` and burst 0 when rps is 0.
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::rate_limiter::{RateLimiter, Reservation};

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

impl DynamicRateLimiter {
    /// Go `Wait(ctx)` with an optional maximum wait (ctx deadline): reserves a
    /// token and returns the delay plus the reservation so the caller can
    /// `cancel` it if its sleep is interrupted. `None` mirrors Go's
    /// errWaitZeroBurst / errWaitLongerThanDeadline (nothing consumed).
    pub fn reserve_at(
        &self,
        now: i64,
        max_wait: Option<Duration>,
    ) -> Option<(Duration, Reservation)> {
        self.maybe_refresh(now);
        let deadline = max_wait
            .filter(|d| *d < Duration::from_nanos(i64::MAX as u64))
            .map(|d| now.saturating_add(d.as_nanos() as i64));
        let r = self
            .rl
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reserve_wait(now, deadline)
            .ok()?;
        Some((Duration::from_nanos(r.delay_nanos().max(0) as u64), r))
    }

    /// Real-clock `reserve_at`.
    pub fn reserve_now(&self, max_wait: Option<Duration>) -> Option<(Duration, Reservation)> {
        self.reserve_at(unix_nanos(), max_wait)
    }

    /// Go `res.CancelAt(latestNow)` after an interrupted wait.
    pub fn cancel(&self, r: &Reservation) {
        self.rl.lock().unwrap_or_else(|e| e.into_inner()).cancel(r);
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

    const MS: i64 = 1_000_000;

    fn shared(rps: f64) -> (std::sync::Arc<std::sync::Mutex<f64>>, DynamicRateLimiter) {
        let cell = std::sync::Arc::new(std::sync::Mutex::new(rps));
        let c = cell.clone();
        (cell, DynamicRateLimiter::new(Box::new(move || *c.lock().unwrap())))
    }

    /// History transferProcessorMaxPollRPS default: 20 rps, burst 20 (Go x/time/rate).
    #[test]
    fn history_default_20rps_burst_20_table() {
        // (offset_ms, allows attempted, expected allowed count)
        let table = [(0, 25, 20), (50, 3, 1), (100, 1, 1), (1100, 25, 20), (1100, 1, 0)];
        let (_c, l) = shared(20.0);
        let base = unix_nanos();
        for (off, tries, want) in table {
            let got = (0..tries).filter(|_| l.allow_at(base + off * MS)).count();
            assert_eq!(got, want, "offset {off}ms");
        }
    }

    #[test]
    fn wait_delays_are_spaced_at_rate_after_burst() {
        let (_c, l) = shared(20.0);
        let base = unix_nanos();
        for _ in 0..20 {
            assert_eq!(l.reserve_at(base, None).unwrap().0, Duration::ZERO);
        }
        for i in 1..=3u64 {
            assert_eq!(l.reserve_at(base, None).unwrap().0, Duration::from_millis(50 * i));
        }
    }

    #[test]
    fn wait_longer_than_deadline_consumes_nothing() {
        let (_c, l) = shared(20.0);
        let base = unix_nanos();
        for _ in 0..20 {
            assert!(l.allow_at(base));
        }
        assert!(l.reserve_at(base, Some(Duration::from_millis(40))).is_none());
        assert_eq!(
            l.reserve_at(base, Some(Duration::from_millis(50))).unwrap().0,
            Duration::from_millis(50)
        );
    }

    #[test]
    fn fractional_tokens_accumulate() {
        // 2.5 rps => burst 3; one token per 400ms.
        let (_c, l) = shared(2.5);
        let base = unix_nanos();
        for _ in 0..3 {
            assert!(l.allow_at(base));
        }
        assert!(!l.allow_at(base + 200 * MS)); // 0.5 token
        assert!(l.allow_at(base + 400 * MS)); // 1.0 token
        assert!(!l.allow_at(base + 600 * MS)); // 0.5 token
        assert!(l.allow_at(base + 800 * MS));
    }

    #[test]
    fn dynamic_rps_decrease_clamps_bucket_to_new_burst_after_ttl() {
        let (c, l) = shared(20.0);
        let base = unix_nanos();
        for _ in 0..5 {
            assert!(l.allow_at(base));
        }
        *c.lock().unwrap() = 10.0;
        // Before TTL the old limit still applies (15 tokens left).
        assert_eq!((0..30).filter(|_| l.allow_at(base + 4 * 1000 * MS)).count(), 20);
        // After TTL the new rps (burst 10) applies; bucket refills capped at 10.
        let later = base + DYNAMIC_LIMITER_TTL_NANOS + 1000 * MS;
        assert_eq!((0..30).filter(|_| l.allow_at(later)).count(), 10);
    }

    #[test]
    fn dynamic_rps_increase_raises_burst_and_refills_at_new_rate() {
        let (c, l) = shared(1.0);
        let base = unix_nanos();
        assert!(l.allow_at(base) && !l.allow_at(base));
        *c.lock().unwrap() = 20.0;
        let later = base + DYNAMIC_LIMITER_TTL_NANOS + 1;
        // 5s at the new 20 rps fills to burst 20.
        assert_eq!((0..30).filter(|_| l.allow_at(later)).count(), 20);
    }

    #[test]
    fn dynamic_rps_zero_then_restored() {
        let (c, l) = shared(20.0);
        let base = unix_nanos();
        *c.lock().unwrap() = 0.0;
        let t1 = base + DYNAMIC_LIMITER_TTL_NANOS + 1;
        assert!(!l.allow_at(t1));
        assert!(l.reserve_at(t1, None).is_none());
        *c.lock().unwrap() = 20.0;
        let t2 = t1 + DYNAMIC_LIMITER_TTL_NANOS + 1;
        assert!(l.allow_at(t2));
    }

    #[test]
    fn limit_and_burst_table() {
        for (rps, want) in [(0.0, (0.0, 0)), (0.2, (0.2, 1)), (2.5, (2.5, 3)), (20.0, (20.0, 20))] {
            assert_eq!(limit_and_burst(rps), want);
        }
    }
}
