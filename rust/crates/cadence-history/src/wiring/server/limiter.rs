//! Simple token-bucket host rate limiter (Go: history `ratelimited` wrapper uses
//! quotas; the per-host limit is applied before dispatch).
use std::sync::Mutex;
use std::time::Instant;

pub struct RateLimiter {
    rps: f64,
    state: Mutex<(f64, Instant)>,
}

impl RateLimiter {
    pub fn new(rps: f64) -> Self {
        Self { rps, state: Mutex::new((rps.max(1.0), Instant::now())) }
    }
    pub fn allow(&self) -> bool {
        if self.rps <= 0.0 {
            return true;
        }
        let mut s = self.state.lock().unwrap();
        let now = Instant::now();
        let burst = self.rps.max(1.0);
        s.0 = (s.0 + now.duration_since(s.1).as_secs_f64() * self.rps).min(burst);
        s.1 = now;
        if s.0 >= 1.0 {
            s.0 -= 1.0;
            true
        } else {
            false
        }
    }
}
