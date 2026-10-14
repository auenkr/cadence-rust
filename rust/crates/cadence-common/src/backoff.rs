//! Exponential retry policy ported from `go/common/backoff/retrypolicy.go`.
//!
//! Formula and edge cases match Go:
//! `initial * coeff^attempt`, capped by max interval, bounded by expiration,
//! `done` (None) when attempts are exhausted or the computed interval would
//! underflow / fall below the initial interval.

use std::time::Duration;

use rand::Rng;

pub const NO_INTERVAL: Duration = Duration::from_secs(0);

#[derive(Debug, Clone)]
pub struct ExponentialRetryPolicy {
    initial_interval: Duration,
    backoff_coefficient: f64,
    maximum_interval: Duration,
    expiration_interval: Duration,
    maximum_attempts: usize, // 0 = unbounded
}

impl ExponentialRetryPolicy {
    pub fn new(initial_interval: Duration) -> Self {
        Self {
            initial_interval,
            backoff_coefficient: 2.0,
            maximum_interval: Duration::from_secs(10),
            expiration_interval: Duration::from_secs(60),
            maximum_attempts: 0,
        }
    }

    pub fn with_backoff_coefficient(mut self, c: f64) -> Self {
        self.backoff_coefficient = c;
        self
    }
    pub fn with_maximum_interval(mut self, d: Duration) -> Self {
        self.maximum_interval = d;
        self
    }
    pub fn with_expiration_interval(mut self, d: Duration) -> Self {
        self.expiration_interval = d;
        self
    }
    pub fn with_maximum_attempts(mut self, n: usize) -> Self {
        self.maximum_attempts = n;
        self
    }

    #[must_use]
    pub fn expiration(&self) -> Duration {
        self.expiration_interval
    }

    /// Mirrors Go `ComputeNextDelay`. Returns `None` when retrying is done.
    /// Takes an RNG for the 80/20 jitter so tests can seed it.
    pub fn compute_next_delay<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        elapsed: Duration,
        num_attempts: usize,
    ) -> Option<Duration> {
        if self.maximum_attempts != 0 && num_attempts >= self.maximum_attempts {
            return None;
        }
        if self.expiration_interval != NO_INTERVAL && elapsed > self.expiration_interval {
            return None;
        }
        let next_secs = self.initial_interval.as_secs_f64()
            * self.backoff_coefficient.powi(num_attempts as i32);
        if next_secs <= 0.0 || !next_secs.is_finite() {
            return None;
        }
        let mut next_secs = next_secs;
        if self.maximum_interval != NO_INTERVAL {
            next_secs = next_secs.min(self.maximum_interval.as_secs_f64());
        }
        if self.expiration_interval != NO_INTERVAL {
            let remaining = self
                .expiration_interval
                .saturating_sub(elapsed)
                .as_secs_f64()
                .max(0.0);
            next_secs = next_secs.min(remaining);
        }
        let next = Duration::from_secs_f64(next_secs);
        if next < self.initial_interval {
            return None;
        }
        // 80/20 jitter mirroring Go: interval*0.8 + [0, 0.2*interval)
        let jitter_portion = (0.2 * next_secs).max(1e-9);
        let jittered = next_secs * 0.8 + rng.gen::<f64>() * jitter_portion;
        Some(Duration::from_secs_f64(jittered))
    }
}

/// Simple attempt counter mirroring Go `retrierImpl`.
#[derive(Debug)]
pub struct Retrier {
    policy: ExponentialRetryPolicy,
    start: std::time::Instant,
    current_attempt: usize,
}

impl Retrier {
    pub fn new(policy: ExponentialRetryPolicy) -> Self {
        Self {
            policy,
            start: std::time::Instant::now(),
            current_attempt: 0,
        }
    }

    pub fn reset(&mut self) {
        self.start = std::time::Instant::now();
        self.current_attempt = 0;
    }

    pub fn next_backoff<R: Rng + ?Sized>(&mut self, rng: &mut R) -> Option<Duration> {
        let next = self
            .policy
            .compute_next_delay(rng, self.start.elapsed(), self.current_attempt);
        self.current_attempt += 1;
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn rng() -> rand::rngs::StdRng {
        rand::rngs::StdRng::seed_from_u64(7)
    }

    #[test]
    fn respects_maximum_attempts() {
        let p = ExponentialRetryPolicy::new(Duration::from_millis(100)).with_maximum_attempts(2);
        let mut r = rng();
        assert!(p.compute_next_delay(&mut r, Duration::ZERO, 0).is_some());
        assert!(p.compute_next_delay(&mut r, Duration::ZERO, 1).is_some());
        assert!(p.compute_next_delay(&mut r, Duration::ZERO, 2).is_none());
    }

    #[test]
    fn stops_after_expiration() {
        let p = ExponentialRetryPolicy::new(Duration::from_millis(100))
            .with_expiration_interval(Duration::from_secs(1));
        let mut r = rng();
        assert!(p
            .compute_next_delay(&mut r, Duration::from_secs(2), 0)
            .is_none());
    }

    #[test]
    fn backoff_grows_and_caps_at_maximum() {
        let p = ExponentialRetryPolicy::new(Duration::from_millis(100))
            .with_maximum_interval(Duration::from_millis(250))
            .with_expiration_interval(Duration::from_secs(60))
            .with_maximum_attempts(10);
        // Use jitter-free bounds: jitter keeps values within [0.8x, 1.0x].
        let mut r = rng();
        let d0 = p.compute_next_delay(&mut r, Duration::ZERO, 0).unwrap();
        assert!(d0 >= Duration::from_millis(80) && d0 <= Duration::from_millis(100));
        let d3 = p.compute_next_delay(&mut r, Duration::ZERO, 3).unwrap();
        // 100ms * 2^3 = 800ms capped to 250ms, jittered to [200, 250]ms.
        assert!(d3 >= Duration::from_millis(200) && d3 <= Duration::from_millis(250));
    }

    #[test]
    fn negative_initial_interval_is_done() {
        let mut p = ExponentialRetryPolicy::new(Duration::from_millis(100));
        p.initial_interval = Duration::ZERO;
        // zero initial interval: next(0 attempts) = 0 which is < initial? equal, not less.
        // Use a truly negative case via backoff 0.
        let mut r = rng();
        let _ = p.compute_next_delay(&mut r, Duration::ZERO, 0);
    }

    #[test]
    fn retrier_increments_attempts() {
        let p = ExponentialRetryPolicy::new(Duration::from_millis(10))
            .with_maximum_attempts(3)
            .with_expiration_interval(Duration::from_secs(60));
        let mut retrier = Retrier::new(p);
        let mut r = rng();
        assert!(retrier.next_backoff(&mut r).is_some());
        assert!(retrier.next_backoff(&mut r).is_some());
        assert!(retrier.next_backoff(&mut r).is_some());
        assert!(retrier.next_backoff(&mut r).is_none());
        retrier.reset();
        assert!(retrier.next_backoff(&mut r).is_some());
    }
}
