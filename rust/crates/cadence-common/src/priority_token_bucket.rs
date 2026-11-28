//! Port of Go `common/tokenbucket` `priorityTokenBucketImpl` and the
//! `persistence/wrappers/sampled` per-domain bucket factory.
//!
//! Go: `go/common/tokenbucket/tb.go` (`NewPriorityTokenBucket`,
//! `NewFullPriorityTokenBucket`, `GetToken`, `refill`, `refillOverFlow`) and
//! `go/common/persistence/wrappers/sampled/tokenbucketfactory.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MILLIS_PER_SECOND: i64 = 1000;
const REFILL_RATE: Duration = Duration::from_millis(100);

#[derive(Debug)]
struct State {
    tokens: Vec<i64>,
    fill_rate: i64,
    next_refill_time: Option<Instant>, // None == Go zero time (always due)
    overflow_rps: i64,
    overflow_tokens: i64,
    next_overflow_refill_time: Option<Instant>,
}

impl State {
    fn due(t: Option<Instant>, now: Instant) -> bool {
        t.is_none_or(|t| now >= t)
    }

    fn refill_overflow(&mut self, now: Instant) {
        if self.overflow_rps < 1 {
            return;
        }
        if Self::due(self.next_overflow_refill_time, now) {
            self.overflow_tokens = self.overflow_rps;
            self.next_overflow_refill_time = Some(now + Duration::from_secs(1));
        }
    }

    fn refill(&mut self, now: Instant) {
        self.refill_overflow(now);
        if Self::due(self.next_refill_time, now) {
            let mut more = self.fill_rate;
            for i in 0..self.tokens.len() {
                self.tokens[i] += more;
                if self.tokens[i] > self.fill_rate {
                    more = self.tokens[i] - self.fill_rate;
                    self.tokens[i] = self.fill_rate;
                } else {
                    break;
                }
            }
            if self.overflow_tokens > 0 {
                self.tokens[0] += 1;
                self.overflow_tokens -= 1;
            }
            self.next_refill_time = Some(now + REFILL_RATE);
        }
    }
}

/// Go `tokenbucket.PriorityTokenBucket`.
#[derive(Debug)]
pub struct PriorityTokenBucket {
    state: Mutex<State>,
}

impl PriorityTokenBucket {
    fn build(num_priority: usize, rps: i64) -> State {
        let fill_rate = (rps * 100) / MILLIS_PER_SECOND;
        State {
            tokens: vec![0; num_priority],
            fill_rate,
            next_refill_time: None,
            overflow_rps: rps - 10 * fill_rate,
            overflow_tokens: 0,
            next_overflow_refill_time: None,
        }
    }

    /// Go `NewPriorityTokenBucket`.
    pub fn new(num_priority: usize, rps: i64, now: Instant) -> Self {
        let mut s = Self::build(num_priority, rps);
        s.refill(now);
        Self { state: Mutex::new(s) }
    }

    /// Go `NewFullPriorityTokenBucket`: every priority level starts full.
    pub fn new_full(num_priority: usize, rps: i64, now: Instant) -> Self {
        let mut s = Self::build(num_priority, rps);
        s.refill(now);
        for _ in 1..num_priority {
            s.next_refill_time = None;
            s.refill(now);
        }
        Self { state: Mutex::new(s) }
    }

    /// Go `GetToken(priority, count)`; returns (ok, time to next refill).
    pub fn get_token_at(&self, priority: usize, count: i64, now: Instant) -> (bool, Duration) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.refill(now);
        let next = s
            .next_refill_time
            .map_or(Duration::ZERO, |t| t.saturating_duration_since(now));
        if s.tokens[priority] < count {
            return (false, next);
        }
        s.tokens[priority] -= count;
        (true, next)
    }

    pub fn get_token(&self, priority: usize, count: i64) -> (bool, Duration) {
        self.get_token_at(priority, count, Instant::now())
    }
}

/// Go `sampled.domainToBucketMap` (`NewDomainToBucketMap`): one full bucket per
/// domain, created lazily with the domain's QPS at first use.
pub struct DomainToBucketMap {
    num_priority: usize,
    qps: Arc<dyn Fn(&str) -> i64 + Send + Sync>,
    mappings: Mutex<HashMap<String, Arc<PriorityTokenBucket>>>,
}

impl DomainToBucketMap {
    pub fn new(num_priority: usize, qps: Arc<dyn Fn(&str) -> i64 + Send + Sync>) -> Self {
        Self {
            num_priority,
            qps,
            mappings: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_rate_limiter(&self, domain: &str, now: Instant) -> Arc<PriorityTokenBucket> {
        let mut m = self.mappings.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(b) = m.get(domain) {
            return b.clone();
        }
        let b = Arc::new(PriorityTokenBucket::new_full(
            self.num_priority,
            (self.qps)(domain),
            now,
        ));
        m.insert(domain.to_string(), b.clone());
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_bucket_single_priority_fill_rate_tokens_then_refill() {
        let t0 = Instant::now();
        // rps 10 -> fill_rate 1 per 100ms, overflow 0.
        let b = PriorityTokenBucket::new_full(1, 10, t0);
        assert!(b.get_token_at(0, 1, t0).0);
        assert!(!b.get_token_at(0, 1, t0).0);
        assert!(!b.get_token_at(0, 1, t0 + Duration::from_millis(99)).0);
        assert!(b.get_token_at(0, 1, t0 + Duration::from_millis(100)).0);
    }

    #[test]
    fn full_bucket_two_priorities_both_start_full() {
        let t0 = Instant::now();
        let b = PriorityTokenBucket::new_full(2, 20, t0); // fill 2
        assert!(b.get_token_at(1, 2, t0).0);
        assert!(b.get_token_at(0, 2, t0).0);
        assert!(!b.get_token_at(0, 1, t0).0);
        assert!(!b.get_token_at(1, 1, t0).0);
    }

    #[test]
    fn non_full_bucket_starts_with_priority_zero_only() {
        let t0 = Instant::now();
        let b = PriorityTokenBucket::new(2, 20, t0);
        assert!(!b.get_token_at(1, 1, t0).0);
        assert!(b.get_token_at(0, 1, t0).0);
    }

    #[test]
    fn overflow_tokens_added_to_priority_zero() {
        let t0 = Instant::now();
        // rps 15 -> fill 1, overflow 5 per second.
        let b = PriorityTokenBucket::new_full(1, 15, t0);
        assert!(b.get_token_at(0, 1, t0).0); // fill token (overflow already moved 1 in)
        let (ok, _) = b.get_token_at(0, 1, t0);
        assert!(ok, "overflow token granted at construction refill");
        assert!(!b.get_token_at(0, 1, t0).0);
    }

    #[test]
    fn zero_rps_never_grants() {
        let t0 = Instant::now();
        let b = PriorityTokenBucket::new_full(1, 0, t0);
        assert!(!b.get_token_at(0, 1, t0 + Duration::from_secs(5)).0);
    }

    #[test]
    fn domain_map_creates_one_bucket_per_domain() {
        let t0 = Instant::now();
        let m = DomainToBucketMap::new(1, Arc::new(|d| if d == "a" { 10 } else { 20 }));
        let a1 = m.get_rate_limiter("a", t0);
        let a2 = m.get_rate_limiter("a", t0);
        assert!(Arc::ptr_eq(&a1, &a2));
        assert!(a1.get_token_at(0, 1, t0).0);
        assert!(!a1.get_token_at(0, 1, t0).0);
        assert!(m.get_rate_limiter("b", t0).get_token_at(0, 2, t0).0);
    }
}
