//! Token-bucket policies backing the Frontend rate-limited wrapper
//! (Go: `quotas.NewMultiStageRateLimiter(NewDynamicRateLimiter(global), perDomain)`).
//!
//! A request is admitted only if both the instance-wide bucket and the
//! per-domain bucket have a token. Limits are re-read from dynamic config on
//! every call so overrides take effect without restart.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cadence_api::errors::CadenceError;

use super::ratelimited::{CallerBypass, DomainNameLookup, Policy, QuotaInfo, WaitFailure};

/// Go's `quotas.UnlimitedRPS`.
pub const UNLIMITED_RPS: f64 = 100_000.0;

pub type RpsSource = Arc<dyn Fn() -> f64 + Send + Sync>;
pub type DomainRpsSource = Arc<dyn Fn(&str) -> f64 + Send + Sync>;

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn new(rps: f64) -> Self {
        Self {
            tokens: burst(rps),
            last: Instant::now(),
        }
    }

    fn refill(&mut self, rps: f64, now: Instant) {
        let burst = burst(rps);
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * rps).min(burst);
        self.last = now;
    }
}

fn burst(rps: f64) -> f64 {
    rps.max(1.0)
}

pub struct TokenBucketPolicy {
    global_rps: RpsSource,
    domain_rps: DomainRpsSource,
    global: Mutex<Bucket>,
    domains: Mutex<HashMap<String, Bucket>>,
}

impl TokenBucketPolicy {
    pub fn new(global_rps: RpsSource, domain_rps: DomainRpsSource) -> Self {
        let initial = global_rps();
        Self {
            global_rps,
            domain_rps,
            global: Mutex::new(Bucket::new(initial)),
            domains: Mutex::new(HashMap::new()),
        }
    }

    /// Try to take one token from both stages; on failure return how long
    /// until both would have a token.
    fn reserve(&self, info: &QuotaInfo) -> Result<(), Duration> {
        let now = Instant::now();
        let global_rps = (self.global_rps)();
        let domain_rps = (self.domain_rps)(&info.domain);
        let mut global = self.global.lock().unwrap_or_else(|e| e.into_inner());
        let mut domains = self.domains.lock().unwrap_or_else(|e| e.into_inner());
        let domain = domains
            .entry(info.domain.clone())
            .or_insert_with(|| Bucket::new(domain_rps));
        global.refill(global_rps, now);
        domain.refill(domain_rps, now);
        if global.tokens >= 1.0 && domain.tokens >= 1.0 {
            global.tokens -= 1.0;
            domain.tokens -= 1.0;
            return Ok(());
        }
        let need = |tokens: f64, rps: f64| {
            if tokens >= 1.0 || rps <= 0.0 {
                0.0
            } else {
                (1.0 - tokens) / rps
            }
        };
        let seconds = need(global.tokens, global_rps).max(need(domain.tokens, domain_rps));
        Err(Duration::from_secs_f64(seconds.max(0.001)))
    }
}

impl Policy for TokenBucketPolicy {
    fn allow(&self, info: &QuotaInfo) -> bool {
        self.reserve(info).is_ok()
    }

    fn wait(&self, info: &QuotaInfo, timeout: Duration) -> Result<(), WaitFailure> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.reserve(info) {
                Ok(()) => return Ok(()),
                Err(delay) => {
                    let now = Instant::now();
                    if now + delay > deadline {
                        return Err(WaitFailure::Deadline);
                    }
                    std::thread::sleep(delay);
                }
            }
        }
    }
}

/// Go's default `quotas.CallerBypass`: no caller type is exempt.
pub struct NoBypass;

impl CallerBypass for NoBypass {
    fn should_bypass(&self) -> bool {
        false
    }

    fn allow_policy(&self, policy: &dyn Policy, info: &QuotaInfo) -> bool {
        policy.allow(info)
    }
}

/// Reverse domain-UUID lookup used for task-token APIs.
pub struct LookupDomainNames {
    lookup: Arc<dyn crate::handler_workflow::WorkflowDomainLookup>,
}

impl LookupDomainNames {
    pub fn new(lookup: Arc<dyn crate::handler_workflow::WorkflowDomainLookup>) -> Self {
        Self { lookup }
    }
}

impl DomainNameLookup for LookupDomainNames {
    fn domain_name(&self, domain_id: &str) -> Result<String, CadenceError> {
        Ok(self
            .lookup
            .domain_name(domain_id)?
            .unwrap_or_else(|| domain_id.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(domain: &str) -> QuotaInfo {
        QuotaInfo {
            domain: domain.to_owned(),
            task_list: String::new(),
        }
    }

    #[test]
    fn domain_bucket_limits_and_isolates_domains() {
        let policy = TokenBucketPolicy::new(
            Arc::new(|| 1000.0),
            Arc::new(|d| if d == "a" { 2.0 } else { 1000.0 }),
        );
        assert!(policy.allow(&info("a")));
        assert!(policy.allow(&info("a")));
        assert!(!policy.allow(&info("a")));
        assert!(policy.allow(&info("b")));
    }

    #[test]
    fn global_bucket_limits_all_domains() {
        let policy = TokenBucketPolicy::new(Arc::new(|| 1.0), Arc::new(|_| 1000.0));
        assert!(policy.allow(&info("a")));
        assert!(!policy.allow(&info("b")));
    }

    #[test]
    fn wait_times_out_when_exhausted() {
        let policy = TokenBucketPolicy::new(Arc::new(|| 1.0), Arc::new(|_| 1.0));
        assert!(policy.allow(&info("a")));
        assert_eq!(
            policy.wait(&info("a"), Duration::from_millis(5)),
            Err(WaitFailure::Deadline)
        );
    }
}
