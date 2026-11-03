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
/// `(domain, task list)` -> rps, Go's `IntPropertyFnWithDomainAndTaskListFilter`.
pub type TaskListRpsSource = Arc<dyn Fn(&str, &str) -> f64 + Send + Sync>;

/// Go `permember.PerMember`: the per-instance limit is the global limit
/// averaged over the member count, capped by the instance limit; a
/// non-positive global limit or unknown member count falls back to the instance limit.
pub fn per_member(global_rps: f64, instance_rps: f64, member_count: usize) -> f64 {
    if global_rps <= 0.0 || member_count < 1 {
        return instance_rps;
    }
    let avg = (global_rps / member_count as f64).max(1.0);
    avg.min(instance_rps)
}

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
    task_list_rps: Option<TaskListRpsSource>,
    task_lists: Mutex<HashMap<(String, String), Bucket>>,
}

impl TokenBucketPolicy {
    pub fn new(global_rps: RpsSource, domain_rps: DomainRpsSource) -> Self {
        let initial = global_rps();
        Self {
            global_rps,
            domain_rps,
            global: Mutex::new(Bucket::new(initial)),
            domains: Mutex::new(HashMap::new()),
            task_list_rps: None,
            task_lists: Mutex::new(HashMap::new()),
        }
    }

    /// Add Go's task-list stage (`MultiStageRateLimiter.taskListLimiters`),
    /// consulted before the domain stage for requests that carry a task list.
    pub fn with_task_list_rps(mut self, source: TaskListRpsSource) -> Self {
        self.task_list_rps = Some(source);
        self
    }

    /// Try to take one token from both stages; on failure return how long
    /// until both would have a token.
    fn reserve(&self, info: &QuotaInfo) -> Result<(), Duration> {
        let now = Instant::now();
        let global_rps = (self.global_rps)();
        let domain_rps = (self.domain_rps)(&info.domain);
        let task_list_rps = match (&self.task_list_rps, info.task_list.is_empty()) {
            (Some(source), false) => Some(source(&info.domain, &info.task_list)),
            _ => None,
        };
        let mut task_lists = self.task_lists.lock().unwrap_or_else(|e| e.into_inner());
        let mut global = self.global.lock().unwrap_or_else(|e| e.into_inner());
        let mut domains = self.domains.lock().unwrap_or_else(|e| e.into_inner());
        let domain = domains
            .entry(info.domain.clone())
            .or_insert_with(|| Bucket::new(domain_rps));
        global.refill(global_rps, now);
        domain.refill(domain_rps, now);
        let task_list = task_list_rps.map(|rps| {
            let bucket = task_lists
                .entry((info.domain.clone(), info.task_list.clone()))
                .or_insert_with(|| Bucket::new(rps));
            bucket.refill(rps, now);
            (bucket, rps)
        });
        let task_list_ok = task_list.as_ref().is_none_or(|(b, _)| b.tokens >= 1.0);
        if global.tokens >= 1.0 && domain.tokens >= 1.0 && task_list_ok {
            global.tokens -= 1.0;
            domain.tokens -= 1.0;
            if let Some((bucket, _)) = task_list {
                bucket.tokens -= 1.0;
            }
            return Ok(());
        }
        let need = |tokens: f64, rps: f64| {
            if tokens >= 1.0 || rps <= 0.0 {
                0.0
            } else {
                (1.0 - tokens) / rps
            }
        };
        let task_list_need = task_list.as_ref().map_or(0.0, |(b, rps)| need(b.tokens, *rps));
        let seconds = need(global.tokens, global_rps)
            .max(need(domain.tokens, domain_rps))
            .max(task_list_need);
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

/// Go's `quotas.CallerBypass` over `system.rateLimiterBypassCallerTypes`:
/// a denied request is still admitted when its caller type is listed.
pub struct ConfiguredBypass {
    caller_types: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
}

impl ConfiguredBypass {
    pub fn new(caller_types: Arc<dyn Fn() -> Vec<String> + Send + Sync>) -> Self {
        Self { caller_types }
    }
}

/// No caller type is exempt (Go with a nil bypass list).
pub struct NoBypass;

impl CallerBypass for NoBypass {
    fn should_bypass(&self, _: &QuotaInfo) -> bool {
        false
    }

    fn allow_policy(&self, policy: &dyn Policy, info: &QuotaInfo) -> bool {
        policy.allow(info)
    }
}

impl CallerBypass for ConfiguredBypass {
    fn should_bypass(&self, info: &QuotaInfo) -> bool {
        // Go: ParseCallerType("") is CallerTypeUnknown ("unknown").
        let caller = if info.caller_type.is_empty() {
            "unknown"
        } else {
            info.caller_type.as_str()
        };
        (self.caller_types)().iter().any(|t| {
            let t = if t.is_empty() { "unknown" } else { t.as_str() };
            t == caller
        })
    }

    fn allow_policy(&self, policy: &dyn Policy, info: &QuotaInfo) -> bool {
        policy.allow(info) || self.should_bypass(info)
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
            caller_type: String::new(),
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
    fn task_list_bucket_limits_only_that_task_list() {
        let policy = TokenBucketPolicy::new(Arc::new(|| 1000.0), Arc::new(|_| 1000.0))
            .with_task_list_rps(Arc::new(|_, tl| if tl == "hot" { 1.0 } else { 1000.0 }));
        let on = |tl: &str| QuotaInfo {
            domain: "a".into(),
            task_list: tl.into(),
            caller_type: String::new(),
        };
        assert!(policy.allow(&on("hot")));
        assert!(!policy.allow(&on("hot")));
        assert!(policy.allow(&on("cold")));
        assert!(policy.allow(&info("a")));
    }

    #[test]
    fn configured_bypass_admits_listed_caller_type() {
        let deny = TokenBucketPolicy::new(Arc::new(|| 1.0), Arc::new(|_| 1.0));
        let bypass = ConfiguredBypass::new(Arc::new(|| vec!["cli".to_owned()]));
        let mut cli = info("a");
        cli.caller_type = "cli".into();
        assert!(bypass.allow_policy(&deny, &cli));
        assert!(bypass.allow_policy(&deny, &cli));
        let sdk = info("a");
        assert!(!bypass.allow_policy(&deny, &sdk));
    }

    #[test]
    fn per_member_matches_go() {
        assert_eq!(per_member(0.0, 7.0, 3), 7.0);
        assert_eq!(per_member(100.0, 7.0, 0), 7.0);
        assert_eq!(per_member(100.0, 50.0, 4), 25.0);
        assert_eq!(per_member(100.0, 10.0, 4), 10.0);
        assert_eq!(per_member(2.0, 10.0, 4), 1.0);
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
