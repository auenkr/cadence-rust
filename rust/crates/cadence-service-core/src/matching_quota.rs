//! Matching handler RPC quotas ported from Go `handler.NewHandler`.
//!
//! These are admission checks (not waits): domain reservations happen before
//! the host bucket, and are returned if either stage rejects the call.
use crate::admin::DynamicConfigOverlay;
use cadence_common::go_string::GoString;
use cadence_persistence::dynamic_config;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const USER_RPS: i64 = 1_200;
const UNLIMITED_RPS: i64 = i32::MAX as i64;
const REFRESH_NANOS: i64 = 5_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RpcClass {
    User,
    Worker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MatchingRpc {
    AddActivityTask,
    AddDecisionTask,
    PollForActivityTask,
    PollForDecisionTask,
    QueryWorkflow,
    DescribeTaskList,
    ListTaskListPartitions,
    GetTaskListsByDomain,
    UpdateTaskListPartitionConfig,
    AcquireSemaphore,
    RespondQueryTaskCompleted,
    CancelOutstandingPoll,
    RefreshTaskListPartitionConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuotaRejection {
    pub method: MatchingRpc,
    pub class: RpcClass,
    pub domain: Option<String>,
}

pub type QuotaMetricSink = Arc<dyn Fn(QuotaRejection) + Send + Sync>;

impl MatchingRpc {
    pub fn policy(self) -> (RpcClass, bool) {
        use MatchingRpc::*;
        match self {
            AddActivityTask | AddDecisionTask | PollForActivityTask | PollForDecisionTask => {
                (RpcClass::Worker, true)
            }
            QueryWorkflow
            | DescribeTaskList
            | ListTaskListPartitions
            | GetTaskListsByDomain
            | UpdateTaskListPartitionConfig
            | AcquireSemaphore => (RpcClass::User, true),
            RespondQueryTaskCompleted | CancelOutstandingPoll => (RpcClass::Worker, false),
            RefreshTaskListPartitionConfig => (RpcClass::User, false),
        }
    }
}

type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;
type RateSource = Arc<dyn Fn() -> f64 + Send + Sync>;

struct DynamicBucket {
    limiter: cadence_common::rate_limiter::RateLimiter,
    last_update: i64,
    source: RateSource,
}

impl DynamicBucket {
    fn rate_and_burst(rate: f64) -> (f64, i64) {
        let burst = if rate == 0.0 {
            0
        } else {
            // Go's amd64 float-to-int conversion returns MinInt for an
            // out-of-range value; Rust casts saturate instead.
            let rounded = rate.ceil();
            if rounded >= 9_223_372_036_854_775_808.0 {
                1
            } else {
                (rounded as i64).max(1)
            }
        };
        (rate, burst)
    }

    fn new(source: RateSource, now: i64) -> Self {
        let (rate, burst) = Self::rate_and_burst(source());
        Self {
            limiter: cadence_common::rate_limiter::RateLimiter::new(rate, burst),
            last_update: now,
            source,
        }
    }

    fn refresh(&mut self, now: i64) {
        // Go refreshes only when now.After(lastUpdate + TTL - 1ns).
        if now > self.last_update.saturating_add(REFRESH_NANOS - 1) {
            let (rate, burst) = Self::rate_and_burst((self.source)());
            // Go's SetLimitAndBurst updates against the limiter's latest
            // recorded time; it does not advance the limiter to `now`.
            self.limiter.set_limit_and_burst(rate, burst);
            self.last_update = now;
        }
    }

    fn reserve(&mut self, now: i64) -> cadence_common::rate_limiter::Reservation {
        self.refresh(now);
        self.limiter.reserve(now)
    }

    fn allow(&mut self, now: i64) -> bool {
        self.refresh(now);
        self.limiter.allow(now)
    }

    fn used(&mut self, reservation: &cadence_common::rate_limiter::Reservation, used: bool) {
        self.limiter.used(reservation, used);
    }
}

struct State {
    user_host: DynamicBucket,
    worker_host: DynamicBucket,
    domains: HashMap<(RpcClass, GoString), Arc<Mutex<DynamicBucket>>>,
}

#[cfg(test)]
type RejectedCounts = HashMap<(MatchingRpc, RpcClass, Option<String>), u64>;

/// Per-Matching-process user and worker admission policy. Domain buckets are
/// cached for the process lifetime, matching Go `quotas.Collection`.
pub struct MatchingRpcQuota {
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    clock: Clock,
    state: Mutex<State>,
    #[cfg(test)]
    rejected: Mutex<RejectedCounts>,
    metric_sink: Mutex<QuotaMetricSink>,
}

fn host_key(class: RpcClass) -> &'static str {
    match class {
        RpcClass::User => "matching.rps",
        RpcClass::Worker => "matching.workerrps",
    }
}
fn domain_key(class: RpcClass) -> &'static str {
    match class {
        RpcClass::User => "matching.domainrps",
        RpcClass::Worker => "matching.domainworkerrps",
    }
}
fn host_default(class: RpcClass) -> i64 {
    match class {
        RpcClass::User => USER_RPS,
        RpcClass::Worker => UNLIMITED_RPS,
    }
}
fn domain_default(class: RpcClass) -> i64 {
    match class {
        RpcClass::User => 0,
        RpcClass::Worker => UNLIMITED_RPS,
    }
}

impl MatchingRpcQuota {
    pub fn with_metric_sink(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        metric_sink: QuotaMetricSink,
    ) -> Self {
        Self::with_clock_and_sink(
            overlay,
            Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
                    .unwrap_or_default()
            }),
            metric_sink,
        )
    }

    #[cfg(test)]
    fn with_clock(overlay: Arc<Mutex<DynamicConfigOverlay>>, clock: Clock) -> Self {
        Self::with_clock_and_sink(overlay, clock, Arc::new(|_| {}))
    }

    fn with_clock_and_sink(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        clock: Clock,
        metric_sink: QuotaMetricSink,
    ) -> Self {
        let now = clock();
        let user_host = Self::host_bucket(overlay.clone(), RpcClass::User, now);
        let worker_host = Self::host_bucket(overlay.clone(), RpcClass::Worker, now);
        Self {
            overlay,
            clock,
            state: Mutex::new(State {
                user_host,
                worker_host,
                domains: HashMap::new(),
            }),
            #[cfg(test)]
            rejected: Mutex::new(HashMap::new()),
            metric_sink: Mutex::new(metric_sink),
        }
    }

    pub fn set_metric_sink(&self, metric_sink: QuotaMetricSink) {
        *self
            .metric_sink
            .lock()
            .expect("Matching quota metric sink lock") = metric_sink;
    }

    fn host_rate(overlay: &Arc<Mutex<DynamicConfigOverlay>>, class: RpcClass) -> f64 {
        let config = overlay.lock().expect("admin config lock");
        dynamic_config::integer(
            config.property_values(host_key(class)),
            None,
            host_default(class),
        ) as f64
    }

    fn host_bucket(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        class: RpcClass,
        now: i64,
    ) -> DynamicBucket {
        let source: RateSource = Arc::new(move || Self::host_rate(&overlay, class));
        DynamicBucket::new(source, now)
    }

    fn domain_bucket(
        &self,
        class: RpcClass,
        domain: GoString,
        now: i64,
    ) -> Arc<Mutex<DynamicBucket>> {
        let overlay = self.overlay.clone();
        let source: RateSource = Arc::new(move || {
            let config = overlay.lock().expect("admin config lock");
            // An invalid-byte name cannot match a UTF-8 domain filter, but
            // the unfiltered domain quota still applies. Keep its bucket key
            // raw and fall back to the host rate when that quota is nonpositive.
            let primary = dynamic_config::integer(
                config.property_values(domain_key(class)),
                std::str::from_utf8(domain.as_bytes()).ok(),
                domain_default(class),
            );
            if primary > 0 {
                primary as f64
            } else {
                dynamic_config::integer(
                    config.property_values(host_key(class)),
                    None,
                    host_default(class),
                ) as f64
            }
        });
        Arc::new(Mutex::new(DynamicBucket::new(source, now)))
    }

    /// Consume quota for a request. `reject` mirrors Go call sites: false for
    /// control RPCs that call Allow but deliberately accept over-limit calls.
    pub fn allow(&self, method: MatchingRpc, domain: Option<&str>) -> bool {
        let raw_domain = domain.map(GoString::from);
        self.allow_go_string(method, raw_domain.as_ref())
    }

    /// Byte-valued entry point for native Matching identifiers. Bucket keys
    /// retain the exact Go string bytes, including invalid UTF-8.
    pub fn allow_go_string(&self, method: MatchingRpc, domain: Option<&GoString>) -> bool {
        let (class, reject) = method.policy();
        let now = (self.clock)();
        let domain_bucket = domain.filter(|name| !name.as_bytes().is_empty()).map(|name| {
            let key = (class, name.clone());
            let mut state = self.state.lock().expect("Matching quota lock");
            state
                .domains
                .entry(key)
                .or_insert_with(|| self.domain_bucket(class, name.clone(), now))
                .clone()
        });

        let reservation = domain_bucket.as_ref().map(|bucket| {
            bucket
                .lock()
                .expect("Matching domain quota lock")
                .reserve(now)
        });
        let domain_allowed = reservation
            .as_ref()
            .is_none_or(|reservation| reservation.allowed());

        let allowed = if domain_allowed {
            let mut state = self.state.lock().expect("Matching quota lock");
            match class {
                RpcClass::User => state.user_host.allow(now),
                RpcClass::Worker => state.worker_host.allow(now),
            }
        } else {
            false
        };

        if let (Some(bucket), Some(reservation)) = (domain_bucket, reservation.as_ref()) {
            bucket
                .lock()
                .expect("Matching domain quota lock")
                .used(reservation, allowed);
        }
        if reject && !allowed {
            let metric = QuotaRejection {
                method,
                class,
                domain: domain
                    .and_then(|name| std::str::from_utf8(name.as_bytes()).ok())
                    .map(str::to_owned),
            };
            #[cfg(test)]
            let () = {
                *self
                    .rejected
                    .lock()
                    .expect("Matching quota metric lock")
                    .entry((metric.method, metric.class, metric.domain.clone()))
                    .or_default() += 1;
            };
            (self
                .metric_sink
                .lock()
                .expect("Matching quota metric sink lock"))(metric);
        }
        allowed || !reject
    }

    /// Count hard quota rejections by Matching RPC. The current Rust server
    /// does not yet have Go's tagged metrics registry; this counter keeps the
    /// handler metric event available to that integration without counting
    /// the three Go count-only methods as errors.
    #[cfg(test)]
    fn rejected_count(&self, method: MatchingRpc) -> u64 {
        self.rejected
            .lock()
            .unwrap()
            .iter()
            .filter(|((candidate, _, _), _)| *candidate == method)
            .map(|(_, count)| *count)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::{
        admin::{DynamicConfigFilter, DynamicConfigValue},
        enums::EncodingType,
        types::DataBlob,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn entry(value: i64, filters: &[(&str, serde_json::Value)]) -> DynamicConfigValue {
        let blob = |value: &serde_json::Value| {
            Some(DataBlob::new(
                EncodingType::JSON,
                serde_json::to_vec(value).unwrap(),
            ))
        };
        DynamicConfigValue {
            value: blob(&json!(value)),
            filters: filters
                .iter()
                .map(|(name, value)| DynamicConfigFilter {
                    name: (*name).into(),
                    value: blob(value),
                })
                .collect(),
        }
    }
    fn set(overlay: &Arc<Mutex<DynamicConfigOverlay>>, key: &str, values: Vec<DynamicConfigValue>) {
        overlay.lock().unwrap().update(false, key, values);
    }

    #[test]
    fn host_and_domain_reservations_follow_go_order_and_return_domain_token_on_host_reject() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(&overlay, "matching.rps", vec![entry(1, &[])]);
        set(
            &overlay,
            "matching.domainrps",
            vec![entry(10, &[("domainName", json!("alpha"))])],
        );
        let now = Arc::new(AtomicI64::new(10));
        let clock: Clock = Arc::new({
            let now = now.clone();
            move || now.load(Ordering::SeqCst)
        });
        let quota = MatchingRpcQuota::with_clock(overlay, clock);
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, Some("alpha")));
        // Host is exhausted. The rejected call's domain reservation is returned.
        assert!(!quota.allow(MatchingRpc::GetTaskListsByDomain, Some("alpha")));
        assert_eq!(quota.rejected_count(MatchingRpc::GetTaskListsByDomain), 1);
        let alpha = quota.state.lock().unwrap().domains[&(RpcClass::User, "alpha".into())].clone();
        assert_eq!(alpha.lock().unwrap().limiter.tokens(10), 9.0);
        // A different domain shares the same host bucket and is also rejected.
        assert!(!quota.allow(MatchingRpc::GetTaskListsByDomain, Some("beta")));
        now.store(1_000_000_010, Ordering::SeqCst);
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, Some("alpha")));
    }

    #[test]
    fn zero_domain_rate_falls_back_to_host_and_count_only_calls_are_admitted() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(&overlay, "matching.rps", vec![entry(1, &[])]);
        set(
            &overlay,
            "matching.domainrps",
            vec![entry(0, &[("domainName", json!("alpha"))])],
        );
        let now = Arc::new(AtomicI64::new(10));
        let clock: Clock = Arc::new({
            let now = now.clone();
            move || now.load(Ordering::SeqCst)
        });
        let quota = MatchingRpcQuota::with_clock(overlay, clock);
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, Some("alpha")));
        assert!(quota.allow(MatchingRpc::RefreshTaskListPartitionConfig, Some("alpha")));
        assert!(!quota.allow(MatchingRpc::GetTaskListsByDomain, Some("alpha")));
        now.store(1_000_000_010, Ordering::SeqCst);
        // Dynamic values are sampled on the Go five-second refresh boundary.
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, Some("alpha")));
    }

    #[test]
    fn user_and_worker_buckets_are_independent_and_refresh_live_values() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(&overlay, "matching.rps", vec![entry(1, &[])]);
        set(&overlay, "matching.workerrps", vec![entry(1, &[])]);
        let now = Arc::new(AtomicI64::new(10));
        let clock: Clock = Arc::new({
            let now = now.clone();
            move || now.load(Ordering::SeqCst)
        });
        let quota = MatchingRpcQuota::with_clock(overlay.clone(), clock);
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, None));
        assert!(quota.allow(MatchingRpc::AddDecisionTask, None));
        assert!(!quota.allow(MatchingRpc::GetTaskListsByDomain, None));
        set(&overlay, "matching.rps", vec![entry(2, &[])]);
        now.store(5_000_000_011, Ordering::SeqCst);
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, None));
    }

    #[test]
    fn worker_domain_default_does_not_follow_host_and_empty_domain_skips_bucket() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(&overlay, "matching.workerrps", vec![entry(1, &[])]);
        set(&overlay, "matching.domainrps", vec![entry(1, &[])]);
        let quota = MatchingRpcQuota::with_clock(overlay, Arc::new(|| 10));
        assert!(quota.allow(MatchingRpc::AddDecisionTask, Some("alpha")));
        let state = quota.state.lock().unwrap();
        let worker = state.domains[&(RpcClass::Worker, "alpha".into())].clone();
        drop(state);
        assert_eq!(worker.lock().unwrap().limiter.burst(), UNLIMITED_RPS);
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, Some("")));
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, Some("")));
        assert!(!quota.state.lock().unwrap().domains.contains_key(&(RpcClass::User, "".into())));
    }

    #[test]
    fn extreme_positive_rate_uses_go_amd64_minimum_burst() {
        assert_eq!(DynamicBucket::rate_and_burst(i64::MAX as f64).1, 1);
        assert_eq!(DynamicBucket::rate_and_burst(i32::MAX as f64).1, i32::MAX as i64);
        assert_eq!(DynamicBucket::rate_and_burst(0.0).1, 0);
        assert_eq!(DynamicBucket::rate_and_burst(-1.0).1, 1);
    }

    #[test]
    fn byte_domains_have_independent_buckets_without_utf8_aliasing() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(
            &overlay,
            "matching.domainrps",
            vec![
                entry(1, &[]),
                entry(2, &[("domainName", json!("tenant-�"))]),
            ],
        );
        let quota = MatchingRpcQuota::with_clock(overlay, Arc::new(|| 10));
        let ff = GoString::from(&b"tenant-\xff"[..]);
        let fe = GoString::from(&b"tenant-\xfe"[..]);
        let replacement = GoString::from("tenant-�");

        assert!(quota.allow_go_string(MatchingRpc::GetTaskListsByDomain, Some(&ff)));
        assert!(!quota.allow_go_string(MatchingRpc::GetTaskListsByDomain, Some(&ff)));
        assert!(quota.allow_go_string(MatchingRpc::GetTaskListsByDomain, Some(&fe)));
        assert!(quota.allow_go_string(MatchingRpc::GetTaskListsByDomain, Some(&replacement)));
        assert!(quota.allow_go_string(MatchingRpc::GetTaskListsByDomain, Some(&replacement)));
        assert!(!quota.allow_go_string(MatchingRpc::GetTaskListsByDomain, Some(&replacement)));
        let state = quota.state.lock().unwrap();
        assert_eq!(state.domains.len(), 3);
        assert!(state.domains.contains_key(&(RpcClass::User, ff)));
        assert!(state.domains.contains_key(&(RpcClass::User, fe)));
        assert!(state.domains.contains_key(&(RpcClass::User, replacement)));
    }

    #[test]
    fn refreshing_rate_does_not_refill_at_the_configuration_refresh_instant() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(&overlay, "matching.rps", vec![entry(1, &[])]);
        let now = Arc::new(AtomicI64::new(10));
        let clock: Clock = Arc::new({
            let now = now.clone();
            move || now.load(Ordering::SeqCst)
        });
        let quota = MatchingRpcQuota::with_clock(overlay.clone(), clock);
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, None));
        set(&overlay, "matching.rps", vec![entry(2, &[])]);
        now.store(5_000_000_011, Ordering::SeqCst);
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, None));
        assert!(quota.allow(MatchingRpc::GetTaskListsByDomain, None));
        assert!(!quota.allow(MatchingRpc::GetTaskListsByDomain, None));
    }

    #[test]
    fn hard_rejection_emits_one_classified_metric_event_but_count_only_does_not() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(&overlay, "matching.rps", vec![entry(0, &[])]);
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink: QuotaMetricSink = Arc::new({
            let events = events.clone();
            move |event| events.lock().unwrap().push(event)
        });
        let quota = MatchingRpcQuota::with_clock_and_sink(overlay, Arc::new(|| 10), sink);
        assert!(!quota.allow(MatchingRpc::GetTaskListsByDomain, Some("domain-a")));
        assert!(quota.allow(
            MatchingRpc::RefreshTaskListPartitionConfig,
            Some("domain-a")
        ));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            &[QuotaRejection {
                method: MatchingRpc::GetTaskListsByDomain,
                class: RpcClass::User,
                domain: Some("domain-a".into()),
            }]
        );
    }

    #[test]
    fn go_handler_method_classes_and_exemptions_are_preserved() {
        use MatchingRpc::*;
        assert_eq!(AddActivityTask.policy(), (RpcClass::Worker, true));
        assert_eq!(AddDecisionTask.policy(), (RpcClass::Worker, true));
        assert_eq!(PollForActivityTask.policy(), (RpcClass::Worker, true));
        assert_eq!(PollForDecisionTask.policy(), (RpcClass::Worker, true));
        assert_eq!(QueryWorkflow.policy(), (RpcClass::User, true));
        assert_eq!(DescribeTaskList.policy(), (RpcClass::User, true));
        assert_eq!(ListTaskListPartitions.policy(), (RpcClass::User, true));
        assert_eq!(GetTaskListsByDomain.policy(), (RpcClass::User, true));
        assert_eq!(
            UpdateTaskListPartitionConfig.policy(),
            (RpcClass::User, true)
        );
        assert_eq!(AcquireSemaphore.policy(), (RpcClass::User, true));
        assert_eq!(
            RespondQueryTaskCompleted.policy(),
            (RpcClass::Worker, false)
        );
        assert_eq!(CancelOutstandingPoll.policy(), (RpcClass::Worker, false));
        assert_eq!(
            RefreshTaskListPartitionConfig.policy(),
            (RpcClass::User, false)
        );
    }
}
