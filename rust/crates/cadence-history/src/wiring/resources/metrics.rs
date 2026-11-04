//! Real metrics for the History startup path (replaces `NoopScope`/`NoopMetrics`).
//! A single in-process `MetricsRegistry` records counters, gauges and timer/histogram
//! count+sum keyed by `scope/name{tags}`; scopes/clients of every module write into it
//! and `snapshot()` exposes the values to the service's metrics reporter.
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::failover::{FailoverMetric, FailoverMetrics};
use crate::queue::deps as q1;
use crate::queuev2::deps as q2;
use crate::replication::deps::{ReplicationMetrics, Tags};
use crate::task::deps as task;

#[derive(Default)]
pub struct MetricsRegistry {
    values: Mutex<BTreeMap<String, f64>>,
}

impl MetricsRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn key(scope: &str, name: &str, tags: &[(String, String)], suffix: &str) -> String {
        let mut k = format!("{scope}/{name}{suffix}");
        if !tags.is_empty() {
            let t: Vec<String> = tags.iter().map(|(a, b)| format!("{a}={b}")).collect();
            k.push('{');
            k.push_str(&t.join(","));
            k.push('}');
        }
        k
    }
    fn add(&self, scope: &str, name: &str, tags: &[(String, String)], suffix: &str, delta: f64) {
        *self.values.lock().unwrap().entry(Self::key(scope, name, tags, suffix)).or_default() += delta;
    }
    fn set(&self, scope: &str, name: &str, tags: &[(String, String)], v: f64) {
        self.values.lock().unwrap().insert(Self::key(scope, name, tags, ""), v);
    }
    /// Observation for timers/histograms: `<name>_count` and `<name>_sum`.
    fn observe(&self, scope: &str, name: &str, tags: &[(String, String)], v: f64) {
        self.add(scope, name, tags, "_count", 1.0);
        self.add(scope, name, tags, "_sum", v);
    }
    pub fn snapshot(&self) -> BTreeMap<String, f64> {
        self.values.lock().unwrap().clone()
    }
    pub fn get(&self, scope: &str, name: &str) -> f64 {
        self.values.lock().unwrap().get(&Self::key(scope, name, &[], "")).copied().unwrap_or(0.0)
    }
}

/// Scope bound to a registry, a scope name and a fixed tag set. Implements the
/// `MetricsScope` trait of `task`, `queue` and `queuev2`.
#[derive(Clone)]
pub struct Scope {
    reg: Arc<MetricsRegistry>,
    scope: String,
    tags: Vec<(String, String)>,
}

impl Scope {
    pub fn new(reg: Arc<MetricsRegistry>, scope: &str, tags: Vec<(String, String)>) -> Self {
        Self { reg, scope: scope.to_string(), tags }
    }
    fn counter(&self, n: &str, d: f64) {
        self.reg.add(&self.scope, n, &self.tags, "", d);
    }
    fn obs(&self, n: &str, v: f64) {
        self.reg.observe(&self.scope, n, &self.tags, v);
    }
    fn with(&self, extra: Vec<(String, String)>) -> Scope {
        let mut tags = self.tags.clone();
        tags.extend(extra);
        Scope { reg: self.reg.clone(), scope: self.scope.clone(), tags }
    }
}

impl task::MetricsScope for Scope {
    fn inc_counter(&self, name: task::MetricIdx) {
        self.counter(name, 1.0)
    }
    fn add_counter(&self, name: task::MetricIdx, delta: i64) {
        self.counter(name, delta as f64)
    }
    fn record_timer(&self, name: task::MetricIdx, d: Duration) {
        self.obs(name, d.as_secs_f64())
    }
    fn exponential_histogram(&self, name: task::MetricIdx, d: Duration) {
        self.obs(name, d.as_secs_f64())
    }
    fn int_exponential_histogram(&self, name: task::MetricIdx, v: usize) {
        self.obs(name, v as f64)
    }
    fn record_histogram_duration(&self, name: task::MetricIdx, d: Duration) {
        self.obs(name, d.as_secs_f64())
    }
    fn record_histogram_value(&self, name: task::MetricIdx, v: f64) {
        self.obs(name, v)
    }
    fn update_gauge(&self, name: task::MetricIdx, v: f64) {
        self.reg.set(&self.scope, name, &self.tags, v)
    }
    fn tagged(&self, tags: Vec<task::Tag>) -> Arc<dyn task::MetricsScope> {
        Arc::new(self.with(tags.into_iter().map(|t| (t.key.to_string(), t.value)).collect()))
    }
}

impl q1::MetricsScope for Scope {
    fn inc_counter(&self, name: &'static str) {
        self.counter(name, 1.0)
    }
    fn add_counter(&self, name: &'static str, delta: i64) {
        self.counter(name, delta as f64)
    }
    fn record_timer(&self, name: &'static str, d: Duration) {
        self.obs(name, d.as_secs_f64())
    }
    fn int_exponential_histogram(&self, name: &'static str, v: i64) {
        self.obs(name, v as f64)
    }
}

struct Stopwatch {
    scope: Scope,
    timer: String,
    hist: String,
    start: Instant,
}
impl q2::Stopwatch for Stopwatch {
    fn stop(&self) {
        let e = self.start.elapsed().as_secs_f64();
        self.scope.obs(&self.timer, e);
        self.scope.obs(&self.hist, e);
    }
}

impl q2::MetricsScope for Scope {
    fn inc_counter(&self, name: &str) {
        self.counter(name, 1.0)
    }
    fn add_counter(&self, name: &str, v: i64) {
        self.counter(name, v as f64)
    }
    fn update_gauge(&self, name: &str, v: f64) {
        self.reg.set(&self.scope, name, &self.tags, v)
    }
    fn record_histogram_duration(&self, name: &str, d: Duration) {
        self.obs(name, d.as_secs_f64())
    }
    fn record_histogram_value(&self, name: &str, v: f64) {
        self.obs(name, v)
    }
    fn exponential_histogram(&self, name: &str, d: chrono::Duration) {
        self.obs(name, d.num_nanoseconds().unwrap_or(0) as f64 / 1e9)
    }
    fn int_exponential_histogram(&self, name: &str, v: i64) {
        self.obs(name, v as f64)
    }
    fn start_timer_with_exponential_histogram(&self, timer: &str, hist: &str) -> Box<dyn q2::Stopwatch> {
        Box::new(Stopwatch {
            scope: self.clone(),
            timer: timer.to_string(),
            hist: hist.to_string(),
            start: Instant::now(),
        })
    }
    fn tagged(&self, tags: &[(&str, String)]) -> Arc<dyn q2::MetricsScope> {
        Arc::new(self.with(tags.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()))
    }
}

/// `metrics.Client` for task and queuev2.
#[derive(Clone)]
pub struct MetricsClientImpl {
    reg: Arc<MetricsRegistry>,
}

impl MetricsClientImpl {
    pub fn new(reg: Arc<MetricsRegistry>) -> Self {
        Self { reg }
    }
    pub fn registry(&self) -> &Arc<MetricsRegistry> {
        &self.reg
    }
}

impl task::MetricsClient for MetricsClientImpl {
    fn scope(&self, scope: task::ScopeIdx, tags: Vec<task::Tag>) -> Arc<dyn task::MetricsScope> {
        Arc::new(Scope::new(
            self.reg.clone(),
            scope,
            tags.into_iter().map(|t| (t.key.to_string(), t.value)).collect(),
        ))
    }
}

impl q2::MetricsClient for MetricsClientImpl {
    fn scope(&self, scope: &str) -> Arc<dyn q2::MetricsScope> {
        Arc::new(Scope::new(self.reg.clone(), scope, vec![]))
    }
    fn record_timer(&self, scope: &str, name: &str, d: Duration) {
        self.reg.observe(scope, name, &[], d.as_secs_f64());
    }
}

fn to_owned_tags(tags: Tags<'_>) -> Vec<(String, String)> {
    tags.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect()
}

impl ReplicationMetrics for MetricsClientImpl {
    fn inc_counter(&self, scope: &str, name: &str, tags: Tags<'_>) {
        self.reg.add(scope, name, &to_owned_tags(tags), "", 1.0);
    }
    fn add_counter(&self, scope: &str, name: &str, tags: Tags<'_>, delta: i64) {
        self.reg.add(scope, name, &to_owned_tags(tags), "", delta as f64);
    }
    fn update_gauge(&self, scope: &str, name: &str, tags: Tags<'_>, value: f64) {
        self.reg.set(scope, name, &to_owned_tags(tags), value);
    }
    fn record_timer(&self, scope: &str, name: &str, tags: Tags<'_>, d: Duration) {
        self.reg.observe(scope, name, &to_owned_tags(tags), d.as_secs_f64());
    }
    fn record_histogram_value(&self, scope: &str, name: &str, tags: Tags<'_>, value: f64) {
        self.reg.observe(scope, name, &to_owned_tags(tags), value);
    }
    fn record_histogram_duration(&self, scope: &str, name: &str, tags: Tags<'_>, d: Duration) {
        self.reg.observe(scope, name, &to_owned_tags(tags), d.as_secs_f64());
    }
    fn exponential_histogram(&self, scope: &str, name: &str, tags: Tags<'_>, d: Duration) {
        self.reg.observe(scope, name, &to_owned_tags(tags), d.as_secs_f64());
    }
}

fn failover_name(m: FailoverMetric) -> &'static str {
    match m {
        FailoverMetric::DroppedRegressedDomain => "DroppedRegressedDomain",
        FailoverMetric::CadenceFailures => "CadenceFailures",
        FailoverMetric::NotificationFailure => "NotificationFailure",
        FailoverMetric::UpdateShardFailure => "UpdateShardFailure",
        FailoverMetric::MarkerCount => "MarkerCount",
        FailoverMetric::GracefulFailoverLatency => "GracefulFailoverLatency",
        FailoverMetric::GracefulFailoverLatencyHistogram => "GracefulFailoverLatencyHistogram",
    }
}

fn domain_tags(domain: Option<&str>) -> Vec<(String, String)> {
    domain.map(|d| vec![("domain".to_string(), d.to_string())]).unwrap_or_default()
}

impl FailoverMetrics for MetricsClientImpl {
    fn inc_counter(&self, metric: FailoverMetric, domain: Option<&str>) {
        self.reg.add("FailoverMarker", failover_name(metric), &domain_tags(domain), "", 1.0);
    }
    fn update_gauge(&self, metric: FailoverMetric, domain: Option<&str>, value: f64) {
        self.reg.set("FailoverMarker", failover_name(metric), &domain_tags(domain), value);
    }
    fn record_timer(&self, metric: FailoverMetric, domain: Option<&str>, d: Duration) {
        self.reg.observe("FailoverMarker", failover_name(metric), &domain_tags(domain), d.as_secs_f64());
    }
}

impl crate::engine::runtime::EngineMetrics for MetricsClientImpl {
    fn stale_mutable_state(&self, operation: &str) {
        self.reg.add(operation, "StaleMutableStateCounter", &[], "", 1.0);
    }
    fn activity_e2e_latency(
        &self,
        operation: &str,
        domain: &str,
        workflow_type: &str,
        activity_type: &str,
        task_list: &str,
        started_time: i64,
    ) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let tags = vec![
            ("domain".to_string(), domain.to_string()),
            ("workflowType".to_string(), workflow_type.to_string()),
            ("activityType".to_string(), activity_type.to_string()),
            ("tasklist".to_string(), task_list.to_string()),
        ];
        self.reg.observe(operation, "ActivityE2ELatency", &tags, (now - started_time).max(0) as f64 / 1e9);
    }
    fn replication_latency(&self, operation: &str, elapsed: Duration) {
        self.reg.observe(operation, "ReplicationLatency", &[], elapsed.as_secs_f64());
    }
}

/// `domain.MetricsScopeCache`: domain-tagged scopes cached per (domainID, scope).
#[derive(Default)]
type ScopeMap = Mutex<HashMap<(String, &'static str), Arc<dyn task::MetricsScope>>>;

pub struct DomainMetricsScopeCacheImpl {
    scopes: ScopeMap,
}

impl task::DomainMetricsScopeCache for DomainMetricsScopeCacheImpl {
    fn get(&self, domain_id: &str, scope: task::ScopeIdx) -> Option<Arc<dyn task::MetricsScope>> {
        self.scopes.lock().unwrap().get(&(domain_id.to_string(), scope)).cloned()
    }
    fn put(&self, domain_id: &str, scope: task::ScopeIdx, s: Arc<dyn task::MetricsScope>) {
        self.scopes.lock().unwrap().insert((domain_id.to_string(), scope), s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::deps::MetricsClient;

    #[test]
    fn counters_and_timers_are_recorded() {
        let reg = MetricsRegistry::new();
        let c = MetricsClientImpl::new(reg.clone());
        let s = MetricsClient::scope(&c, "Scope", vec![]);
        s.inc_counter("X");
        s.add_counter("X", 2);
        s.record_timer("T", Duration::from_secs(1));
        assert_eq!(reg.get("Scope", "X"), 3.0);
        assert_eq!(reg.get("Scope", "T_count"), 1.0);
    }
}
