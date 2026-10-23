//! Matching persistence host and cluster QPS enforcement.
//!
//! Go constructs one dynamic limiter per configured datastore and shares it
//! among that datastore's task, domain, and semaphore managers. The effective
//! rate is refreshed every five seconds from matching.persistenceMaxQPS and
//! matching.persistenceGlobalMaxQPS.

use crate::admin::DynamicConfigOverlay;
use cadence_common::rate_limiter::RateLimiter;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const HOST_QPS_DEFAULT: i64 = 3000;
const GLOBAL_QPS_DEFAULT: i64 = 0;
const REFRESH_NANOS: i64 = 5_000_000_000;

type DomainNameResolver = Arc<dyn Fn(uuid::Uuid) -> Option<String> + Send + Sync>;

type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PersistenceRateSnapshot {
    pub host_qps: f64,
    pub global_qps: f64,
    pub matching_members: usize,
    pub effective_qps: f64,
}

struct LimiterState {
    limiter: RateLimiter,
    last_refresh_nanos: i64,
    snapshot: PersistenceRateSnapshot,
}

/// A per-process limiter shared by all Matching persistence clients using the
/// configured default datastore. Rejections are counted for observability.
pub struct MatchingPersistenceLimiter {
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    matching_members: Arc<AtomicUsize>,
    clock: Clock,
    state: Mutex<LimiterState>,
    enabled: bool,
    rejected: AtomicU64,
    metric_sink: RwLock<Arc<dyn crate::matching_metrics::MetricSink>>,
    domain_name: RwLock<DomainNameResolver>,
}

impl std::fmt::Debug for MatchingPersistenceLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchingPersistenceLimiter")
            .field("rejected", &self.rejected())
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl MatchingPersistenceLimiter {
    pub fn new(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        matching_members: Arc<AtomicUsize>,
    ) -> Arc<Self> {
        Self::with_clock(overlay, matching_members, Arc::new(monotonic_nanos))
    }

    fn with_clock(
        overlay: Arc<Mutex<DynamicConfigOverlay>>,
        matching_members: Arc<AtomicUsize>,
        clock: Clock,
    ) -> Arc<Self> {
        let now = clock();
        let snapshot = read_rate_snapshot(&overlay, matching_members.load(Ordering::Relaxed));
        let enabled = snapshot.effective_qps > 0.0;
        let mut limiter = RateLimiter::new(snapshot.effective_qps, burst(snapshot.effective_qps));
        limiter.tokens(now);
        Arc::new(Self {
            overlay,
            matching_members,
            clock,
            state: Mutex::new(LimiterState {
                limiter,
                last_refresh_nanos: now,
                snapshot,
            }),
            enabled,
            rejected: AtomicU64::new(0),
            metric_sink: RwLock::new(Arc::new(crate::matching_metrics::NoopMetricSink)),
            domain_name: RwLock::new(Arc::new(|_| None)),
        })
    }

    /// Attach the configured production sink before Matching listeners start.
    pub fn with_metric_sink(&self, sink: Arc<dyn crate::matching_metrics::MetricSink>) {
        *self
            .metric_sink
            .write()
            .expect("Matching metrics sink lock") = sink;
    }

    pub(crate) fn record_rejection(
        &self,
        operation: &'static str,
        domain: Option<&str>,
        go_metered_operation: Option<&'static str>,
    ) {
        let sink = self.metric_sink.read().expect("Matching metrics sink lock");
        crate::matching_metrics::record_persistence_qps_rejection(
            sink.as_ref(),
            operation,
            domain,
            go_metered_operation,
        );
    }

    pub fn with_domain_cache(
        &self,
        cache: &Arc<crate::matching_domain_cache::MatchingDomainCache>,
    ) {
        let cache = Arc::downgrade(cache);
        *self
            .domain_name
            .write()
            .expect("Matching metric domain resolver lock") = Arc::new(move |id| {
            cache
                .upgrade()
                .and_then(|cache| cache.cached_domain_name(&id.to_string()))
        });
    }

    pub(crate) fn record_task_rejection(
        &self,
        operation: &'static str,
        domain_id: Option<uuid::Uuid>,
    ) {
        let domain = domain_id.map(|id| {
            (self
                .domain_name
                .read()
                .expect("Matching metric domain resolver lock"))(id)
            .unwrap_or_else(|| "_unknown_".into())
        });
        self.record_rejection(operation, domain.as_deref(), Some(operation));
    }

    pub(crate) fn record_task_call<T>(
        &self,
        operation: &'static str,
        domain_id: Option<uuid::Uuid>,
        elapsed: std::time::Duration,
        result: &cadence_persistence::matching_task_store::Result<T>,
    ) {
        use crate::matching_metrics::*;
        use cadence_persistence::matching_task_store::{BackendErrorKind, MatchingTaskStoreError};
        let sink = self.metric_sink.read().expect("Matching metrics sink lock");
        let mut tags = std::collections::BTreeMap::from([
            (OPERATION_TAG.to_owned(), operation.to_owned()),
            (IS_RETRY_TAG.to_owned(), "false".to_owned()),
        ]);
        if let Some(id) = domain_id {
            let domain = (self
                .domain_name
                .read()
                .expect("Matching metric domain resolver lock"))(id);
            tags.insert(
                DOMAIN_TAG.to_owned(),
                domain.unwrap_or_else(|| "_unknown_".into()),
            );
        } else {
            tags.insert(TASK_CATEGORY_TAG.to_owned(), "none".into());
        }
        let per_domain = domain_id.is_some();
        let record = |name, kind, value, histogram| {
            sink.record(MetricEvent {
                name,
                kind,
                value,
                tags: tags.clone(),
                histogram,
            })
        };
        record(
            if per_domain {
                PERSISTENCE_REQUESTS_PER_DOMAIN
            } else {
                PERSISTENCE_REQUESTS
            },
            MetricKind::Counter,
            MetricValue::Count(1),
            None,
        );
        record(
            if per_domain {
                "persistence_latency_per_domain"
            } else {
                "persistence_latency"
            },
            MetricKind::Timer,
            MetricValue::Duration(elapsed),
            None,
        );
        let mut histogram_tags = tags.clone();
        for (key, value) in [
            ("histogram_start", "1ms"),
            ("histogram_end", "17m28.576s"),
            ("histogram_scale", "2"),
        ] {
            histogram_tags.insert(key.to_owned(), value.to_owned());
        }
        sink.record(MetricEvent {
            name: if per_domain {
                "persistence_latency_per_domain_ns"
            } else {
                "persistence_latency_ns"
            },
            kind: MetricKind::Histogram,
            value: MetricValue::Duration(elapsed),
            tags: histogram_tags,
            histogram: Some(HistogramSpec::GoDefault1ms100s),
        });
        if let Err(error) = result {
            let (name, failure) = match error {
                MatchingTaskStoreError::ConditionFailed(_) => (
                    if per_domain {
                        "persistence_errors_condition_failed_per_domain"
                    } else {
                        "persistence_errors_condition_failed"
                    },
                    false,
                ),
                MatchingTaskStoreError::NotFound => (
                    if per_domain {
                        "persistence_errors_entity_not_exists_per_domain"
                    } else {
                        "persistence_errors_entity_not_exists"
                    },
                    false,
                ),
                MatchingTaskStoreError::Backend {
                    kind: BackendErrorKind::Timeout,
                    ..
                } => (
                    if per_domain {
                        "persistence_errors_timeout_per_domain"
                    } else {
                        "persistence_errors_timeout"
                    },
                    true,
                ),
                MatchingTaskStoreError::Backend {
                    kind: BackendErrorKind::Throttled,
                    ..
                } => (
                    if per_domain {
                        PERSISTENCE_ERRORS_BUSY_PER_DOMAIN
                    } else {
                        PERSISTENCE_ERRORS_BUSY
                    },
                    true,
                ),
                _ => (
                    if per_domain {
                        PERSISTENCE_FAILURES_PER_DOMAIN
                    } else {
                        PERSISTENCE_FAILURES
                    },
                    false,
                ),
            };
            record(name, MetricKind::Counter, MetricValue::Count(1), None);
            if failure {
                record(
                    if per_domain {
                        PERSISTENCE_FAILURES_PER_DOMAIN
                    } else {
                        PERSISTENCE_FAILURES
                    },
                    MetricKind::Counter,
                    MetricValue::Count(1),
                    None,
                );
            }
        }
    }

    /// Consume one backend-operation permit without waiting, as Go's
    /// persistence wrappers do. Rejected calls must not reach the backend.
    pub fn allow(&self) -> bool {
        // Go's persistence factory omits the wrapper if the initial dynamic
        // rate is zero. A later config update does not install one in place.
        if !self.enabled {
            return true;
        }
        let now = (self.clock)();
        let mut state = self
            .state
            .lock()
            .expect("Matching persistence limiter lock");
        if now.saturating_sub(state.last_refresh_nanos) >= REFRESH_NANOS {
            let snapshot =
                read_rate_snapshot(&self.overlay, self.matching_members.load(Ordering::Relaxed));
            state
                .limiter
                .set_limit_and_burst(snapshot.effective_qps, burst(snapshot.effective_qps));
            state.snapshot = snapshot;
            state.last_refresh_nanos = now;
        }
        let allowed = state.limiter.allow(now);
        if !allowed {
            self.rejected.fetch_add(1, Ordering::Relaxed);
        }
        allowed
    }

    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> PersistenceRateSnapshot {
        self.state
            .lock()
            .expect("Matching persistence limiter lock")
            .snapshot
    }
}

fn burst(qps: f64) -> i64 {
    if qps == 0.0 {
        0
    } else {
        qps.ceil().max(1.0).min(i64::MAX as f64) as i64
    }
}

fn read_rate_snapshot(
    overlay: &Arc<Mutex<DynamicConfigOverlay>>,
    matching_members: usize,
) -> PersistenceRateSnapshot {
    let overlay = overlay.lock().expect("admin dynamic config lock");
    let host_values = overlay.property_values("matching.persistenceMaxQPS");
    let host_qps = if host_values.is_empty() {
        HOST_QPS_DEFAULT
    } else {
        cadence_persistence::dynamic_config::integer(host_values, None, HOST_QPS_DEFAULT)
    }
    .max(0) as f64;
    let global_values = overlay.property_values("matching.persistenceGlobalMaxQPS");
    let global_qps = if global_values.is_empty() {
        GLOBAL_QPS_DEFAULT
    } else {
        cadence_persistence::dynamic_config::integer(global_values, None, GLOBAL_QPS_DEFAULT)
    }
    .max(0) as f64;
    let effective_qps = if global_qps > 0.0 && matching_members > 0 {
        (global_qps / matching_members as f64)
            .max(1.0)
            .min(host_qps)
    } else {
        host_qps
    };
    PersistenceRateSnapshot {
        host_qps,
        global_qps,
        matching_members,
        effective_qps,
    }
}

fn monotonic_nanos() -> i64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
    use std::sync::atomic::AtomicI64;

    fn set(overlay: &Arc<Mutex<DynamicConfigOverlay>>, key: &str, value: i64) {
        overlay.lock().unwrap().update(
            false,
            key,
            vec![DynamicConfigValue {
                filters: vec![],
                value: Some(DataBlob {
                    encoding_type: Some(EncodingType::JSON),
                    data: value.to_string().into_bytes(),
                }),
            }],
        );
    }

    #[test]
    fn persistence_rate_is_shared_live_and_uses_per_member_global_cap() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let members = Arc::new(AtomicUsize::new(2));
        set(&overlay, "matching.persistenceMaxQPS", 10);
        set(&overlay, "matching.persistenceGlobalMaxQPS", 6);
        let now = Arc::new(AtomicI64::new(0));
        let clock_now = now.clone();
        let limiter = MatchingPersistenceLimiter::with_clock(
            overlay.clone(),
            members.clone(),
            Arc::new(move || clock_now.load(Ordering::SeqCst)),
        );

        let initial = limiter.snapshot();
        assert_eq!(initial.effective_qps, 3.0);
        assert!(limiter.allow());
        assert!(limiter.allow());
        assert!(limiter.allow());
        assert!(!limiter.allow());
        assert_eq!(limiter.rejected(), 1);

        set(&overlay, "matching.persistenceMaxQPS", 20);
        set(&overlay, "matching.persistenceGlobalMaxQPS", 0);
        members.store(1, Ordering::SeqCst);
        now.store(REFRESH_NANOS, Ordering::SeqCst);
        assert!(limiter.allow());
        assert_eq!(limiter.snapshot().effective_qps, 20.0);
    }

    #[test]
    fn zero_global_qps_falls_back_to_go_host_default() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let limiter = MatchingPersistenceLimiter::with_clock(
            overlay,
            Arc::new(AtomicUsize::new(3)),
            Arc::new(|| 0),
        );
        assert_eq!(limiter.snapshot().host_qps, 3000.0);
        assert_eq!(limiter.snapshot().effective_qps, 3000.0);
    }

    #[test]
    fn zero_rate_at_resource_construction_leaves_the_store_unwrapped() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        set(&overlay, "matching.persistenceMaxQPS", 0);
        let limiter = MatchingPersistenceLimiter::with_clock(
            overlay.clone(),
            Arc::new(AtomicUsize::new(1)),
            Arc::new(|| 0),
        );
        set(&overlay, "matching.persistenceMaxQPS", 1);
        assert!(limiter.allow());
        assert_eq!(limiter.rejected(), 0);
    }
}

#[cfg(test)]
mod metered_task_tests {
    use super::*;
    use crate::matching_metrics::*;
    use cadence_persistence::matching_task_store::MatchingTaskStoreError;

    #[test]
    fn task_domain_family_omits_task_category_and_preserves_unknown_domain() {
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        limiter.record_task_call(
            "GetTasks",
            Some(uuid::Uuid::nil()),
            std::time::Duration::ZERO,
            &Err::<(), _>(MatchingTaskStoreError::NotFound),
        );
        let events = sink.events();
        assert!(events
            .iter()
            .any(|event| event.name == "persistence_requests_per_domain"));
        assert!(events
            .iter()
            .any(|event| event.name == "persistence_errors_entity_not_exists_per_domain"));
        assert!(events
            .iter()
            .all(|event| event.tags["domain"] == "_unknown_"
                && !event.tags.contains_key("task_category")));
    }

    #[test]
    fn success_and_condition_failure_use_go_names_and_tags() {
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        limiter.record_task_call(
            "GetTasks",
            None,
            std::time::Duration::from_millis(3),
            &Ok::<_, MatchingTaskStoreError>(()),
        );
        limiter.record_task_call(
            "UpdateTaskList",
            None,
            std::time::Duration::from_millis(1),
            &Err::<(), _>(MatchingTaskStoreError::ConditionFailed("lease".into())),
        );
        let events = sink.events();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.name == "persistence_requests")
                .count(),
            2
        );
        assert!(events
            .iter()
            .any(|e| e.name == "persistence_errors_condition_failed"));
        assert!(!events.iter().any(|e| e.name == "persistence_errors"));
        assert!(events
            .iter()
            .all(|e| e.tags["is_retry"] == "false" && e.tags["task_category"] == "none"));
        let histogram = events
            .iter()
            .find(|e| e.name == "persistence_latency_ns")
            .unwrap();
        assert_eq!(histogram.histogram, Some(HistogramSpec::GoDefault1ms100s));
        assert_eq!(histogram.tags["histogram_scale"], "2");
    }
}
