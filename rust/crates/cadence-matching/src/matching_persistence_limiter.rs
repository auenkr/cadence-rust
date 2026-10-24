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

/// Go metered-wrapper error classes (`base.updateErrorMetric`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistenceErrorClass {
    ConditionFailed,
    NotFound,
    Timeout,
    Busy,
    DbUnavailable,
    BadRequest,
    Internal,
}

impl PersistenceErrorClass {
    /// (overall counter, per-domain counter, also counts `persistence_errors`).
    fn metric_names(self) -> (&'static str, &'static str, bool) {
        use crate::matching_metrics::*;
        match self {
            Self::ConditionFailed => (
                "persistence_errors_condition_failed",
                "persistence_errors_condition_failed_per_domain",
                false,
            ),
            Self::NotFound => (
                "persistence_errors_entity_not_exists",
                "persistence_errors_entity_not_exists_per_domain",
                false,
            ),
            Self::Timeout => (
                "persistence_errors_timeout",
                "persistence_errors_timeout_per_domain",
                true,
            ),
            Self::Busy => (PERSISTENCE_ERRORS_BUSY, PERSISTENCE_ERRORS_BUSY_PER_DOMAIN, true),
            Self::DbUnavailable => (
                PERSISTENCE_ERRORS_DB_UNAVAILABLE,
                PERSISTENCE_ERRORS_DB_UNAVAILABLE_PER_DOMAIN,
                true,
            ),
            Self::BadRequest => (
                PERSISTENCE_ERRORS_BAD_REQUEST,
                PERSISTENCE_ERRORS_BAD_REQUEST_PER_DOMAIN,
                false,
            ),
            Self::Internal => (PERSISTENCE_FAILURES, PERSISTENCE_FAILURES_PER_DOMAIN, false),
        }
    }
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
    host_name: RwLock<String>,
    manual_latency_histogram: RwLock<bool>,
}

impl std::fmt::Debug for MatchingPersistenceLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchingPersistenceLimiter")
            .field("rejected", &self.rejected())
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

/// `metricRollupName` of the `persistence_errors_*_per_domain` defs
/// (go/common/metrics/defs.go:3360-3371).
fn persistence_error_rollup_name(per_domain_name: &str) -> Option<&'static str> {
    Some(match per_domain_name {
        "persistence_errors_condition_failed_per_domain" => "persistence_errors_condition_failed_rollup",
        "persistence_errors_timeout_per_domain" => "persistence_errors_timeout_rollup",
        "persistence_errors_busy_per_domain" => "persistence_errors_busy_rollup",
        "persistence_errors_entity_not_exists_per_domain" => "persistence_errors_entity_not_exists_rollup",
        "persistence_errors_bad_request_per_domain" => "persistence_errors_bad_request_rollup",
        "persistence_errors_db_unavailable_per_domain" => "persistence_errors_db_unavailable_rollup",
        _ => return None,
    })
}

tokio::task_local! {
    /// Go `backoff.retryCountKey` context value: failed attempts so far in the
    /// enclosing `ThrottleRetry`. Absent context reads as 0 (go/common/backoff/retry.go:39).
    static PERSISTENCE_RETRY_COUNT: usize;
}

/// Go `backoff.GetRetryCountFromContext`.
pub(crate) fn persistence_retry_count() -> usize {
    PERSISTENCE_RETRY_COUNT.try_with(|count| *count).unwrap_or(0)
}

/// Go `context.WithValue(ctx, retryCountKey, retryCount)` (retry.go:270).
pub(crate) async fn with_persistence_retry_count<F: std::future::Future>(count: usize, future: F) -> F::Output {
    PERSISTENCE_RETRY_COUNT.scope(count, future).await
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
            host_name: RwLock::new(String::new()),
            manual_latency_histogram: RwLock::new(false),
        })
    }

    /// Go's metered persistence reads `HostName` from the process (`os.Hostname`);
    /// an empty name disables the per-host series, as in Go.
    pub fn with_host_name(&self, host: &str) {
        *self.host_name.write().expect("Matching persistence host lock") = host.to_owned();
    }

    /// Go `enablePersistenceLatencyHistogramMetrics`, read once at startup.
    pub fn with_manual_latency_histogram(&self, enabled: bool) {
        *self
            .manual_latency_histogram
            .write()
            .expect("Matching persistence histogram lock") = enabled;
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
        use cadence_persistence::matching_task_store::{BackendErrorKind, MatchingTaskStoreError};
        let class = match result {
            Ok(_) => None,
            Err(MatchingTaskStoreError::ConditionFailed(_)) => Some(PersistenceErrorClass::ConditionFailed),
            Err(MatchingTaskStoreError::NotFound) => Some(PersistenceErrorClass::NotFound),
            Err(MatchingTaskStoreError::Backend { kind, .. }) => Some(match kind {
                BackendErrorKind::Timeout => PersistenceErrorClass::Timeout,
                BackendErrorKind::Throttled => PersistenceErrorClass::Busy,
                BackendErrorKind::DbUnavailable => PersistenceErrorClass::DbUnavailable,
                BackendErrorKind::BadRequest => PersistenceErrorClass::BadRequest,
                _ => PersistenceErrorClass::Internal,
            }),
            Err(_) => Some(PersistenceErrorClass::Internal),
        };
        let domain = domain_id.map(|id| {
            (self
                .domain_name
                .read()
                .expect("Matching metric domain resolver lock"))(id)
            .unwrap_or_else(|| "_unknown_".into())
        });
        self.record_call(operation, domain, elapsed, class);
    }

    /// Go `metered.base.call` for the domain manager (no domain tag on its
    /// requests, so the overall family with `task_category=none`).
    pub(crate) fn record_domain_call(
        &self,
        operation: &'static str,
        elapsed: std::time::Duration,
        class: Option<PersistenceErrorClass>,
    ) {
        self.record_call(operation, None, elapsed, class);
    }

    fn record_call(
        &self,
        operation: &'static str,
        domain: Option<String>,
        elapsed: std::time::Duration,
        class: Option<PersistenceErrorClass>,
    ) {
        use crate::matching_metrics::*;
        let sink = self.metric_sink.read().expect("Matching metrics sink lock");
        let mut tags = std::collections::BTreeMap::from([
            (OPERATION_TAG.to_owned(), operation.to_owned()),
            (IS_RETRY_TAG.to_owned(), (persistence_retry_count() > 0).to_string()),
        ]);
        let per_domain = domain.is_some();
        match domain {
            Some(domain) => {
                tags.insert(DOMAIN_TAG.to_owned(), domain);
            }
            None => {
                tags.insert(TASK_CATEGORY_TAG.to_owned(), "none".into());
            }
        }
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
        if per_domain {
            // Go metricsScope.{AddCounter,RecordTimer} (scope.go:55-125): the
            // per-domain defs also emit their `metricRollupName` on the root
            // scope (no operation/domain/is_retry tags). Default (zero)
            // MigrationConfig: counters and the latency timer emit; the _ns
            // rollup histogram is suppressed (config.go:125-126, EmitHistogram
            // default false).
            let rollup = |name: &'static str, kind, value| {
                sink.record(MetricEvent {
                    name,
                    kind,
                    value,
                    tags: Default::default(),
                    histogram: None,
                })
            };
            rollup("persistence_requests_rollup", MetricKind::Counter, MetricValue::Count(1));
            rollup("persistence_latency_rollup", MetricKind::Timer, MetricValue::Duration(elapsed));
            // Go scope.go emits the root histogram rollup when the migration
            // policy enables it. The policy sink suppresses this by default.
            sink.record(MetricEvent {
                name: "persistence_latency_ns_rollup",
                kind: MetricKind::Histogram,
                value: MetricValue::Duration(elapsed),
                tags: std::collections::BTreeMap::from([
                    ("histogram_start".into(), "1ms".into()),
                    ("histogram_end".into(), "17m28.576s".into()),
                    ("histogram_scale".into(), "2".into()),
                ]),
                histogram: Some(HistogramSpec::GoDefault1ms100s),
            });
            if let Some(class) = class {
                let (_, per_domain_name, failure) = class.metric_names();
                if let Some(name) = persistence_error_rollup_name(per_domain_name) {
                    rollup(name, MetricKind::Counter, MetricValue::Count(1));
                }
                if failure {
                    rollup("persistence_errors_rollup", MetricKind::Counter, MetricValue::Count(1));
                }
            }
        }
        let host = self.host_name.read().expect("Matching persistence host lock");
        if !host.is_empty() {
            record_persistence_host_latency(sink.as_ref(), &host, elapsed);
        }
        if *self
            .manual_latency_histogram
            .read()
            .expect("Matching persistence histogram lock")
        {
            record_persistence_manual_latency(sink.as_ref(), operation, elapsed);
        }
        if let Some(class) = class {
            let (name, per_domain_name, failure) = class.metric_names();
            record(
                if per_domain { per_domain_name } else { name },
                MetricKind::Counter,
                MetricValue::Count(1),
                None,
            );
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

    /// Go `emitRowCountMetrics` / `emitPayloadSizeMetrics` for a successful list
    /// call: empty count or row-size histogram, plus payload bytes.
    pub(crate) fn record_list_response(&self, operation: &'static str, rows: u64, payload_bytes: u64) {
        use crate::matching_metrics::*;
        let sink = self.metric_sink.read().expect("Matching metrics sink lock");
        if rows == 0 {
            record_persistence_empty_response(sink.as_ref(), operation);
        } else {
            record_persistence_response_rows(sink.as_ref(), operation, rows);
        }
        record_persistence_response_payload(sink.as_ref(), operation, payload_bytes);
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
        // Rollups are root-scope (untagged) in Go.
        assert!(events
            .iter()
            .filter(|event| !event.name.ends_with("_rollup"))
            .all(|event| event.tags["domain"] == "_unknown_"
                && !event.tags.contains_key("task_category")));
    }

    #[test]
    fn empty_rows_and_host_latency_follow_go_metered_wrapper() {
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        limiter.with_host_name("matching-host-a");
        limiter.record_task_call(
            "GetTasks",
            Some(uuid::Uuid::nil()),
            std::time::Duration::from_millis(1),
            &Ok::<_, MatchingTaskStoreError>(()),
        );
        limiter.record_list_response("GetTasks", 0, 24);
        let events = sink.events();
        let empty = events
            .iter()
            .find(|e| e.name == "persistence_empty_response")
            .unwrap();
        assert_eq!(empty.tags["operation"], "GetTasks");
        assert_eq!(empty.tags["task_category"], "none");
        assert!(!empty.tags.contains_key("domain"));
        let host = events
            .iter()
            .find(|e| e.name == "persistence_latency_histogram_per_host")
            .unwrap();
        assert_eq!(host.tags["host"], "matching-host-a");
        assert_eq!(host.tags["operation"], "persistence_operations");
        assert_eq!(host.histogram, Some(HistogramSpec::GoPersistenceLatency));
        let payload = events
            .iter()
            .find(|e| e.name == "persistence_response_payload_size")
            .unwrap();
        assert_eq!(payload.value, crate::matching_metrics::MetricValue::Count(24));
        assert_eq!(payload.histogram, Some(HistogramSpec::GoResponsePayloadSize));
        assert!(!events.iter().any(|e| e.name == "persistence_response_row_size"));
    }

    #[test]
    fn non_empty_list_emits_row_size_and_db_unavailable_and_bad_request_classes() {
        use cadence_persistence::matching_task_store::BackendErrorKind;
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        limiter.record_list_response("GetTasks", 5, 700);
        limiter.record_task_call(
            "GetTasks",
            None,
            std::time::Duration::from_millis(1),
            &Err::<(), _>(MatchingTaskStoreError::Backend {
                kind: BackendErrorKind::DbUnavailable,
                message: "unavailable".into(),
            }),
        );
        limiter.record_task_call(
            "GetTasks",
            Some(uuid::Uuid::nil()),
            std::time::Duration::from_millis(1),
            &Err::<(), _>(MatchingTaskStoreError::Backend {
                kind: BackendErrorKind::BadRequest,
                message: "bad".into(),
            }),
        );
        let events = sink.events();
        let rows = events
            .iter()
            .find(|e| e.name == "persistence_response_row_size")
            .unwrap();
        assert_eq!(rows.value, crate::matching_metrics::MetricValue::Count(5));
        assert_eq!(rows.histogram, Some(HistogramSpec::GoResponseRowSize));
        assert!(!events.iter().any(|e| e.name == "persistence_empty_response"));
        assert!(events
            .iter()
            .any(|e| e.name == "persistence_errors_db_unavailable"));
        // DBUnavailable also counts the generic failure; BadRequest does not.
        assert_eq!(
            events.iter().filter(|e| e.name == "persistence_errors").count(),
            1
        );
        assert!(events
            .iter()
            .any(|e| e.name == "persistence_errors_bad_request_per_domain"));
    }

    #[test]
    fn domain_wrapper_calls_use_overall_family_with_task_category_none() {
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        limiter.record_domain_call(
            "GetDomain",
            std::time::Duration::from_millis(2),
            Some(PersistenceErrorClass::NotFound),
        );
        let events = sink.events();
        let request = events
            .iter()
            .find(|e| e.name == "persistence_requests")
            .unwrap();
        assert_eq!(request.tags["operation"], "GetDomain");
        assert_eq!(request.tags["task_category"], "none");
        assert!(events
            .iter()
            .any(|e| e.name == "persistence_errors_entity_not_exists"));
    }

    #[test]
    fn manual_latency_histogram_is_opt_in_and_operation_tagged() {
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        let call = |limiter: &MatchingPersistenceLimiter| {
            limiter.record_task_call(
                "GetTasks",
                None,
                std::time::Duration::from_millis(2),
                &Ok::<_, MatchingTaskStoreError>(()),
            );
        };
        call(&limiter);
        assert!(!sink
            .events()
            .iter()
            .any(|e| e.name == "persistence_latency_histogram"));
        limiter.with_manual_latency_histogram(true);
        call(&limiter);
        let manual = sink
            .events()
            .into_iter()
            .find(|e| e.name == "persistence_latency_histogram")
            .unwrap();
        assert_eq!(manual.kind, MetricKind::Histogram);
        assert_eq!(
            manual.tags,
            std::collections::BTreeMap::from([("operation".to_owned(), "GetTasks".to_owned())])
        );
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

    /// Go retry.go:217-270 + metered *_generated.go: is_retry = retryCount > 0,
    /// where the count is failed attempts so far; no context reads as 0.
    #[tokio::test]
    async fn is_retry_tag_follows_go_retry_count_context() {
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        let d = std::time::Duration::from_millis(1);
        limiter.record_domain_call("GetDomain", d, None);
        for count in [0usize, 1, 2] {
            with_persistence_retry_count(count, async {
                limiter.record_domain_call("GetDomain", d, None);
            })
            .await;
        }
        let flags: Vec<String> = sink
            .events()
            .iter()
            .filter(|e| e.name == "persistence_requests")
            .map(|e| e.tags["is_retry"].clone())
            .collect();
        assert_eq!(flags, ["false", "false", "true", "true"]);
    }

    /// Root rollup candidates include the histogram. The runtime migration sink
    /// suppresses that histogram with the Go default and enables it by config.
    #[test]
    fn per_domain_task_calls_emit_go_rollups() {
        let limiter = MatchingPersistenceLimiter::new(
            Arc::new(Mutex::new(DynamicConfigOverlay::default())),
            Arc::new(AtomicUsize::new(1)),
        );
        let sink = Arc::new(RecordingMetricSink::default());
        limiter.with_metric_sink(sink.clone());
        limiter.record_task_call(
            "GetTasks",
            Some(uuid::Uuid::nil()),
            std::time::Duration::from_millis(2),
            &Err::<(), _>(MatchingTaskStoreError::ConditionFailed("x".into())),
        );
        let events = sink.events();
        let names: Vec<_> = events.iter().filter(|e| e.name.ends_with("_rollup")).map(|e| e.name).collect();
        assert_eq!(
            names,
            ["persistence_requests_rollup", "persistence_latency_rollup", "persistence_latency_ns_rollup", "persistence_errors_condition_failed_rollup"]
        );
        assert!(events.iter().filter(|e| e.name.ends_with("_rollup") && e.kind != MetricKind::Histogram).all(|e| e.tags.is_empty()));
    }
}
