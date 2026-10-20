//! Small backend-neutral metrics interface for the Matching service.
//!
//! Names and dimensions here intentionally follow `go/common/metrics/defs.go`
//! and `tags.go`; an exporter can translate these events into its native API.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Timer,
    Histogram,
    Gauge,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    Count(i64),
    Duration(Duration),
    Number(f64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramSpec {
    Exponential {
        minimum: Duration,
        maximum: Duration,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricEvent {
    pub name: &'static str,
    pub kind: MetricKind,
    pub value: MetricValue,
    pub tags: BTreeMap<String, String>,
    pub histogram: Option<HistogramSpec>,
}

pub trait MetricSink: Send + Sync + 'static {
    fn record(&self, event: MetricEvent);
}

#[derive(Debug, Default)]
pub struct NoopMetricSink;
impl MetricSink for NoopMetricSink {
    fn record(&self, _: MetricEvent) {}
}

#[derive(Debug, Default)]
pub struct RecordingMetricSink(Mutex<Vec<MetricEvent>>);
impl RecordingMetricSink {
    pub fn events(&self) -> Vec<MetricEvent> {
        self.0.lock().expect("metrics lock poisoned").clone()
    }
}
impl MetricSink for RecordingMetricSink {
    fn record(&self, event: MetricEvent) {
        self.0.lock().expect("metrics lock poisoned").push(event);
    }
}

pub const CADENCE_CLIENT_REQUESTS: &str = "cadence_client_requests";
pub const CADENCE_CLIENT_ERRORS: &str = "cadence_client_errors";
pub const CADENCE_CLIENT_LATENCY: &str = "cadence_client_latency";
pub const CADENCE_CLIENT_LATENCY_NS: &str = "cadence_client_latency_ns";
pub const OPERATION_TAG: &str = "operation";
pub const CADENCE_ROLE_TAG: &str = "cadence_role";
pub const IS_RETRY_TAG: &str = "is_retry";
pub const HISTORY_CLIENT_ROLE: &str = "history_client";

/// Go `HistoryClient*Scope` plus its static role tag. The Go client metering
/// wrapper is outside History's retry and redirect implementation, so events
/// are emitted once for the complete operation, not once per transport try.
#[derive(Clone)]
pub struct MatchingMetrics(Arc<dyn MetricSink>);

impl Default for MatchingMetrics {
    fn default() -> Self {
        Self(Arc::new(NoopMetricSink))
    }
}

impl MatchingMetrics {
    pub fn new(sink: Arc<dyn MetricSink>) -> Self {
        Self(sink)
    }

    pub fn history_started(
        &self,
        method: &'static str,
        retry: Option<bool>,
    ) -> BTreeMap<String, String> {
        let mut tags = BTreeMap::from([
            (OPERATION_TAG.to_owned(), format!("HistoryClient{method}")),
            (CADENCE_ROLE_TAG.to_owned(), HISTORY_CLIENT_ROLE.to_owned()),
        ]);
        // Go adds is_retry only when a retry context carries a known count.
        // Matching's History RPC adapter currently supplies no such context.
        if let Some(retry) = retry {
            tags.insert(IS_RETRY_TAG.to_owned(), retry.to_string());
        }
        self.0.record(MetricEvent {
            name: CADENCE_CLIENT_REQUESTS,
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags: tags.clone(),
            histogram: None,
        });
        tags
    }

    pub fn history_completed(
        &self,
        tags: BTreeMap<String, String>,
        elapsed: Duration,
        failed: bool,
    ) {
        if failed {
            self.0.record(MetricEvent {
                name: CADENCE_CLIENT_ERRORS,
                kind: MetricKind::Counter,
                value: MetricValue::Count(1),
                tags: tags.clone(),
                histogram: None,
            });
        }
        self.0.record(MetricEvent {
            name: CADENCE_CLIENT_LATENCY,
            kind: MetricKind::Timer,
            value: MetricValue::Duration(elapsed),
            tags: tags.clone(),
            histogram: None,
        });
        self.0.record(MetricEvent {
            name: CADENCE_CLIENT_LATENCY_NS,
            kind: MetricKind::Histogram,
            value: MetricValue::Duration(elapsed),
            tags,
            histogram: Some(HistogramSpec::Exponential {
                minimum: Duration::from_millis(1),
                maximum: Duration::from_secs(100),
            }),
        });
    }
}
