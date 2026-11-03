//! Production Frontend metrics: adapts the metered wrapper's `Metrics` seam to
//! the shared `MetricSink` (StatsD/M3/Prometheus exporters from Matching).
//!
//! Go (`wrappers/metered`) emits on a per-operation scope tagged with
//! `operation=<ScopeFrontendX>` plus the request tags: `cadence_requests`,
//! `cadence_latency` (timer), `cadence_latency_ns` (histogram) and the error
//! counters of [`Counter::metric_name`].

use super::metered::{Counter, MetricTag, Metrics};
use cadence_service_core::matching_metrics::{
    HistogramSpec, MetricEvent, MetricKind, MetricSink, MetricValue,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub const CADENCE_REQUESTS: &str = "cadence_requests";
pub const CADENCE_LATENCY: &str = "cadence_latency";
pub const CADENCE_LATENCY_NS: &str = "cadence_latency_ns";

pub struct SinkMetrics {
    sink: Arc<dyn MetricSink>,
}

impl SinkMetrics {
    pub fn new(sink: Arc<dyn MetricSink>) -> Self {
        Self { sink }
    }

    fn tags(operation: &str, tags: &[MetricTag]) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        out.insert("operation".to_owned(), operation.to_owned());
        for tag in tags {
            out.insert(tag.key.to_owned(), tag.value.clone());
            if tag.key == "wf-domain-name" {
                out.insert("domain".to_owned(), tag.value.clone());
            }
        }
        out
    }

    fn emit(
        &self,
        name: &'static str,
        kind: MetricKind,
        value: MetricValue,
        tags: BTreeMap<String, String>,
        histogram: Option<HistogramSpec>,
    ) {
        self.sink.record(MetricEvent {
            name,
            kind,
            value,
            tags,
            histogram,
        });
    }
}

impl Metrics for SinkMetrics {
    fn increment(&self, counter: Counter) {
        self.emit(
            counter.metric_name(),
            MetricKind::Counter,
            MetricValue::Count(1),
            BTreeMap::new(),
            None,
        );
    }

    fn increment_error(&self, operation: &str, tags: &[MetricTag], counter: Counter) {
        // Go increments exactly one typed counter (`cadence_errors` only for
        // uncategorized failures).
        self.emit(
            counter.metric_name(),
            MetricKind::Counter,
            MetricValue::Count(1),
            Self::tags(operation, tags),
            None,
        );
    }

    fn increment_requests(&self, operation: &str) {
        self.emit(
            CADENCE_REQUESTS,
            MetricKind::Counter,
            MetricValue::Count(1),
            Self::tags(operation, &[]),
            None,
        );
    }

    fn record_operation(&self, operation: &str, elapsed: Duration, tags: &[MetricTag]) {
        self.emit(
            CADENCE_LATENCY,
            MetricKind::Timer,
            MetricValue::Duration(elapsed),
            Self::tags(operation, tags),
            None,
        );
    }

    fn record_latency_histogram(&self, operation: &str, elapsed: Duration) {
        self.emit(
            CADENCE_LATENCY_NS,
            MetricKind::Histogram,
            MetricValue::Duration(elapsed),
            Self::tags(operation, &[]),
            Some(HistogramSpec::GoDefault1ms100s),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_service_core::matching_metrics::RecordingMetricSink;

    fn domain_tag() -> Vec<MetricTag> {
        vec![MetricTag {
            key: "wf-domain-name",
            value: "d1".into(),
        }]
    }

    #[test]
    fn request_latency_and_errors_carry_operation_and_domain_tags() {
        let sink = Arc::new(RecordingMetricSink::default());
        let m = SinkMetrics::new(sink.clone());
        m.increment_requests("StartWorkflowExecution");
        m.record_operation(
            "StartWorkflowExecution",
            Duration::from_millis(3),
            &domain_tag(),
        );
        m.record_latency_histogram("StartWorkflowExecution", Duration::from_millis(3));
        m.increment_error("StartWorkflowExecution", &domain_tag(), Counter::BadRequest);
        let events = sink.events();
        let names: Vec<_> = events.iter().map(|e| e.name).collect();
        assert_eq!(
            names,
            [
                "cadence_requests",
                "cadence_latency",
                "cadence_latency_ns",
                "cadence_errors_bad_request"
            ]
        );
        assert_eq!(events[1].tags["operation"], "StartWorkflowExecution");
        assert_eq!(events[1].tags["domain"], "d1");
        assert_eq!(events[1].tags["wf-domain-name"], "d1");
        assert_eq!(events[1].kind, MetricKind::Timer);
        assert_eq!(events[2].kind, MetricKind::Histogram);
        assert_eq!(events[3].tags["domain"], "d1");
    }

    #[test]
    fn each_failure_emits_one_typed_counter() {
        let sink = Arc::new(RecordingMetricSink::default());
        let m = SinkMetrics::new(sink.clone());
        m.increment_error("X", &[], Counter::Failures);
        m.increment_error("X", &[], Counter::ServiceBusy);
        let names: Vec<_> = sink.events().iter().map(|e| e.name).collect();
        assert_eq!(names, ["cadence_errors", "cadence_errors_service_busy"]);
    }
}
