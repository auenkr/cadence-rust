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
    /// Go `common/metrics.Low1ms100s`: 1 ms to 100 s intent, scale 1,
    /// with the same 40 sampled exponential buckets (plus sentinel bounds).
    GoLow1ms100s,
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
pub const CADENCE_REQUESTS_PER_TASK_LIST: &str = "cadence_requests_per_tl";
pub const CADENCE_FAILURES_PER_TASK_LIST: &str = "cadence_errors_per_tl";
pub const CADENCE_LATENCY_PER_TASK_LIST: &str = "cadence_latency_per_tl";
pub const CADENCE_LATENCY_PER_TASK_LIST_NS: &str = "cadence_latency_per_tl_ns";
pub const DOMAIN_TAG: &str = "domain";
pub const TASK_LIST_TAG: &str = "task_list";
pub const SEMAPHORE_NAME_TAG: &str = "semaphore_name";

fn go_sanitize_metric_value(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Go `HistoryClient*Scope` plus its static role tag. The Go client metering
/// wrapper is outside History's retry and redirect implementation, so events
/// are emitted once for the complete operation, not once per transport try.
#[derive(Clone)]
pub struct MatchingMetrics(Arc<dyn MetricSink>);

struct InboundRequestTimer {
    sink: Arc<dyn MetricSink>,
    tags: BTreeMap<String, String>,
    start: std::time::Instant,
    finished: bool,
}
impl InboundRequestTimer {
    fn finish(&mut self, failed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed = self.start.elapsed();
        if failed {
            self.sink.record(MetricEvent {
                name: CADENCE_FAILURES_PER_TASK_LIST,
                kind: MetricKind::Counter,
                value: MetricValue::Count(1),
                tags: self.tags.clone(),
                histogram: None,
            });
        }
        self.sink.record(MetricEvent {
            name: CADENCE_LATENCY_PER_TASK_LIST,
            kind: MetricKind::Timer,
            value: MetricValue::Duration(elapsed),
            tags: self.tags.clone(),
            histogram: None,
        });
        self.sink.record(MetricEvent {
            name: CADENCE_LATENCY_PER_TASK_LIST_NS,
            kind: MetricKind::Histogram,
            value: MetricValue::Duration(elapsed),
            tags: self.tags.clone(),
            histogram: Some(HistogramSpec::GoLow1ms100s),
        });
    }
}
impl Drop for InboundRequestTimer {
    fn drop(&mut self) {
        self.finish(false);
    }
}

pub trait MatchingErrorClass {
    fn matching_error_metric(&self) -> &'static str;
}
impl MatchingErrorClass for tonic::Status {
    fn matching_error_metric(&self) -> &'static str {
        let detail = String::from_utf8_lossy(self.details());
        for (type_name, metric) in [
            ("InternalServiceError", CADENCE_FAILURES_PER_TASK_LIST),
            ("BadRequestError", "cadence_errors_bad_request_per_tl"),
            (
                "DomainNotActiveError",
                "cadence_errors_domain_not_active_per_tl",
            ),
            ("ServiceBusyError", "cadence_errors_service_busy_per_tl"),
            (
                "EntityNotExistsError",
                "cadence_errors_entity_not_exists_per_tl",
            ),
            (
                "WorkflowExecutionAlreadyStartedError",
                "cadence_errors_execution_already_started_per_tl",
            ),
            (
                "DomainAlreadyExistsError",
                "cadence_errors_domain_already_exists_per_tl",
            ),
            (
                "CancellationAlreadyRequestedError",
                "cadence_errors_cancellation_already_requested_per_tl",
            ),
            ("QueryFailedError", "cadence_errors_query_failed_per_tl"),
            ("LimitExceededError", "cadence_errors_limit_exceeded_per_tl"),
            (
                "RemoteSyncMatchedError",
                "cadence_errors_remote_syncmatch_failed_per_tl",
            ),
            (
                "StickyWorkerUnavailableError",
                "cadence_errors_sticky_worker_unavailable_per_tl",
            ),
            (
                "ReadOnlyPartitionError",
                "cadence_errors_read_only_partition_per_tl",
            ),
            (
                "TaskListNotOwnedByHostError",
                "cadence_errors_task_list_not_owned_by_host_per_tl",
            ),
            (
                "SemaphoreNotOwnedByHostError",
                "cadence_errors_semaphore_not_owned_by_host",
            ),
        ] {
            if detail.contains(type_name) {
                return metric;
            }
        }
        match self.code() {
            tonic::Code::InvalidArgument => "cadence_errors_bad_request_per_tl",
            tonic::Code::NotFound => "cadence_errors_entity_not_exists_per_tl",
            tonic::Code::ResourceExhausted => "cadence_errors_service_busy_per_tl",
            tonic::Code::DeadlineExceeded => "cadence_errors_context_timeout_per_tl",
            _ => CADENCE_FAILURES_PER_TASK_LIST,
        }
    }
}
impl MatchingErrorClass for &'static str {
    fn matching_error_metric(&self) -> &'static str {
        CADENCE_FAILURES_PER_TASK_LIST
    }
}

impl Default for MatchingMetrics {
    fn default() -> Self {
        Self(Arc::new(NoopMetricSink))
    }
}

impl MatchingMetrics {
    pub fn new(sink: Arc<dyn MetricSink>) -> Self {
        Self(sink)
    }

    /// Record one complete inbound Matching RPC. This is called at the shared
    /// MatchingAPI trait boundary, which Thrift dispatch also invokes.
    pub async fn matching_request<F, T, E>(
        &self,
        method: &'static str,
        domain: Option<&str>,
        task_list: Option<(&str, Option<i32>)>,
        future: F,
    ) -> Result<T, E>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: MatchingErrorClass,
    {
        let mut tags = BTreeMap::from([(OPERATION_TAG.to_owned(), method.to_owned())]);
        tags.insert(
            DOMAIN_TAG.to_owned(),
            domain
                .filter(|v| !v.is_empty())
                .unwrap_or("_unknown_")
                .to_owned(),
        );
        if let Some((task_list, kind)) = task_list {
            let key = if method == "AcquireSemaphore" {
                SEMAPHORE_NAME_TAG
            } else {
                TASK_LIST_TAG
            };
            let value = match kind {
                Some(1) => "__sticky__",
                Some(2) => "__ephemeral__",
                Some(0) if !task_list.is_empty() => task_list,
                _ => "_unknown_",
            };
            tags.insert(key.to_owned(), go_sanitize_metric_value(value));
        } else {
            if method == "AcquireSemaphore" {
                tags.insert(SEMAPHORE_NAME_TAG.to_owned(), "_unknown_".to_owned());
            } else {
                tags.insert(TASK_LIST_TAG.to_owned(), "_unknown_".to_owned());
            }
        }
        self.0.record(MetricEvent {
            name: CADENCE_REQUESTS_PER_TASK_LIST,
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags: tags.clone(),
            histogram: None,
        });
        let mut timer = InboundRequestTimer {
            sink: self.0.clone(),
            tags,
            start: std::time::Instant::now(),
            finished: false,
        };
        let result = future.await;
        if let Err(ref error) = result {
            let metric = error.matching_error_metric();
            if metric != CADENCE_FAILURES_PER_TASK_LIST {
                timer.sink.record(MetricEvent {
                    name: metric,
                    kind: MetricKind::Counter,
                    value: MetricValue::Count(1),
                    tags: timer.tags.clone(),
                    histogram: None,
                });
                timer.finish(false);
            } else {
                timer.finish(true);
            }
        } else {
            timer.finish(false);
        }
        result
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
            histogram: Some(HistogramSpec::GoLow1ms100s),
        });
    }
}

#[cfg(test)]
mod inbound_tests {
    use super::*;

    #[tokio::test]
    async fn inbound_rpc_records_request_latency_and_failure_once() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let result: Result<(), &'static str> = metrics
            .matching_request(
                "AcquireSemaphore",
                Some("domain-a"),
                Some(("locks", Some(0))),
                async { Err("typed service error") },
            )
            .await;
        assert_eq!(result, Err("typed service error"));
        let events = sink.events();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].name, CADENCE_REQUESTS_PER_TASK_LIST);
        assert_eq!(events[1].name, CADENCE_FAILURES_PER_TASK_LIST);
        assert_eq!(events[2].name, CADENCE_LATENCY_PER_TASK_LIST);
        assert_eq!(events[3].name, CADENCE_LATENCY_PER_TASK_LIST_NS);
        assert_eq!(events[0].tags[OPERATION_TAG], "AcquireSemaphore");
        assert_eq!(events[0].tags[DOMAIN_TAG], "domain-a");
        assert_eq!(events[0].tags[SEMAPHORE_NAME_TAG], "locks");
    }

    #[tokio::test]
    async fn inbound_rpc_success_has_no_failure_event() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        metrics
            .matching_request::<_, (), &'static str>("QueryWorkflow", None, None, async { Ok(()) })
            .await
            .unwrap();
        let events = sink.events();
        assert_eq!(events.len(), 3);
        assert!(events
            .iter()
            .all(|event| event.name != CADENCE_FAILURES_PER_TASK_LIST));
    }

    #[tokio::test]
    async fn inbound_task_list_scope_masks_sticky_name_and_unknown_domain() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        metrics
            .matching_request::<_, (), &'static str>(
                "PollForDecisionTask",
                None,
                Some(("worker-unique", Some(1))),
                async { Ok(()) },
            )
            .await
            .unwrap();
        let event = sink.events().remove(0);
        assert_eq!(event.tags[DOMAIN_TAG], "_unknown_");
        assert_eq!(event.tags[TASK_LIST_TAG], "__sticky__");
    }

    #[tokio::test]
    async fn inbound_cancelled_future_records_latency_without_failure() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let task = tokio::spawn(async move {
            metrics
                .matching_request::<_, (), tonic::Status>(
                    "PollForActivityTask",
                    Some("d"),
                    None,
                    std::future::pending::<Result<(), tonic::Status>>(),
                )
                .await
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;
        let events = sink.events();
        assert_eq!(events.len(), 3);
        assert!(events
            .iter()
            .any(|event| event.name == CADENCE_LATENCY_PER_TASK_LIST));
        assert!(events
            .iter()
            .all(|event| event.name != CADENCE_FAILURES_PER_TASK_LIST));
    }

    #[tokio::test]
    async fn inbound_rpc_classifies_typed_query_failure() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let status = tonic::Status::with_details(
            tonic::Code::FailedPrecondition,
            "query failed",
            bytes::Bytes::from_static(b"type.googleapis.com/uber.cadence.api.v1.QueryFailedError"),
        );
        let _: Result<(), _> = metrics
            .matching_request("QueryWorkflow", Some("domain-a"), None, async {
                Err(status)
            })
            .await;
        let events = sink.events();
        assert!(events
            .iter()
            .any(|event| event.name == "cadence_errors_query_failed_per_tl"));
        assert!(!events
            .iter()
            .any(|event| event.name == CADENCE_FAILURES_PER_TASK_LIST));
    }
}
