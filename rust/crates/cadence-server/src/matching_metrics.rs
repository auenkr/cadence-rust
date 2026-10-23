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
    /// Go `common/metrics.Mid1To16k`, used for integer attempt counts.
    GoMid1To16k,
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

    /// Flush buffered reporter state after Matching has drained traffic.
    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
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
pub const CADENCE_TASKLIST_REQUEST: &str = "cadence_tasklist_request";
pub const TASKLIST_MANAGERS: &str = "tasklist_managers";
pub const CADENCE_LATENCY_PER_TASK_LIST: &str = "cadence_latency_per_tl";
pub const CADENCE_LATENCY_PER_TASK_LIST_NS: &str = "cadence_latency_per_tl_ns";
pub const DOMAIN_TAG: &str = "domain";
pub const TASK_LIST_TAG: &str = "task_list";
pub const GO_TASK_LIST_TAG: &str = "tasklist";
pub const TASK_LIST_TYPE_TAG: &str = "tasklistType";
pub const MATCHING_HOST_TAG: &str = "matching_host";
pub const SEMAPHORE_NAME_TAG: &str = "semaphore_name";
pub const CALLER_TAG: &str = "caller";
pub const TRANSPORT_TAG: &str = "transport";
pub const POLL_SUCCESS: &str = "poll_success_per_tl";
pub const POLL_TIMEOUT: &str = "poll_timeouts_per_tl";
pub const POLL_SUCCESS_SYNC: &str = "poll_success_sync_per_tl";
pub const LOCAL_TO_LOCAL_MATCH: &str = "local_to_local_matches_per_tl";
pub const LOCAL_TO_REMOTE_MATCH: &str = "local_to_remote_matches_per_tl";
pub const REMOTE_TO_LOCAL_MATCH: &str = "remote_to_local_matches_per_tl";
pub const REMOTE_TO_REMOTE_MATCH: &str = "remote_to_remote_matches_per_tl";
pub const ISOLATION_TASK_MATCH: &str = "isolation_task_matches_per_tl";
pub const ISOLATION_SUCCESS: &str = "isolation_success_per_tl";
pub const POLL_DECISION_ALREADY_STARTED: &str = "poll_decision_task_already_started_per_tl";
pub const POLL_ACTIVITY_ALREADY_STARTED: &str = "poll_activity_task_already_started_per_tl";
pub const POLLER_PER_TASK_LIST: &str = "poller_count_per_tl";
pub const RATE_LIMIT_PER_TASK_LIST: &str = "rate_limit_per_tl";
pub const POLLER_INVALID_ISOLATION_GROUP: &str = "poller_invalid_isolation_group_per_tl";
pub const TASK_ISOLATION_LEAK: &str = "task_isolation_leak_per_tl";
pub const PARTITION_UPDATE_FAILED: &str = "tasklist_partition_update_failed_per_tl";
pub const TASK_LIST_PARTITION_CONFIG_NUM_READ: &str = "task_list_partition_config_num_read";
pub const TASK_LIST_PARTITION_CONFIG_NUM_WRITE: &str = "task_list_partition_config_num_write";
pub const TASK_LIST_PARTITION_CONFIG_VERSION: &str = "task_list_partition_config_version";
pub const TASK_LIST_READ_WRITE_PARTITION_MISMATCH: &str = "tasklist_read_write_partition_mismatch";
pub const TASK_LIST_POLLER_PARTITION_MISMATCH: &str = "tasklist_poller_partition_mismatch";
pub const STANDBY_TASKS_COMPLETED: &str = "standby_cluster_tasks_completed_per_tl";
pub const STANDBY_TASKS_NOT_STARTED: &str = "standby_cluster_tasks_not_started_per_tl";
pub const STANDBY_TASKS_COMPLETION_FAILURE: &str = "standby_cluster_tasks_completion_failure_per_tl";
pub const RESPOND_QUERY_TASK_FAILED: &str = "respond_query_failed_per_tl";
pub const CONDITION_FAILED_ERRORS: &str = "condition_failed_errors_per_tl";
pub const SYNC_MATCH_LATENCY: &str = "syncmatch_latency_per_tl";
pub const SYNC_MATCH_LATENCY_NS: &str = "syncmatch_latency_per_tl_ns";
pub const ASYNC_FORWARD_TASK_THROTTLE_ERRORS: &str = "async_forward_task_throttle_errors_per_tl";
pub const SEMAPHORE_CONTEXT_TIMEOUT: &str = "cadence_errors_semaphore_context_timeout";
pub const FORWARDED_PER_TASK_LIST: &str = "forwarded_per_tl";
pub const FORWARD_POLL_LATENCY: &str = "forward_poll_latency_per_tl";
pub const FORWARD_POLL_LATENCY_NS: &str = "forward_poll_latency_per_tl_ns";
pub const FORWARD_TASK_LATENCY: &str = "forward_task_latency_per_tl";
pub const FORWARD_TASK_LATENCY_NS: &str = "forward_task_latency_per_tl_ns";
pub const FORWARD_QUERY_LATENCY: &str = "forward_query_latency_per_tl";
pub const FORWARD_QUERY_LATENCY_NS: &str = "forward_query_latency_per_tl_ns";
pub const SYNC_MATCH_LOCAL_POLL_LATENCY: &str = "syncmatch_local_poll_latency_per_tl";
pub const SYNC_MATCH_LOCAL_POLL_LATENCY_NS: &str = "syncmatch_local_poll_latency_per_tl_ns";
pub const SYNC_MATCH_FORWARD_POLL_LATENCY: &str = "syncmatch_forward_poll_latency_per_tl";
pub const SYNC_MATCH_FORWARD_POLL_LATENCY_NS: &str = "syncmatch_forward_poll_latency_per_tl_ns";
pub const SYNC_THROTTLE_COUNT: &str = "sync_throttle_count_per_tl";
pub const SYNC_FORWARD_TASK_THROTTLE_ERRORS: &str = "sync_forward_task_throttle_errors_per_tl";
pub const LEASE_REQUESTS: &str = "lease_requests_per_tl";
pub const LEASE_FAILURES: &str = "lease_failures_per_tl";
pub const ESTIMATED_ADD_TASK_QPS: &str = "estimated_add_task_qps_per_tl";
pub const PARTITION_UPSCALE_THRESHOLD: &str = "tasklist_partition_upscale_threshold";
pub const PARTITION_DOWNSCALE_THRESHOLD: &str = "tasklist_partition_downscale_threshold";
pub const PARTITION_UPSCALE: &str = "partition_upscale_per_tl";
pub const PARTITION_DOWNSCALE: &str = "partition_downscale_per_tl";
pub const PARTITION_DRAINED: &str = "partition_drained_per_tl";
pub const ISOLATION_REBALANCE: &str = "isolation_rebalance_per_tl";
pub const ISOLATION_GROUP_STARTED_POLLING: &str = "ig_started_polling_per_tl";
pub const ISOLATION_GROUP_STOPPED_POLLING: &str = "ig_stopped_polling_per_tl";
pub const ISOLATION_GROUP_UPSCALE: &str = "ig_upscale_per_tl";
pub const ISOLATION_GROUP_DOWNSCALE: &str = "ig_downscale_per_tl";
pub const ISOLATION_GROUP_PARTITIONS: &str = "ig_partitions_per_tl";
pub const TASK_BACKLOG_PER_TASK_LIST: &str = "task_backlog_per_tl";
pub const TASK_LAG_PER_TASK_LIST: &str = "task_lag_per_tl";
pub const TASK_COUNT_PER_TASK_LIST: &str = "task_count_per_tl";
pub const EXPIRED_TASKS_PER_TASK_LIST: &str = "tasks_expired_per_tl";
pub const ASYNC_MATCH_LATENCY: &str = "asyncmatch_latency_per_tl";
pub const ASYNC_MATCH_LATENCY_NS: &str = "asyncmatch_latency_per_tl_ns";
pub const ASYNC_MATCH_LOCAL_POLL: &str = "asyncmatch_local_poll_per_tl";
pub const ASYNC_MATCH_LOCAL_POLL_ATTEMPT: &str = "asyncmatch_local_poll_attempt_per_tl";
pub const ASYNC_MATCH_LOCAL_POLL_ATTEMPT_COUNTS: &str =
    "asyncmatch_local_poll_attempt_per_tl_counts";
pub const ASYNC_MATCH_LOCAL_POLL_LATENCY: &str = "asyncmatch_local_poll_latency_per_tl";
pub const ASYNC_MATCH_LOCAL_POLL_LATENCY_NS: &str = "asyncmatch_local_poll_latency_per_tl_ns";
pub const ASYNC_MATCH_FORWARD_POLL: &str = "asyncmatch_forward_poll_per_tl";
pub const ASYNC_MATCH_FORWARD_POLL_ATTEMPT: &str = "asyncmatch_forward_poll_attempt_per_tl";
pub const ASYNC_MATCH_FORWARD_POLL_ATTEMPT_COUNTS: &str =
    "asyncmatch_forward_poll_attempt_per_tl_counts";
pub const ASYNC_MATCH_FORWARD_POLL_LATENCY: &str = "asyncmatch_forward_poll_latency_per_tl";
pub const ASYNC_MATCH_FORWARD_POLL_LATENCY_NS: &str = "asyncmatch_forward_poll_latency_per_tl_ns";
pub const ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED: &str =
    "asyncmatch_local_poll_after_forward_failed_per_tl";
pub const ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED_ATTEMPT: &str =
    "asyncmatch_local_poll_after_forward_failed_attempt_per_tl";
pub const ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED_ATTEMPT_COUNTS: &str =
    "asyncmatch_local_poll_after_forward_failed_attempt_per_tl_counts";
pub const ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED_LATENCY: &str =
    "asyncmatch_local_poll_after_forward_failed_latency_per_tl";
pub const ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED_LATENCY_NS: &str =
    "asyncmatch_local_poll_after_forward_failed_latency_per_tl_ns";
pub const ASYNC_MATCH_DISPATCH_TIMEOUTS: &str = "asyncmatch_dispatch_timeouts_per_tl";
pub const BUFFER_THROTTLE_COUNT: &str = "buffer_throttle_count_per_tl";
pub const BUFFER_UNKNOWN_DISPATCH_ERROR: &str = "buffer_unknown_task_dispatch_error_per_tl";
pub const POLL_LOCAL_MATCH_LATENCY: &str = "poll_local_match_latency_per_tl";
pub const POLL_LOCAL_MATCH_LATENCY_NS: &str = "poll_local_match_latency_per_tl_ns";
pub const POLL_FORWARD_MATCH_LATENCY: &str = "poll_forward_match_latency_per_tl";
pub const POLL_FORWARD_MATCH_LATENCY_NS: &str = "poll_forward_match_latency_per_tl_ns";
pub const POLL_LOCAL_MATCH_AFTER_FORWARD_FAILED_LATENCY: &str =
    "poll_local_match_after_forward_failed_latency_per_tl";
pub const POLL_LOCAL_MATCH_AFTER_FORWARD_FAILED_LATENCY_NS: &str =
    "poll_local_match_after_forward_failed_latency_per_tl_ns";
pub const ISOLATION_GROUP_TAG: &str = "isolation_group";
pub const POLLER_ISOLATION_GROUP_TAG: &str = "poller_isolation_group";
pub const PERSISTENCE_REQUESTS: &str = "persistence_requests";
pub const PERSISTENCE_FAILURES: &str = "persistence_failures";
pub const PERSISTENCE_ERRORS_BUSY: &str = "persistence_errors_busy";
pub const PERSISTENCE_REQUESTS_PER_DOMAIN: &str = "persistence_requests_per_domain";
pub const PERSISTENCE_FAILURES_PER_DOMAIN: &str = "persistence_failures_per_domain";
pub const PERSISTENCE_ERRORS_BUSY_PER_DOMAIN: &str = "persistence_errors_busy_per_domain";
pub const MATCHING_PERSISTENCE_QPS_REJECTIONS: &str = "matching_persistence_qps_rejections";
pub const TASK_CATEGORY_TAG: &str = "task_category";

/// Emit the Go metered persistence counters for a call rejected by Matching's
/// shared host/cluster persistence limiter. Go records the request before the
/// rate-limited wrapper and classifies its ServiceBusyError as a persistence
/// busy failure; a domain tag selects the per-domain metric family.
pub(crate) fn record_persistence_qps_rejection(
    sink: &dyn MetricSink,
    operation: &'static str,
    domain: Option<&str>,
    go_metered_operation: Option<&'static str>,
) {
    let mut tags = BTreeMap::new();
    tags.insert(OPERATION_TAG.to_owned(), operation.to_owned());
    tags.insert("persistence_scope".to_owned(), if go_metered_operation.is_some() { "go" } else { "semaphore" }.to_owned());
    if let Some(domain) = domain {
        tags.insert(DOMAIN_TAG.to_owned(), go_sanitize_metric_value(domain));
    }
    sink.record(MetricEvent {
        name: MATCHING_PERSISTENCE_QPS_REJECTIONS,
        kind: MetricKind::Counter,
        value: MetricValue::Count(1),
        tags: tags.clone(),
        histogram: None,
    });
    if let Some(go_operation) = go_metered_operation {
        let per_domain = domain.is_some();
        let mut go_tags = BTreeMap::new();
        go_tags.insert(OPERATION_TAG.to_owned(), go_operation.to_owned());
        go_tags.insert(TASK_CATEGORY_TAG.to_owned(), "none".to_owned());
        if let Some(domain) = domain {
            go_tags.insert(DOMAIN_TAG.to_owned(), go_sanitize_metric_value(domain));
        }
        let counters = if per_domain {
            [PERSISTENCE_REQUESTS_PER_DOMAIN, PERSISTENCE_ERRORS_BUSY_PER_DOMAIN, PERSISTENCE_FAILURES_PER_DOMAIN]
        } else {
            [PERSISTENCE_REQUESTS, PERSISTENCE_ERRORS_BUSY, PERSISTENCE_FAILURES]
        };
        for name in counters {
            sink.record(MetricEvent {
                name,
                kind: MetricKind::Counter,
                value: MetricValue::Count(1),
                tags: go_tags.clone(),
                histogram: None,
            });
        }
    }
}

#[cfg(test)]
mod persistence_qps_metric_tests {
    use super::*;

    #[test]
    fn qps_rejection_exports_dedicated_counter_and_go_busy_scope_once() {
        let sink = RecordingMetricSink::default();
        record_persistence_qps_rejection(&sink, "GetTaskList", None, Some("GetTaskList"));
        let events = sink.events();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].name, MATCHING_PERSISTENCE_QPS_REJECTIONS);
        assert_eq!(events[0].tags["persistence_scope"], "go");
        assert!(events.iter().any(|event| event.name == PERSISTENCE_REQUESTS));
        assert!(events.iter().any(|event| event.name == PERSISTENCE_ERRORS_BUSY));
        assert!(events.iter().any(|event| event.name == PERSISTENCE_FAILURES));
        assert!(events.iter().filter(|event| event.name == MATCHING_PERSISTENCE_QPS_REJECTIONS).count() == 1);
    }

    #[test]
    fn semaphore_qps_rejection_keeps_its_native_scope() {
        let sink = RecordingMetricSink::default();
        record_persistence_qps_rejection(&sink, "GrantSemaphoreToken", Some("domain-a"), None);
        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, MATCHING_PERSISTENCE_QPS_REJECTIONS);
        assert_eq!(events[0].tags["persistence_scope"], "semaphore");
        assert_eq!(events[0].tags[DOMAIN_TAG], "domain_a");
    }
}

#[derive(Clone, Debug)]
pub struct PollMetricContext {
    pub domain: String,
    pub task_list: String,
    pub task_list_kind: i32,
    pub task_type: &'static str,
    pub poller_isolation_group: String,
}

#[derive(Clone, Debug, Default)]
pub struct InboundMetricContext {
    pub caller: Option<String>,
    pub transport: Option<String>,
}

tokio::task_local! {
    static INBOUND_METRIC_CONTEXT: InboundMetricContext;
}

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

    pub(crate) fn quota_rejection(&self, rejection: crate::matching_quota::QuotaRejection) {
        let mut tags = BTreeMap::new();
        tags.insert("operation".to_owned(), format!("{:?}", rejection.method));
        tags.insert(
            "quota_class".to_owned(),
            match rejection.class {
                crate::matching_quota::RpcClass::User => "user",
                crate::matching_quota::RpcClass::Worker => "worker",
            }
            .to_owned(),
        );
        if let Some(domain) = rejection.domain {
            tags.insert(DOMAIN_TAG.to_owned(), domain);
        }
        self.0.record(MetricEvent {
            name: "matching_rpc_quota_rejections",
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags,
            histogram: None,
        });
    }

    pub fn context_from_metadata(
        metadata: &tonic::metadata::MetadataMap,
        transport: &str,
    ) -> InboundMetricContext {
        InboundMetricContext {
            caller: Some(
                metadata
                    .get("rpc-caller")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned(),
            ),
            transport: Some(transport.to_owned()),
        }
    }

    pub async fn with_inbound_context<F: std::future::Future>(
        &self,
        context: InboundMetricContext,
        future: F,
    ) -> F::Output {
        INBOUND_METRIC_CONTEXT.scope(context, future).await
    }

    /// Go `matchingEngine.Add{Decision,Activity}Task` traffic counter. The
    /// handler calls this only after domain resolution and before manager
    /// lookup, and Go excludes sticky and forwarded traffic.
    pub fn add_task_request(
        &self,
        domain: &str,
        task_list: &str,
        task_list_kind: i32,
        forwarded: bool,
        task_list_type: &'static str,
        host: &str,
    ) {
        if task_list_kind != 0 || forwarded {
            return;
        }
        self.0.record(MetricEvent {
            name: CADENCE_TASKLIST_REQUEST,
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags: BTreeMap::from([
                (OPERATION_TAG.to_owned(), "AddTask".to_owned()),
                (
                    DOMAIN_TAG.to_owned(),
                    if domain.is_empty() {
                        "_unknown_"
                    } else {
                        domain
                    }
                    .to_owned(),
                ),
                (
                    GO_TASK_LIST_TAG.to_owned(),
                    if task_list.is_empty() {
                        "_unknown_".to_owned()
                    } else {
                        go_sanitize_metric_value(task_list)
                    },
                ),
                (
                    TASK_LIST_TYPE_TAG.to_owned(),
                    if task_list_type.is_empty() {
                        "_unknown_".to_owned()
                    } else {
                        task_list_type.to_owned()
                    },
                ),
                (
                    MATCHING_HOST_TAG.to_owned(),
                    if host.is_empty() { "_unknown_" } else { host }.to_owned(),
                ),
            ]),
            histogram: None,
        });
    }

    /// Go `MatchingTaskListMgrScope.UpdateGauge` uses only the static scope
    /// operation tag; manager count is the registry's current membership.
    pub fn task_list_managers_gauge(&self, count: usize) {
        self.0.record(MetricEvent {
            name: TASKLIST_MANAGERS,
            kind: MetricKind::Gauge,
            value: MetricValue::Number(count as f64),
            tags: BTreeMap::from([(OPERATION_TAG.to_owned(), "TaskListMgr".to_owned())]),
            histogram: None,
        });
    }

    fn task_list_counter(
        &self,
        name: &'static str,
        context: &PollMetricContext,
        extra: BTreeMap<String, String>,
    ) {
        let list = match context.task_list_kind {
            1 => "__sticky__",
            2 => "__ephemeral__",
            0 if !context.task_list.is_empty() => context.task_list.as_str(),
            _ => "_unknown_",
        };
        let mut tags = BTreeMap::from([
            (
                DOMAIN_TAG.to_owned(),
                if context.domain.is_empty() {
                    "_unknown_".to_owned()
                } else {
                    context.domain.clone()
                },
            ),
            (TASK_LIST_TAG.to_owned(), go_sanitize_metric_value(list)),
        ]);
        tags.extend(extra);
        self.0.record(MetricEvent {
            name,
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags,
            histogram: None,
        });
    }

    pub fn poll_success(&self, context: &PollMetricContext, sync: bool) {
        self.task_list_counter(POLL_SUCCESS, context, BTreeMap::new());
        if sync {
            self.task_list_counter(POLL_SUCCESS_SYNC, context, BTreeMap::new());
        }
    }

    pub fn poll_timeout(&self, context: &PollMetricContext) {
        self.task_list_counter(POLL_TIMEOUT, context, BTreeMap::new());
    }

    pub fn invalid_isolation_group(&self, context: &PollMetricContext) {
        self.task_list_counter(POLLER_INVALID_ISOLATION_GROUP, context, BTreeMap::new());
    }

    /// Go Matching handlers count every request forwarded from another
    /// partition, including add-task and query RPCs.
    pub fn forwarded_per_task_list(&self, context: &PollMetricContext) {
        self.task_list_counter(FORWARDED_PER_TASK_LIST, context, BTreeMap::new());
    }

    pub fn task_isolation_leak(
        &self,
        context: &PollMetricContext,
        group: &str,
        cause: &'static str,
    ) {
        self.task_list_counter(
            TASK_ISOLATION_LEAK,
            context,
            BTreeMap::from([
                (ISOLATION_GROUP_TAG.to_owned(), go_sanitize_metric_value(group)),
                ("leak_cause".to_owned(), cause.to_owned()),
            ]),
        );
    }

    pub fn partition_config_gauges(
        &self,
        context: &PollMetricContext,
        root: &str,
        read: usize,
        write: usize,
        version: Option<i64>,
    ) {
        let mut tags = self.task_list_tags(context);
        tags.insert("tasklist_root_partition".to_owned(), go_sanitize_metric_value(root));
        for (name, value) in [
            (TASK_LIST_PARTITION_CONFIG_NUM_READ, read as f64),
            (TASK_LIST_PARTITION_CONFIG_NUM_WRITE, write as f64),
        ] {
            self.0.record(MetricEvent {
                name,
                kind: MetricKind::Gauge,
                value: MetricValue::Number(value),
                tags: tags.clone(),
                histogram: None,
            });
        }
        if let Some(version) = version {
            self.0.record(MetricEvent {
                name: TASK_LIST_PARTITION_CONFIG_VERSION,
                kind: MetricKind::Gauge,
                value: MetricValue::Number(version as f64),
                tags,
                histogram: None,
            });
        }
    }

    pub fn partition_mismatch(
        &self,
        context: &PollMetricContext,
        read_write_mismatch: bool,
        poller_mismatch: bool,
        isolation_enabled: bool,
    ) {
        let tags = self.task_list_tags(context);
        if read_write_mismatch {
            self.0.record(MetricEvent {
                name: TASK_LIST_READ_WRITE_PARTITION_MISMATCH,
                kind: MetricKind::Gauge,
                value: MetricValue::Number(1.0),
                tags: tags.clone(),
                histogram: None,
            });
        }
        if poller_mismatch {
            let mut tags = tags;
            tags.insert("isolation_enabled".to_owned(), isolation_enabled.to_string());
            self.0.record(MetricEvent {
                name: TASK_LIST_POLLER_PARTITION_MISMATCH,
                kind: MetricKind::Gauge,
                value: MetricValue::Number(1.0),
                tags,
                histogram: None,
            });
        }
    }

    pub fn poll_match(
        &self,
        context: &PollMetricContext,
        task_forwarded: bool,
        poll_forwarded: bool,
        partition: Option<&BTreeMap<String, String>>,
    ) {
        let name = match (task_forwarded, poll_forwarded) {
            (false, false) => LOCAL_TO_LOCAL_MATCH,
            (false, true) => LOCAL_TO_REMOTE_MATCH,
            (true, false) => REMOTE_TO_LOCAL_MATCH,
            (true, true) => REMOTE_TO_REMOTE_MATCH,
        };
        self.task_list_counter(name, context, BTreeMap::new());
        let Some(config) = partition else {
            return;
        };
        let Some(current) = config.get("isolation-group") else {
            return;
        };
        let original = config.get("original-isolation-group").unwrap_or(current);
        self.task_list_counter(
            ISOLATION_TASK_MATCH,
            context,
            BTreeMap::from([
                (
                    ISOLATION_GROUP_TAG.to_owned(),
                    go_sanitize_metric_value(original),
                ),
                (
                    POLLER_ISOLATION_GROUP_TAG.to_owned(),
                    if context.poller_isolation_group.is_empty() {
                        "_unknown_".to_owned()
                    } else {
                        go_sanitize_metric_value(&context.poller_isolation_group)
                    },
                ),
            ]),
        );
        if original == &context.poller_isolation_group {
            self.task_list_counter(
                ISOLATION_SUCCESS,
                context,
                BTreeMap::from([(
                    ISOLATION_GROUP_TAG.to_owned(),
                    go_sanitize_metric_value(original),
                )]),
            );
        }
    }

    pub fn poll_already_started(&self, context: &PollMetricContext) {
        let name = if context.task_type == "activity_task" {
            POLL_ACTIVITY_ALREADY_STARTED
        } else {
            POLL_DECISION_ALREADY_STARTED
        };
        self.task_list_counter(name, context, BTreeMap::new());
    }

    pub fn poller_count(&self, context: &PollMetricContext, count: usize) {
        self.task_list_gauge(POLLER_PER_TASK_LIST, context, count as f64);
    }

    pub fn task_list_gauge(&self, name: &'static str, context: &PollMetricContext, value: f64) {
        let list = match context.task_list_kind {
            1 => "__sticky__",
            2 => "__ephemeral__",
            0 if !context.task_list.is_empty() => context.task_list.as_str(),
            _ => "_unknown_",
        };
        self.0.record(MetricEvent {
            name,
            kind: MetricKind::Gauge,
            value: MetricValue::Number(value),
            tags: BTreeMap::from([
                (
                    DOMAIN_TAG.to_owned(),
                    if context.domain.is_empty() {
                        "_unknown_".to_owned()
                    } else {
                        context.domain.clone()
                    },
                ),
                (TASK_LIST_TAG.to_owned(), go_sanitize_metric_value(list)),
            ]),
            histogram: None,
        });
    }

    pub fn expired_task(&self, context: &PollMetricContext) {
        self.task_list_counter(EXPIRED_TASKS_PER_TASK_LIST, context, BTreeMap::new());
    }

    pub fn expired_tasks(&self, context: &PollMetricContext, count: usize) {
        if count == 0 {
            return;
        }
        self.0.record(MetricEvent {
            name: EXPIRED_TASKS_PER_TASK_LIST,
            kind: MetricKind::Counter,
            value: MetricValue::Count(count as i64),
            tags: self.task_list_tags(context),
            histogram: None,
        });
    }

    pub fn task_list_counter_event(&self, name: &'static str, context: &PollMetricContext) {
        self.task_list_counter(name, context, BTreeMap::new());
    }

    pub fn async_match_latency(&self, context: &PollMetricContext, elapsed: Duration) {
        for (name, kind, histogram) in [
            (ASYNC_MATCH_LATENCY, MetricKind::Timer, None),
            (
                ASYNC_MATCH_LATENCY_NS,
                MetricKind::Histogram,
                Some(HistogramSpec::GoLow1ms100s),
            ),
        ] {
            self.0.record(MetricEvent {
                name,
                kind,
                value: MetricValue::Duration(elapsed),
                tags: self.task_list_tags(context),
                histogram,
            });
        }
    }

    pub fn async_match_local(
        &self,
        context: &PollMetricContext,
        attempt: Option<u64>,
        elapsed: Duration,
    ) {
        self.async_match_handoff(
            context,
            ASYNC_MATCH_LOCAL_POLL,
            ASYNC_MATCH_LOCAL_POLL_ATTEMPT,
            ASYNC_MATCH_LOCAL_POLL_ATTEMPT_COUNTS,
            ASYNC_MATCH_LOCAL_POLL_LATENCY,
            ASYNC_MATCH_LOCAL_POLL_LATENCY_NS,
            attempt,
            elapsed,
        );
    }

    pub fn async_match_forward(
        &self,
        context: &PollMetricContext,
        attempt: u64,
        elapsed: Duration,
    ) {
        self.async_match_handoff(
            context,
            ASYNC_MATCH_FORWARD_POLL,
            ASYNC_MATCH_FORWARD_POLL_ATTEMPT,
            ASYNC_MATCH_FORWARD_POLL_ATTEMPT_COUNTS,
            ASYNC_MATCH_FORWARD_POLL_LATENCY,
            ASYNC_MATCH_FORWARD_POLL_LATENCY_NS,
            Some(attempt),
            elapsed,
        );
    }

    pub fn async_match_local_after_forward_failure(
        &self,
        context: &PollMetricContext,
        attempt: u64,
        elapsed: Duration,
    ) {
        self.async_match_handoff(
            context,
            ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED,
            ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED_ATTEMPT,
            ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED_ATTEMPT_COUNTS,
            ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED_LATENCY,
            ASYNC_MATCH_LOCAL_AFTER_FORWARD_FAILED_LATENCY_NS,
            Some(attempt),
            elapsed,
        );
    }

    // Keep the distinct metric names explicit so the caller cannot mix the
    // attempt, latency, and histogram series for the two handoff paths.
    #[allow(clippy::too_many_arguments)]
    fn async_match_handoff(
        &self,
        context: &PollMetricContext,
        counter: &'static str,
        attempt_timer: &'static str,
        attempt_histogram: &'static str,
        latency_timer: &'static str,
        latency_histogram: &'static str,
        attempt: Option<u64>,
        elapsed: Duration,
    ) {
        self.task_list_counter(counter, context, BTreeMap::new());
        if let Some(attempt) = attempt {
            self.0.record(MetricEvent {
                name: attempt_timer,
                kind: MetricKind::Timer,
                value: MetricValue::Duration(Duration::from_nanos(attempt)),
                tags: self.task_list_tags(context),
                histogram: None,
            });
            self.0.record(MetricEvent {
                name: attempt_histogram,
                kind: MetricKind::Histogram,
                value: MetricValue::Count(attempt.min(i64::MAX as u64) as i64),
                tags: self.task_list_tags(context),
                histogram: Some(HistogramSpec::GoMid1To16k),
            });
        }
        for (name, kind, histogram) in [
            (latency_timer, MetricKind::Timer, None),
            (
                latency_histogram,
                MetricKind::Histogram,
                Some(HistogramSpec::GoLow1ms100s),
            ),
        ] {
            self.0.record(MetricEvent {
                name,
                kind,
                value: MetricValue::Duration(elapsed),
                tags: self.task_list_tags(context),
                histogram,
            });
        }
    }

    pub fn forward_poll_latency(&self, context: &PollMetricContext, elapsed: Duration) {
        self.duration_pair(
            context,
            FORWARD_POLL_LATENCY,
            FORWARD_POLL_LATENCY_NS,
            elapsed,
        );
    }

    pub fn forward_task_latency(&self, context: &PollMetricContext, elapsed: Duration) {
        self.duration_pair(
            context,
            FORWARD_TASK_LATENCY,
            FORWARD_TASK_LATENCY_NS,
            elapsed,
        );
    }

    pub fn forward_query_latency(&self, context: &PollMetricContext, elapsed: Duration) {
        self.duration_pair(
            context,
            FORWARD_QUERY_LATENCY,
            FORWARD_QUERY_LATENCY_NS,
            elapsed,
        );
    }

    pub fn sync_match_latency(
        &self,
        context: &PollMetricContext,
        forwarded: bool,
        elapsed: Duration,
    ) {
        let (timer, histogram) = if forwarded {
            (
                SYNC_MATCH_FORWARD_POLL_LATENCY,
                SYNC_MATCH_FORWARD_POLL_LATENCY_NS,
            )
        } else {
            (
                SYNC_MATCH_LOCAL_POLL_LATENCY,
                SYNC_MATCH_LOCAL_POLL_LATENCY_NS,
            )
        };
        self.duration_pair(context, timer, histogram, elapsed);
    }

    pub fn sync_throttled(&self, context: &PollMetricContext) {
        self.task_list_counter(SYNC_THROTTLE_COUNT, context, BTreeMap::new());
    }

    pub fn sync_forward_throttled(&self, context: &PollMetricContext) {
        self.task_list_counter(SYNC_FORWARD_TASK_THROTTLE_ERRORS, context, BTreeMap::new());
    }

    pub fn lease_attempt(&self, context: &PollMetricContext, failed: bool) {
        self.lease_request(context);
        if failed {
            self.lease_failure(context);
        }
    }

    pub fn lease_request(&self, context: &PollMetricContext) {
        self.task_list_counter(LEASE_REQUESTS, context, BTreeMap::new());
    }

    pub fn lease_failure(&self, context: &PollMetricContext) {
        self.task_list_counter(LEASE_FAILURES, context, BTreeMap::new());
    }

    pub fn scaler_tick(
        &self,
        context: &PollMetricContext,
        qps: f64,
        write_partitions: usize,
        upscale_rps: f64,
        downscale_factor: f64,
    ) {
        for (name, value) in [
            (ESTIMATED_ADD_TASK_QPS, qps),
            (
                PARTITION_UPSCALE_THRESHOLD,
                write_partitions as f64 * upscale_rps,
            ),
            (
                PARTITION_DOWNSCALE_THRESHOLD,
                write_partitions.saturating_sub(1) as f64 * upscale_rps * downscale_factor,
            ),
        ] {
            self.0.record(MetricEvent {
                name,
                kind: MetricKind::Gauge,
                value: MetricValue::Number(value),
                tags: self.task_list_tags(context),
                histogram: None,
            });
        }
    }

    pub fn partition_scale_change(&self, context: &PollMetricContext, up: bool) {
        self.task_list_counter(
            if up {
                PARTITION_UPSCALE
            } else {
                PARTITION_DOWNSCALE
            },
            context,
            BTreeMap::new(),
        );
    }

    pub fn isolation_group_counter(
        &self,
        name: &'static str,
        context: &PollMetricContext,
        group: &str,
    ) {
        self.task_list_counter(
            name,
            context,
            BTreeMap::from([(
                ISOLATION_GROUP_TAG.to_owned(),
                go_sanitize_metric_value(group),
            )]),
        );
    }

    pub fn isolation_group_partitions(&self, context: &PollMetricContext, group: &str, count: i64) {
        self.0.record(MetricEvent {
            name: ISOLATION_GROUP_PARTITIONS,
            kind: MetricKind::Gauge,
            value: MetricValue::Number(count as f64),
            tags: self.task_list_tags_with(
                context,
                BTreeMap::from([(
                    ISOLATION_GROUP_TAG.to_owned(),
                    go_sanitize_metric_value(group),
                )]),
            ),
            histogram: None,
        });
    }

    pub fn isolation_rebalance(&self, context: &PollMetricContext) {
        self.task_list_counter(ISOLATION_REBALANCE, context, BTreeMap::new());
    }

    pub fn partition_drained(&self, context: &PollMetricContext) {
        self.task_list_counter(PARTITION_DRAINED, context, BTreeMap::new());
    }

    fn duration_pair(
        &self,
        context: &PollMetricContext,
        timer: &'static str,
        histogram: &'static str,
        elapsed: Duration,
    ) {
        for (name, kind, histogram) in [
            (timer, MetricKind::Timer, None),
            (
                histogram,
                MetricKind::Histogram,
                Some(HistogramSpec::GoLow1ms100s),
            ),
        ] {
            self.0.record(MetricEvent {
                name,
                kind,
                value: MetricValue::Duration(elapsed),
                tags: self.task_list_tags(context),
                histogram,
            });
        }
    }

    fn task_list_tags(&self, context: &PollMetricContext) -> BTreeMap<String, String> {
        let list = match context.task_list_kind {
            1 => "__sticky__",
            2 => "__ephemeral__",
            0 if !context.task_list.is_empty() => context.task_list.as_str(),
            _ => "_unknown_",
        };
        BTreeMap::from([
            (
                DOMAIN_TAG.to_owned(),
                if context.domain.is_empty() {
                    "_unknown_".to_owned()
                } else {
                    context.domain.clone()
                },
            ),
            (TASK_LIST_TAG.to_owned(), go_sanitize_metric_value(list)),
        ])
    }

    fn task_list_tags_with(
        &self,
        context: &PollMetricContext,
        extra: BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        let mut tags = self.task_list_tags(context);
        tags.extend(extra);
        tags
    }

    pub fn poll_match_latency(
        &self,
        context: &PollMetricContext,
        elapsed: Duration,
        forwarded: bool,
        after_forward_failure: bool,
    ) {
        let (timer, histogram) = if forwarded {
            (POLL_FORWARD_MATCH_LATENCY, POLL_FORWARD_MATCH_LATENCY_NS)
        } else if after_forward_failure {
            (
                POLL_LOCAL_MATCH_AFTER_FORWARD_FAILED_LATENCY,
                POLL_LOCAL_MATCH_AFTER_FORWARD_FAILED_LATENCY_NS,
            )
        } else {
            (POLL_LOCAL_MATCH_LATENCY, POLL_LOCAL_MATCH_LATENCY_NS)
        };
        let tags = BTreeMap::from([
            (
                DOMAIN_TAG.to_owned(),
                if context.domain.is_empty() {
                    "_unknown_".to_owned()
                } else {
                    context.domain.clone()
                },
            ),
            (
                TASK_LIST_TAG.to_owned(),
                go_sanitize_metric_value(match context.task_list_kind {
                    1 => "__sticky__",
                    2 => "__ephemeral__",
                    0 if !context.task_list.is_empty() => context.task_list.as_str(),
                    _ => "_unknown_",
                }),
            ),
        ]);
        for name in [timer, histogram] {
            self.0.record(MetricEvent {
                name,
                kind: if name == timer {
                    MetricKind::Timer
                } else {
                    MetricKind::Histogram
                },
                value: MetricValue::Duration(elapsed),
                tags: tags.clone(),
                histogram: (name == histogram).then_some(HistogramSpec::GoLow1ms100s),
            });
        }
    }

    /// Tags of Go's per-request handler scope (`handlerContext.scope`).
    fn request_tags(
        method: &'static str,
        domain: Option<&str>,
        task_list: Option<(&str, Option<i32>)>,
        context: Option<InboundMetricContext>,
    ) -> BTreeMap<String, String> {
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
        let context = INBOUND_METRIC_CONTEXT
            .try_with(Clone::clone)
            .ok()
            .or(context);
        if let Some(context) = context {
            if let Some(caller) = context.caller {
                tags.insert(CALLER_TAG.to_owned(), caller);
            }
            if let Some(transport) = context.transport {
                tags.insert(TRANSPORT_TAG.to_owned(), transport);
            }
        }
        tags
    }

    /// Go `Add{Decision,Activity}Task` handler `SyncMatchLatencyPerTaskList`
    /// timer and histogram, recorded on the request scope when the task was
    /// sync matched.
    pub fn handler_sync_match_latency(
        &self,
        method: &'static str,
        domain: Option<&str>,
        task_list: Option<(&str, Option<i32>)>,
        context: Option<InboundMetricContext>,
        elapsed: Duration,
    ) {
        let tags = Self::request_tags(method, domain, task_list, context);
        for (name, kind, histogram) in [
            (SYNC_MATCH_LATENCY, MetricKind::Timer, None),
            (
                SYNC_MATCH_LATENCY_NS,
                MetricKind::Histogram,
                Some(HistogramSpec::GoLow1ms100s),
            ),
        ] {
            self.0.record(MetricEvent {
                name,
                kind,
                value: MetricValue::Duration(elapsed),
                tags: tags.clone(),
                histogram,
            });
        }
    }

    /// Go `reportConditionFailed`: the task list lost its persistence lease.
    pub fn condition_failed(&self, context: &PollMetricContext) {
        self.task_list_counter(CONDITION_FAILED_ERRORS, context, BTreeMap::new());
    }

    /// Go `AsyncMatchForwardTaskThrottleErrorPerTasklist`.
    pub fn async_forward_throttled(&self, context: &PollMetricContext) {
        self.task_list_counter(
            ASYNC_FORWARD_TASK_THROTTLE_ERRORS,
            context,
            BTreeMap::new(),
        );
    }

    /// Go `CadenceErrSemaphoreContextTimeoutCounter` on the request scope.
    pub fn semaphore_context_timeout(
        &self,
        domain: Option<&str>,
        semaphore_name: &str,
        context: Option<InboundMetricContext>,
    ) {
        self.0.record(MetricEvent {
            name: SEMAPHORE_CONTEXT_TIMEOUT,
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags: Self::request_tags(
                "AcquireSemaphore",
                domain,
                Some((semaphore_name, Some(0))),
                context,
            ),
            histogram: None,
        });
    }

    /// Record one complete inbound Matching RPC. This is called at the shared
    /// MatchingAPI trait boundary, which Thrift dispatch also invokes.
    pub async fn matching_request<F, T, E>(
        &self,
        method: &'static str,
        domain: Option<&str>,
        task_list: Option<(&str, Option<i32>)>,
        context: Option<InboundMetricContext>,
        future: F,
    ) -> Result<T, E>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: MatchingErrorClass,
    {
        let tags = Self::request_tags(method, domain, task_list, context);
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

    #[test]
    fn add_task_counter_matches_go_dimensions_and_exclusions() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        metrics.add_task_request(
            "matching-test",
            "metric-task",
            0,
            false,
            "decision_task",
            "oracle-host",
        );
        metrics.add_task_request(
            "matching-test",
            "sticky",
            1,
            false,
            "activity_task",
            "oracle-host",
        );
        metrics.add_task_request(
            "matching-test",
            "forwarded",
            0,
            true,
            "activity_task",
            "oracle-host",
        );

        let events = sink.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.name, CADENCE_TASKLIST_REQUEST);
        assert_eq!(event.kind, MetricKind::Counter);
        assert_eq!(event.value, MetricValue::Count(1));
        assert_eq!(event.tags[OPERATION_TAG], "AddTask");
        assert_eq!(event.tags[DOMAIN_TAG], "matching-test");
        assert_eq!(event.tags[GO_TASK_LIST_TAG], "metric_task");
        assert_eq!(event.tags[TASK_LIST_TYPE_TAG], "decision_task");
        assert_eq!(event.tags[MATCHING_HOST_TAG], "oracle-host");
    }

    #[tokio::test]
    async fn inbound_rpc_records_request_latency_and_failure_once() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let result: Result<(), &'static str> = metrics
            .matching_request(
                "AcquireSemaphore",
                Some("domain-a"),
                Some(("locks", Some(0))),
                None,
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
            .matching_request::<_, (), &'static str>("QueryWorkflow", None, None, None, async {
                Ok(())
            })
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
                None,
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
            tonic::codegen::Bytes::from_static(
                b"type.googleapis.com/uber.cadence.api.v1.QueryFailedError",
            ),
        );
        let _: Result<(), _> = metrics
            .matching_request("QueryWorkflow", Some("domain-a"), None, None, async {
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

    #[tokio::test]
    async fn inbound_request_context_is_isolated_between_concurrent_calls() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let first = metrics.clone();
        let second = metrics.clone();
        let (a, b) = tokio::join!(
            first.with_inbound_context(
                InboundMetricContext {
                    caller: Some("caller-a".into()),
                    transport: Some("grpc".into())
                },
                first.matching_request::<_, (), &'static str>(
                    "QueryWorkflow",
                    Some("a"),
                    None,
                    None,
                    async { Ok(()) }
                ),
            ),
            second.with_inbound_context(
                InboundMetricContext {
                    caller: Some("caller-b".into()),
                    transport: Some("http".into())
                },
                second.matching_request::<_, (), &'static str>(
                    "QueryWorkflow",
                    Some("b"),
                    None,
                    None,
                    async { Ok(()) }
                ),
            ),
        );
        a.unwrap();
        b.unwrap();
        let requests = sink
            .events()
            .into_iter()
            .filter(|event| event.name == CADENCE_REQUESTS_PER_TASK_LIST)
            .collect::<Vec<_>>();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().any(
            |event| event.tags[CALLER_TAG] == "caller-a" && event.tags[TRANSPORT_TAG] == "grpc"
        ));
        assert!(requests.iter().any(
            |event| event.tags[CALLER_TAG] == "caller-b" && event.tags[TRANSPORT_TAG] == "http"
        ));
    }
}

#[cfg(test)]
mod matcher_metric_tests {
    use super::*;

    fn context() -> PollMetricContext {
        PollMetricContext {
            domain: "domain-a".into(),
            task_list: "queue/a".into(),
            task_list_kind: 0,
            task_type: "decision_task",
            poller_isolation_group: String::new(),
        }
    }

    #[test]
    fn poll_match_latency_emits_go_timer_and_histogram_pairs() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let elapsed = Duration::from_millis(12);
        metrics.poll_match_latency(&context(), elapsed, false, true);
        metrics.poll_match_latency(&context(), elapsed, true, false);
        let events = sink.events();
        assert_eq!(events.len(), 4);
        assert_eq!(
            events[0].name,
            POLL_LOCAL_MATCH_AFTER_FORWARD_FAILED_LATENCY
        );
        assert_eq!(events[0].kind, MetricKind::Timer);
        assert_eq!(
            events[1].name,
            POLL_LOCAL_MATCH_AFTER_FORWARD_FAILED_LATENCY_NS
        );
        assert_eq!(events[1].kind, MetricKind::Histogram);
        assert_eq!(events[2].name, POLL_FORWARD_MATCH_LATENCY);
        assert_eq!(events[3].name, POLL_FORWARD_MATCH_LATENCY_NS);
        assert!(events.iter().all(|event| {
            event.value == MetricValue::Duration(elapsed)
                && event
                    .tags
                    .get(DOMAIN_TAG)
                    .is_some_and(|tag| tag == "domain-a")
                && event
                    .tags
                    .get(TASK_LIST_TAG)
                    .is_some_and(|tag| tag == "queue_a")
        }));
        assert_eq!(events[1].histogram, Some(HistogramSpec::GoLow1ms100s));
        assert_eq!(events[3].histogram, Some(HistogramSpec::GoLow1ms100s));
    }

    #[test]
    fn async_local_handoff_emits_attempt_counter_and_go_integer_histogram() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        let elapsed = Duration::from_millis(7);
        metrics.async_match_local(&context(), Some(3), elapsed);
        let events = sink.events();
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].name, ASYNC_MATCH_LOCAL_POLL);
        assert_eq!(events[1].name, ASYNC_MATCH_LOCAL_POLL_ATTEMPT);
        assert_eq!(
            events[1].value,
            MetricValue::Duration(Duration::from_nanos(3))
        );
        assert_eq!(events[2].name, ASYNC_MATCH_LOCAL_POLL_ATTEMPT_COUNTS);
        assert_eq!(events[2].value, MetricValue::Count(3));
        assert_eq!(events[2].histogram, Some(HistogramSpec::GoMid1To16k));
        assert_eq!(events[3].name, ASYNC_MATCH_LOCAL_POLL_LATENCY);
    }

    #[test]
    fn poller_gauge_uses_go_task_list_dimensions() {
        let sink = Arc::new(RecordingMetricSink::default());
        MatchingMetrics::new(sink.clone()).poller_count(&context(), 3);
        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, POLLER_PER_TASK_LIST);
        assert_eq!(events[0].kind, MetricKind::Gauge);
        assert_eq!(events[0].value, MetricValue::Number(3.0));
        assert_eq!(
            events[0].tags.get(DOMAIN_TAG).map(String::as_str),
            Some("domain-a")
        );
        assert_eq!(
            events[0].tags.get(TASK_LIST_TAG).map(String::as_str),
            Some("queue_a")
        );
    }

    #[test]
    fn async_match_latency_emits_timer_and_histogram() {
        let sink = Arc::new(RecordingMetricSink::default());
        let elapsed = Duration::from_millis(7);
        MatchingMetrics::new(sink.clone()).async_match_latency(&context(), elapsed);
        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].name, ASYNC_MATCH_LATENCY);
        assert_eq!(events[0].kind, MetricKind::Timer);
        assert_eq!(events[1].name, ASYNC_MATCH_LATENCY_NS);
        assert_eq!(events[1].kind, MetricKind::Histogram);
        assert!(events
            .iter()
            .all(|event| event.value == MetricValue::Duration(elapsed)));
        assert_eq!(events[1].histogram, Some(HistogramSpec::GoLow1ms100s));
    }

    #[test]
    fn handler_sync_match_latency_uses_request_scope_tags() {
        let sink = Arc::new(RecordingMetricSink::default());
        let elapsed = Duration::from_millis(5);
        MatchingMetrics::new(sink.clone()).handler_sync_match_latency(
            "AddDecisionTask",
            Some("domain-a"),
            Some(("queue/a", Some(0))),
            Some(InboundMetricContext {
                caller: Some("frontend".into()),
                transport: Some("grpc".into()),
            }),
            elapsed,
        );
        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].name, "syncmatch_latency_per_tl");
        assert_eq!(events[0].kind, MetricKind::Timer);
        assert_eq!(events[1].name, "syncmatch_latency_per_tl_ns");
        assert_eq!(events[1].kind, MetricKind::Histogram);
        assert_eq!(events[1].histogram, Some(HistogramSpec::GoLow1ms100s));
        for event in &events {
            assert_eq!(event.value, MetricValue::Duration(elapsed));
            assert_eq!(event.tags[OPERATION_TAG], "AddDecisionTask");
            assert_eq!(event.tags[DOMAIN_TAG], "domain-a");
            assert_eq!(event.tags[TASK_LIST_TAG], "queue_a");
            assert_eq!(event.tags[CALLER_TAG], "frontend");
            assert_eq!(event.tags[TRANSPORT_TAG], "grpc");
        }
    }

    #[test]
    fn condition_failed_and_async_forward_throttle_are_tasklist_counters() {
        let sink = Arc::new(RecordingMetricSink::default());
        let metrics = MatchingMetrics::new(sink.clone());
        metrics.condition_failed(&context());
        metrics.async_forward_throttled(&context());
        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].name, "condition_failed_errors_per_tl");
        assert_eq!(events[1].name, "async_forward_task_throttle_errors_per_tl");
        for event in &events {
            assert_eq!(event.kind, MetricKind::Counter);
            assert_eq!(event.value, MetricValue::Count(1));
            assert_eq!(event.tags[DOMAIN_TAG], "domain-a");
        }
    }

    #[test]
    fn semaphore_context_timeout_counts_on_request_scope() {
        let sink = Arc::new(RecordingMetricSink::default());
        MatchingMetrics::new(sink.clone()).semaphore_context_timeout(
            Some("domain-a"),
            "sem",
            None,
        );
        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "cadence_errors_semaphore_context_timeout");
        assert_eq!(events[0].kind, MetricKind::Counter);
        assert_eq!(events[0].tags[OPERATION_TAG], "AcquireSemaphore");
        assert_eq!(events[0].tags[SEMAPHORE_NAME_TAG], "sem");
    }

    #[test]
    fn respond_query_failed_uses_go_name() {
        assert_eq!(RESPOND_QUERY_TASK_FAILED, "respond_query_failed_per_tl");
    }
}
