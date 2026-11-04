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
pub const CADENCE_REQUESTS_PER_TL: &str = "cadence_requests_per_tl";
pub const CADENCE_LATENCY_PER_TL: &str = "cadence_latency_per_tl";
pub const CADENCE_LATENCY_PER_TL_NS: &str = "cadence_latency_per_tl_ns";
const UNKNOWN: &str = "_unknown_";

/// Go `common.NewPerTaskListScope` users in `metered/api_generated.go`.
const PER_TASK_LIST_OPERATIONS: &[&str] = &[
    "DescribeTaskList",
    "ListTaskListPartitions",
    "PollForActivityTask",
    "PollForDecisionTask",
    "SignalWithStartWorkflowExecution",
    "SignalWithStartWorkflowExecutionAsync",
    "StartWorkflowExecution",
    "StartWorkflowExecutionAsync",
];

/// Go `defs.go` scope operation tag for a metered API method. Identical to the
/// method name except for the four scopes Go names differently.
pub fn scope_operation(method: &str) -> &str {
    match method {
        "GetTaskListsByDomain" => "FrontendGetTaskListsByDomain",
        "ListDomains" => "ListDomain",
        "ListTaskListPartitions" => "FrontendListTaskListPartitions",
        "RefreshWorkflowTasks" => "FrontendRefreshWorkflowTasks",
        other => other,
    }
}

pub struct SinkMetrics {
    sink: Arc<dyn MetricSink>,
}

impl SinkMetrics {
    pub fn new(sink: Arc<dyn MetricSink>) -> Self {
        Self { sink }
    }

    /// Go's per-domain scope tags: `operation`, `domain` (`_unknown_` when
    /// blank) and, when `frontend.emitSignalNameMetricsTag` is on, `signalName`.
    /// The request log tags (`wf-*`) are NOT metric tags in Go.
    fn tags(operation: &str, tags: &[MetricTag]) -> BTreeMap<String, String> {
        let unknown = |v: &str| {
            if v.is_empty() {
                UNKNOWN.to_owned()
            } else {
                v.to_owned()
            }
        };
        let mut out = BTreeMap::new();
        out.insert(
            "operation".to_owned(),
            scope_operation(operation).to_owned(),
        );
        let mut domain = "";
        for tag in tags {
            match tag.key {
                "wf-domain-name" => domain = &tag.value,
                "signalName" => {
                    out.insert("signalName".to_owned(), unknown(&tag.value));
                }
                _ => {}
            }
        }
        out.insert("domain".to_owned(), unknown(domain));
        out
    }

    /// Go `NewPerTaskListScope` tag: sticky/ephemeral fixed names, the name for
    /// a normal list, `_unknown_` otherwise.
    fn task_list_tag(tags: &[MetricTag]) -> String {
        let find = |key: &str| {
            tags.iter()
                .find(|t| t.key == key)
                .map_or("", |t| t.value.as_str())
        };
        match find("wf-task-list-kind") {
            "1" => "__sticky__".to_owned(),
            "2" => "__ephemeral__".to_owned(),
            _ => match find("wf-task-list-name") {
                "" => UNKNOWN.to_owned(),
                name => name.to_owned(),
            },
        }
    }

    fn per_task_list(operation: &str, tags: &[MetricTag]) -> Option<BTreeMap<String, String>> {
        if !PER_TASK_LIST_OPERATIONS.contains(&operation) {
            return None;
        }
        let mut out = Self::tags(operation, tags);
        out.insert("tasklist".to_owned(), Self::task_list_tag(tags));
        Some(out)
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
        self.increment_requests_tagged(operation, &[]);
    }

    fn increment_requests_tagged(&self, operation: &str, tags: &[MetricTag]) {
        if let Some(tl) = Self::per_task_list(operation, tags) {
            self.emit(
                CADENCE_REQUESTS_PER_TL,
                MetricKind::Counter,
                MetricValue::Count(1),
                tl,
                None,
            );
        }
        self.emit(
            CADENCE_REQUESTS,
            MetricKind::Counter,
            MetricValue::Count(1),
            Self::tags(operation, tags),
            None,
        );
    }

    fn record_latency_histogram_tagged(
        &self,
        operation: &str,
        elapsed: Duration,
        tags: &[MetricTag],
    ) {
        if let Some(tl) = Self::per_task_list(operation, tags) {
            self.emit(
                CADENCE_LATENCY_PER_TL_NS,
                MetricKind::Histogram,
                MetricValue::Duration(elapsed),
                tl,
                Some(HistogramSpec::GoLow1ms100s),
            );
        }
        self.emit(
            CADENCE_LATENCY_NS,
            MetricKind::Histogram,
            MetricValue::Duration(elapsed),
            Self::tags(operation, tags),
            Some(HistogramSpec::GoLow1ms100s),
        );
    }

    fn record_operation(&self, operation: &str, elapsed: Duration, tags: &[MetricTag]) {
        if let Some(tl) = Self::per_task_list(operation, tags) {
            self.emit(
                CADENCE_LATENCY_PER_TL,
                MetricKind::Timer,
                MetricValue::Duration(elapsed),
                tl,
                None,
            );
        }
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
            Some(HistogramSpec::GoLow1ms100s),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every Go `metered.apiHandler` method with the `operation` tag of the
    /// `metrics.Frontend*Scope` it uses (go/common/metrics/defs.go).
    const GO_FRONTEND_SCOPES: &[(&str, &str)] = &[
        ("BackfillSchedule", "BackfillSchedule"),
        ("CountWorkflowExecutions", "CountWorkflowExecutions"),
        ("CreateSchedule", "CreateSchedule"),
        ("CreateSemaphore", "CreateSemaphore"),
        ("DeleteDomain", "DeleteDomain"),
        ("DeleteSchedule", "DeleteSchedule"),
        ("DeprecateDomain", "DeprecateDomain"),
        ("DescribeDomain", "DescribeDomain"),
        ("DescribeSchedule", "DescribeSchedule"),
        ("DescribeTaskList", "DescribeTaskList"),
        ("DescribeWorkflowExecution", "DescribeWorkflowExecution"),
        ("DiagnoseWorkflowExecution", "DiagnoseWorkflowExecution"),
        ("FailoverDomain", "FailoverDomain"),
        ("GetClusterInfo", "GetClusterInfo"),
        ("GetSearchAttributes", "GetSearchAttributes"),
        ("GetTaskListsByDomain", "FrontendGetTaskListsByDomain"),
        ("GetWorkflowExecutionHistory", "GetWorkflowExecutionHistory"),
        (
            "ListArchivedWorkflowExecutions",
            "ListArchivedWorkflowExecutions",
        ),
        (
            "ListClosedWorkflowExecutions",
            "ListClosedWorkflowExecutions",
        ),
        ("ListDomains", "ListDomain"),
        ("ListFailoverHistory", "ListFailoverHistory"),
        ("ListOpenWorkflowExecutions", "ListOpenWorkflowExecutions"),
        ("ListSchedules", "ListSchedules"),
        ("ListTaskListPartitions", "FrontendListTaskListPartitions"),
        ("ListWorkflowExecutions", "ListWorkflowExecutions"),
        ("PauseSchedule", "PauseSchedule"),
        ("PollForActivityTask", "PollForActivityTask"),
        ("PollForDecisionTask", "PollForDecisionTask"),
        ("QueryWorkflow", "QueryWorkflow"),
        ("RecordActivityTaskHeartbeat", "RecordActivityTaskHeartbeat"),
        (
            "RecordActivityTaskHeartbeatByID",
            "RecordActivityTaskHeartbeatByID",
        ),
        ("RefreshWorkflowTasks", "FrontendRefreshWorkflowTasks"),
        ("RegisterDomain", "RegisterDomain"),
        (
            "RequestCancelWorkflowExecution",
            "RequestCancelWorkflowExecution",
        ),
        ("ResetStickyTaskList", "ResetStickyTaskList"),
        ("ResetWorkflowExecution", "ResetWorkflowExecution"),
        ("RespondActivityTaskCanceled", "RespondActivityTaskCanceled"),
        (
            "RespondActivityTaskCanceledByID",
            "RespondActivityTaskCanceledByID",
        ),
        (
            "RespondActivityTaskCompleted",
            "RespondActivityTaskCompleted",
        ),
        (
            "RespondActivityTaskCompletedByID",
            "RespondActivityTaskCompletedByID",
        ),
        ("RespondActivityTaskFailed", "RespondActivityTaskFailed"),
        (
            "RespondActivityTaskFailedByID",
            "RespondActivityTaskFailedByID",
        ),
        (
            "RespondDecisionTaskCompleted",
            "RespondDecisionTaskCompleted",
        ),
        ("RespondDecisionTaskFailed", "RespondDecisionTaskFailed"),
        ("RespondQueryTaskCompleted", "RespondQueryTaskCompleted"),
        ("RestartWorkflowExecution", "RestartWorkflowExecution"),
        ("ScanWorkflowExecutions", "ScanWorkflowExecutions"),
        (
            "SignalWithStartWorkflowExecution",
            "SignalWithStartWorkflowExecution",
        ),
        (
            "SignalWithStartWorkflowExecutionAsync",
            "SignalWithStartWorkflowExecutionAsync",
        ),
        ("SignalWorkflowExecution", "SignalWorkflowExecution"),
        ("StartWorkflowExecution", "StartWorkflowExecution"),
        ("StartWorkflowExecutionAsync", "StartWorkflowExecutionAsync"),
        ("TerminateWorkflowExecution", "TerminateWorkflowExecution"),
        ("UnpauseSchedule", "UnpauseSchedule"),
        ("UpdateDomain", "UpdateDomain"),
        ("UpdateSchedule", "UpdateSchedule"),
    ];

    #[test]
    fn every_go_metered_method_is_a_rust_operation() {
        let rust: Vec<&str> = crate::handler_workflow::WorkflowOperation::ALL
            .iter()
            .map(|op| op.name())
            .collect();
        // Schedule APIs are metered by `Handler::observe_local` (served by the
        // in-process router rather than `WorkflowOperation`).
        let schedules = cadence_service_core::workflow_api::REDIRECTED_SCHEDULE_OPERATIONS;
        // Domain and semaphore APIs are metered the same way (grpc.rs).
        let local = [
            "RegisterDomain",
            "DescribeDomain",
            "ListDomains",
            "UpdateDomain",
            "FailoverDomain",
            "DeprecateDomain",
            "DeleteDomain",
            "ListFailoverHistory",
            "CreateSemaphore",
        ];
        for (method, _) in GO_FRONTEND_SCOPES {
            assert!(
                rust.contains(method) || schedules.contains(method) || local.contains(method),
                "missing Rust operation {method}"
            );
        }
    }

    #[test]
    fn every_frontend_scope_operation_matches_go_defs() {
        assert_eq!(GO_FRONTEND_SCOPES.len(), 56);
        for (method, operation) in GO_FRONTEND_SCOPES {
            let tags = SinkMetrics::tags(method, &[]);
            assert_eq!(&tags["operation"], operation, "method {method}");
        }
    }

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
        m.increment_requests("DescribeWorkflowExecution");
        m.record_operation(
            "DescribeWorkflowExecution",
            Duration::from_millis(3),
            &domain_tag(),
        );
        m.record_latency_histogram("DescribeWorkflowExecution", Duration::from_millis(3));
        m.increment_error(
            "DescribeWorkflowExecution",
            &domain_tag(),
            Counter::BadRequest,
        );
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
        assert_eq!(events[1].tags["operation"], "DescribeWorkflowExecution");
        assert_eq!(events[1].tags["domain"], "d1");
        assert!(!events[1].tags.contains_key("wf-domain-name"));
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

    #[test]
    fn blank_domain_is_unknown_and_per_tasklist_metrics_match_go() {
        let sink = Arc::new(RecordingMetricSink::default());
        let m = SinkMetrics::new(sink.clone());
        let tags = vec![
            MetricTag {
                key: "wf-domain-name",
                value: String::new(),
            },
            MetricTag {
                key: "wf-task-list-name",
                value: "tl".into(),
            },
            MetricTag {
                key: "wf-task-list-kind",
                value: "1".into(),
            },
        ];
        m.increment_requests_tagged("PollForDecisionTask", &tags);
        m.record_operation("PollForDecisionTask", Duration::from_millis(1), &tags);
        m.record_latency_histogram_tagged("PollForDecisionTask", Duration::from_millis(1), &tags);
        let events = sink.events();
        let names: Vec<_> = events.iter().map(|e| e.name).collect();
        assert_eq!(
            names,
            [
                "cadence_requests_per_tl",
                "cadence_requests",
                "cadence_latency_per_tl",
                "cadence_latency",
                "cadence_latency_per_tl_ns",
                "cadence_latency_ns"
            ]
        );
        assert_eq!(events[0].tags["tasklist"], "__sticky__");
        assert_eq!(events[0].tags["domain"], "_unknown_");
        assert!(!events[1].tags.contains_key("tasklist"));
        m.increment_requests_tagged("StartWorkflowExecution", &[]);
        let events = sink.events();
        assert_eq!(events[6].tags["tasklist"], "_unknown_");
    }
}
