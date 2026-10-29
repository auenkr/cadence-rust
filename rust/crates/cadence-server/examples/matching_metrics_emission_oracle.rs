use cadence_service_core::matching_metrics::{
    record_persistence_empty_response, record_persistence_response_payload,
    record_persistence_response_rows, InboundMetricContext, MatchingMetrics, MetricEvent,
    MetricKind, MetricSink, PollMetricContext, RecordingMetricSink,
};
use cadence_service_core::matching_metrics_migration::{
    MigrationMetricSink, MigrationPolicy,
};
use serde::Serialize;
use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

#[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    name: &'static str,
    kind: &'static str,
    tags: BTreeMap<String, String>,
}

fn kind_name(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Counter => "counter",
        MetricKind::Timer => "timer",
        MetricKind::Histogram => "histogram",
        MetricKind::Gauge => "gauge",
    }
}

fn main() {
    let mode = env::args().nth(1).unwrap_or_else(|| "default".into());
    let mut policy = MigrationPolicy::default();
    if mode == "both" || mode == "name-off" {
        policy.histograms.default = "both".into();
    }
    if mode == "name-on" {
        policy.histograms.names.insert("syncmatch_latency_per_tl_ns".into(), true);
    }
    if mode == "name-off" {
        policy.histograms.names.insert("syncmatch_latency_per_tl_ns".into(), false);
    }
    let recording = Arc::new(RecordingMetricSink::default());
    let migration: Arc<dyn MetricSink> = Arc::new(MigrationMetricSink::new(recording.clone(), policy));
    let metrics = MatchingMetrics::new(migration.clone());
    let context = PollMetricContext {
        domain: "domain-a".into(),
        task_list: "queue-a".into(),
        task_list_kind: 0,
        task_type: "decision_task",
        poller_isolation_group: String::new(),
    };
    // Match Go's actual tally sanitizer, including truncated UTF-8 and long names.
    for name in [
        b"raw-\xe2\x82".as_slice(),
        b"raw-\xff".as_slice(),
        "raw-�".as_bytes(),
        "raw-é".as_bytes(),
        format!("{}tail", "x".repeat(300)).as_bytes(),
    ] {
        let mut raw_context = context.clone();
        raw_context.task_list = cadence_common::go_string::GoString::from(name).go_rune_string();
        metrics.poll_success(&raw_context, false);
    }
    metrics.poll_success(&context, false);
    metrics.poll_timeout(&context);
    metrics.condition_failed(&context);
    metrics.forwarded_per_task_list(&context);
    metrics.sync_forward_throttled(&context);
    metrics.poller_count(&context, 2);
    metrics.sync_match_latency(&context, false, Duration::from_millis(5));
    metrics.add_task_request("domain-a", "queue/a", 0, false, "decision_task", "matching-test");
    metrics.handler_sync_match_latency(
        "AddDecisionTask",
        Some("domain-a"),
        Some(("queue/a", Some(0))),
        Some(InboundMetricContext { caller: Some("history".into()), transport: Some("grpc".into()) }),
        Duration::from_millis(5),
    );

    // These are the production persistence wrapper's response-row/payload and
    // empty-response emitters, run through the same configured migration sink.
    record_persistence_empty_response(migration.as_ref(), "GetTasks");
    record_persistence_response_rows(migration.as_ref(), "GetTasks", 3);
    record_persistence_response_payload(migration.as_ref(), "GetTasks", 4096);

    let mut rows: Vec<Row> = recording.events().into_iter().map(|event: MetricEvent| Row {
        name: event.name,
        kind: kind_name(event.kind),
        tags: event.tags,
    }).collect();
    rows.sort();
    rows.dedup();
    serde_json::to_writer(std::io::stdout(), &rows).expect("serialize emitted metric series");
}
