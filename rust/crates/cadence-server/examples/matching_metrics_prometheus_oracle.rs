use cadence_server::matching_metrics::{
    HistogramSpec, MetricEvent, MetricKind, MetricSink, MetricValue, CADENCE_CLIENT_LATENCY,
    CADENCE_CLIENT_LATENCY_NS, CADENCE_CLIENT_REQUESTS,
};
use cadence_server::matching_metrics_prometheus::{
    HistogramObjective, PrometheusMetricSink, PrometheusMetricsConfig, SummaryObjective,
};
use std::{collections::BTreeMap, time::Duration};

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let histogram_timers = args.first().is_some_and(|mode| mode == "histogram");
    let custom = args.iter().any(|arg| arg == "custom");
    let config = PrometheusMetricsConfig {
        timer_type: if histogram_timers { "histogram" } else { "" }.to_owned(),
        default_histogram_buckets: if custom {
            vec![
                HistogramObjective { upper: 0.003 },
                HistogramObjective { upper: 0.01 },
            ]
        } else {
            Vec::new()
        },
        default_summary_objectives: if custom {
            vec![
                SummaryObjective {
                    percentile: 0.5,
                    allowed_error: 0.05,
                },
                SummaryObjective {
                    percentile: 0.9,
                    allowed_error: 0.01,
                },
            ]
        } else {
            Vec::new()
        },
        ..Default::default()
    };
    let prefix = if custom { "matching-custom" } else { "cadence" };
    let root_tags = if custom {
        BTreeMap::from([
            ("zone".to_owned(), "north-west".to_owned()),
            ("deployment".to_owned(), "canary.v2".to_owned()),
        ])
    } else {
        BTreeMap::from([("zone".to_owned(), "north-west".to_owned())])
    };
    let sink = PrometheusMetricSink::new(config, prefix, root_tags).expect("valid oracle config");
    let tags = BTreeMap::from([
        (
            "operation".to_owned(),
            "HistoryClientGetMutableState".to_owned(),
        ),
        ("cadence_role".to_owned(), "history_client".to_owned()),
    ]);
    sink.record(MetricEvent {
        name: CADENCE_CLIENT_REQUESTS,
        kind: MetricKind::Counter,
        value: MetricValue::Count(1),
        tags: tags.clone(),
        histogram: None,
    });
    sink.record(MetricEvent {
        name: CADENCE_CLIENT_LATENCY,
        kind: MetricKind::Timer,
        value: MetricValue::Duration(Duration::from_millis(2)),
        tags: tags.clone(),
        histogram: None,
    });
    sink.record(MetricEvent {
        name: CADENCE_CLIENT_LATENCY_NS,
        kind: MetricKind::Histogram,
        value: MetricValue::Duration(Duration::from_millis(2)),
        tags,
        histogram: Some(HistogramSpec::GoLow1ms100s),
    });
    print!("{}", sink.render());
}
