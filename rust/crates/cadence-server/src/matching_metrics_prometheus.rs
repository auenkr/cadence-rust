//! Prometheus exposition for Matching metrics.
//!
//! This mirrors Cadence's Tally Prometheus reporter for the metric kinds
//! emitted by Matching. The reporter is separate from the Matching RPC
//! listener; the runtime may mount `router()` or start the configured listener.

use crate::matching_metrics::{HistogramSpec, MetricEvent, MetricKind, MetricSink, MetricValue};
use axum::{
    extract::State,
    http::{header, HeaderValue},
    response::IntoResponse,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::oneshot,
    task::{JoinError, JoinHandle},
};

const CADENCE_SERVICE_TAG: &str = "cadence_service";
const MATCHING_SERVICE: &str = "cadence_matching";

/// Field names and defaults correspond to Tally's Go Prometheus `Configuration`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrometheusMetricsConfig {
    #[serde(default)]
    pub handler_path: String,
    #[serde(default)]
    pub listen_network: String,
    #[serde(default)]
    pub listen_address: String,
    #[serde(default)]
    pub timer_type: String,
    #[serde(default)]
    pub default_histogram_buckets: Vec<HistogramObjective>,
    #[serde(default)]
    pub default_summary_objectives: Vec<SummaryObjective>,
    #[serde(default)]
    pub on_error: String,
}

impl Default for PrometheusMetricsConfig {
    fn default() -> Self {
        Self {
            handler_path: String::new(),
            listen_network: String::new(),
            listen_address: String::new(),
            timer_type: String::new(),
            default_histogram_buckets: Vec::new(),
            default_summary_objectives: Vec::new(),
            on_error: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SummaryObjective {
    pub percentile: f64,
    pub allowed_error: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistogramObjective {
    pub upper: f64,
}

#[derive(Debug)]
pub enum PrometheusConfigError {
    UnsupportedTimerType(String),
    InvalidHandlerPath(String),
    InvalidListenNetwork(String),
}

impl std::fmt::Display for PrometheusConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedTimerType(value) => write!(
                f,
                "Prometheus timer type {value:?} is not supported yet; set timerType to histogram"
            ),
            Self::InvalidHandlerPath(value) => {
                write!(f, "Prometheus handlerPath must begin with '/': {value:?}")
            }
            Self::InvalidListenNetwork(value) => {
                write!(f, "unsupported Prometheus listenNetwork {value:?}")
            }
        }
    }
}

impl std::error::Error for PrometheusConfigError {}

#[derive(Clone)]
pub struct PrometheusMetricSink {
    inner: Arc<SinkInner>,
}

struct SinkInner {
    config: PrometheusMetricsConfig,
    prefix: String,
    root_tags: BTreeMap<String, String>,
    timer_buckets_seconds: Vec<f64>,
    series: Mutex<BTreeMap<SeriesKey, Aggregate>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SeriesKey {
    name: String,
    tags: BTreeMap<String, String>,
}

enum Aggregate {
    Counter(i64),
    Gauge(f64),
    Histogram(HistogramAggregate),
}

struct HistogramAggregate {
    bounds_seconds: Vec<f64>,
    duration_bounds_nanos: Option<Vec<u64>>,
    bucket_counts: Vec<u64>,
    count: u64,
    sum_seconds: f64,
}

impl PrometheusMetricSink {
    /// Create a Matching sink, applying Go's prefix, root labels, and service label.
    pub fn new(
        config: PrometheusMetricsConfig,
        prefix: impl Into<String>,
        root_tags: BTreeMap<String, String>,
    ) -> Result<Self, PrometheusConfigError> {
        let handler_path = if config.handler_path.trim().is_empty() {
            "/metrics"
        } else {
            config.handler_path.trim()
        };
        if !handler_path.starts_with('/') {
            return Err(PrometheusConfigError::InvalidHandlerPath(
                config.handler_path.clone(),
            ));
        }
        if config.timer_type != "histogram" {
            return Err(PrometheusConfigError::UnsupportedTimerType(
                if config.timer_type.is_empty() {
                    "summary (Go default when omitted)".to_owned()
                } else {
                    config.timer_type.clone()
                },
            ));
        }
        let network = config.listen_network.trim();
        if !matches!(network, "" | "tcp" | "tcp4" | "tcp6" | "unix") {
            return Err(PrometheusConfigError::InvalidListenNetwork(
                config.listen_network.clone(),
            ));
        }

        let timer_buckets_seconds = if config.default_histogram_buckets.is_empty() {
            go_default_timer_buckets_seconds()
        } else {
            config
                .default_histogram_buckets
                .iter()
                .map(|bucket| bucket.upper)
                .collect()
        };
        let mut root_tags = root_tags;
        root_tags.insert(CADENCE_SERVICE_TAG.to_owned(), MATCHING_SERVICE.to_owned());
        let mut sink_config = config;
        sink_config.handler_path = handler_path.to_owned();

        Ok(Self {
            inner: Arc::new(SinkInner {
                config: sink_config,
                prefix: prefix.into(),
                root_tags,
                timer_buckets_seconds,
                series: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// Router for embedding at Go's `handlerPath`; runtime owns HTTP serving
    /// when Go's `listenAddress` is empty.
    pub fn router(&self) -> Router {
        Router::new()
            .route(&self.inner.config.handler_path, get(scrape))
            .with_state(Arc::new(self.clone()))
    }

    /// Start a dedicated metrics listener when `listenAddress` is configured.
    /// Empty addresses return `None`, matching Go's default-mux registration.
    pub async fn start_listener(&self) -> io::Result<Option<PrometheusListener>> {
        let address = self.inner.config.listen_address.trim();
        if address.is_empty() {
            return Ok(None);
        }
        let network = match self.inner.config.listen_network.trim() {
            "" => "tcp",
            value => value,
        };
        let router = self.router();
        let handle = match network {
            "tcp" | "tcp4" | "tcp6" => {
                let listener = TcpListener::bind(address).await?;
                spawn_listener(listener, router)
            }
            "unix" => {
                let listener = UnixListener::bind(address)?;
                spawn_listener(listener, router)
            }
            _ => unreachable!("network validated in PrometheusMetricSink::new"),
        };
        Ok(Some(handle))
    }

    /// Snapshot the Prometheus text exposition. Durations are reported in
    /// seconds, matching Go Tally's Prometheus reporter even for `_ns` names.
    pub fn render(&self) -> String {
        let series = self.inner.series.lock().expect("metrics lock poisoned");
        let mut grouped: BTreeMap<String, Vec<(&SeriesKey, &Aggregate)>> = BTreeMap::new();
        for (key, value) in series.iter() {
            grouped
                .entry(key.name.clone())
                .or_default()
                .push((key, value));
        }
        let mut output = String::new();
        for (name, samples) in grouped {
            let (kind, help) = match samples.first().map(|(_, value)| value) {
                Some(Aggregate::Counter(_)) => ("counter", format!("{name} counter")),
                Some(Aggregate::Gauge(_)) => ("gauge", format!("{name} gauge")),
                Some(Aggregate::Histogram(_)) => ("histogram", format!("{name} histogram")),
                None => continue,
            };
            output.push_str(&format!(
                "# HELP {name} {}\n# TYPE {name} {kind}\n",
                escape_help(&help)
            ));
            for (key, value) in samples {
                match value {
                    Aggregate::Counter(count) => {
                        write_sample(&mut output, &name, &key.tags, None, *count as f64);
                    }
                    Aggregate::Gauge(gauge) => {
                        write_sample(&mut output, &name, &key.tags, None, *gauge);
                    }
                    Aggregate::Histogram(histogram) => {
                        for (bound, count) in histogram
                            .bounds_seconds
                            .iter()
                            .zip(histogram.bucket_counts.iter())
                        {
                            write_sample(
                                &mut output,
                                &format!("{name}_bucket"),
                                &key.tags,
                                Some(("le", format_float(*bound))),
                                *count as f64,
                            );
                        }
                        write_sample(
                            &mut output,
                            &format!("{name}_bucket"),
                            &key.tags,
                            Some(("le", "+Inf".to_owned())),
                            histogram.count as f64,
                        );
                        write_sample(
                            &mut output,
                            &format!("{name}_sum"),
                            &key.tags,
                            None,
                            histogram.sum_seconds,
                        );
                        write_sample(
                            &mut output,
                            &format!("{name}_count"),
                            &key.tags,
                            None,
                            histogram.count as f64,
                        );
                    }
                }
            }
        }
        output
    }
}

impl MetricSink for PrometheusMetricSink {
    fn record(&self, event: MetricEvent) {
        let name = sanitize(&format!(
            "{}{}{}",
            self.inner.prefix,
            if self.inner.prefix.is_empty() {
                ""
            } else {
                "_"
            },
            event.name
        ));
        let mut tags: BTreeMap<String, String> = self
            .inner
            .root_tags
            .iter()
            .map(|(key, value)| (sanitize(key), sanitize(value)))
            .collect();
        for (key, value) in event.tags {
            tags.insert(sanitize(&key), sanitize(&value));
        }
        let key = SeriesKey { name, tags };
        let mut series = self.inner.series.lock().expect("metrics lock poisoned");
        let aggregate = series.entry(key).or_insert_with(|| match event.kind {
            MetricKind::Counter => Aggregate::Counter(0),
            MetricKind::Gauge => Aggregate::Gauge(0.0),
            MetricKind::Timer => Aggregate::Histogram(HistogramAggregate::new(
                self.inner.timer_buckets_seconds.clone(),
                None,
            )),
            MetricKind::Histogram => match event.histogram {
                Some(HistogramSpec::GoLow1ms100s) => Aggregate::Histogram(HistogramAggregate::new(
                    go_low_1ms100s_bucket_bounds_seconds(),
                    Some(go_low_1ms100s_bucket_bounds_nanos()),
                )),
                None => Aggregate::Histogram(HistogramAggregate::new(
                    self.inner.timer_buckets_seconds.clone(),
                    None,
                )),
            },
        });
        match (aggregate, event.kind, event.value) {
            (Aggregate::Counter(counter), MetricKind::Counter, MetricValue::Count(delta)) => {
                *counter += delta;
            }
            (Aggregate::Gauge(gauge), MetricKind::Gauge, MetricValue::Number(value)) => {
                *gauge = value;
            }
            (
                Aggregate::Histogram(histogram),
                MetricKind::Timer | MetricKind::Histogram,
                MetricValue::Duration(duration),
            ) => {
                histogram.observe(duration);
            }
            _ => {}
        }
    }
}

impl HistogramAggregate {
    fn new(bounds_seconds: Vec<f64>, duration_bounds_nanos: Option<Vec<u64>>) -> Self {
        Self {
            bucket_counts: vec![0; bounds_seconds.len()],
            bounds_seconds,
            duration_bounds_nanos,
            count: 0,
            sum_seconds: 0.0,
        }
    }

    fn observe(&mut self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        let observed_seconds = if let Some(bounds_nanos) = &self.duration_bounds_nanos {
            let upper_bound = bounds_nanos
                .iter()
                .position(|bound| duration.as_nanos() <= u128::from(*bound));
            upper_bound
                .and_then(|index| self.bounds_seconds.get(index).copied())
                .unwrap_or(f64::INFINITY)
        } else {
            seconds
        };
        self.count += 1;
        self.sum_seconds += observed_seconds;
        for (bound, count) in self
            .bounds_seconds
            .iter()
            .zip(self.bucket_counts.iter_mut())
        {
            if observed_seconds <= *bound {
                *count += 1;
            }
        }
    }
}

async fn scrape(State(sink): State<Arc<PrometheusMetricSink>>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        sink.render(),
    )
}

pub struct PrometheusListener {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<io::Result<()>>,
}

#[derive(Debug, thiserror::Error)]
pub enum PrometheusListenerError {
    #[error("Prometheus listener failed: {0}")]
    Io(#[from] io::Error),
    #[error("Prometheus listener task failed: {0}")]
    Join(#[from] JoinError),
}

impl PrometheusListener {
    pub async fn shutdown(mut self) -> Result<(), PrometheusListenerError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.await??;
        Ok(())
    }
}

fn spawn_listener<L>(listener: L, router: Router) -> PrometheusListener
where
    L: axum::serve::Listener,
{
    let (shutdown, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = receiver.await;
            })
            .await
    });
    PrometheusListener {
        shutdown: Some(shutdown),
        task,
    }
}

fn write_sample(
    output: &mut String,
    name: &str,
    tags: &BTreeMap<String, String>,
    extra: Option<(&str, String)>,
    value: f64,
) {
    output.push_str(name);
    if !tags.is_empty() || extra.is_some() {
        output.push('{');
        let mut first = true;
        for (key, value) in tags {
            if !first {
                output.push(',');
            }
            first = false;
            output.push_str(key);
            output.push_str("=\"");
            output.push_str(&escape_label(value));
            output.push('"');
        }
        if let Some((key, value)) = extra {
            if !first {
                output.push(',');
            }
            output.push_str(key);
            output.push_str("=\"");
            output.push_str(&escape_label(&value));
            output.push('"');
        }
        output.push('}');
    }
    output.push(' ');
    output.push_str(&format_float(value));
    output.push('\n');
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn escape_help(value: &str) -> String {
    escape_label(value)
}

fn format_float(value: f64) -> String {
    if value.is_infinite() {
        if value.is_sign_positive() {
            "+Inf"
        } else {
            "-Inf"
        }
        .to_owned()
    } else if value.is_nan() {
        "NaN".to_owned()
    } else if value != 0.0 && (value.abs() < 1e-4 || value.abs() >= 1e6) {
        let scientific = format!("{value:e}");
        let (mantissa, exponent) = scientific.split_once('e').expect("scientific float has e");
        let exponent = exponent
            .parse::<i32>()
            .expect("scientific exponent is an integer");
        format!("{mantissa}e{exponent:+03}")
    } else {
        value.to_string()
    }
}

/// Match Tally's Prometheus sanitizer: unsupported ASCII is replaced by `_`.
fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn go_default_timer_buckets_seconds() -> Vec<f64> {
    let one_or_nanosecond = 1.0 / 1_000_000_000.0;
    let microsecond_or_1k = one_or_nanosecond * 1_000.0;
    let millisecond_or_1m = one_or_nanosecond * 1_000_000.0;
    vec![
        5.0 * one_or_nanosecond,
        10.0 * one_or_nanosecond,
        50.0 * one_or_nanosecond,
        100.0 * one_or_nanosecond,
        microsecond_or_1k,
        10.0 * microsecond_or_1k,
        50.0 * microsecond_or_1k,
        200.0 * microsecond_or_1k,
        millisecond_or_1m,
        2.0 * millisecond_or_1m,
        5.0 * millisecond_or_1m,
        10.0 * millisecond_or_1m,
        20.0 * millisecond_or_1m,
        50.0 * millisecond_or_1m,
        100.0 * millisecond_or_1m,
        200.0 * millisecond_or_1m,
        500.0 * millisecond_or_1m,
        1_000.0 * millisecond_or_1m,
        2_000.0 * millisecond_or_1m,
        5_000.0 * millisecond_or_1m,
        10_000.0 * millisecond_or_1m,
    ]
}

/// Go `Low1ms100s` is `makeSubsettableHistogram(2, 1ms, 100s, 80).subsetTo(1)`.
/// Its duration boundaries are converted to seconds by Tally's Prom reporter.
fn go_low_1ms100s_bucket_bounds_seconds() -> Vec<f64> {
    go_low_1ms100s_bucket_bounds_nanos()
        .into_iter()
        .map(|nanos| nanos as f64 / 1_000_000_000.0)
        .collect()
}

fn go_low_1ms100s_bucket_bounds_nanos() -> Vec<u64> {
    let mut bounds = Vec::with_capacity(42);
    bounds.push(0);
    for index in 0..=40 {
        let exponent = (index * 2) as f64 / 4.0;
        let nanos = (1_000_000.0 * 2_f64.powf(exponent)) as u64;
        bounds.push(nanos);
    }
    bounds
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching_metrics::{MetricValue, CADENCE_CLIENT_REQUESTS};

    #[test]
    fn go_low_buckets_match_source_boundaries_and_prometheus_units() {
        let bounds = go_low_1ms100s_bucket_bounds_seconds();
        assert_eq!(bounds.len(), 42);
        assert_eq!(bounds[0], 0.0);
        assert_eq!(bounds[1], 0.001);
        assert_eq!(bounds[2], 0.001414213);
        assert_eq!(bounds[3], 0.002);
        assert_eq!(bounds[41], 1_048_576.0 / 1_000.0);
    }

    #[test]
    fn renders_cadence_labels_prefix_and_histograms_in_seconds() {
        let sink = PrometheusMetricSink::new(
            PrometheusMetricsConfig {
                timer_type: "histogram".to_owned(),
                ..Default::default()
            },
            "cadence",
            BTreeMap::from([("zone".to_owned(), "north-west".to_owned())]),
        )
        .unwrap();
        sink.record(MetricEvent {
            name: CADENCE_CLIENT_REQUESTS,
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags: BTreeMap::from([(
                "operation".to_owned(),
                "HistoryClientGetMutableState".to_owned(),
            )]),
            histogram: None,
        });
        sink.record(MetricEvent {
            name: "cadence_client_latency_ns",
            kind: MetricKind::Histogram,
            value: MetricValue::Duration(Duration::from_millis(2)),
            tags: BTreeMap::from([(
                "operation".to_owned(),
                "HistoryClientGetMutableState".to_owned(),
            )]),
            histogram: Some(HistogramSpec::GoLow1ms100s),
        });
        let text = sink.render();
        assert!(text.contains("cadence_cadence_client_requests"));
        assert!(text.contains("cadence_service=\"cadence_matching\""));
        assert!(text.contains("zone=\"north_west\""));
        assert!(text.contains("le=\"0.002\""));
        assert!(text.contains("cadence_cadence_client_latency_ns_sum"));
        assert!(text.contains(" 0.002\n"));
    }

    #[test]
    fn summary_configuration_is_rejected_instead_of_silently_becoming_histogram() {
        let config = PrometheusMetricsConfig {
            timer_type: "summary".to_owned(),
            ..Default::default()
        };
        assert!(matches!(
            PrometheusMetricSink::new(config, "", BTreeMap::new()),
            Err(PrometheusConfigError::UnsupportedTimerType(_))
        ));
        assert!(matches!(
            PrometheusMetricSink::new(PrometheusMetricsConfig::default(), "", BTreeMap::new()),
            Err(PrometheusConfigError::UnsupportedTimerType(_))
        ));
    }
}
