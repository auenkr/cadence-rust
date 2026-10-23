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
    collections::{BTreeMap, VecDeque},
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
const SUMMARY_MAX_AGE: Duration = Duration::from_secs(10 * 60);
const SUMMARY_STREAM_DURATION: Duration = Duration::from_secs(2 * 60);

/// Field names and defaults correspond to Tally's Go Prometheus `Configuration`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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
            Self::UnsupportedTimerType(value) => {
                write!(f, "unsupported Prometheus timer type {value:?}")
            }
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
    Summary(SummaryAggregate),
}

struct HistogramAggregate {
    bounds_seconds: Vec<f64>,
    duration_bounds_nanos: Option<Vec<u64>>,
    bucket_counts: Vec<u64>,
    count: u64,
    sum_seconds: f64,
}

struct SummaryAggregate {
    objectives: Vec<SummaryObjective>,
    created: std::time::Instant,
    observations: VecDeque<(std::time::Instant, f64)>,
    count: u64,
    sum_seconds: f64,
}

impl SummaryAggregate {
    fn new(objectives: Vec<SummaryObjective>) -> Self {
        Self {
            objectives,
            created: std::time::Instant::now(),
            observations: VecDeque::new(),
            count: 0,
            sum_seconds: 0.0,
        }
    }

    fn observe(&mut self, duration: Duration) {
        let value = duration.as_secs_f64();
        let now = std::time::Instant::now();
        self.observations.push_back((now, value));
        self.count += 1;
        self.sum_seconds += value;
        self.expire(now);
    }

    fn expire(&mut self, now: std::time::Instant) {
        while self
            .observations
            .front()
            .is_some_and(|(observed, _)| now.duration_since(*observed) > SUMMARY_MAX_AGE)
        {
            self.observations.pop_front();
        }
    }

    fn live_samples(&self) -> Vec<f64> {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.created);
        let rotations = (elapsed.as_nanos() / SUMMARY_STREAM_DURATION.as_nanos()) as u32;
        // The pinned client rotates the active CKMS stream every 2 minutes.
        // Each of its five streams receives every observation; a stream is
        // reset when it leaves the head, so the active stream contains about
        // 8 to 10 minutes of observations (the age bucket boundary is coarse).
        let oldest = if rotations < 5 {
            self.created
        } else {
            self.created + SUMMARY_STREAM_DURATION * (rotations - 4)
        };
        self.observations
            .iter()
            .filter(|(observed, _)| {
                *observed >= oldest && now.duration_since(*observed) <= SUMMARY_MAX_AGE
            })
            .map(|(_, value)| *value)
            .collect()
    }
}

impl PrometheusMetricSink {
    /// Create a Matching sink, applying Go's prefix, root labels, and service label.
    pub fn new(
        config: PrometheusMetricsConfig,
        prefix: impl Into<String>,
        root_tags: BTreeMap<String, String>,
    ) -> Result<Self, PrometheusConfigError> {
        let handler_path = if config.handler_path.trim().is_empty() {
            "/metrics".to_owned()
        } else {
            config.handler_path.trim().to_owned()
        };
        if !handler_path.starts_with('/') {
            return Err(PrometheusConfigError::InvalidHandlerPath(
                config.handler_path.clone(),
            ));
        }
        if !matches!(config.timer_type.as_str(), "" | "histogram" | "summary") {
            return Err(PrometheusConfigError::UnsupportedTimerType(
                config.timer_type.clone(),
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
        sink_config.handler_path = handler_path;

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
                Some(Aggregate::Summary(_)) => ("summary", format!("{name} summary")),
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
                    Aggregate::Summary(summary) => {
                        let samples = summary.live_samples();
                        for objective in &summary.objectives {
                            let value = summary_quantile(
                                &samples,
                                objective.percentile,
                                &summary.objectives,
                            );
                            write_sample(
                                &mut output,
                                &name,
                                &key.tags,
                                Some(("quantile", format_float(objective.percentile))),
                                value,
                            );
                        }
                        write_sample(
                            &mut output,
                            &format!("{name}_sum"),
                            &key.tags,
                            None,
                            summary.sum_seconds,
                        );
                        write_sample(
                            &mut output,
                            &format!("{name}_count"),
                            &key.tags,
                            None,
                            summary.count as f64,
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
            MetricKind::Timer if self.inner.config.timer_type == "histogram" => {
                Aggregate::Histogram(HistogramAggregate::new(
                    self.inner.timer_buckets_seconds.clone(),
                    None,
                ))
            }
            MetricKind::Timer => Aggregate::Summary(SummaryAggregate::new(
                if self.inner.config.default_summary_objectives.is_empty() {
                    go_default_summary_objectives()
                } else {
                    self.inner.config.default_summary_objectives.clone()
                },
            )),
            MetricKind::Histogram => match event.histogram {
                Some(HistogramSpec::GoDefault1ms100s) => {
                    let bounds: Vec<u64> = std::iter::once(0)
                        .chain(
                            (0..=80)
                                .map(|index| (1_000_000.0 * 2_f64.powf(index as f64 / 4.0)) as u64),
                        )
                        .collect();
                    Aggregate::Histogram(HistogramAggregate::new(
                        bounds.iter().map(|n| *n as f64 / 1_000_000_000.0).collect(),
                        Some(bounds),
                    ))
                }
                Some(HistogramSpec::GoLow1ms100s) => Aggregate::Histogram(HistogramAggregate::new(
                    go_low_1ms100s_bucket_bounds_seconds(),
                    Some(go_low_1ms100s_bucket_bounds_nanos()),
                )),
                Some(HistogramSpec::GoMid1To16k) => Aggregate::Histogram(HistogramAggregate::new(
                    go_mid_1_to_16k_bucket_bounds(),
                    None,
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
            (Aggregate::Histogram(histogram), MetricKind::Histogram, MetricValue::Count(value)) => {
                histogram.observe_value(value as f64);
            }
            (Aggregate::Summary(summary), MetricKind::Timer, MetricValue::Duration(duration)) => {
                summary.observe(duration);
            }
            _ => {}
        }
    }
}

fn summary_quantile(samples: &[f64], percentile: f64, objectives: &[SummaryObjective]) -> f64 {
    if samples.is_empty() {
        return f64::NAN;
    }
    // Before its 500-observation flush, the pinned Go stream returns the exact
    // nearest rank. Larger windows use its targeted CKMS stream below.
    if samples.len() < 500 {
        let mut sorted = samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        let index = ((percentile * sorted.len() as f64).ceil() as usize).saturating_sub(1);
        return sorted[index.min(sorted.len() - 1)];
    }
    let mut stream = CkmsStream::new(objectives);
    for chunk in samples.chunks(500) {
        let mut sorted = chunk.to_vec();
        sorted.sort_by(f64::total_cmp);
        stream.merge_sorted(&sorted);
    }
    stream.query(percentile)
}

#[derive(Clone, Copy)]
struct CkmsSample {
    value: f64,
    width: f64,
    delta: f64,
}

struct CkmsStream {
    count: f64,
    samples: Vec<CkmsSample>,
    objectives: Vec<(f64, f64)>,
}

impl CkmsStream {
    fn new(objectives: &[SummaryObjective]) -> Self {
        Self {
            count: 0.0,
            samples: Vec::new(),
            objectives: objectives
                .iter()
                .map(|o| (o.percentile, o.allowed_error))
                .collect(),
        }
    }

    fn invariant(&self, rank: f64) -> f64 {
        self.objectives
            .iter()
            .map(|(q, epsilon)| {
                if *q * self.count <= rank {
                    (2.0 * *epsilon * rank) / *q
                } else {
                    (2.0 * *epsilon * (self.count - rank)) / (1.0 - *q)
                }
            })
            .fold(f64::MAX, f64::min)
    }

    fn merge_sorted(&mut self, values: &[f64]) {
        let mut rank = 0.0;
        let mut index = 0;
        for value in values {
            while index < self.samples.len() && self.samples[index].value <= *value {
                rank += self.samples[index].width;
                index += 1;
            }
            let delta = if index == 0 || index == self.samples.len() {
                0.0
            } else {
                (self.invariant(rank).floor() - 1.0).max(0.0)
            };
            self.samples.insert(
                index,
                CkmsSample {
                    value: *value,
                    width: 1.0,
                    delta,
                },
            );
            index += 1;
            self.count += 1.0;
            rank += 1.0;
        }
        self.compress();
    }

    fn compress(&mut self) {
        if self.samples.len() < 2 {
            return;
        }
        let mut candidate = *self.samples.last().unwrap();
        let mut candidate_index = self.samples.len() - 1;
        let mut rank = self.count - 1.0 - candidate.width;
        for index in (0..self.samples.len() - 1).rev() {
            let current = self.samples[index];
            if current.width + candidate.width + candidate.delta <= self.invariant(rank) {
                candidate.width += current.width;
                self.samples[candidate_index] = candidate;
                self.samples.remove(index);
                candidate_index -= 1;
            } else {
                candidate = current;
                candidate_index = index;
            }
            rank -= current.width;
        }
    }

    fn query(&self, percentile: f64) -> f64 {
        let target_rank = (percentile * self.count).ceil();
        let threshold = target_rank + (self.invariant(target_rank) / 2.0).ceil();
        let mut previous = self.samples[0];
        let mut rank = 0.0;
        for current in self.samples.iter().skip(1) {
            rank += previous.width;
            if rank + current.width + current.delta > threshold {
                return previous.value;
            }
            previous = *current;
        }
        previous.value
    }
}

fn go_default_summary_objectives() -> Vec<SummaryObjective> {
    vec![
        SummaryObjective {
            percentile: 0.5,
            allowed_error: 0.01,
        },
        SummaryObjective {
            percentile: 0.75,
            allowed_error: 0.001,
        },
        SummaryObjective {
            percentile: 0.95,
            allowed_error: 0.001,
        },
        SummaryObjective {
            percentile: 0.99,
            allowed_error: 0.001,
        },
        SummaryObjective {
            percentile: 0.999,
            allowed_error: 0.0001,
        },
    ]
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
        self.observe_value(observed_seconds);
    }

    fn observe_value(&mut self, observed_seconds: f64) {
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
    L::Addr: std::fmt::Debug,
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
        let mut labels = tags.clone();
        if let Some((key, value)) = extra {
            labels.insert(key.to_owned(), value);
        }
        for (key, value) in &labels {
            if !first {
                output.push(',');
            }
            first = false;
            output.push_str(key);
            output.push_str("=\"");
            output.push_str(&escape_label(value));
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

fn go_mid_1_to_16k_bucket_bounds() -> Vec<f64> {
    std::iter::once(0.0)
        .chain((0..=64).map(|index| (8.0 * 2_f64.powf(index as f64 / 4.0)) as i64 as f64))
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
    fn omitted_timer_type_uses_go_default_summary() {
        let sink =
            PrometheusMetricSink::new(PrometheusMetricsConfig::default(), "", BTreeMap::new())
                .unwrap();
        sink.record(MetricEvent {
            name: "latency",
            kind: MetricKind::Timer,
            value: MetricValue::Duration(Duration::from_millis(2)),
            tags: BTreeMap::new(),
            histogram: None,
        });
        let text = sink.render();
        assert!(text.contains("# TYPE latency summary"));
        assert!(text.contains("quantile=\"0.5\""));
        assert!(text.contains("latency_sum{cadence_service=\"cadence_matching\"} 0.002"));
        assert!(text.contains("latency_count{cadence_service=\"cadence_matching\"} 1"));
    }

    #[test]
    fn explicit_histogram_timer_type_is_preserved() {
        let sink = PrometheusMetricSink::new(
            PrometheusMetricsConfig {
                timer_type: "histogram".to_owned(),
                ..Default::default()
            },
            "",
            BTreeMap::new(),
        )
        .unwrap();
        sink.record(MetricEvent {
            name: "latency",
            kind: MetricKind::Timer,
            value: MetricValue::Duration(Duration::from_millis(2)),
            tags: BTreeMap::new(),
            histogram: None,
        });
        assert!(sink.render().contains("# TYPE latency histogram"));
    }

    #[test]
    fn custom_summary_objective_is_emitted() {
        let sink = PrometheusMetricSink::new(
            PrometheusMetricsConfig {
                timer_type: "summary".to_owned(),
                default_summary_objectives: vec![SummaryObjective {
                    percentile: 0.9,
                    allowed_error: 0.01,
                }],
                ..Default::default()
            },
            "",
            BTreeMap::new(),
        )
        .unwrap();
        sink.record(MetricEvent {
            name: "latency",
            kind: MetricKind::Timer,
            value: MetricValue::Duration(Duration::from_millis(2)),
            tags: BTreeMap::new(),
            histogram: None,
        });
        assert!(sink.render().contains("quantile=\"0.9\""));
    }
}
