//! StatsD output for Matching metrics.
//!
//! Cadence's Go wrapper encodes labels in the metric name as sorted
//! `.key.value` suffixes because the Tally StatsD reporter cannot tag. It then
//! passes the name to cactus/go-statsd-client, which applies its own prefix and
//! buffers newline-separated UDP packets.

use crate::matching_metrics::{HistogramSpec, MetricEvent, MetricKind, MetricSink, MetricValue};
use serde::{de::Error as _, Deserialize, Deserializer};
use std::{
    collections::BTreeMap,
    io,
    net::{ToSocketAddrs, UdpSocket},
    sync::{mpsc, Arc, Mutex, Weak},
    thread::{self, JoinHandle},
    time::Duration,
};

const CADENCE_SERVICE_TAG: &str = "cadence_service";
const MATCHING_SERVICE: &str = "cadence_matching";
const DEFAULT_PACKET_BYTES: usize = 1432;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(300);
const DEFAULT_REPORTING_INTERVAL: Duration = Duration::from_secs(1);
const GO_DURATION_MIN: i64 = i64::MIN;
const GO_DURATION_MAX: i64 = i64::MAX;

/// Mirrors `common/config.Metrics.Statsd` and the separate scope prefix.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsdMetricsConfig {
    #[serde(default)]
    pub host_port: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(
        default,
        rename = "flushInterval",
        deserialize_with = "deserialize_go_duration"
    )]
    pub flush_interval: Duration,
    #[serde(
        default,
        rename = "reportingInterval",
        deserialize_with = "deserialize_go_duration"
    )]
    pub reporting_interval: Duration,
    #[serde(default, rename = "flushBytes")]
    pub flush_bytes: i64,
}

impl Default for StatsdMetricsConfig {
    fn default() -> Self {
        Self {
            host_port: String::new(),
            prefix: String::new(),
            flush_interval: Duration::ZERO,
            reporting_interval: Duration::ZERO,
            flush_bytes: 0,
        }
    }
}

fn deserialize_go_duration<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Duration, D::Error> {
    let value = serde_yaml::Value::deserialize(deserializer)?;
    let nanos = match value {
        serde_yaml::Value::Null => 0,
        serde_yaml::Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| D::Error::custom("invalid duration"))?,
        serde_yaml::Value::String(value) => parse_go_duration(&value).map_err(D::Error::custom)?,
        other => return Err(D::Error::custom(format!("invalid duration {other:?}"))),
    };
    Ok(if nanos > 0 {
        Duration::from_nanos(nanos as u64)
    } else {
        Duration::ZERO
    })
}

fn parse_go_duration(input: &str) -> Result<i64, String> {
    let (negative, mut rest) = if let Some(rest) = input.strip_prefix('-') {
        (true, rest)
    } else {
        (false, input.strip_prefix('+').unwrap_or(input))
    };
    if rest.is_empty() {
        return Err(format!("invalid duration {input:?}"));
    }
    if rest == "0" {
        return Ok(0);
    }
    let mut total = 0.0_f64;
    while !rest.is_empty() {
        let number_end = rest
            .find(|character: char| !character.is_ascii_digit() && character != '.')
            .unwrap_or(rest.len());
        if number_end == 0 {
            return Err(format!("invalid duration {input:?}"));
        }
        let number = rest[..number_end]
            .parse::<f64>()
            .map_err(|_| format!("invalid duration {input:?}"))?;
        rest = &rest[number_end..];
        let (unit, scale) = [
            ("ns", 1.0),
            ("us", 1_000.0),
            ("µs", 1_000.0),
            ("μs", 1_000.0),
            ("ms", 1_000_000.0),
            ("s", 1_000_000_000.0),
            ("m", 60_000_000_000.0),
            ("h", 3_600_000_000_000.0),
        ]
        .into_iter()
        .find(|(unit, _)| rest.starts_with(unit))
        .ok_or_else(|| format!("invalid duration {input:?}"))?;
        rest = &rest[unit.len()..];
        total += number * scale;
    }
    if total > i64::MAX as f64 {
        return Err(format!("duration out of range {input:?}"));
    }
    let nanos = total as i64;
    Ok(if negative { -nanos } else { nanos })
}

struct StatsdInner {
    socket: UdpSocket,
    buffer: Mutex<Vec<u8>>,
    max_packet_bytes: usize,
    client_prefix: String,
    scope_prefix: String,
    root_tags: BTreeMap<String, String>,
    aggregates: Mutex<BTreeMap<(String, String), Aggregate>>,
}

#[derive(Debug, Clone, Copy)]
enum Aggregate {
    Counter(i64),
    Gauge(f64),
}

pub struct StatsdMetricSink {
    inner: Arc<StatsdInner>,
    stop: Option<mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
    report_stop: Option<mpsc::Sender<()>>,
    report_worker: Option<JoinHandle<()>>,
}

impl StatsdMetricSink {
    /// Connect to a StatsD UDP endpoint and start the configured packet flusher.
    pub fn new(
        config: StatsdMetricsConfig,
        scope_prefix: impl Into<String>,
        root_tags: BTreeMap<String, String>,
    ) -> io::Result<Self> {
        let reporting_interval = if config.reporting_interval.is_zero() {
            DEFAULT_REPORTING_INTERVAL
        } else {
            config.reporting_interval
        };
        if config.host_port.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "StatsD hostPort is empty",
            ));
        }
        let address = config.host_port.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "StatsD hostPort resolved to no addresses",
            )
        })?;
        let socket = UdpSocket::bind(if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        })?;
        socket.connect(address)?;
        let mut root_tags = root_tags;
        root_tags.insert(CADENCE_SERVICE_TAG.to_owned(), MATCHING_SERVICE.to_owned());
        let inner = Arc::new(StatsdInner {
            socket,
            buffer: Mutex::new(Vec::new()),
            max_packet_bytes: if config.flush_bytes <= 0 {
                DEFAULT_PACKET_BYTES
            } else {
                config.flush_bytes as usize
            },
            client_prefix: config.prefix,
            scope_prefix: scope_prefix.into(),
            root_tags,
            aggregates: Mutex::new(BTreeMap::new()),
        });
        let interval = if config.flush_interval.is_zero() {
            DEFAULT_FLUSH_INTERVAL
        } else {
            config.flush_interval
        };
        let (stop, receiver) = mpsc::channel();
        let weak = Arc::downgrade(&inner);
        let worker = thread::Builder::new()
            .name("cadence-statsd-flush".to_owned())
            .spawn(move || flush_worker(weak, receiver, interval))?;
        let report_weak = Arc::downgrade(&inner);
        let (report_stop, report_receiver) = mpsc::channel();
        let report_worker = thread::Builder::new()
            .name("cadence-statsd-report".to_owned())
            .spawn(move || report_worker(report_weak, report_receiver, reporting_interval))?;
        Ok(Self {
            inner,
            stop: Some(stop),
            worker: Some(worker),
            report_stop: Some(report_stop),
            report_worker: Some(report_worker),
        })
    }

    /// Flush a packet immediately. Useful during service shutdown.
    pub fn flush(&self) -> io::Result<()> {
        self.flush_aggregates();
        flush_buffer(&self.inner)
    }

    fn aggregate(&self, name: String, kind: &str, value: Aggregate) {
        let mut aggregates = self
            .inner
            .aggregates
            .lock()
            .expect("StatsD aggregates poisoned");
        match aggregates.entry((name, kind.to_owned())) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                match (entry.get_mut(), value) {
                    (Aggregate::Counter(current), Aggregate::Counter(add)) => *current += add,
                    (current, Aggregate::Gauge(latest)) => *current = Aggregate::Gauge(latest),
                    _ => {}
                }
            }
        }
    }

    fn flush_aggregates(&self) {
        let pending = {
            let mut aggregates = self
                .inner
                .aggregates
                .lock()
                .expect("StatsD aggregates poisoned");
            std::mem::take(&mut *aggregates)
        };
        for ((name, kind), value) in pending {
            match (kind.as_str(), value) {
                ("c", Aggregate::Counter(value)) => self.enqueue(&format!("{name}:{value}|c")),
                ("g", Aggregate::Gauge(value)) => {
                    self.enqueue(&format!("{name}:{}|g", value as i64))
                }
                _ => {}
            }
        }
    }

    fn metric_name(&self, name: &str, event_tags: &BTreeMap<String, String>) -> String {
        let mut tags = self.inner.root_tags.clone();
        tags.extend(
            event_tags
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        let mut name = if self.inner.scope_prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{}.{}", self.inner.scope_prefix, name)
        };
        for (key, value) in tags {
            name.push('.');
            name.push_str(&key);
            name.push('.');
            name.push_str(&value);
        }
        if self.inner.client_prefix.is_empty() {
            name
        } else {
            format!("{}.{}", self.inner.client_prefix, name)
        }
    }

    fn enqueue(&self, metric_line: &str) {
        enqueue_inner(&self.inner, metric_line);
    }
}

fn enqueue_inner(inner: &StatsdInner, metric_line: &str) {
    let mut buffer = inner.buffer.lock().expect("StatsD packet buffer poisoned");
    if !buffer.is_empty() && buffer.len() + metric_line.len() + 1 >= inner.max_packet_bytes {
        let _ = send_buffer(inner, &mut buffer);
    }
    buffer.extend_from_slice(metric_line.as_bytes());
    buffer.push(b'\n');
    if buffer.len() >= inner.max_packet_bytes {
        let _ = send_buffer(inner, &mut buffer);
    }
}

impl MetricSink for StatsdMetricSink {
    fn record(&self, event: MetricEvent) {
        let name = self.metric_name(event.name, &event.tags);
        match (event.kind, event.value) {
            (MetricKind::Counter, MetricValue::Count(value)) => {
                self.aggregate(name, "c", Aggregate::Counter(value))
            }
            (MetricKind::Gauge, MetricValue::Number(value)) => {
                self.aggregate(name, "g", Aggregate::Gauge(value))
            }
            (MetricKind::Timer, MetricValue::Duration(duration)) => {
                let milliseconds = duration.as_nanos() as f64 / 1_000_000.0;
                self.enqueue(&format!("{name}:{milliseconds}|ms"));
            }
            (MetricKind::Histogram, MetricValue::Duration(duration)) => {
                let bounds = match event.histogram {
                    Some(HistogramSpec::GoLow1ms100s) => go_low_1ms100s_bounds_nanos(),
                    None => go_default_duration_bounds_nanos(),
                };
                let (lower, upper) = duration_bucket_bounds(duration, &bounds);
                let bucket_name = format!(
                    "{name}.{}-{}",
                    go_duration_string(lower),
                    go_duration_string(upper)
                );
                self.aggregate(bucket_name, "c", Aggregate::Counter(1));
            }
            _ => {}
        }
    }
}

impl Drop for StatsdMetricSink {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        if let Some(stop) = self.report_stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.report_worker.take() {
            let _ = worker.join();
        }
        self.flush_aggregates();
        let _ = flush_buffer(&self.inner);
    }
}

fn report_worker(inner: Weak<StatsdInner>, stop: mpsc::Receiver<()>, interval: Duration) {
    loop {
        match stop.recv_timeout(interval) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let Some(inner) = inner.upgrade() else { break };
                let pending = {
                    let mut aggregates =
                        inner.aggregates.lock().expect("StatsD aggregates poisoned");
                    std::mem::take(&mut *aggregates)
                };
                for ((name, kind), value) in pending {
                    match (kind.as_str(), value) {
                        ("c", Aggregate::Counter(value)) => {
                            enqueue_inner(&inner, &format!("{name}:{value}|c"))
                        }
                        ("g", Aggregate::Gauge(value)) => {
                            enqueue_inner(&inner, &format!("{name}:{}|g", value as i64))
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn flush_worker(inner: Weak<StatsdInner>, stop: mpsc::Receiver<()>, interval: Duration) {
    loop {
        match stop.recv_timeout(interval) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(inner) = inner.upgrade() {
                    let _ = flush_buffer(&inner);
                } else {
                    break;
                }
            }
        }
    }
}

fn flush_buffer(inner: &StatsdInner) -> io::Result<()> {
    let mut buffer = inner.buffer.lock().expect("StatsD packet buffer poisoned");
    send_buffer(inner, &mut buffer)
}

fn send_buffer(inner: &StatsdInner, buffer: &mut Vec<u8>) -> io::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let result = inner.socket.send(buffer);
    if result.is_ok() {
        buffer.clear();
    }
    result.map(|_| ())
}

fn duration_bucket_bounds(duration: Duration, bounds_nanos: &[u64]) -> (i64, i64) {
    let observed = duration.as_nanos();
    let mut lower = GO_DURATION_MIN;
    for upper in bounds_nanos {
        if observed <= u128::from(*upper) {
            return (lower, (*upper).min(i64::MAX as u64) as i64);
        }
        lower = (*upper).min(i64::MAX as u64) as i64;
    }
    (lower, GO_DURATION_MAX)
}

fn go_low_1ms100s_bounds_nanos() -> Vec<u64> {
    let mut bounds = Vec::with_capacity(42);
    bounds.push(0);
    for index in 0..=40 {
        let exponent = (index * 2) as f64 / 4.0;
        bounds.push((1_000_000.0 * 2_f64.powf(exponent)) as u64);
    }
    bounds
}

fn go_default_duration_bounds_nanos() -> Vec<u64> {
    [
        0_u64,
        10_000_000,
        25_000_000,
        50_000_000,
        75_000_000,
        100_000_000,
        200_000_000,
        300_000_000,
        400_000_000,
        500_000_000,
        600_000_000,
        800_000_000,
        1_000_000_000,
        2_000_000_000,
        5_000_000_000,
    ]
    .to_vec()
}

fn go_duration_string(duration_nanos: i64) -> String {
    if duration_nanos == GO_DURATION_MIN {
        return "-infinity".to_owned();
    }
    if duration_nanos == GO_DURATION_MAX {
        return "infinity".to_owned();
    }
    if duration_nanos == 0 {
        return "0s".to_owned();
    }
    let negative = duration_nanos < 0;
    let nanos = duration_nanos.unsigned_abs();
    let sign = if negative { "-" } else { "" };
    if nanos < 1_000 {
        return format!("{sign}{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format!("{sign}{}µs", decimal_units(nanos / 1_000, nanos % 1_000, 3));
    }
    if nanos < 1_000_000_000 {
        return format!(
            "{sign}{}ms",
            decimal_units(nanos / 1_000_000, nanos % 1_000_000, 6)
        );
    }
    let secs = nanos / 1_000_000_000;
    let frac = nanos % 1_000_000_000;
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    let mut out = sign.to_owned();
    if hours > 0 {
        out.push_str(&format!("{hours}h"));
    }
    if minutes > 0 || hours > 0 {
        out.push_str(&format!("{minutes}m"));
    }
    out.push_str(&format!("{}s", decimal_units(seconds, frac, 9)));
    out
}

fn decimal_units(integer: u64, fraction: u64, precision: usize) -> String {
    if fraction == 0 {
        return integer.to_string();
    }
    let fractional = format!("{fraction:0precision$}");
    format!("{integer}.{}", fractional.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_accepts_go_statsd_duration_and_integer_packet_limit() {
        let config: StatsdMetricsConfig = serde_yaml::from_str(
            "hostPort: 127.0.0.1:8125\nprefix: agent\nflushInterval: 2s\nreportingInterval: 250ms\nflushBytes: 512\n",
        )
        .unwrap();
        assert_eq!(config.flush_interval, Duration::from_secs(2));
        assert_eq!(config.reporting_interval, Duration::from_millis(250));
        assert_eq!(config.flush_bytes, 512);
        assert_eq!(parse_go_duration("250ms"), Ok(250_000_000));
        assert!(parse_go_duration("").is_err());
    }

    #[test]
    fn go_duration_strings_and_low_bucket_selection_match_tally() {
        assert_eq!(go_duration_string(1_414_213), "1.414213ms");
        assert_eq!(go_duration_string(2_000_000), "2ms");
        assert_eq!(go_duration_string(GO_DURATION_MIN), "-infinity");
        assert_eq!(go_duration_string(GO_DURATION_MAX), "infinity");
        let bounds = go_low_1ms100s_bounds_nanos();
        assert_eq!(
            duration_bucket_bounds(Duration::from_millis(2), &bounds),
            (1_414_213, 2_000_000)
        );
    }

    #[test]
    fn sends_sorted_tag_suffixes_with_both_prefixes_and_statsd_counter_type() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let config = StatsdMetricsConfig {
            host_port: receiver.local_addr().unwrap().to_string(),
            prefix: "agent".to_owned(),
            ..Default::default()
        };
        let sink = StatsdMetricSink::new(
            config,
            "cadence",
            BTreeMap::from([("zone".to_owned(), "west".to_owned())]),
        )
        .unwrap();
        sink.record(MetricEvent {
            name: "cadence_client_requests",
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags: BTreeMap::from([
                (
                    "operation".to_owned(),
                    "HistoryClientGetMutableState".to_owned(),
                ),
                ("cadence_role".to_owned(), "history_client".to_owned()),
            ]),
            histogram: None,
        });
        sink.flush().unwrap();
        let mut packet = [0; 1024];
        let size = receiver.recv(&mut packet).unwrap();
        let line = std::str::from_utf8(&packet[..size]).unwrap();
        assert_eq!(line, "agent.cadence.cadence_client_requests.cadence_role.history_client.cadence_service.cadence_matching.operation.HistoryClientGetMutableState.zone.west:1|c\n");
    }

    #[test]
    fn counters_and_histogram_buckets_aggregate_while_gauges_keep_latest_value() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let sink = StatsdMetricSink::new(
            StatsdMetricsConfig {
                host_port: receiver.local_addr().unwrap().to_string(),
                reporting_interval: Duration::from_secs(60),
                ..Default::default()
            },
            "scope",
            BTreeMap::new(),
        )
        .unwrap();
        let event = |name, kind, value| MetricEvent {
            name,
            kind,
            value,
            tags: BTreeMap::new(),
            histogram: None,
        };
        sink.record(event(
            "requests",
            MetricKind::Counter,
            MetricValue::Count(2),
        ));
        sink.record(event(
            "requests",
            MetricKind::Counter,
            MetricValue::Count(3),
        ));
        sink.record(event("queue", MetricKind::Gauge, MetricValue::Number(4.0)));
        sink.record(event("queue", MetricKind::Gauge, MetricValue::Number(9.0)));
        sink.record(event(
            "latency",
            MetricKind::Histogram,
            MetricValue::Duration(Duration::from_millis(20)),
        ));
        sink.record(event(
            "latency",
            MetricKind::Histogram,
            MetricValue::Duration(Duration::from_millis(21)),
        ));
        // Timers bypass the reporting interval.
        sink.record(event(
            "elapsed",
            MetricKind::Timer,
            MetricValue::Duration(Duration::from_millis(7)),
        ));
        let mut packet = [0; 2048];
        let timer = receiver.recv(&mut packet).unwrap();
        assert!(std::str::from_utf8(&packet[..timer])
            .unwrap()
            .contains("scope.elapsed.cadence_service.cadence_matching:7|ms"));
        sink.flush().unwrap();
        let size = receiver.recv(&mut packet).unwrap();
        let lines: std::collections::BTreeSet<_> = std::str::from_utf8(&packet[..size])
            .unwrap()
            .lines()
            .collect();
        assert!(lines.contains("scope.requests.cadence_service.cadence_matching:5|c"));
        assert!(lines.contains("scope.queue.cadence_service.cadence_matching:9|g"));
        assert!(lines.contains("scope.latency.cadence_service.cadence_matching.10ms-25ms:2|c"));
    }
}
