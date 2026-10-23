//! M3 metrics sink using Tally's default v2 `MetricBatch` compact Thrift wire protocol.
//!
//! This is intentionally isolated from server configuration/runtime mounting.

use crate::matching_metrics::{HistogramSpec, MetricEvent, MetricKind, MetricSink, MetricValue};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    net::{ToSocketAddrs, UdpSocket},
    sync::{mpsc, Arc, Mutex, Weak},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const DEFAULT_QUEUE: usize = 4096;
const DEFAULT_PACKET_SIZE: usize = 32768;

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct M3MetricsConfig {
    #[serde(default)]
    pub host_port: String,
    #[serde(default)]
    pub host_ports: Vec<String>,
    #[serde(default)]
    pub service: String,
    #[serde(default)]
    pub env: String,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub queue: i64,
    #[serde(default)]
    pub packet_size: i32,
    #[serde(default)]
    pub include_host: bool,
    #[serde(default)]
    pub histogram_bucket_tag_precision: usize,
    #[serde(default)]
    pub internal_tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct M3Tag {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Copy)]
enum Value {
    Counter(i64),
    Gauge(f64),
    Timer(i64),
}

#[derive(Debug, Clone)]
struct M3Metric {
    name: String,
    value: Value,
    timestamp: i64,
    tags: Vec<M3Tag>,
    bucket: Option<(String, String)>,
}

struct Inner {
    sockets: Vec<UdpSocket>,
    common_tags: Vec<M3Tag>,
    internal_tags: Vec<M3Tag>,
    scope_prefix: String,
    scope_tags: BTreeMap<String, String>,
    max_packet_size: usize,
    bucket_value_precision: usize,
    pending: Mutex<VecDeque<M3Metric>>,
    tag_cache: Mutex<BTreeSet<Vec<(String, String)>>>,
    stats: Mutex<ReporterStats>,
}

#[derive(Default)]
struct ReporterStats {
    batches: i64,
    metrics: i64,
    write_errors: i64,
}

pub struct M3MetricSink {
    inner: Arc<Inner>,
    sender: Option<mpsc::SyncSender<BackendMessage>>,
    report_stop: Option<mpsc::Sender<()>>,
    report_worker: Option<JoinHandle<()>>,
    packet_worker: Option<JoinHandle<()>>,
}

enum BackendMessage {
    Metric(M3Metric),
    Flush(mpsc::Sender<io::Result<()>>),
}

impl M3MetricSink {
    /// Bind the M3 v2 compact Thrift UDP sink. `scope_tags` are the per-metric
    /// Tally root tags; M3 config tags become per-metric common tags.
    pub fn new(
        config: M3MetricsConfig,
        scope_prefix: impl Into<String>,
        scope_tags: BTreeMap<String, String>,
        reporting_interval: Duration,
    ) -> io::Result<Self> {
        let mut destinations = config.host_ports.clone();
        if destinations.is_empty() && !config.host_port.is_empty() {
            destinations.push(config.host_port.clone());
        }
        if destinations.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "M3 hostPort or hostPorts is required",
            ));
        }
        let mut sockets = Vec::new();
        for destination in destinations {
            let address = destination.to_socket_addrs()?.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "M3 destination resolved to no addresses",
                )
            })?;
            let socket = UdpSocket::bind(if address.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            })?;
            socket.connect(address)?;
            sockets.push(socket);
        }
        let mut common = config.tags;
        if common.get("service").is_none_or(String::is_empty) && config.service.is_empty()
            || common.get("env").is_none_or(String::is_empty) && config.env.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "M3 service and env are required",
            ));
        }
        common
            .entry("service".into())
            .and_modify(|value: &mut String| {
                if value.is_empty() {
                    *value = config.service.clone();
                }
            })
            .or_insert(config.service);
        common
            .entry("env".into())
            .and_modify(|value: &mut String| {
                if value.is_empty() {
                    *value = config.env.clone();
                }
            })
            .or_insert(config.env);
        if config.include_host {
            common
                .entry("host".into())
                .and_modify(|value: &mut String| {
                    if value.is_empty() {
                        *value = std::env::var("HOSTNAME").unwrap_or_default();
                    }
                })
                .or_insert_with(|| std::env::var("HOSTNAME").unwrap_or_default());
        }
        let mut internal = BTreeMap::from([
            ("version".to_owned(), "3.5.8".to_owned()),
            ("host".to_owned(), "global".to_owned()),
            ("instance".to_owned(), "global".to_owned()),
        ]);
        internal.extend(config.internal_tags);
        let internal_tags: Vec<M3Tag> = internal
            .into_iter()
            .map(|(name, value)| M3Tag { name, value })
            .collect();
        let internal_cache_key: Vec<(String, String)> = internal_tags
            .iter()
            .map(|tag| (tag.name.clone(), tag.value.clone()))
            .collect();
        let inner = Arc::new(Inner {
            sockets,
            common_tags: common
                .into_iter()
                .map(|(name, value)| M3Tag { name, value })
                .collect(),
            internal_tags,
            scope_prefix: scope_prefix.into(),
            scope_tags,
            max_packet_size: if config.packet_size <= 0 {
                DEFAULT_PACKET_SIZE
            } else {
                config.packet_size as usize
            },
            bucket_value_precision: if config.histogram_bucket_tag_precision == 0 {
                6
            } else {
                config.histogram_bucket_tag_precision
            },
            pending: Mutex::new(VecDeque::new()),
            tag_cache: Mutex::new(BTreeSet::from([internal_cache_key])),
            stats: Mutex::new(ReporterStats::default()),
        });
        if encode_batch(&[], &inner.common_tags).len() > inner.max_packet_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "M3 commonTags exceed packetSize",
            ));
        }
        let queue_capacity = if config.queue <= 0 {
            DEFAULT_QUEUE
        } else {
            config.queue as usize
        };
        let (sender, packet_receiver) = mpsc::sync_channel(queue_capacity);
        let packet_inner = inner.clone();
        let packet_handle = thread::Builder::new()
            .name("cadence-m3-packet".into())
            .spawn(move || packet_worker(packet_inner, packet_receiver))?;
        let weak = Arc::downgrade(&inner);
        let report_sender = sender.clone();
        let (stop, receiver) = mpsc::channel();
        let interval = if reporting_interval.is_zero() {
            Duration::from_secs(1)
        } else {
            reporting_interval
        };
        let worker = thread::Builder::new()
            .name("cadence-m3-report".into())
            .spawn(move || flush_worker(weak, receiver, interval, report_sender))?;
        Ok(Self {
            inner,
            sender: Some(sender),
            report_stop: Some(stop),
            report_worker: Some(worker),
            packet_worker: Some(packet_handle),
        })
    }

    /// Emit pending records at one explicit Tally reporting boundary.
    pub fn flush(&self) -> io::Result<()> {
        flush_pending(
            &self.inner,
            self.sender.as_ref().expect("M3 packet worker stopped"),
        )
    }

    fn enqueue(&self, metric: M3Metric) {
        let mut pending = self.inner.pending.lock().expect("M3 metric queue poisoned");
        if let Some(existing) = pending.iter_mut().find(|existing| {
            existing.name == metric.name
                && existing.tags == metric.tags
                && existing.bucket == metric.bucket
                && !matches!(metric.value, Value::Timer(_))
        }) {
            match (&mut existing.value, metric.value) {
                (Value::Counter(total), Value::Counter(value)) => *total += value,
                (slot, Value::Gauge(value)) => *slot = Value::Gauge(value),
                _ => {}
            }
        } else {
            pending.push_back(metric);
        }
    }

    fn event_tags(&self, event: &MetricEvent) -> Vec<M3Tag> {
        let mut combined = self.inner.scope_tags.clone();
        combined.extend(event.tags.clone());
        let tags: Vec<M3Tag> = combined
            .iter()
            .map(|(name, value)| M3Tag {
                name: name.clone(),
                value: value.clone(),
            })
            .collect();
        if !tags.is_empty() {
            self.inner
                .tag_cache
                .lock()
                .expect("M3 tag cache poisoned")
                .insert(
                    tags.iter()
                        .map(|tag| (tag.name.clone(), tag.value.clone()))
                        .collect(),
                );
        }
        tags
    }

    fn metric_name(&self, name: &str) -> String {
        if self.inner.scope_prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{}.{}", self.inner.scope_prefix, name)
        }
    }
}

impl MetricSink for M3MetricSink {
    fn record(&self, event: MetricEvent) {
        let timestamp = 0;
        let tags = self.event_tags(&event);
        match (event.kind, event.value) {
            (MetricKind::Counter, MetricValue::Count(value)) => self.enqueue(M3Metric {
                name: self.metric_name(event.name),
                value: Value::Counter(value),
                timestamp,
                tags,
                bucket: None,
            }),
            (MetricKind::Gauge, MetricValue::Number(value)) => self.enqueue(M3Metric {
                name: self.metric_name(event.name),
                value: Value::Gauge(value),
                timestamp,
                tags,
                bucket: None,
            }),
            (MetricKind::Timer, MetricValue::Duration(value)) => self.enqueue(M3Metric {
                name: self.metric_name(event.name),
                value: Value::Timer(value.as_nanos().min(i64::MAX as u128) as i64),
                timestamp,
                tags,
                bucket: None,
            }),
            (MetricKind::Histogram, MetricValue::Duration(value)) => {
                let bounds = match event.histogram {
                    Some(HistogramSpec::GoDefault1ms100s) => default_exponential_bounds(),
                    Some(HistogramSpec::GoLow1ms100s) | None => low_1ms100s_bounds(),
                    Some(HistogramSpec::GoMid1To16k) => low_1ms100s_bounds(),
                    Some(HistogramSpec::GoPersistenceLatency) => {
                        crate::matching_metrics::go_persistence_latency_bounds_nanos()
                    }
                    Some(HistogramSpec::GoResponseRowSize | HistogramSpec::GoResponsePayloadSize) => {
                        return;
                    }
                };
                let index = bounds
                    .iter()
                    .position(|bound| value.as_nanos() <= u128::from(*bound))
                    .unwrap_or(bounds.len());
                let lower = if index == 0 {
                    i64::MIN
                } else {
                    bounds[index - 1] as i64
                };
                let upper = if index == bounds.len() {
                    i64::MAX
                } else {
                    bounds[index] as i64
                };
                self.enqueue(M3Metric {
                    name: self.metric_name(event.name),
                    value: Value::Counter(1),
                    timestamp,
                    tags,
                    bucket: Some((
                        format!("{index:04}"),
                        format!("{}-{}", duration_string(lower), duration_string(upper)),
                    )),
                });
            }
            (MetricKind::Histogram, MetricValue::Count(value))
                if event.histogram.is_some_and(|spec| {
                    spec == HistogramSpec::GoMid1To16k
                        || crate::matching_metrics::go_value_histogram_bounds(spec).is_some()
                }) =>
            {
                let bounds = match event.histogram.and_then(crate::matching_metrics::go_value_histogram_bounds) {
                    Some(bounds) => bounds,
                    None => mid_1_to_16k_bounds(),
                };
                let index = bounds
                    .iter()
                    .position(|bound| value <= *bound)
                    .unwrap_or(bounds.len());
                let lower = if index == 0 {
                    i64::MIN
                } else {
                    bounds[index - 1]
                };
                let upper = bounds.get(index).copied().unwrap_or(i64::MAX);
                self.enqueue(M3Metric {
                    name: self.metric_name(event.name),
                    value: Value::Counter(1),
                    timestamp,
                    tags,
                    bucket: Some((format!("{index:04}"), format!("{lower}-{upper}"))),
                });
            }
            _ => {}
        }
    }

    fn flush(&self) -> io::Result<()> {
        M3MetricSink::flush(self)
    }
}

impl Drop for M3MetricSink {
    fn drop(&mut self) {
        if let Some(stop) = self.report_stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.report_worker.take() {
            let _ = worker.join();
        }
        if let Some(sender) = self.sender.as_ref() {
            let _ = flush_pending(&self.inner, sender);
        }
        self.sender.take();
        if let Some(worker) = self.packet_worker.take() {
            let _ = worker.join();
        }
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

fn flush_worker(
    inner: Weak<Inner>,
    stop: mpsc::Receiver<()>,
    interval: Duration,
    sender: mpsc::SyncSender<BackendMessage>,
) {
    loop {
        match stop.recv_timeout(interval) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Some(inner) = inner.upgrade() {
                    let _ = flush_pending(&inner, &sender);
                } else {
                    break;
                }
            }
        }
    }
}

fn flush_pending(inner: &Inner, sender: &mpsc::SyncSender<BackendMessage>) -> io::Result<()> {
    let mut metrics = {
        let mut pending = inner.pending.lock().expect("M3 metric queue poisoned");
        let timestamp = now_nanos();
        let metrics = pending
            .drain(..)
            .map(|mut metric| {
                metric.timestamp = timestamp;
                metric
            })
            .collect::<Vec<_>>();
        metrics
    };
    metrics.extend(internal_metrics(inner));
    for metric in metrics {
        sender
            .send(BackendMessage::Metric(metric))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "M3 packet worker stopped"))?;
    }
    let (ack_tx, ack_rx) = mpsc::channel();
    sender
        .send(BackendMessage::Flush(ack_tx))
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "M3 packet worker stopped"))?;
    ack_rx
        .recv()
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "M3 packet worker stopped"))?
}

fn packet_worker(inner: Arc<Inner>, receiver: mpsc::Receiver<BackendMessage>) {
    let mut batch = Vec::new();
    while let Ok(message) = receiver.recv() {
        match message {
            BackendMessage::Metric(metric) => {
                batch.push(metric);
                if encode_batch(&batch, &inner.common_tags).len() > inner.max_packet_size {
                    let last = batch.pop().expect("M3 batch contains current metric");
                    if !batch.is_empty() {
                        let old = std::mem::take(&mut batch);
                        let _ = send_batch(&inner, &old);
                    }
                    let _ = send_batch(&inner, std::slice::from_ref(&last));
                }
            }
            BackendMessage::Flush(reply) => {
                let result = if batch.is_empty() {
                    Ok(())
                } else {
                    send_batch(&inner, &batch)
                };
                batch.clear();
                let _ = reply.send(result);
            }
        }
    }
    if !batch.is_empty() {
        let _ = send_batch(&inner, &batch);
    }
}

fn send_batch(inner: &Inner, metrics: &[M3Metric]) -> io::Result<()> {
    let packet = encode_batch(metrics, &inner.common_tags);
    let mut first_error = None;
    send_to_all(inner, &packet, &mut first_error);
    let mut stats = inner.stats.lock().expect("M3 reporter stats poisoned");
    stats.batches += 1;
    stats.metrics += metrics.len() as i64;
    if first_error.is_some() {
        stats.write_errors += 1;
    }
    first_error.map_or(Ok(()), Err)
}

fn internal_metrics(inner: &Inner) -> Vec<M3Metric> {
    let (batches, metrics, write_errors) = {
        let mut stats = inner.stats.lock().expect("M3 reporter stats poisoned");
        let snapshot = (stats.batches, stats.metrics, stats.write_errors);
        stats.batches = 0;
        stats.metrics = 0;
        stats.write_errors = 0;
        snapshot
    };
    let batch_size = metrics as f64 / batches as f64;
    let mut bounds = vec![0.0];
    let mut bound = 2.0;
    for _ in 0..11 {
        bounds.push(bound);
        bound *= 2.0;
    }
    let bucket = bounds
        .iter()
        .position(|upper| *upper >= batch_size)
        .unwrap_or(bounds.len());
    let lower = if bucket == 0 {
        f64::NEG_INFINITY
    } else {
        bounds[bucket - 1]
    };
    let upper = if bucket == bounds.len() {
        f64::INFINITY
    } else {
        bounds[bucket]
    };
    let bucket_name = format!(
        "{}-{}",
        value_bucket_string(lower, inner.bucket_value_precision),
        value_bucket_string(upper, inner.bucket_value_precision)
    );
    let bucket_tags = [
        M3Tag {
            name: "bucketid".into(),
            value: format!("{bucket:04}"),
        },
        M3Tag {
            name: "bucket".into(),
            value: bucket_name,
        },
    ];
    let tag_cache = inner.tag_cache.lock().expect("M3 tag cache poisoned").len() as i64;
    let timestamp = 0;
    let mut result = vec![M3Metric {
        name: "tally.internal.batch-size".into(),
        value: Value::Counter(1),
        timestamp,
        tags: inner.internal_tags.clone(),
        bucket: Some((bucket_tags[0].value.clone(), bucket_tags[1].value.clone())),
    }];
    for (name, count) in [
        ("tally.internal.num-batches", batches),
        ("tally.internal.num-metrics", metrics),
        ("tally.internal.num-write-errors", write_errors),
        ("tally.internal.num-tag-cache", tag_cache),
    ] {
        result.push(M3Metric {
            name: name.into(),
            value: Value::Counter(count),
            timestamp,
            tags: inner.internal_tags.clone(),
            bucket: None,
        });
    }
    result
}

fn value_bucket_string(value: f64, precision: usize) -> String {
    if value == f64::NEG_INFINITY {
        "-infinity".into()
    } else if value == f64::INFINITY {
        "infinity".into()
    } else {
        format!("{value:.precision$}")
    }
}

fn send_to_all(inner: &Inner, packet: &[u8], first_error: &mut Option<io::Error>) {
    for socket in &inner.sockets {
        if let Err(error) = socket.send(packet) {
            first_error.get_or_insert(error);
        }
    }
}

fn encode_batch(metrics: &[M3Metric], common_tags: &[M3Tag]) -> Vec<u8> {
    let mut w = CompactWriter::default();
    w.write_message_begin("emitMetricBatchV2");
    w.struct_begin(); // args
    w.field_begin(12, 1); // STRUCT: args.batch
    w.struct_begin(); // MetricBatch
    w.field_begin(9, 1); // LIST: metrics
    w.list_begin(12, metrics.len()); // STRUCT elements
    for metric in metrics {
        write_metric(&mut w, metric);
    }
    w.field_begin(9, 2); // LIST: commonTags
    w.list_begin(12, common_tags.len());
    for tag in common_tags {
        write_tag(&mut w, tag);
    }
    w.struct_end(); // MetricBatch
    w.struct_end(); // args
    w.bytes
}

fn write_metric(w: &mut CompactWriter, metric: &M3Metric) {
    w.struct_begin();
    w.field_begin(8, 1); // BINARY: name
    w.string(&metric.name);
    w.field_begin(12, 2); // STRUCT: value
    w.struct_begin();
    let (ty, count, gauge, timer) = match metric.value {
        Value::Counter(v) => (1, v, 0.0, 0),
        Value::Gauge(v) => (2, 0, v, 0),
        Value::Timer(v) => (3, 0, 0.0, v),
    };
    w.field_begin(5, 1); // I32: metricType
    w.i32(ty);
    w.field_begin(6, 2); // I64: count
    w.i64(count);
    w.field_begin(7, 3); // DOUBLE: gauge
    w.f64(gauge);
    w.field_begin(6, 4); // I64: timer
    w.i64(timer);
    w.struct_end(); // MetricValue
    w.field_begin(6, 3); // I64: timestamp
    w.i64(metric.timestamp);
    let mut tags = metric.tags.clone();
    if let Some((bucket_id, bucket)) = &metric.bucket {
        tags.push(M3Tag {
            name: "bucketid".into(),
            value: bucket_id.clone(),
        });
        tags.push(M3Tag {
            name: "bucket".into(),
            value: bucket.clone(),
        });
    }
    if !tags.is_empty() {
        w.field_begin(9, 4); // LIST: tags
        w.list_begin(12, tags.len());
        for tag in &tags {
            write_tag(w, tag);
        }
    }
    w.struct_end(); // Metric
}

fn write_tag(w: &mut CompactWriter, tag: &M3Tag) {
    w.struct_begin();
    w.field_begin(8, 1); // BINARY: name
    w.string(&tag.name);
    w.field_begin(8, 2); // BINARY: value
    w.string(&tag.value);
    w.struct_end();
}

#[derive(Default)]
struct CompactWriter {
    bytes: Vec<u8>,
    last_field_ids: Vec<i16>,
}

impl CompactWriter {
    fn write_message_begin(&mut self, name: &str) {
        self.bytes.extend_from_slice(&[0x82, 0x81]); // compact v1, ONEWAY
        self.varint(0); // sequence id
        self.string(name);
    }
    fn struct_begin(&mut self) {
        self.last_field_ids.push(0);
    }
    fn struct_end(&mut self) {
        self.bytes.push(0); // STOP
        self.last_field_ids.pop().expect("compact struct stack");
    }
    fn field_begin(&mut self, kind: u8, id: i16) {
        let previous = *self.last_field_ids.last().expect("inside compact struct");
        let delta = id - previous;
        if (1..=15).contains(&delta) {
            self.bytes.push(((delta as u8) << 4) | kind);
        } else {
            self.bytes.push(kind);
            self.i16(id);
        }
        *self
            .last_field_ids
            .last_mut()
            .expect("inside compact struct") = id;
    }
    fn list_begin(&mut self, element_kind: u8, size: usize) {
        if size <= 14 {
            self.bytes.push(((size as u8) << 4) | element_kind);
        } else {
            self.bytes.push(0xf0 | element_kind);
            self.varint(size as u64);
        }
    }
    fn string(&mut self, value: &str) {
        self.varint(value.len() as u64);
        self.bytes.extend_from_slice(value.as_bytes());
    }
    fn i16(&mut self, value: i16) {
        self.varint(((value << 1) ^ (value >> 15)) as u16 as u64);
    }
    fn i32(&mut self, value: i32) {
        self.varint(((value << 1) ^ (value >> 31)) as u32 as u64);
    }
    fn i64(&mut self, value: i64) {
        self.varint(((value << 1) ^ (value >> 63)) as u64);
    }
    fn f64(&mut self, value: f64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }
    fn varint(&mut self, mut value: u64) {
        while value > 0x7f {
            self.bytes.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        self.bytes.push(value as u8);
    }
}

fn low_1ms100s_bounds() -> Vec<u64> {
    let mut bounds = vec![0];
    for index in 0..=40 {
        bounds.push((1_000_000.0_f64 * 2_f64.powf(index as f64 / 2.0)) as u64);
    }
    bounds
}

fn mid_1_to_16k_bounds() -> Vec<i64> {
    std::iter::once(0)
        .chain((0..=64).map(|index| (8.0 * 2_f64.powf(index as f64 / 4.0)) as i64))
        .collect()
}

fn duration_string(nanos: i64) -> String {
    if nanos == i64::MIN {
        return "-infinity".into();
    }
    if nanos == i64::MAX {
        return "infinity".into();
    }
    if nanos == 0 {
        return "0s".into();
    }
    let negative = nanos < 0;
    let absolute = nanos.unsigned_abs();
    let sign = if negative { "-" } else { "" };
    if absolute < 1_000 {
        return format!("{sign}{absolute}ns");
    }
    if absolute < 1_000_000 {
        return format!(
            "{sign}{}µs",
            decimal_units(absolute / 1_000, absolute % 1_000, 3)
        );
    }
    if absolute < 1_000_000_000 {
        return format!(
            "{sign}{}ms",
            decimal_units(absolute / 1_000_000, absolute % 1_000_000, 6)
        );
    }
    let seconds = absolute / 1_000_000_000;
    let mut output = sign.to_owned();
    if seconds >= 3600 {
        output.push_str(&format!("{}h", seconds / 3600));
    }
    if seconds % 3600 >= 60 || seconds >= 3600 {
        output.push_str(&format!("{}m", (seconds % 3600) / 60));
    }
    output.push_str(&format!(
        "{}s",
        decimal_units(seconds % 60, absolute % 1_000_000_000, 9)
    ));
    output
}

fn decimal_units(integer: u64, fraction: u64, precision: usize) -> String {
    if fraction == 0 {
        return integer.to_string();
    }
    let fractional = format!("{fraction:0precision$}");
    format!("{integer}.{}", fractional.trim_end_matches('0'))
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    fn event(name: &'static str, kind: MetricKind, value: MetricValue) -> MetricEvent {
        MetricEvent {
            name,
            kind,
            value,
            tags: BTreeMap::new(),
            histogram: None,
        }
    }

    fn test_sink(receiver: &UdpSocket, queue: i64) -> M3MetricSink {
        M3MetricSink::new(
            M3MetricsConfig {
                host_port: receiver.local_addr().unwrap().to_string(),
                service: "cadence_matching".into(),
                env: "test".into(),
                queue,
                ..Default::default()
            },
            "scope",
            BTreeMap::from([("cadence_service".into(), "cadence-matching".into())]),
            Duration::from_secs(60),
        )
        .unwrap()
    }
    #[test]
    fn compact_batch_starts_with_go_v2_oneway_envelope() {
        let packet = encode_batch(&[], &[]);
        assert_eq!(&packet[..3], &[0x82, 0x81, 0x00]);
        assert!(packet
            .windows(b"emitMetricBatchV2".len())
            .any(|window| window == b"emitMetricBatchV2"));
    }

    #[test]
    fn low_histogram_ranges_use_tally_ids_and_duration_tags() {
        let bounds = low_1ms100s_bounds();
        let index = bounds
            .iter()
            .position(|bound| 20_000_000_u128 <= u128::from(*bound))
            .unwrap();
        assert_eq!(index, 10);
        assert_eq!(duration_string(bounds[index - 1] as i64), "16ms");
        assert_eq!(duration_string(bounds[index] as i64), "22.627416ms");
    }

    #[test]
    fn tally_internal_metrics_include_cached_tag_and_batch_counters() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sink = test_sink(&receiver, 64);
        let metrics = internal_metrics(&sink.inner);
        assert_eq!(metrics.len(), 5);
        assert!(metrics
            .iter()
            .any(|metric| metric.name == "tally.internal.num-batches"));
        assert!(metrics
            .iter()
            .any(|metric| metric.name == "tally.internal.num-tag-cache"));
        let batch_size = metrics
            .iter()
            .find(|metric| metric.name == "tally.internal.batch-size")
            .unwrap();
        assert_eq!(
            batch_size.bucket.as_ref().unwrap().1,
            "2048.000000-infinity"
        );
        assert!(batch_size
            .tags
            .iter()
            .any(|tag| tag.name == "version" && tag.value == "3.5.8"));
    }

    #[test]
    fn m3_scope_aggregates_counters_and_latest_gauges_but_keeps_timer_samples() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sink = test_sink(&receiver, 64);
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
        sink.record(event("depth", MetricKind::Gauge, MetricValue::Number(4.0)));
        sink.record(event("depth", MetricKind::Gauge, MetricValue::Number(9.0)));
        sink.record(event(
            "elapsed",
            MetricKind::Timer,
            MetricValue::Duration(Duration::from_millis(7)),
        ));
        let pending = sink.inner.pending.lock().unwrap();
        assert!(pending
            .iter()
            .any(|metric| metric.name == "scope.requests"
                && matches!(metric.value, Value::Counter(5))));
        assert!(pending.iter().any(|metric| metric.name == "scope.depth"
            && matches!(metric.value, Value::Gauge(value) if value == 9.0)));
        assert_eq!(
            pending
                .iter()
                .filter(|metric| metric.name == "scope.elapsed")
                .count(),
            1
        );
    }

    #[test]
    fn m3_udp_fans_out_to_hosts_and_drop_flushes_pending_metrics() {
        let left = UdpSocket::bind("127.0.0.1:0").unwrap();
        let right = UdpSocket::bind("127.0.0.1:0").unwrap();
        left.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        right
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let sink = M3MetricSink::new(
            M3MetricsConfig {
                host_ports: vec![
                    left.local_addr().unwrap().to_string(),
                    right.local_addr().unwrap().to_string(),
                ],
                service: "cadence_matching".into(),
                env: "test".into(),
                ..Default::default()
            },
            "cadence",
            BTreeMap::new(),
            Duration::from_secs(60),
        )
        .unwrap();
        sink.record(event(
            "requests",
            MetricKind::Counter,
            MetricValue::Count(1),
        ));
        drop(sink);
        let mut packet = [0; 32768];
        let left_len = left.recv(&mut packet).unwrap();
        assert!(packet[..left_len]
            .windows(b"emitMetricBatchV2".len())
            .any(|window| window == b"emitMetricBatchV2"));
        let right_len = right.recv(&mut packet).unwrap();
        assert!(packet[..right_len]
            .windows(b"emitMetricBatchV2".len())
            .any(|window| window == b"emitMetricBatchV2"));
    }

    #[test]
    fn m3_capacity_one_queue_drains_without_losing_metrics_at_flush() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let sink = test_sink(&receiver, 1);
        sink.record(event("one", MetricKind::Counter, MetricValue::Count(1)));
        sink.record(event("two", MetricKind::Counter, MetricValue::Count(1)));
        sink.flush().unwrap();
        let mut packet = [0; 32768];
        let size = receiver.recv(&mut packet).unwrap();
        assert!(packet[..size]
            .windows(b"scope.one".len())
            .any(|part| part == b"scope.one"));
        assert!(packet[..size]
            .windows(b"scope.two".len())
            .any(|part| part == b"scope.two"));
    }
}

fn default_exponential_bounds() -> Vec<u64> {
    std::iter::once(0)
        .chain((0..=80).map(|index| (1_000_000.0 * 2_f64.powf(index as f64 / 4.0)) as u64))
        .collect()
}
