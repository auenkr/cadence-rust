use cadence_server::matching_metrics::{
    HistogramSpec, MetricEvent, MetricKind, MetricSink, MetricValue,
};
use cadence_server::matching_metrics_statsd::{StatsdMetricSink, StatsdMetricsConfig};
use std::{collections::BTreeMap, io, net::UdpSocket, time::Duration};

fn main() -> io::Result<()> {
    let custom = std::env::args().nth(1).as_deref() == Some("custom");
    let receiver = UdpSocket::bind("127.0.0.1:0")?;
    let client_prefix = if custom { "agent-canary" } else { "agent" };
    let scope_prefix = if custom { "matching-custom" } else { "scope" };
    let zone = if custom { "canary_v2" } else { "west" };
    let sink = StatsdMetricSink::new(
        StatsdMetricsConfig {
            host_port: receiver.local_addr()?.to_string(),
            prefix: client_prefix.to_owned(),
            flush_interval: Duration::from_millis(20),
            reporting_interval: Duration::from_millis(100),
            flush_bytes: 512,
        },
        scope_prefix,
        BTreeMap::from([("zone".to_owned(), zone.to_owned())]),
    )
    .map_err(io::Error::other)?;
    let tags = BTreeMap::from([
        ("cadence_role".to_owned(), "history_client".to_owned()),
        (
            "operation".to_owned(),
            "HistoryClientGetMutableState".to_owned(),
        ),
    ]);
    let record = |name, kind, value, histogram| {
        sink.record(MetricEvent {
            name,
            kind,
            value,
            tags: tags.clone(),
            histogram,
        });
    };
    record("requests", MetricKind::Counter, MetricValue::Count(2), None);
    record("requests", MetricKind::Counter, MetricValue::Count(3), None);
    record("queue", MetricKind::Gauge, MetricValue::Number(4.0), None);
    record("queue", MetricKind::Gauge, MetricValue::Number(9.0), None);
    record(
        "elapsed",
        MetricKind::Timer,
        MetricValue::Duration(Duration::from_millis(7)),
        None,
    );
    record(
        "latency",
        MetricKind::Histogram,
        MetricValue::Duration(Duration::from_millis(20)),
        Some(HistogramSpec::GoLow1ms100s),
    );
    record(
        "latency",
        MetricKind::Histogram,
        MetricValue::Duration(Duration::from_millis(21)),
        Some(HistogramSpec::GoLow1ms100s),
    );
    sink.flush()?;

    receiver.set_read_timeout(Some(Duration::from_millis(60)))?;
    let mut lines = Vec::new();
    let mut packet = [0; 2048];
    loop {
        match receiver.recv(&mut packet) {
            Ok(size) => lines.extend(
                std::str::from_utf8(&packet[..size])
                    .map_err(io::Error::other)?
                    .lines()
                    .map(str::to_owned),
            ),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                break
            }
            Err(error) => return Err(error),
        }
    }
    lines.sort();
    for line in lines {
        println!("{line}");
    }
    Ok(())
}
