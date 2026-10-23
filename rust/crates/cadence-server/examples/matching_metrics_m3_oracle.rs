use cadence_server::matching_metrics::{
    HistogramSpec, MetricEvent, MetricKind, MetricSink, MetricValue,
};
use cadence_server::matching_metrics_m3::{M3MetricSink, M3MetricsConfig};
use std::{collections::BTreeMap, io, net::UdpSocket, time::Duration};

fn main() -> io::Result<()> {
    let left = UdpSocket::bind("127.0.0.1:0")?;
    let right = UdpSocket::bind("127.0.0.1:0")?;
    left.set_read_timeout(Some(Duration::from_millis(100)))?;
    right.set_read_timeout(Some(Duration::from_millis(100)))?;
    let sink = M3MetricSink::new(
        M3MetricsConfig {
            host_ports: vec![
                left.local_addr()?.to_string(),
                right.local_addr()?.to_string(),
            ],
            service: "cadence_matching".to_owned(),
            env: "test".to_owned(),
            tags: BTreeMap::from([("zone".to_owned(), "west".to_owned())]),
            queue: 1,
            packet_size: 32768,
            histogram_bucket_tag_precision: 6,
            ..Default::default()
        },
        "cadence",
        BTreeMap::from([("cadence_service".to_owned(), "cadence-matching".to_owned())]),
        Duration::from_secs(60),
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
    record(
        "cadence_client_requests",
        MetricKind::Counter,
        MetricValue::Count(5),
        None,
    );
    record(
        "matching_queue_depth",
        MetricKind::Gauge,
        MetricValue::Number(9.0),
        None,
    );
    record(
        "cadence_client_latency",
        MetricKind::Timer,
        MetricValue::Duration(Duration::from_millis(7)),
        None,
    );
    for duration in [20, 21] {
        record(
            "cadence_client_latency_ns",
            MetricKind::Histogram,
            MetricValue::Duration(Duration::from_millis(duration)),
            Some(HistogramSpec::GoLow1ms100s),
        );
    }
    sink.flush()?;
    drop(sink);

    let mut packet = [0; 32768];
    for receiver in [left, right] {
        loop {
            match receiver.recv(&mut packet) {
                Ok(size) => {
                    for byte in &packet[..size] {
                        print!("{byte:02x}");
                    }
                    println!();
                }
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
    }
    Ok(())
}
