# Matching M3 metrics sink

The isolated Rust sink is `cadence_server::matching_metrics_m3::M3MetricSink`.
It implements the object-safe `matching_metrics::MetricSink` contract and
uses a local compact Thrift writer because Tally's default M3 protocol is
compact; binary is only selected by an explicit Go option that Cadence's YAML
does not expose. `matching_metrics_m3::M3MetricsConfig` mirrors the
Go `tally/m3.Configuration` YAML fields. Construct it with the M3 config,
generic metrics prefix, generic root tags (including `cadence_service`), and
the generic reporting interval. This deliberately leaves mounting in the
Matching service runtime to the integration worker.

The exporter emits compact Thrift v2 oneway `emitMetricBatchV2` calls.
The outer args struct contains `MetricBatch`, with metrics field 1 and common
tags field 2. Each metric carries name, `MetricValue` (type/count/gauge/timer),
report timestamp, and per-metric tags. M3 config tags plus service/env and an
optional host are batch common tags; generic root tags and event tags are
metric tags. Counters aggregate and gauges retain their latest value between
report boundaries; timer samples stay separate. Duration histograms map to
Tally bucket ranges and append the `bucketid`/`bucket` tags. The reporter
`tally.internal.*` metrics, default tags (`version`, redacted host and
instance), queue backpressure, packet byte sizing, multi-host fan-out and
final shutdown flush are represented in the current source draft. The
reporting worker emits the scope aggregate at each reporting boundary into a
bounded queue; the packet worker drains that queue into a staging batch, sends
on size or flush marker, and unblocks producers as it drains (matching Tally's
bounded channel plus process goroutine model).

The pinned Go oracle lives at `go/tools/matching-metrics-m3-oracle`. It uses
Tally's real compact reporter and UDP transport, captures raw packets, and
decodes v2 packets into JSON for the source parity comparison.
and exercises counter/gauge/timer/histogram fields, tags, two-host fan-out,
explicit flush, and reporter close. Rust has focused source tests for
aggregation, queue backpressure, multi-host sends and drop flush. Neither the
Go oracle nor Cargo tests have been run; do not treat this sink as validated
or Matching as exporter-complete until Rust packets are compared against the
Go batches, including Tally internal-metric accounting and shutdown behavior.
