# Matching Prometheus metrics checkpoint

Date: 2026-10-20

> **Superseded source status:** this document began as a Prometheus-only
> checkpoint. Rust StatsD and M3 sinks and runtime wiring have since been
> implemented. The old design-audit statements below saying that no M3 sink or
> StatsD oracle exists are historical; use
> [matching-metrics-runtime-next.md](matching-metrics-runtime-next.md) and the
> comparison commands below for the current candidate. The telemetry/MySQL
> integration candidate is still unvalidated until the gated commands pass.

Commits in the `matching-telemetry` worktree:

- `9a137f96a` Go-compatible Matching History metrics.
- `a64c4bb0b` cancellation completion metrics.
- `27cf90152` Go `Low1ms100s` histogram bucket scheme.
- `d4d3715db` configurable Prometheus sink, exposition, and listener.
- `d7127725a` summary timers and Go reporter oracle harness.
- `85729927c` rotating-stream audit and one-command Go/Rust exposition comparison.

The Rust sink is in `rust/crates/cadence-server/src/matching_metrics_prometheus.rs`.
It applies the Matching service tag, metric prefix, sanitizer, Prometheus
histogram buckets, and seconds units for timer and duration histogram output.
An omitted `timerType` selects Go Tally's default summary; explicit `summary`
and `histogram` select those timer forms. Summary quantiles use the pinned
Prometheus client's targeted CKMS stream behavior after each 500-observation
flush, with the default objectives, five 2-minute rotating streams, and
cumulative summary count/sum semantics. Because the client queries only its
active stream, the effective quantile age window is coarse (roughly 8–10
minutes). Configurable objectives and histogram bounds are supported.

The pinned Go oracle harness is `go/tools/matching-metrics-prometheus-oracle`.
From `go/`, run it without an argument to exercise Tally's default summary, or
pass `histogram` to exercise an explicit histogram timer. It records the same
counter, tagged timer, and `Low1ms100s` duration histogram fixture and emits
the actual Tally/M3 Prometheus text format. The harness imports the repository's
Cadence histogram defaults and applies the same prefix, root tags, and sanitizer.

Source checks completed: `rustfmt` on the Prometheus module, `gofmt` on the
oracle, and `git diff --check`. Cargo tests and Go oracle execution were not run
because the runtime lane is occupied. Therefore the exposition has not yet
been compared against live pinned Go output. Once the lane is released, run
from the repo's `go/` directory:

```sh
go run ./tools/matching-metrics-prometheus-oracle
go run ./tools/matching-metrics-prometheus-oracle histogram
```

Then compare both outputs against the Rust renderer after running the focused
`cadence-server` tests. Do not treat these source checks as parity evidence.

## StatsD source port

Pinned Go behavior was audited in `go/common/config/metrics.go`,
`go/common/metrics/tally/statsd/reporter.go`, Tally v3.5.8, and
cactus/go-statsd-client. The Cadence StatsD wrapper does not send protocol
tags; it sorts tag keys and appends each as `.key.value` to the metric name.
The Tally scope prefix is part of that name, then the cactus client prefix is
prepended. Counters use `|c`, gauges truncate to `int64` and use `|g`, timers
emit duration in milliseconds with `|ms`, and histogram buckets are counters
named with Go duration ranges (including `-infinity` and `infinity` sentinels).
Buffered UDP uses newline-separated lines, a 1432-byte default packet limit,
and a 300ms default flush interval; shutdown flushes the final packet.

`matching_metrics_statsd.rs` ports those name and wire conventions, the
Matching root service tag, the Go histogram bounds/duration formatter, UDP
packet sizing, timer-based flushing, and flush-on-drop. The runtime now owns
and configures the sink. `matching_metrics_statsd_oracle.rs` emits Rust UDP
packets for the pinned Go fixture; `tests/verify-matching-metrics-udp-oracles.sh`
sorts and compares actual packet lines for default and custom tags/prefixes.

The StatsD sink now also stages counters and histogram bucket counts at a
separate `reportingInterval` (default one second), while replacing gauges with
their latest value and emitting timers immediately. `flush()` forces pending
aggregates into the UDP buffer before flushing packets; the periodic StatsD
packet flush remains governed independently by `flushInterval`. A source-only
UDP test covers these aggregate semantics with an explicit flush boundary.
The Go packet producer and Rust comparison example are now present, but their
comparison remains unrun until the shared runtime lane is available.

## M3 design audit

Go M3 metrics are a distinct backend, not StatsD with a different formatter.
`common/config/metrics.go` builds a Tally cached root scope using the shared
`reportingInterval` (default 1s), configured scope prefix and tags, then
`tally/m3.Configuration.NewReporter` creates a reporter that sends M3 Thrift
`MetricBatch` packets over UDP. It selects one UDP host or a multi-host
transport; the reporter batches a queue (default 4096), caps datagrams at
32768 bytes, includes common `service` and `env` tags and optionally the host,
and closes its reporter with the scope lifecycle. Histogram bucket values
become `bucketid` and `bucket` tags with default precision 6. The config also
accepts common tags, queue, packet size and internal tags.

The Tally M3 reporter defaults to the Thrift compact protocol; binary protocol
is available in Tally's API but is not exposed by Cadence's YAML configuration.
Rust now has a dedicated M3 v2 compact Thrift UDP sink with reporting aggregation,
packet sizing, multi-address fan-out, host/service/env/common tags, histogram
bucket formatting, and flush/close semantics. `matching_metrics_m3_oracle.rs`
emits packets to two local receivers. The pinned Go oracle's `decode-hex` mode
decodes Rust packets with Go's generated v2 Thrift reader; the comparison
normalizes timestamp and reporter-internal fields before comparing metric
values/tags and common tags. Direct comparisons now pass for both default and
custom StatsD output and the pinned Go M3 v2 UDP producer/decoder. See
`memory/evidence/matching-telemetry/m3/` for raw Go and Rust packet hex,
decoded JSON, and the matching canonical JSON hash.

## Candidate validation sequence

Run `tests/run-matching-telemetry-validation.sh` after the shared Cargo/Go lane
is released. It runs nonzero focused Rust tests, pinned Go Matching/config
oracles, the four-scenario Prometheus text diff, direct StatsD/M3 UDP
comparisons, strict workspace Clippy, the production server build, and the
Go → Rust → Go MySQL persistence interoperability runner. Every Cargo command
uses the shared target and one build job. The UDP comparison runner decodes M3
packets with the pinned Go reporter dependency.
