# Matching Prometheus metrics checkpoint

Date: 2026-10-20

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
packet sizing, timer-based flushing, and flush-on-drop. Source verification is
limited to rustfmt and `git diff --check`; neither the new UDP test nor Cargo
tests were run because the runtime lane is occupied. The service runtime still
needs to own/configure this sink in a later integration step.

The StatsD sink now also stages counters and histogram bucket counts at a
separate `reportingInterval` (default one second), while replacing gauges with
their latest value and emitting timers immediately. `flush()` forces pending
aggregates into the UDP buffer before flushing packets; the periodic StatsD
packet flush remains governed independently by `flushInterval`. A source-only
UDP test covers these aggregate semantics with an explicit flush boundary.
This has not yet been run, and there is not yet a pinned-Go StatsD UDP oracle;
do not treat the implementation as validated until both are exercised and
compared semantically.
