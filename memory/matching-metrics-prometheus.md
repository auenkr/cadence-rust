# Matching Prometheus metrics checkpoint

Date: 2026-10-20

Commits in the `matching-telemetry` worktree:

- `9a137f96a` Go-compatible Matching History metrics.
- `a64c4bb0b` cancellation completion metrics.
- `27cf90152` Go `Low1ms100s` histogram bucket scheme.
- `d4d3715db` configurable Prometheus sink, exposition, and listener.
- `d7127725a` summary timers and Go reporter oracle harness.

The Rust sink is in `rust/crates/cadence-server/src/matching_metrics_prometheus.rs`.
It applies the Matching service tag, metric prefix, sanitizer, Prometheus
histogram buckets, and seconds units for timer and duration histogram output.
An omitted `timerType` selects Go Tally's default summary; explicit `summary`
and `histogram` select those timer forms. Summary quantiles use the pinned
Prometheus client's targeted CKMS stream behavior after the 500-observation
flush, with the default objectives, 10-minute age, and summary count/sum
semantics. Configurable objectives and histogram bounds are supported.

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
