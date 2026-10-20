# Matching Prometheus reporter oracle

Run from `go/` with the repository-pinned Go dependencies:

```sh
go run ./tools/matching-metrics-prometheus-oracle       # omitted timerType (Tally default)
go run ./tools/matching-metrics-prometheus-oracle histogram
```

The program records the same counter, tagged timer, and `Low1ms100s` duration
histogram fixture as `matching_metrics_prometheus.rs`, then prints the pinned
Tally Prometheus exposition. Use it for parity verification when the Rust
runtime lane is free; source-only work does not run this program.
