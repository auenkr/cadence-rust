# Matching Prometheus reporter oracle

Run from `go/` with the repository-pinned Go dependencies:

```sh
go run ./tools/matching-metrics-prometheus-oracle       # omitted timerType (Tally default)
go run ./tools/matching-metrics-prometheus-oracle histogram
```

The program records the same counter, tagged timer, and `Low1ms100s` duration
histogram fixture as the Rust example, then prints pinned Tally Prometheus
exposition. The one-command comparison runs summary and histogram timers with
default and custom labels, prefixes, objectives, and buckets:

```sh
./scripts/verify-matching-prometheus-oracle.sh
```

Run it after the runtime lane is released; source-only work does not build or
execute either oracle.
