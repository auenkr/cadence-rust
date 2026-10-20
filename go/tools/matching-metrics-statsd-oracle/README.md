# Matching StatsD oracle

`../../../../scripts/emit-matching-statsd-go-oracle.sh` runs Cadence's pinned
Go StatsD wrapper, Tally v3.5.8, and cactus UDP client against a loopback UDP
listener. It records repeated counters, repeated gauges, a timer, and repeated
histogram durations, waits through one explicit 100 ms Tally reporting
boundary, closes the scope and client, then prints packet lines in sorted order.

Use this output as the Go side of semantic comparisons: compare metric names,
counter totals, latest gauge values, timer milliseconds, and histogram bucket
counts without requiring packet ordering or packet segmentation to match.
The default and `custom` fixtures use the same Cadence `Low1ms100s` histogram
buckets, service tag, and event set as the Rust comparison example. The oracle
is source-only until run in an available Go runtime lane.
