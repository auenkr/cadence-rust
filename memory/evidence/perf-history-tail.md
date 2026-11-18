# History p99 tail latency (2026-11-19) — NOT RESOLVED

Branch `perf-history-tail`, commit `e5507985f26730123100c73d3f2063be8ffae98c` (base master `d30ba5f06`).

## Hypothesis tested
Per-hop data from baseline run 44395962649 (History lane, 25/s): Rust p50 lower than Go, but StartWorkflow RPC p99 100–200 ms
(Go 15–25) and decision STS p95 59–75 ms (Go ~29). History SQL persistence (`SqlBackend::transaction`, MySQL/Postgres shard stores)
runs the sync mysql/postgres client inline inside `async fn` on tokio workers (Go database/sql parks only the goroutine).
Change: wrap those bodies in `tokio::task::block_in_place` (multi-thread runtime only), plus env-gated
`CADENCE_HISTORY_SQL_SLOW_MS` stderr log. No SQL/API/logic change. Visibility already used spawn_blocking; state-map stores are not on the runtime path.

## Result: no measurable p99 improvement (tail cause still open)
sdk-matching-benchmark.yml, repetitions=3 (alternating Go/Rust pairs), p50/p99 completion ms, 0 errors everywhere.

| lane/rate | run | before Go | before Rust | after Go | after Rust |
|---|---|---|---|---|---|
| history 5 | 44410057457 / 44410069201 | 63–66 / 83–295 | 52–53 / 68–69 | 60–64 / 169–382 | 47–49 / 127–479 |
| all 5 | 44410102983 / 44410113170 | 53 / 63–70 | 40–41 / 48–53 | 61 / 74–81 | 45–46 / 57–64 |
| history 25 (rerun) | 44417365154 / 44417370102 | 49–54 / 112–254 | 36–38 / 200–576 | 38.5 / 108–113 | 32 / 193–198 |
| all 25 (rerun) | 44417375766 / 44417379825 | 73–96 / 864–3933 | 43–67 / 300–635 | 83–11010 / 495–20364 | 43–3899 / 266–9109 |

The first 25/s batch (44410081511, 44410091917, 44410123722, 44410134696) saturated the Go arms too (p99 2.6–17 s) and is discarded.
The only clean 25/s comparison (history-after) still shows Rust p99 ~195 ms vs Go ~110 ms, the same gap as the baseline.
all-25 runs were noisy in both arms (Go p99 up to 20 s).

## Validation
- rust-ci cadence-persistence https://github.com/auenkr/cadence-rust/actions/runs/44409914846 success; cadence-history https://github.com/auenkr/cadence-rust/actions/runs/44409920827 success
- history-mixed-gate mysql/grpc https://github.com/auenkr/cadence-rust/actions/runs/44417354676 success, `GATE SUMMARY failed: []`
- history-mixed-gate postgres/tchannel https://github.com/auenkr/cadence-rust/actions/runs/44417359681 success, `GATE SUMMARY failed: []`

## Next leads
Rust p99 sits at ~190–200 ms consistently (not random), which suggests a fixed period/timer (e.g. a ~200 ms poll/flush/ack interval or
long-poll re-dispatch) rather than contention. Check History timer/transfer queue processor intervals vs Go
(`go/service/history/queue`, TimerProcessorUpdateAckInterval, max poll interval jitter) and the matching AddDecisionTask retry backoff.
Temp refs `tmp/bench-tail-*` hold only the benchmark runs; delete when no longer needed.
