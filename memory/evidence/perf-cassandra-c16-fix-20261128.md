# Cassandra c=16 fix: prepared-statement cache parity with gocql (2026-11-28)

Branch `perf-cassandra-c16-fix` (from origin/master 1ee60ef82). Before: [perf-cassandra-c16b-20261126.md](perf-cassandra-c16b-20261126.md) (Rust − Go c=16 −8.8 wf/s [−11.2, −7.0]).

## Root cause
gocql prepares each bound CQL statement once and reuses it from its LRU (`Session.stmtsLRU`, `ClusterConfig.MaxPreparedStmts` = 1000), so every Go query/batch is one EXECUTE round trip. The Rust stores call the Scylla driver's `Session::query_unpaged` / `query_single_page` / `batch` with unprepared `Statement`s plus bound values; the driver sends a PREPARE before every such call (and for each batch statement), i.e. 2 round trips per statement, plus server-side prepare work on Cassandra. This adds latency per persistence call, which shows up under concurrency (c=16/64) when Cassandra is the bottleneck.
Other paths compared and matching Go: consistency LOCAL_QUORUM / serial LOCAL_SERIAL defaults, per-statement ALL/LOCAL_ONE overrides, page sizes, NumConns→pool_size, token-aware/round-robin host policy. Retry/timeout config left to branch `gocql-retry-parity`.

## Fix
`rust/crates/cadence-persistence/src/cassandra.rs`: new `CqlSession` wrapping Scylla `CachingSession` (capacity 1000 = gocql default); its `query_unpaged` / `query_single_page` / `batch` route through the prepared cache, `Deref<Target=Session>` for everything else. `CassandraSessionConfig::connect`, `CassandraBlockingSession` and `CassandraSemaphorePersistence` use it. No query text, consistency, paging or program logic changed. Statement config (consistency, page size, timestamp) is carried onto the cached prepared statement by the driver.

## CI
- rust-ci cadence-persistence (clippy + tests): [46110042967](https://github.com/auenkr/cadence-rust/actions/runs/46110042967) success (head ffa85781f). First attempt [46108848695](https://github.com/auenkr/cadence-rust/actions/runs/46108848695) failed only on `clippy::result_large_err` (fixed with allow).
- Benchmarks: `frontend-matching-benchmark.yml` on ref perf-cassandra-c16-fix, same inputs as c16b: database=cassandra, rust_services=frontend,matching,history, control_ref=origin/master (control = pre-fix master = the "before" arm, `control-same-tree`=0), shape=echo, 1000 req, warmup 10, 1 KiB, c=1,16,64, repetitions=4. Six successful runs: [46110044968](https://github.com/auenkr/cadence-rust/actions/runs/46110044968) [46110051822](https://github.com/auenkr/cadence-rust/actions/runs/46110051822) [46110071314](https://github.com/auenkr/cadence-rust/actions/runs/46110071314) [46110081540](https://github.com/auenkr/cadence-rust/actions/runs/46110081540) [46110093569](https://github.com/auenkr/cadence-rust/actions/runs/46110093569) [46119223912](https://github.com/auenkr/cadence-rust/actions/runs/46119223912). Run [46110059801](https://github.com/auenkr/cadence-rust/actions/runs/46110059801) failed before any Rust cell (harness collector-readiness timeout in the first Go cell) and was replaced by 46119223912.
- Source heads: ffa85781fba0913a219d1ec6f6f644d9bfa165f8 (5 runs), c4520e79eb8a5b0aa8d401d24d85cadaf11fb83b (1 run; adds only the analysis script). Rust `cadence-server` SHA256 identical in all 6: `e9660289d780cd57f0a469a374733395fe4100b5de1c81895a075c8549a8d9be`; `cadence-history` `fa16d11f719093be5ac018747391fcec5c9b81c3b7cff89fe1125f91bccbae64`.
- Control (origin/master at dispatch) heads: 0aa22b9e8 (1 run), ce9cc23ab (4), b51458fd1 (1); master moved during the batch but no Cassandra driver/session changes landed. Runners: Intel Xeon 6973P/6975P ×4, AMD EPYC 9V74 ×2.
- Method: per run, mean over 4 reps of paired (A−B); 95% percentile cluster bootstrap over the 6 runs (10000 resamples, seed 1). Script: [perf-cassandra-c16-fix-analysis.py](perf-cassandra-c16-fix-analysis.py).

## Results
### Rust(fix) - Go

| c | metric | diff | 95% CI | excludes 0 | per-run |
|--|--|--|--|--|--|
| 1 | wfs | +1.86 | [+1.05, +2.66] | YES | +0.1, +3.1, +1.5, +3.2, +1.5, +1.8 |
| 1 | p95 | +14.73 | [+7.27, +24.98] | YES | +15.5, +14.5, +10.9, +38.3, +3.2, +5.9 |
| 1 | p99 | -4.89 | [-8.59, -0.27] | YES | -5.4, -2.6, -9.0, +5.6, -10.3, -7.6 |
| 1 | cpu | -9.26 | [-10.83, -8.04] | YES | -8.5, -10.3, -8.0, -12.9, -7.9, -8.0 |
| 16 | wfs | +9.96 | [+7.44, +12.55] | YES | +5.0, +10.4, +9.5, +15.5, +8.3, +11.1 |
| 16 | p95 | -30.57 | [-36.26, -25.16] | YES | -19.9, -30.6, -30.4, -42.3, -33.9, -26.3 |
| 16 | p99 | -34.83 | [-45.62, -25.60] | YES | -31.9, -30.0, -19.1, -26.8, -41.8, -59.5 |
| 16 | cpu | -3.86 | [-4.50, -3.43] | YES | -3.6, -3.9, -3.5, -5.4, -3.5, -3.3 |
| 64 | wfs | +25.28 | [+22.82, +27.55] | YES | +26.0, +20.2, +25.8, +25.3, +24.7, +29.8 |
| 64 | p95 | -201.13 | [-227.74, -169.27] | YES | -232.8, -138.7, -174.7, -235.7, -187.6, -237.3 |
| 64 | p99 | -161.20 | [-235.73, -83.43] | YES | -245.2, -8.1, -237.7, -27.6, -236.3, -212.5 |
| 64 | cpu | -3.54 | [-3.99, -3.20] | YES | -3.7, -3.4, -3.1, -4.6, -3.2, -3.2 |

### Rust(fix) - Control(origin/master, before)

| c | metric | diff | 95% CI | excludes 0 | per-run |
|--|--|--|--|--|--|
| 1 | wfs | +0.87 | [+0.20, +1.49] | YES | -0.5, +1.4, +1.0, +2.1, +0.8, +0.4 |
| 1 | p95 | -0.02 | [-0.20, +0.17] | no | -0.4, +0.0, -0.2, +0.4, -0.0, +0.1 |
| 1 | p99 | -1.23 | [-2.92, +0.53] | no | +2.1, -0.0, -3.5, +0.5, -3.8, -2.6 |
| 1 | cpu | -2.35 | [-2.98, -1.84] | YES | -1.6, -2.8, -2.0, -3.8, -1.8, -2.1 |
| 16 | wfs | +18.65 | [+15.72, +20.76] | YES | +11.9, +20.9, +19.2, +21.9, +19.7, +18.3 |
| 16 | p95 | -82.76 | [-95.68, -69.55] | YES | -60.2, -94.0, -77.4, -107.6, -92.1, -65.2 |
| 16 | p99 | -122.66 | [-142.11, -104.05] | YES | -100.9, -150.5, -95.4, -140.4, -152.6, -96.1 |
| 16 | cpu | -2.93 | [-3.66, -2.31] | YES | -2.2, -3.8, -2.5, -4.5, -2.3, -2.3 |
| 64 | wfs | +22.42 | [+20.40, +24.56] | YES | +18.5, +24.8, +22.3, +21.2, +21.0, +26.7 |
| 64 | p95 | -206.41 | [-238.34, -174.26] | YES | -164.8, -265.6, -202.7, -243.5, -212.0, -149.8 |
| 64 | p99 | -198.72 | [-258.01, -132.97] | YES | -214.8, -270.8, -244.5, -63.1, -276.3, -122.8 |
| 64 | cpu | -2.76 | [-3.29, -2.24] | YES | -2.4, -3.6, -2.4, -3.8, -2.2, -2.2 |

### Control(before) - Go

| c | metric | diff | 95% CI | excludes 0 | per-run |
|--|--|--|--|--|--|
| 1 | wfs | +0.99 | [+0.66, +1.35] | YES | +0.6, +1.7, +0.5, +1.1, +0.7, +1.4 |
| 1 | p95 | +14.75 | [+7.27, +24.77] | YES | +15.9, +14.5, +11.1, +38.0, +3.2, +5.8 |
| 1 | p99 | -3.66 | [-6.33, +0.04] | no | -7.4, -2.6, -5.4, +5.1, -6.5, -5.0 |
| 1 | cpu | -6.91 | [-7.86, -6.12] | YES | -6.9, -7.5, -6.0, -9.1, -6.1, -5.9 |
| 16 | wfs | -8.69 | [-10.22, -7.17] | YES | -7.0, -10.6, -9.6, -6.4, -11.4, -7.2 |
| 16 | p95 | +52.19 | [+43.75, +60.44] | YES | +40.3, +63.4, +47.1, +65.2, +58.3, +38.9 |
| 16 | p99 | +87.83 | [+62.62, +109.68] | YES | +69.0, +120.6, +76.3, +113.7, +110.8, +36.6 |
| 16 | cpu | -0.92 | [-1.20, -0.57] | YES | -1.4, -0.1, -0.9, -0.9, -1.2, -1.0 |
| 64 | wfs | +2.86 | [-0.52, +5.47] | no | +7.5, -4.6, +3.5, +4.0, +3.6, +3.1 |
| 64 | p95 | +5.28 | [-49.08, +60.26] | no | -68.0, +127.0, +28.0, +7.7, +24.3, -87.4 |
| 64 | p99 | +37.52 | [-37.32, +134.13] | no | -30.4, +262.8, +6.9, +35.6, +40.0, -89.7 |
| 64 | cpu | -0.78 | [-1.12, -0.32] | YES | -1.4, +0.3, -0.7, -0.8, -1.0, -1.0 |

### Absolute means

| c | arm | wf/s | p95 | p99 | cpu |
|--|--|--|--|--|--|
| 1 | go | 23.4 | 86 | 111 | 20.3 |
| 1 | rust | 25.3 | 100 | 107 | 11.1 |
| 1 | control | 24.4 | 100 | 108 | 13.4 |
| 16 | go | 80.6 | 320 | 402 | 14.1 |
| 16 | rust | 90.6 | 289 | 367 | 10.3 |
| 16 | control | 72.0 | 372 | 489 | 13.2 |
| 64 | go | 91.8 | 1106 | 1228 | 13.8 |
| 64 | rust | 117.1 | 905 | 1067 | 10.2 |
| 64 | control | 94.6 | 1111 | 1266 | 13.0 |

failed workflows total: 0 cells: 216

## Verdict
**Fixed.** Rust − Go at c=16 went from −8.8 wf/s [−11.2, −7.0] (c16b) / −8.7 [−10.2, −7.2] (same-batch control) to **+10.0 wf/s [+7.4, +12.6]**, p95 −31 ms, p99 −35 ms; c=64 +25.3 wf/s [+22.8, +27.6]. Fix vs before (same runs): c=16 +18.7 wf/s [+15.7, +20.8], CPU −2.9 CPU-s. c=1 unchanged in latency (p95 +14.7 ms vs Go remains, same as before; separate known c=1 gap). 0 failed workflows in 216 cells.
