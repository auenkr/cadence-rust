# Cassandra Go-vs-Rust benchmark (2026-11-26)

Branch `perf-cassandra-bench`, head `17d2fb2cadbee0e3dea979d2f576ea2fd4368375` (origin/master 5f6504e05 + one commit). Workflow `frontend-matching-benchmark.yml` dispatched on that branch with `fresh_build=true source_ref=<head> control_ref=origin/master rust_services=frontend,matching,history database=cassandra repetitions=4`, shape=echo, 1000 requests, warmup 10, 1 KiB payload. Change: `database` input now accepts `cassandra` (no new input; `paired-run.py --database cassandra` uses `tests/docker-drop-in/docker-compose.cassandra.yml`: cassandra:4.1, MAX_HEAP_SIZE=1G, mem 1800m, 1.5 cpus; DB=cassandra, CASSANDRA_SEEDS=cassandra). Note: `verify_artifacts.py` is MySQL-only and is skipped for cassandra, as for postgres.

Arms: **go**, **rust** (candidate), **control** (same tree/binary as rust, `control-same-tree.txt`=1; A/A noise arm). Interleaved, rotated order, fresh DB per arm/rep. Every cell is median [95% bootstrap CI of the median, 5000 resamples over the 4 reps]. wf/s = successful/elapsed. Percentiles computed per rep from successful latency samples. CPU-s = cgroup CPU seconds in the measurement window per container (FE/MT/HI). RSS = median peak cgroup anon MiB (FE/MT/HI). All runs: 0 failed.

| Run | Setting | ID | Conclusion | Runner CPU |
|--|--|--|--|--|
| A | c=1 unpaced | [45748834613](https://github.com/auenkr/cadence-rust/actions/runs/45748834613) | success | AMD EPYC 9V74 |
| B | paced `target_rate=15` wf/s, c=16 | [45748838635](https://github.com/auenkr/cadence-rust/actions/runs/45748838635) | success | AMD EPYC 9V45 |
| C | default capacity, c=1,16,64 | [45748842656](https://github.com/auenkr/cadence-rust/actions/runs/45748842656) | success | AMD EPYC 7763 |

Runner HW (all): 2 cores x 2 threads (nproc 4), 15 GiB RAM, Ubuntu 26.04.1, kernel 7.0.0-1020-azure, image ubuntu26 20261116.202.1 (B: 20261122.9.1). Per-service containers 1 cpu / 1.4 GiB; Cassandra 1.5 cpu / 1.8 GiB, image cassandra:4.1 sha256:f18f9030c121260759e4d3ccef9462c9ea4f8c53ee85e2164144eb5ee9d2814c. Compare arms within a run, not across runs.

## Binary SHA256
- Rust cadence-server (rust and control, all runs): `e778ea2e1a3a9fcbb9afb84a0d04896274bb5f3ec38c2d3777e61d87a821e725`
- Rust cadence-history: `d86114586c140074794f75d57f6ec28c23c1af4699a8cb63f7589dcff3badeb5`
- Go cadence-server (rebuilt each run): A `5e7f3996ed4be77a4f1a3c5d7bf19d137014e12afab67b30b316663bf650146f`, B `be987cc0fff18dd8ab30dc8762072f8916e6b4018029483f45b5eb9a7d821b2a`, C `0ed168e5693f95cac42b839818c5e6efde62fdb64207ed8c944ff12b86037e80`

## Results

### A: c=1 unpaced — run 45748834613

| c | arm | n | wf/s | p50 ms | p90 ms | p95 ms | p99 ms | failed | FE cpu-s | MT cpu-s | HI cpu-s | FE/MT/HI peak anon MiB |
|--|--|--|--|--|--|--|--|--|--|--|--|--|
| 1 | go | 4 | 23.1 [22.4, 23.7] | 37.8 [37.5, 38.2] | 55.9 [52.5, 72.7] | 88.8 [63.7, 99.2] | 112.2 [102.1, 115.5] | 0 | 6.7 [6.6, 6.9] | 3.9 [3.9, 4.0] | 12.0 [11.9, 12.3] | 38/36/77 |
| 1 | control | 4 | 24.8 [24.6, 25.7] | 27.2 [25.7, 27.5] | 92.5 [79.7, 96.7] | 100.4 [100.0, 100.8] | 106.7 [104.3, 110.2] | 0 | 4.1 [3.8, 4.3] | 2.2 [2.0, 2.3] | 9.7 [9.1, 10.0] | 15/22/70 |
| 1 | rust | 4 | 25.4 [24.6, 25.9] | 25.7 [24.5, 26.5] | 94.3 [89.8, 98.7] | 100.4 [100.1, 100.6] | 107.3 [106.4, 109.4] | 0 | 3.8 [3.7, 4.1] | 2.0 [1.9, 2.2] | 8.9 [8.9, 9.7] | 16/22/69 |

### B: paced 15 wf/s (c=16) — run 45748838635

| c | arm | n | wf/s | p50 ms | p90 ms | p95 ms | p99 ms | failed | FE cpu-s | MT cpu-s | HI cpu-s | FE/MT/HI peak anon MiB |
|--|--|--|--|--|--|--|--|--|--|--|--|--|
| 16 | go | 4 | 15.0 [15.0, 15.0] | 32.0 [31.2, 38.0] | 39.5 [38.7, 46.5] | 45.8 [44.9, 51.9] | 56.2 [52.1, 71.1] | 0 | 5.2 [5.0, 6.8] | 3.1 [3.0, 3.8] | 9.5 [9.1, 11.8] | 39/37/75 |
| 16 | control | 4 | 15.0 [15.0, 15.0] | 20.8 [17.9, 26.7] | 27.1 [24.4, 34.9] | 31.2 [27.4, 40.5] | 50.1 [43.0, 57.6] | 0 | 3.7 [3.1, 5.0] | 1.9 [1.6, 2.7] | 8.1 [7.1, 10.2] | 17/25/68 |
| 16 | rust | 4 | 15.0 [15.0, 15.0] | 20.2 [18.4, 23.2] | 25.1 [24.5, 33.3] | 28.3 [26.9, 38.4] | 40.9 [37.4, 54.1] | 0 | 3.4 [3.1, 4.3] | 1.8 [1.6, 2.3] | 7.7 [7.1, 9.0] | 17/24/66 |

### C: default capacity c=1,16,64 — run 45748842656

| c | arm | n | wf/s | p50 ms | p90 ms | p95 ms | p99 ms | failed | FE cpu-s | MT cpu-s | HI cpu-s | FE/MT/HI peak anon MiB |
|--|--|--|--|--|--|--|--|--|--|--|--|--|
| 1 | go | 4 | 22.0 [21.7, 22.3] | 41.3 [41.2, 41.5] | 56.0 [54.5, 59.3] | 69.9 [64.0, 79.2] | 109.5 [101.9, 111.7] | 0 | 8.3 [8.2, 8.3] | 5.0 [5.0, 5.1] | 15.1 [15.0, 15.2] | 38/36/77 |
| 1 | control | 4 | 23.7 [23.5, 23.9] | 31.6 [30.8, 32.5] | 83.3 [75.4, 85.4] | 99.1 [98.9, 99.5] | 109.3 [107.0, 110.8] | 0 | 4.7 [4.6, 4.8] | 2.4 [2.4, 2.5] | 11.7 [11.6, 12.0] | 16/23/70 |
| 1 | rust | 4 | 23.6 [23.3, 24.0] | 31.9 [31.2, 33.3] | 80.1 [75.8, 82.6] | 98.9 [98.3, 99.5] | 108.9 [108.5, 109.6] | 0 | 4.8 [4.7, 5.0] | 2.5 [2.4, 2.6] | 12.0 [11.7, 12.5] | 16/23/69 |
| 16 | go | 4 | 59.4 [56.5, 62.7] | 263.8 [246.3, 277.6] | 377.4 [354.1, 404.9] | 408.1 [389.6, 441.6] | 520.4 [463.7, 658.9] | 0 | 6.0 [5.9, 6.0] | 3.6 [3.5, 3.6] | 10.1 [9.9, 10.2] | 43/38/100 |
| 16 | control | 4 | 49.1 [47.4, 49.8] | 301.3 [298.9, 325.2] | 501.6 [482.2, 502.9] | 581.7 [546.8, 614.0] | 726.6 [671.9, 763.2] | 0 | 4.4 [4.4, 4.5] | 3.3 [3.3, 3.4] | 11.7 [11.2, 12.2] | 19/25/85 |
| 16 | rust | 4 | 49.5 [47.5, 49.8] | 307.3 [303.4, 311.6] | 489.0 [471.7, 524.0] | 552.1 [525.0, 661.6] | 688.4 [625.0, 817.1] | 0 | 4.5 [4.4, 4.6] | 3.4 [3.3, 3.5] | 11.6 [11.3, 11.9] | 19/26/87 |
| 64 | go | 4 | 62.8 [60.9, 64.5] | 1087.8 [1011.5, 1119.6] | 1586.5 [1481.4, 1591.6] | 1832.5 [1656.5, 1930.3] | 2128.6 [1979.8, 2218.9] | 0 | 5.9 [5.8, 6.0] | 3.5 [3.4, 3.6] | 10.2 [10.1, 10.4] | 49/39/123 |
| 64 | control | 4 | 62.3 [59.1, 67.7] | 1013.5 [929.7, 1105.7] | 1517.6 [1437.2, 1579.9] | 1669.8 [1631.0, 1889.5] | 1939.6 [1743.6, 2113.7] | 0 | 4.3 [4.3, 4.3] | 3.2 [3.1, 3.2] | 11.5 [10.9, 11.5] | 24/24/102 |
| 64 | rust | 4 | 63.3 [57.3, 64.6] | 1026.3 [1004.2, 1103.0] | 1532.0 [1463.9, 1702.9] | 1661.5 [1592.5, 1899.1] | 1969.1 [1709.8, 2024.5] | 0 | 4.4 [4.3, 4.4] | 3.3 [3.1, 3.3] | 11.6 [11.2, 11.9] | 23/24/101 |

## Reading
- A/A (control vs rust) agrees within CI on every metric in every run: noise floor is visible and no Rust-vs-Rust regression.
- c=1 (A, C): Rust beats Go on throughput (+10% A, +7% C) and p50 (-32% A, -23% C), and uses roughly half the CPU-s on FE/MT and 25% less on History. Tail p95/p99 is roughly equal (Rust p95 is higher than Go in A/C because Go's p95 is lower while p50 is higher: different latency shape, same ~100 ms p99).
- Paced 15 wf/s (B): all arms hit 15.0 wf/s, 0 failures; Rust p50/p99 are lower than Go (20 vs 32 ms, 41 vs 56 ms; CIs overlap on p99) with ~35% less FE CPU and ~40% less MT CPU.
- Default capacity (C): at c=16 Go leads on throughput (59 vs 49.5 wf/s, CIs disjoint) and latency (p50 264 vs 307 ms). This is the opposite of MySQL/Postgres (Rust was +50% at c=16 there). Cassandra was CPU-throttled (cgroup `nr_throttled` > 0, cpu limit 1.5), so the DB is likely the shared bottleneck and the c=16 gap needs a follow-up before attributing it to Rust (e.g. Rust issues more/different Cassandra round trips than Go; compare per-op statement counts). At c=64 all three arms are equal within CI (62-63 wf/s).
- Not investigated here: root cause of the c=16 Go>Rust gap. Only 4 reps; single run per setting.
