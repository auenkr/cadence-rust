# Benchmark rerun 2026-11-20 (Rust vs Go)

Source: master `b6616d0d06268c2fca882198c4282435672983e0` (queue-v2 restart fixes + efficiency work since 9592b349a).
Workflow `sdk-matching-benchmark.yml` (SDK open-loop, one-activity workflow), dispatched on master with `source_ref`, `mysql_tmpfs=true`, `fresh_build=true`, 10 s warmup + 60 s measurement, `sql_log_secs=60` (MySQL general log covers the whole run), runner ubuntu-latest 4 vCPU (AMD EPYC 9V45).
Each arm = medians over the alternating Go/Rust fresh-DB pairs. "replaced" = the Rust-substituted container(s) (all four cadence containers for `all`); RSS = peak summed process RSS. SELECT counts are per arm (mean over pairs), by table.

## Runs

| Lane | Rate | Pairs | Run | Result | Rust binary SHA256 |
|---|---:|---:|---|---|---|
| History | 25 | 3 | https://github.com/auenkr/cadence-rust/actions/runs/44625313998 | success | ac4f729becae0fbf0c2893edba4ead423e1671069c75a9924e08c714c7c30d34 |
| Matching (Go p99 1962 ms, degraded, superseded) | 25 | 3 | https://github.com/auenkr/cadence-rust/actions/runs/44625319261 | success | 69d339b0…220dc |
| **Matching rerun** | 25 | 3 | https://github.com/auenkr/cadence-rust/actions/runs/44632011164 | success | 69d339b02119590ce511c2cc14fc997911c91459c4340e47d723c4bb479220dc |
| Frontend (Go p99 2306 ms, superseded) | 25 | 3 | https://github.com/auenkr/cadence-rust/actions/runs/44625325248 | success | 69d339b0…220dc |
| **Frontend rerun** (Go p99 still 1466 ms) | 25 | 3 | https://github.com/auenkr/cadence-rust/actions/runs/44632014695 | success | 69d339b02119590ce511c2cc14fc997911c91459c4340e47d723c4bb479220dc |
| All services | 25 | 3 | https://github.com/auenkr/cadence-rust/actions/runs/44625330722 | success | b59b2113…58b52 (history 210fe32b…71510) |
| All services ceiling | 40 | 2 | https://github.com/auenkr/cadence-rust/actions/runs/44625335610 | success | b59b211318358416c8d8746b8b3161d91e705823d9e688e87faaa81a90258b52 / history 210fe32bf50343cdff9a616f09b41b9640125275de059357e4e5c47f0b571510 |
| All services ceiling | 60 | 2 | https://github.com/auenkr/cadence-rust/actions/runs/44625340767 | workflow failure (report.py rejects Go arm errors); computed from samples | 3d0a2bf9c72b693673ae64838ef6f4ac647ffd33df4dec8cb4512cd360251e87 / history 03416ae6ca035b915ea44b5b9bc583cfdcdd1a8b82e3ef1464487e469dad95c7 |

Fresh builds are not bit-reproducible across runs (different SHA per build, same source tree `3c8ac1155b3c`).

## Headline (Rust/Go ratios, valid lanes)

| Lane | p50 | p99 | replaced CPU | replaced RSS | cadence CPU | MySQL q/s |
|---|---:|---:|---:|---:|---:|---:|
| History 25/s | 0.85 (30.5 vs 35.9) | 0.91 (101 vs 111) | 0.71 | 1.02 | 0.86 | 0.88 |
| Matching 25/s (rerun) | 0.97 | 0.97 | 0.74 | 0.62 | 0.94 | 1.00 |
| Frontend 25/s (rerun, Go degraded) | 0.94 | 0.67 | 0.59 | 0.66 | 0.90 | 1.00 |
| All 25/s | 0.55 (35.0 vs 63.7) | 0.77 (103.5 vs 133.7) | 0.68 | 0.84 | 0.69 | 0.86 |
| All 40/s | 0.85 (74.9 vs 88.3) | 0.42 (106.9 vs 254.5) | 0.69 | 0.82 | 0.70 | 0.86 |
| All 60/s | both saturated, see below | | | | | |

### Ceiling 60/s (all services, 2 pairs, from samples.jsonl)

| Arm | correct / 3600 measured | errors | p50 ms (correct) | p99 ms (correct) | cadence CPU s (rust) | Rust RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| go 00 / 01 | 471 / 502 | 3129 / 3098 (deadline-exceeded on StartWorkflow) | 6377 / 6605 | 15155 / 16198 | n/a (Go resources not written) | n/a |
| rust 00 / 01 | 3600 / 3600 | 0 / 0 | 7699 / 7642 | 21612 / 22825 | 79.6 / 80.8 (fe 15, hist 52, match 12.6) | 413 / 411 |

At 60/s both arms are past the 4-vCPU ceiling (multi-second latency); Go collapses (~86% start timeouts), Rust completes every workflow with backlog latency. Ceiling between 40 and 60/s on this runner for both; Rust clean at 40/s with p99 107 ms.

## Comparison vs 2026-11-19 (benchmark-rerun-20261119.md, runs 44395950242..70085)

Caveat: 11-19 used 30 s measurement and disk/relaxed-commit MySQL (no tmpfs); today 60 s with tmpfs, so absolute numbers shift; compare ratios.

| Lane 25/s | 11-19 Rust/Go p50 | today | 11-19 Rust/Go p99 | today | 11-19 replaced CPU | today | 11-19 RSS | today |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| History | 0.74 (32.1/43.5) | 0.85 | **3.49 (418/120)** | **0.91** | 0.55 | 0.71 | 0.83 | 1.02 |
| Matching | 1.00 | 0.97 | 0.91 | 0.97 | 0.88 | 0.74 | 0.63 | 0.62 |
| Frontend | 0.94 | 0.94 | 0.91 | 0.67* | 0.63 | 0.59 | 0.80 | 0.66 |
| All | 0.52 | 0.55 | **1.53 (194/127)** | **0.77** | 0.57 | 0.68 | 0.78 | 0.84 |

Key change: the History tail-latency regression flagged on 11-19 (p99 3.5x Go in History lane, 1.5x in all-Rust) is gone — Rust p99 now below Go in every lane. CPU advantage in History/all shrank somewhat (0.55→0.71, 0.57→0.68) and History RSS is now at parity (157 vs 155 MiB); Matching/Frontend CPU and RSS improved further.
*Frontend lane: Go arm p99 >300 ms in both the original and the rerun (and both arms show ~53 s MySQL CPU vs ~26 s in other lanes), i.e. lane-level contention rather than one bad runner; p99 ratio there is not reliable.

### SELECT observations
- History lane: Rust issues ~1.47x `executions` and 1.50x the per-execution map tables (activity/timer/signal/... `_info_maps`, `buffered_events`), 1.26x `history_node` — more mutable-state reloads (cache misses) than Go, despite lower total q/s (0.88; Go makes ~157 new connections/s vs Rust ~1). Candidate follow-up.
- All-Rust: map tables at parity, `history_node` 1.28x, `cluster_config` 0.25x, `tasks` 0.63x.
- Matching/Frontend lanes: Rust `tasks`/`task_lists` SELECTs 0.65–0.97x Go; Rust never queries `domains`/`domain_metadata` during measurement (cache), Go ~24/arm.

## Full tables (generated by agg.py from report.json)

## history rate=25.0 run 44625313998
| Arm | n | errors | compl/s | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | cadence CPU s | MySQL CPU s | MySQL q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| go | 3 | 0 | 24.98 | 35.9 | 111.2 | 21.32 | 155 | 33.95 | 27.00 | 2106 |
| rust | 3 | 0 | 25.00 | 30.5 | 101.2 | 15.08 | 157 | 29.25 | 16.93 | 1844 |

Rust/Go: p50 0.85, p99 0.91, replaced CPU 0.71, RSS 1.02, cadence CPU 0.86, MySQL q/s 0.88

| Table | Go SELECTs/arm | Rust SELECTs/arm | Rust/Go |
|---|---:|---:|---:|
| shards | 8987 | 8988 | 1.00 |
| current_executions | 8987 | 8988 | 1.00 |
| executions | 4712 | 6948 | 1.47 |
| history_node | 3400 | 4297 | 1.26 |
| request_cancel_info_maps | 866 | 1302 | 1.50 |
| timer_info_maps | 866 | 1302 | 1.50 |
| signals_requested_sets | 866 | 1302 | 1.50 |
| activity_info_maps | 866 | 1302 | 1.50 |
| signal_info_maps | 866 | 1302 | 1.50 |
| child_execution_info_maps | 866 | 1302 | 1.50 |
| buffered_events | 866 | 1302 | 1.50 |
| cluster_config | 256 | 192 | 0.75 |
| task_lists | 33 | 32 | 0.98 |
| tasks | 26 | 26 | 0.99 |
| performance_schema | 24 | 24 | 1.00 |
| domain_metadata | 24 | 0 | 0.00 |
| domains | 24 | 0 | 0.00 |

## matching rate=25.0 run 44625319261
| Arm | n | errors | compl/s | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | cadence CPU s | MySQL CPU s | MySQL q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| go | 3 | 0 | 24.25 | 82.4 | 1962.2 | 6.21 | 100 | 54.30 | 54.41 | 2100 |
| rust | 3 | 0 | 24.45 | 82.6 | 1457.5 | 5.61 | 62 | 54.32 | 53.74 | 2096 |

Rust/Go: p50 1.00, p99 0.74, replaced CPU 0.90, RSS 0.62, cadence CPU 1.00, MySQL q/s 1.00

| Table | Go SELECTs/arm | Rust SELECTs/arm | Rust/Go |
|---|---:|---:|---:|
| shards | 8852 | 8894 | 1.00 |
| current_executions | 8852 | 8894 | 1.00 |
| executions | 7564 | 6805 | 0.90 |
| history_node | 3357 | 3259 | 0.97 |
| activity_info_maps | 1445 | 1296 | 0.90 |
| request_cancel_info_maps | 1444 | 1296 | 0.90 |
| timer_info_maps | 1444 | 1296 | 0.90 |
| signals_requested_sets | 1444 | 1296 | 0.90 |
| signal_info_maps | 1444 | 1296 | 0.90 |
| child_execution_info_maps | 1444 | 1296 | 0.90 |
| buffered_events | 1444 | 1296 | 0.90 |
| task_lists | 440 | 311 | 0.71 |
| tasks | 432 | 287 | 0.67 |
| cluster_config | 256 | 192 | 0.75 |
| domain_metadata | 16 | 0 | 0.00 |
| domains | 16 | 0 | 0.00 |
| performance_schema | 16 | 0 | 0.00 |

## frontend rate=25.0 run 44625325248
| Arm | n | errors | compl/s | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | cadence CPU s | MySQL CPU s | MySQL q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| go | 3 | 0 | 24.27 | 78.3 | 2305.7 | 11.09 | 112 | 52.30 | 53.57 | 2097 |
| rust | 3 | 0 | 24.60 | 72.3 | 2916.0 | 6.43 | 86 | 46.19 | 51.75 | 2097 |

Rust/Go: p50 0.92, p99 1.26, replaced CPU 0.58, RSS 0.77, cadence CPU 0.88, MySQL q/s 1.00

| Table | Go SELECTs/arm | Rust SELECTs/arm | Rust/Go |
|---|---:|---:|---:|
| shards | 8888 | 8942 | 1.01 |
| current_executions | 8888 | 8942 | 1.01 |
| executions | 7604 | 6942 | 0.91 |
| history_node | 3287 | 3306 | 1.01 |
| signals_requested_sets | 1444 | 1307 | 0.91 |
| activity_info_maps | 1444 | 1307 | 0.90 |
| request_cancel_info_maps | 1443 | 1306 | 0.91 |
| timer_info_maps | 1443 | 1306 | 0.91 |
| signal_info_maps | 1443 | 1306 | 0.91 |
| child_execution_info_maps | 1443 | 1306 | 0.91 |
| buffered_events | 1443 | 1306 | 0.91 |
| task_lists | 405 | 340 | 0.84 |
| tasks | 389 | 334 | 0.86 |
| cluster_config | 256 | 192 | 0.75 |
| performance_schema | 24 | 24 | 1.00 |
| domain_metadata | 24 | 0 | 0.00 |
| domains | 24 | 0 | 0.00 |

## all rate=25.0 run 44625330722
| Arm | n | errors | compl/s | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | cadence CPU s | MySQL CPU s | MySQL q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| go | 3 | 0 | 24.98 | 63.7 | 133.7 | 42.33 | 359 | 42.56 | 38.66 | 2098 |
| rust | 3 | 0 | 24.98 | 35.0 | 103.5 | 28.92 | 301 | 29.28 | 23.02 | 1815 |

Rust/Go: p50 0.55, p99 0.77, replaced CPU 0.68, RSS 0.84, cadence CPU 0.69, MySQL q/s 0.86

| Table | Go SELECTs/arm | Rust SELECTs/arm | Rust/Go |
|---|---:|---:|---:|
| shards | 8993 | 8979 | 1.00 |
| current_executions | 8993 | 8979 | 1.00 |
| executions | 7717 | 7730 | 1.00 |
| history_node | 3338 | 4262 | 1.28 |
| signals_requested_sets | 1447 | 1452 | 1.00 |
| activity_info_maps | 1447 | 1452 | 1.00 |
| request_cancel_info_maps | 1447 | 1452 | 1.00 |
| timer_info_maps | 1447 | 1452 | 1.00 |
| signal_info_maps | 1447 | 1452 | 1.00 |
| child_execution_info_maps | 1447 | 1452 | 1.00 |
| buffered_events | 1447 | 1452 | 1.00 |
| cluster_config | 256 | 64 | 0.25 |
| task_lists | 35 | 30 | 0.88 |
| performance_schema | 24 | 24 | 1.00 |
| tasks | 27 | 17 | 0.63 |
| domain_metadata | 24 | 0 | 0.00 |
| domains | 24 | 0 | 0.00 |

## all rate=40.0 run 44625335610
| Arm | n | errors | compl/s | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | cadence CPU s | MySQL CPU s | MySQL q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| go | 2 | 0 | 39.91 | 88.3 | 254.5 | 42.78 | 376 | 42.94 | 30.95 | 3599 |
| rust | 2 | 0 | 39.95 | 74.9 | 106.9 | 29.67 | 308 | 29.94 | 19.42 | 3083 |

Rust/Go: p50 0.85, p99 0.42, replaced CPU 0.69, RSS 0.82, cadence CPU 0.70, MySQL q/s 0.86

## matching rate=25.0 run 44632011164
| Arm | n | errors | compl/s | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | cadence CPU s | MySQL CPU s | MySQL q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| go | 3 | 0 | 24.98 | 36.4 | 112.6 | 4.13 | 99 | 32.63 | 26.32 | 2096 |
| rust | 3 | 0 | 25.00 | 35.3 | 109.2 | 3.07 | 61 | 30.63 | 25.78 | 2093 |

Rust/Go: p50 0.97, p99 0.97, replaced CPU 0.74, RSS 0.62, cadence CPU 0.94, MySQL q/s 1.00

| Table | Go SELECTs/arm | Rust SELECTs/arm | Rust/Go |
|---|---:|---:|---:|
| current_executions | 8984 | 8988 | 1.00 |
| shards | 8984 | 8988 | 1.00 |
| executions | 7648 | 7689 | 1.01 |
| history_node | 3300 | 3350 | 1.02 |
| signal_info_maps | 1438 | 1440 | 1.00 |
| activity_info_maps | 1438 | 1440 | 1.00 |
| child_execution_info_maps | 1438 | 1440 | 1.00 |
| timer_info_maps | 1438 | 1440 | 1.00 |
| signals_requested_sets | 1438 | 1440 | 1.00 |
| buffered_events | 1438 | 1440 | 1.00 |
| request_cancel_info_maps | 1438 | 1440 | 1.00 |
| cluster_config | 256 | 192 | 0.75 |
| task_lists | 33 | 32 | 0.97 |
| tasks | 27 | 26 | 0.96 |
| performance_schema | 24 | 16 | 0.67 |
| domains | 24 | 0 | 0.00 |
| domain_metadata | 24 | 0 | 0.00 |

## frontend rate=25.0 run 44632014695
| Arm | n | errors | compl/s | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | cadence CPU s | MySQL CPU s | MySQL q/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| go | 3 | 0 | 24.43 | 78.4 | 1466.4 | 11.28 | 109 | 52.79 | 53.30 | 2103 |
| rust | 3 | 0 | 24.78 | 73.8 | 977.5 | 6.66 | 72 | 47.43 | 51.54 | 2101 |

Rust/Go: p50 0.94, p99 0.67, replaced CPU 0.59, RSS 0.66, cadence CPU 0.90, MySQL q/s 1.00

| Table | Go SELECTs/arm | Rust SELECTs/arm | Rust/Go |
|---|---:|---:|---:|
| shards | 8902 | 8974 | 1.01 |
| current_executions | 8902 | 8973 | 1.01 |
| executions | 7627 | 8442 | 1.11 |
| history_node | 3302 | 3326 | 1.01 |
| signal_info_maps | 1451 | 1596 | 1.10 |
| activity_info_maps | 1451 | 1596 | 1.10 |
| child_execution_info_maps | 1451 | 1596 | 1.10 |
| timer_info_maps | 1451 | 1596 | 1.10 |
| signals_requested_sets | 1451 | 1596 | 1.10 |
| buffered_events | 1451 | 1596 | 1.10 |
| request_cancel_info_maps | 1451 | 1596 | 1.10 |
| task_lists | 405 | 277 | 0.68 |
| tasks | 390 | 255 | 0.65 |
| cluster_config | 256 | 192 | 0.75 |
| performance_schema | 24 | 24 | 1.00 |
| domains | 24 | 0 | 0.00 |
| domain_metadata | 24 | 0 | 0.00 |
