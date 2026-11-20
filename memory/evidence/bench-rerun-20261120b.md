# Benchmark rerun 2026-11-20 (b): Rust vs Go on master a075a66fb

Source: master `a075a66fbfe2987a7ab7296cba26247d61625959` (parity-cache-audit merge; previous rerun measured `b6616d0d0`). No code changes.
Same method as `bench-rerun-20261120.md`: workflow `sdk-matching-benchmark.yml` dispatched on master with `source_ref=<full SHA>`, `mysql_tmpfs=true`, `fresh_build=true`, 10 s warmup + 60 s measurement, `sql_log_secs=60`, 3 alternating fresh-DB pairs, ubuntu-latest 4 vCPU. Aggregated with `Benchmark/agg.py` (medians over pairs; "replaced" = the Rust-substituted container(s), all four cadence containers for `all`; RSS = peak summed process RSS; SELECTs per arm = mean over pairs). Runs flagged EPYC 7763 or with a degraded Go arm (see bench-fe-lane.md) were rerun.

## Runs (all conclusion success unless noted)

| Lane | Rate | Run | Runner CPU | Used? | Rust binary SHA256 |
|---|---:|---|---|---|---|
| History | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44761717876 | EPYC 9V74 | corroborates (Go p50 56.5, MySQL 38.9 s) | 35e2decb6f3e6030edf7574b649a8b9c778263219670a9862bd12ff84019d3ba |
| History rerun | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44767486915 | **EPYC 7763** (Go p99 3028) | no | see manifest |
| **History** | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44774408423 | EPYC 9V74 | **headline** | 35e2decb6f3e6030edf7574b649a8b9c778263219670a9862bd12ff84019d3ba |
| Matching | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44761725610 | EPYC 9V74 (Go p99 815, MySQL 42 s, degraded) | no | see manifest |
| **Matching rerun** | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44767482064 | Xeon 8573C (Go p99 125) | **headline** | ed0d9124efb4b636284fdb1d35cb49f7b19bf4187ce5fa96f623e3fe9c80242c |
| Frontend | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44761732586 | **EPYC 7763** (Go p99 3249) | no | see manifest |
| **Frontend rerun 1** | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44768173605 | Xeon 8573C | **headline (Rust tail spikes)** | ed0d9124efb4b636284fdb1d35cb49f7b19bf4187ce5fa96f623e3fe9c80242c |
| Frontend rerun 2 | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44774428708 | **EPYC 7763** (both arms >1 s p99) | no | see manifest |
| All | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44761738797 | Xeon 8370C (Go p99 1690, MySQL 45.7 s) | no | see manifest |
| All rerun | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44768878794 | **EPYC 7763** (Go p99 3596) | no | see manifest |
| **All rerun 2** | 25 | https://github.com/auenkr/cadence-rust/actions/runs/44776377431 | EPYC 9V74 (Go p99 118) | **headline** | afd880937bc60f7dfe849883d42b527387efae60c6531ae9b5fb2cede792cf9f (history a9fe5d11ab429d84464173a06121ff3a4f7109d1e8dba2f8ebbe0357413f87f0) |
| All ceiling | 40 | https://github.com/auenkr/cadence-rust/actions/runs/44761743625 | n/a | workflow failure: Go generator failed (Go cannot sustain 40/s; report.py rejects it). Samples: Go 1127–1392 of 2400 correct, Rust 2400/2400 | - |
| **All ceiling rerun** | 40 | https://github.com/auenkr/cadence-rust/actions/runs/44768962064 | EPYC 9V45 | **headline** | afd88093… (same as above) |

Fresh builds are not bit-reproducible across runs (different SHA per build of the same tree). History-lane headline (44774408423) used its own build.

## Side-by-side (headline runs, medians over 3 pairs)

| Lane | Arm | p50 ms | p99 ms | replaced CPU s | replaced RSS MiB | MySQL q/s | cadence CPU s | MySQL CPU s | compl/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| History 25 | Go | 59.3 | 121.8 | 27.37 | 154 | 2077 | 41.83 | 39.77 | 24.97 |
| | Rust | 33.2 | 102.6 | 14.29 | 142 | 1779 | 31.42 | 23.80 | 25.00 |
| | **Rust/Go** | 0.56 | 0.84 | 0.52 | 0.92 | 0.86 | 0.75 | | |
| Matching 25 | Go | 57.5 | 124.8 | 5.20 | 100 | 2047 | 40.86 | 39.53 | 24.98 |
| | Rust | 54.6 | 127.1 | 4.46 | 66 | 2044 | 40.56 | 39.58 | 24.98 |
| | **Rust/Go** | 0.95 | 1.02 | 0.86 | 0.66 | 1.00 | 0.99 | | |
| Frontend 25 (44768173605) | Go | 69.0 | 189.6 | 11.37 | 105 | 2055 | 46.53 | 43.44 | 24.97 |
| | Rust | 65.6 | 805.8 | 7.93 | 80 | 2066 | 43.24 | 42.98 | 24.98 |
| | **Rust/Go** | 0.95 | **4.25** | 0.70 | 0.76 | 1.01 | 0.93 | | |
| All 25 | Go | 55.4 | 117.8 | 40.02 | 366 | 2066 | 40.23 | 38.30 | 24.97 |
| | Rust | 28.7 | 100.2 | 20.38 | 274 | 1777 | 20.72 | 21.05 | 25.00 |
| | **Rust/Go** | 0.52 | 0.85 | 0.51 | 0.75 | 0.86 | 0.51 | | |
| All 40 | Go | 95.9 | 13933 | 54.19 | 432 | 3246 | 54.40 | 46.83 | 34.08 |
| | Rust | 78.1 | 111.2 | 27.28 | 281 | 2870 | 27.60 | 25.25 | 39.95 |
| | **Rust/Go** | 0.81 | 0.01 | 0.50 | 0.65 | 0.88 | 0.51 | | |

All lanes: 0 errors, Rust completes every workflow. At 40/s the Go arm falls behind (34.1 compl/s, p99 13.9 s) while Rust holds 39.95/s with p99 111 ms; the earlier 40/s run (44761743625) had a Go arm that could not even finish the generator.

### Frontend p99 tail (not a clean result)
Per-pair p99 ms (Go / Rust): run 44768173605 (Xeon 8573C): 00 189.6/805.8, 01 162.1/1166.2, 02 646.9/167.9. Run 44774428708 (EPYC 7763): 00 300.4/3443.4, 01 1219.2/1423.9, 02 3477.9/1943.5. Rust has multi-second starts in about half the pairs while Go also spikes in some (647, 3478). p50, CPU, RSS and MySQL work are all as good or better than Go. The Rust Frontend start-p99 tail flake is still present (known deferred item, see MEMORY fe-soak-fix-deferred). The Frontend p99 ratio from this rerun is not reliable in either direction; treat 4.25 as a worst-case sample, not a stable regression.

## SELECTs per table (Go / Rust per arm, headline runs)

| Table | History | Matching | Frontend | All 25 | All 40 |
|---|---|---|---|---|---|
| executions | 9240 / 9239 | 9246 / 9248 | 9237 / 9252 | 9230 / 9252 | 13118 / 14765 |
| shards, current_executions | 8990 / 8988 | ~9005 / ~9004 | 8987 / 9001 | 8980 / 9000 | 12715 / 14365 |
| history_node | 5991 / 5991 | 5993 / 5996 | 5986 / 5994 | 5985 / 6000 | 8155 / 9571 |
| each *_info_maps, buffered_events | 1749 / 1749 | ~1752 / ~1752 | 1748 / 1750 | 1747 / 1752 | 2788 / 2796 |
| cluster_config | 256 / 194 | 257 / 194 | 256 / 257 | 256 / 132 | 256 / 132 |
| task_lists | 42 / 38 | 82 / 81 | 101 / 137 | 41 / 38 | 482 / 40 |
| tasks | 30 / 26 | 70 / 69 | 89 / 125 | 29 / 26 | 469 / 28 |
| domains, domain_metadata | 30 / 30, 24 / 24 | 30 / 30, 24 / 24 | 30 / 30, 24 / 24 | 30 / 30, 24 / 24 | 30 / 30, 24 / 24 |
| NULL-table statements | 13 / 49 | 13 / ~27 | 13 / 33 | 13 / 78 | 13 / 1290 |

The History-lane excess mutable-state reloads from the last rerun (executions 1.47x, map tables 1.50x, history_node 1.26x) are gone: every history table is now at 1.00 Rust/Go. (At 40/s all-Rust, executions/shards/history_node are 1.13-1.17x Go only because Rust completed 17% more workflows in the window; Go finished 34/s vs 40/s.) Rust still reads `cluster_config` less (0.52-0.76x) and `tasks`/`task_lists` at 0.9-1.0x of Go in the History/Matching lanes. Frontend lane: `tasks`/`task_lists` 1.35-1.40x Go in this run (101 vs 137, 89 vs 125), a change from 0.66x seen in the earlier Frontend runs; counts are small (about 4/s) and not investigated. "NULL" statements (non-SELECT-to-table shapes) are higher for Rust, 1290 at 40/s; not investigated.

## Comparison to bench-rerun-20261120.md (master b6616d0d0)

Caveat: runner hardware differs run to run (see table); Go MySQL CPU is 38-40 s on the 9V74/8573C runners here vs 27 s on the 9V45 runner of the earlier History run, which inflates Go CPU and latency a little, so compare ratios with care.

| Lane 25/s (40/s for last row) | prev p50 | now | prev p99 | now | prev repl. CPU | now | prev RSS | now | prev MySQL q/s | now |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| History | 0.85 | 0.56 | 0.91 | 0.84 | 0.71 | 0.52 | 1.02 (157/155) | 0.92 (142/154) | 0.88 | 0.86 |
| Matching | 0.97 | 0.95 | 0.97 | 1.02 | 0.74 | 0.86 | 0.62 | 0.66 | 1.00 | 1.00 |
| Frontend | 0.94 | 0.95 | 0.67 (Go degraded) | 4.25 (Rust spikes) | 0.59 | 0.70 | 0.66 | 0.76 | 1.00 | 1.01 |
| All 25 | 0.55 | 0.52 | 0.77 | 0.85 | 0.68 | 0.51 | 0.84 | 0.75 | 0.86 | 0.86 |
| All 40 | 0.85 | 0.81 | 0.42 | 0.01 | 0.69 | 0.50 | 0.82 | 0.65 | 0.86 | 0.88 |

Absolute Rust values: History p50 30.5 -> 33.2 ms, p99 101.2 -> 102.6, replaced CPU 15.08 -> 14.29 s, RSS 157 -> 142 MiB; All 25 p50 35.0 -> 28.7, p99 103.5 -> 100.2; All 40 p99 106.9 -> 111.2, p50 74.9 -> 78.1; All RSS 0.82-0.84x -> 0.65-0.75x. Rust p99 is flat versus the previous run in every lane except Frontend.

Takeaways:
- History: no regression in Rust CPU/latency; RSS dropped 157 -> 142 MiB and the extra mutable-state SELECTs are gone (parity at 1.00). The better ratios are largely a slower Go arm on this runner.
- Matching and All: Rust holds p99 about equal to or below Go; all-Rust CPU ratio 0.51 and RSS ratio 0.75 are the best seen so far; the 40/s lane shows Rust sustaining 40/s where Go does not.
- Frontend: CPU/RSS/MySQL-parity unchanged or better, but Rust p99 shows multi-second start tail spikes in about half the pairs on both runners used (Go also spikes, less often). This is the one open item; it matches the already-known start-p99 tail flake.
- Matching CPU ratio moved 0.74 -> 0.86 (Rust 4.46 s vs Go 5.20 s here); the Matching lane's replaced-container CPU is small enough that run noise is a few tenths of a second.
