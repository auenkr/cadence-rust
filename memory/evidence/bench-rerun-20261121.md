# Benchmark rerun 2026-11-21: Rust vs Go on master 616107912

Source: master `61610791291c288152570b08461a13b92799d948` (includes the Benchmark/run.py fix that waits for the system Worker before warmup). No code changes. Previous rerun: `bench-rerun-20261120b.md` (a075a66fb).
Method as before: `sdk-matching-benchmark.yml` on master with `source_ref=<full SHA>`, `mysql_tmpfs=true`, `fresh_build=true`, 10 s warmup + 60 s measurement, `sql_log_secs=60`, 3 alternating pairs; aggregated with `Benchmark/agg.py` (medians over pairs; "replaced" = Rust-substituted container(s); all four cadence containers for `all`). Runs on EPYC 7763 were rejected and rerun.

## Runs

| Lane | Run | Runner CPU | Used? |
|---|---|---|---|
| History 25 | https://github.com/auenkr/cadence-rust/actions/runs/44795304811 | EPYC 7763 | rejected |
| History 25 rerun 1 | https://github.com/auenkr/cadence-rust/actions/runs/44801723881 | EPYC 7763 | rejected |
| **History 25** | https://github.com/auenkr/cadence-rust/actions/runs/44807867054 | EPYC 9V45 | **headline** |
| History 25 (extra) | https://github.com/auenkr/cadence-rust/actions/runs/44807871800 | EPYC 9V74 | corroborates |
| Matching 25 | https://github.com/auenkr/cadence-rust/actions/runs/44795311492 | EPYC 7763 | rejected |
| Matching 25 rerun 1 | https://github.com/auenkr/cadence-rust/actions/runs/44801730101 | EPYC 7763 | rejected |
| **Matching 25** | https://github.com/auenkr/cadence-rust/actions/runs/44807876845 | EPYC 9V45 | **headline** |
| Matching 25 (extra) | https://github.com/auenkr/cadence-rust/actions/runs/44807882135 | EPYC 9V74 | corroborates |
| **Frontend 25** | https://github.com/auenkr/cadence-rust/actions/runs/44795316982 | Xeon 6975P-C | **headline** |
| **All 25** | https://github.com/auenkr/cadence-rust/actions/runs/44795322162 | Xeon 8370C | **headline** |
| **All 40** | https://github.com/auenkr/cadence-rust/actions/runs/44795327853 | Xeon 6975P-C | **headline** |

All runs concluded success, 0 errors in every arm. Rust binaries were fresh builds ("rust binaries: fresh build"), so a SHA256 differs per run (see each run's rust-bin-source/manifest artifacts).

## Go vs Rust (headline runs, medians over 3 pairs)

| Lane | Arm | p50 ms | p99 ms | repl. CPU s | repl. RSS MiB | MySQL q/s | compl/s | errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| History 25 | Go | 40.5 | 114.5 | 23.82 | 153 | 2084 | 24.98 | 0 |
| | Rust | 33.7 | 104.3 | 15.11 | 135 | 1801 | 25.00 | 0 |
| | **Rust/Go** | 0.83 | **0.91** | **0.63** | **0.88** | 0.86 | | |
| Matching 25 | Go | 33.9 | 112.3 | 3.77 | 99 | 2075 | 24.98 | 0 |
| | Rust | 34.4 | 108.2 | 3.29 | 62 | 2074 | 24.98 | 0 |
| | **Rust/Go** | 1.01 | **0.96** | **0.87** | **0.62** | 1.00 | | |
| Frontend 25 | Go | 36.2 | 108.7 | 8.07 | 105 | 2075 | 25.00 | 0 |
| | Rust | 35.6 | 112.4 | 4.70 | 73 | 2076 | 24.98 | 0 |
| | **Rust/Go** | 0.98 | **1.03** | **0.58** | **0.69** | 1.00 | | |
| All 25 | Go | 67.4 | 159.2 | 42.11 | 359 | 2056 | 24.98 | 0 |
| | Rust | 32.2 | 104.0 | 23.98 | 277 | 1769 | 25.00 | 0 |
| | **Rust/Go** | 0.48 | **0.65** | **0.57** | **0.77** | 0.86 | | |
| All 40 | Go | 91.6 | 580.3 | 43.44 | 375 | 3313 | 39.93 | 0 |
| | Rust | 76.2 | 107.6 | 23.14 | 280 | 2839 | 39.95 | 0 |
| | **Rust/Go** | 0.83 | **0.19** | **0.53** | **0.75** | 0.86 | | |

Corroborating History (9V74, 44807871800): p99 138.6 / 104.7 (0.76), CPU 0.47, RSS 0.87. Corroborating Matching (9V74, 44807882135): p99 118.8 / 119.4 (1.00), CPU 0.90, RSS 0.57.

## Compared with 20261120b (a075a66fb)

| Lane | p99 R/Go prev -> now | CPU R/Go prev -> now | RSS R/Go prev -> now | Rust p99 ms prev -> now |
|---|---|---|---|---|
| History 25 | 0.84 -> 0.91 | 0.52 -> 0.63 | 0.92 -> 0.88 | 102.6 -> 104.3 |
| Matching 25 | 1.02 -> 0.96 | 0.86 -> 0.87 | 0.66 -> 0.62 | 127.1 -> 108.2 |
| Frontend 25 | **4.25 -> 1.03** | 0.70 -> 0.58 | 0.76 -> 0.69 | **805.8 -> 112.4** |
| All 25 | 0.85 -> 0.65 | 0.51 -> 0.57 | 0.75 -> 0.77 | 100.2 -> 104.0 |
| All 40 | 0.01 -> 0.19 | 0.50 -> 0.53 | 0.65 -> 0.75 | 111.2 -> 107.6 |

Takeaways:
- The Frontend start-p99 tail spikes seen in 20261120b are gone: Rust p99 112 ms vs Go 109 ms (ratio 1.03; was 4.25 with Rust 806 ms). This is one clean run on a Xeon 6975P-C, so it is consistent with the run.py Worker-startup fix but is a single run.
- Rust p99 is flat at about 104-112 ms in every lane; ratio changes elsewhere come mainly from the Go arm (runner dependent).
- All 40: Go sustained 39.93 compl/s this time (previous rerun: 34.1), p99 580 ms vs Rust 108 ms.
- CPU and RSS ratios are in line with the previous rerun (History CPU 0.52 -> 0.63 reflects a faster Go arm on 9V45; the 9V74 corroborating run gives 0.47).
- Runner caveat: EPYC 7763 was drawn on the first two dispatches of History and Matching (Go p99 286-3671 ms); those were rejected.
