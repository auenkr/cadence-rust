# Benchmark rerun 2026-11-23: Rust vs Go on master efd0f6baf

Source: master `efd0f6baf20aaffddbdfded1242fa9546e59f693`. No code changes. Previous rerun: `bench-rerun-20261121.md` (616107912).
Method as before: `sdk-matching-benchmark.yml` on master with `source_ref=<full SHA>`, `rate=25|40`, `mysql_tmpfs=true`, `fresh_build=true`, warmup 10 s + 60 s measurement, `repetitions=3`, `sql_log_secs=60`; aggregated with `Benchmark/agg.py` (medians over 3 alternating pairs; "replaced" = Rust-substituted container(s); all four cadence containers for `all`). Runs on EPYC 7763 rejected. Rust binaries fresh builds; SHA256 per run in each run's artifacts.

## Runs

| Lane | Run | Runner CPU | Used? |
|---|---|---|---|
| **History 25** | https://github.com/auenkr/cadence-rust/actions/runs/45126885144 | EPYC 9V45 | **headline** |
| Matching 25 | https://github.com/auenkr/cadence-rust/actions/runs/45126890500 | EPYC 7763 | rejected (Go p99 1961 ms, MySQL CPU 56 s) |
| **Matching 25** (rerun) | https://github.com/auenkr/cadence-rust/actions/runs/45131823742 | EPYC 9V45 | **headline** |
| Matching 25 (rerun, dup) | https://github.com/auenkr/cadence-rust/actions/runs/45131818912 | not collected | cancelled |
| **Frontend 25** | https://github.com/auenkr/cadence-rust/actions/runs/45126895648 | EPYC 9V74 | **headline** |
| **All 25** | https://github.com/auenkr/cadence-rust/actions/runs/45126900469 | EPYC 9V74 | **headline** |
| All 40 | https://github.com/auenkr/cadence-rust/actions/runs/45126904498 | EPYC 9V74 | failed: Go arm generate failed (workflows timed out), report aborted |
| All 40 rerun 1 | https://github.com/auenkr/cadence-rust/actions/runs/45134289680 | EPYC 9V74 | failed, same Go-arm failure |
| All 40 rerun 2 | https://github.com/auenkr/cadence-rust/actions/runs/45134293804 | EPYC 9V74 | failed, same |
| All 40 rerun 3 | https://github.com/auenkr/cadence-rust/actions/runs/45140275256 | EPYC 9V74 | failed, same |
| All 40 rerun 5 | https://github.com/auenkr/cadence-rust/actions/runs/45140285170 | EPYC 9V74 | failed, same |
| **All 40** rerun 4 | https://github.com/auenkr/cadence-rust/actions/runs/45140280167 | Xeon 6975P-C | **headline** |

Headline runs concluded success with 0 errors in every arm. All 40 on EPYC 9V74 failed 5/5 because the Go arm could not sustain 40 starts/s on that 4-core runner (Go-side workflow timeouts in `00-go-generate`; Rust arm not reached in the failed report). The Xeon 6975P-C run passed (also what 20261121 used).

## Go vs Rust (headline runs, medians over 3 pairs)

| Lane | Arm | p50 ms | p99 ms | repl. CPU s | repl. RSS MiB | MySQL q/s | compl/s | errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| History 25 | Go | 41.9 | 115.5 | 24.32 | 154 | 2085 | 24.98 | 0 |
| | Rust | 30.4 | 103.2 | 13.50 | 135 | 1804 | 25.00 | 0 |
| | **Rust/Go** | 0.73 | **0.89** | **0.56** | **0.88** | 0.87 | | |
| Matching 25 | Go | 34.4 | 111.9 | 3.92 | 99 | 2082 | 24.98 | 0 |
| | Rust | 33.6 | 104.9 | 3.21 | 61 | 2080 | 24.98 | 0 |
| | **Rust/Go** | 0.98 | **0.94** | **0.82** | **0.62** | 1.00 | | |
| Frontend 25 | Go | 63.2 | 141.4 | 9.65 | 105 | 2065 | 24.98 | 0 |
| | Rust | 59.5 | 124.9 | 6.05 | 72 | 2067 | 24.98 | 0 |
| | **Rust/Go** | 0.94 | **0.88** | **0.63** | **0.69** | 1.00 | | |
| All 25 | Go | 54.7 | 116.1 | 39.97 | 364 | 2065 | 24.98 | 0 |
| | Rust | 33.5 | 102.6 | 25.33 | 274 | 1779 | 25.00 | 0 |
| | **Rust/Go** | 0.61 | **0.88** | **0.63** | **0.75** | 0.86 | | |
| All 40 | Go | 97.7 | 1157.7 | 44.77 | 378 | 3360 | 39.77 | 0 |
| | Rust | 75.0 | 106.9 | 23.85 | 281 | 2837 | 39.95 | 0 |
| | **Rust/Go** | 0.77 | **0.09** | **0.53** | **0.74** | 0.84 | | |

## Compared with 20261121 (616107912)

| Lane | p99 R/Go prev -> now | CPU R/Go prev -> now | RSS R/Go prev -> now | Rust p99 ms prev -> now |
|---|---|---|---|---|
| History 25 | 0.91 -> 0.89 | 0.63 -> 0.56 | 0.88 -> 0.88 | 104.3 -> 103.2 |
| Matching 25 | 0.96 -> 0.94 | 0.87 -> 0.82 | 0.62 -> 0.62 | 108.2 -> 104.9 |
| Frontend 25 | 1.03 -> 0.88 | 0.58 -> 0.63 | 0.69 -> 0.69 | 112.4 -> 124.9 |
| All 25 | 0.65 -> 0.88 | 0.57 -> 0.63 | 0.77 -> 0.75 | 104.0 -> 102.6 |
| All 40 | 0.19 -> 0.09 | 0.53 -> 0.53 | 0.75 -> 0.74 | 107.6 -> 106.9 |

## Takeaways

- Rust is at or better than Go on every p99, CPU and RSS ratio. No lane where Go is better on those; closest are Matching p50 (0.98) and Matching CPU (0.82), within noise. MySQL q/s is lower for Rust in History/All lanes (0.84-0.87), same as before (Rust issues fewer queries per workflow there; completions/s equal or higher).
- Rust p99 stays flat at ~103-125 ms in all lanes (Frontend Rust 124.9 ms is the highest, on a 9V74 where Go was 141 ms). Ratio changes versus 20261121 are mostly the Go arm / runner differences (e.g. All 25 Go p99 116 vs 159 earlier).
- All 40: Go arm collapses at 40 starts/s on EPYC 9V74 (5 of 5 runs failed) and is degraded on Xeon (p99 1158 ms, 39.77 compl/s); Rust held 106.9 ms and 39.95 compl/s.
- Runner caveat: EPYC 7763 drawn once (Matching) and rejected; the All 40 headline is a single Xeon 6975P-C run after 5 failures on 9V74.
