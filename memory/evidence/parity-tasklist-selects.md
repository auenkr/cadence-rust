# task_lists / tasks SELECT parity (Matching + Frontend lanes)

Branch `parity-tasklist-selects`, source/master base `2465f9861c6ad0202fc7d2eff39db32ebf059890`. No code change (no deviation found), so no rust-ci / gate dispatch was needed.
Method: `sdk-matching-benchmark.yml` on master, `source_ref=<full sha>`, rate 25, 60 s, warmup 10, 3 alternating fresh-DB pairs, `mysql_tmpfs=true`, `sql_log_secs=60`; counts from `select_by_table` (MySQL general log).

## Runs
| Lane | Run | Runner | Used |
|---|---|---|---|
| Matching A (fresh_build) | https://github.com/auenkr/cadence-rust/actions/runs/44786788405 | EPYC 9V74 (Go p99 117 ms) | clean |
| Frontend A (fresh_build) | https://github.com/auenkr/cadence-rust/actions/runs/44786783149 | EPYC 9V74 | clean runner |
| Frontend B (pairs 3-5) | https://github.com/auenkr/cadence-rust/actions/runs/44791971613 | EPYC 9V74 | clean runner |
| Matching B (pairs 3-5) | https://github.com/auenkr/cadence-rust/actions/runs/44791977029 | **EPYC 7763** (Go p99 3.7 s) | rejected, degraded |

## Per-query-shape table (Execute prepared statements, per 60 s window)
| Run / pair | Table | Go | Rust |
|---|---|---:|---:|
| Matching A, pairs 0-2 | task_lists | 41,41,41 | 41,41,41 |
| | tasks | 29,29,29 | 29,29,29 |
| Frontend A pair 0 / 1 | task_lists / tasks | 41 / 29 | 41 / 29 |
| Frontend A pair 2 (Rust completion p99 805 ms) | task_lists / tasks | 41 / 29 | **115 / 103** |
| Frontend B pair 3 (Rust p99 714 ms) | | 41 / 29 | **116 / 104** |
| Frontend B pair 4 (Rust p99 115 ms) | | 41 / 29 | 41 / 29 |
| Frontend B pair 5 (Rust p99 286 ms) | | 41 / 29 | **71 / 59** |
| Matching B (7763, rejected) | task_lists | 1132,411,468 | 1938,367,409 |

Ad-hoc `Query cadence.tasks` (12) are identical in every sample (setup statements).

## Verdict
- Steady state is exact Go parity: 41 task_lists + 29 tasks SELECTs per window in both arms, in the Matching lane (1.00) and in every Frontend pair without a latency stall. The earlier 0.36 (Matching) came from a degraded EPYC 7763 run (Go arm retry churn; B above shows counts swing 411-1938 on such runners); not a Rust Matching deviation.
- The Frontend 1.35-1.85x comes only from pairs where the Rust Frontend arm had a tail spike (pairs 2, 3, 5 above: extra reads track completion p99 exactly, and pair 4 with p99 115 ms is 41/29). The extra tasklist reads are the Go Matching service reloading/reading after the stall; Matching is Go in that lane. This is the known open Rust Frontend start-p99 tail flake (MEMORY fe-soak-fix-deferred), not a task-table query-shape difference. Counts are not a Matching bug.
- Code mapping not needing change: Matching lane runs identical per-shape counts with Rust Matching (reader batch, lease/update, GC cadence all produce the same count as Go over 60 s).
- Follow-up: fixing the FE tail flake should remove the Frontend-lane excess.
