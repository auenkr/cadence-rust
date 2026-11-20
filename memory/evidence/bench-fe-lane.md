# Frontend benchmark lane: why it looked ~2x MySQL-loaded (2026-11-20)

Branch `bench-fe-lane`. Source measured: master `a23b6bc2b` (full SHA in run inputs), service=frontend, 25/s, 3 pairs, 10 s warmup + 60 s, mysql_tmpfs=true, sql_log_secs=60, fresh_build=true.

## Root cause: runner hardware, not the lane or the Rust Frontend
- The harness is the same for every lane (`Benchmark/run.py`): the same compose file, cpusets, dynamic config, DB visibility and host count. The only difference between lanes is which container runs the Rust image. The Go arm is all-Go in every lane, so it is the same deployment everywhere.
- The `hardware` field in each run's manifest gives the runner CPU (lscpu):
  | Run | Lane | CPU | Go p99 | MySQL CPU s (go) |
  |---|---|---|---:|---:|
  | 44625313998 | history | EPYC 9V45 | 111 | 27.0 |
  | 44625319261 | matching (degraded) | **EPYC 7763** | 1962 | 54.4 |
  | 44632011164 | matching rerun | Xeon 6975P | 113 | 26.3 |
  | 44625325248 | frontend | **EPYC 7763** | 2306 | 53.6 |
  | 44632014695 | frontend rerun | **EPYC 7763** | 1466 | 53.3 |
  | 44625330722 | all | EPYC 9V74 | 134 | 38.7 |
- Every degraded run used an EPYC 7763 runner, and degraded Matching runs show the same ~2x MySQL CPU. Clean Frontend runs show the same MySQL q/s (~2100) and per-table SELECT counts as Go, so the MySQL work is the same and only the CPU cost per query differs. No Rust Frontend parity fix is needed, so the Rust code was not changed and rust-ci/the gate were not rerun.
- Harness change: `Benchmark/agg.py` now prints `Runner CPU:` for each run and warns on EPYC 7763. Run 44656254852 (9V74) was partly degraded too (MySQL 38.9 s, Go p99 1295 ms), so check MySQL CPU before you trust a p99 ratio.

## Results with a clean Go arm (EPYC 9V45)
| Run | Arm | p50 ms | p99 ms | Frontend CPU s | Frontend RSS MiB | cadence CPU s | MySQL CPU s |
|---|---|---:|---:|---:|---:|---:|---:|
| https://github.com/auenkr/cadence-rust/actions/runs/44656247009 | go | 34.4 | 112.9 | 8.02 | 105 | 32.72 | 26.40 |
| | rust | 33.2 | 111.3 | 4.37 | 76 | 28.90 | 25.85 |
| https://github.com/auenkr/cadence-rust/actions/runs/44656251106 | go | 40.2 | 115.8 | 9.31 | 105 | 37.60 | 28.44 |
| | rust | 35.9 | 111.8 | 4.97 | 71 | 31.38 | 26.63 |

Rust/Go: p50 0.97 / 0.89, p99 0.99 / 0.97, Frontend CPU 0.55 / 0.53, RSS 0.72 / 0.68, MySQL q/s 1.00. SELECTs per table match Go (1.00) except cluster_config at 0.75 and domains/domain_metadata at 0.6–0.75 (Rust caches more). Not used: https://github.com/auenkr/cadence-rust/actions/runs/44656254852 (degraded 9V74 runner: p99 1295 vs 273 ms, ratios as in table above; CPU 0.62, RSS 0.68).
