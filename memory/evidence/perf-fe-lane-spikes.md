# Frontend lane p99 spikes (perf-fe-lane-spikes, 2026-11-20/21)

Branch `perf-fe-lane-spikes`, source/head `66e59be89a9c5292d08ea5554ced090de16b2018` (only `Benchmark/run.py` changed; no Rust code change, so rust-ci and frontend-mixed-gate were not rerun).

## Diagnosis (run 44768173605, samples + container logs)
- Slow starts and completions occur only in the first ~12 s after an arm's containers start. Rust pair 00: 51 measured slow completions (>400 ms), all within 16:23:06-16:23:09, plus 101 warmup starts >300 ms (max 5.2 s). Pair 01: 82 slow, all 16:24:54-16:24:58. Rust pair 02: none.
- Go shows the same burst in pair 02 (87 warmup starts >300 ms; measured p99 at 16:28:34-38). Go pairs 00/01 were clean, so it is arm-agnostic, not Rust-Frontend specific.
- The burst coincides with History's first periodic shard-acquire cycle (60 s after start) and the Go system Worker starting its system-domain workers (scheduler/batcher/async consumers) ~60 s after process start. When the benchmark's warmup/measure window overlaps that, decision tasks stall for hundreds of ms to seconds. Whether the window overlaps depends on runner speed, hence the flakiness (worst on EPYC 7763).
- Not the cause: no Frontend log lines in the window, no config/domain refresh stall visible, 70 min soak was 9 ms p99.
- I did not identify which individual RPC stalls inside the burst, and did not separately explain the 1.35-1.40x Frontend `tasks`/`task_lists` SELECTs of that run. In the clean reruns below they are 0.98-1.00x Go (task_lists 43/43), consistent with those extra reads being tied to the same startup burst (matching falling back to persistence) rather than a steady-state difference.

## Fix (harness, parity-neutral)
`Benchmark/run.py`: after starting the Go system Worker, wait until its log shows the batcher worker, `Started Activity Worker` and `Refreshed consumers` (+5 s) before starting the SDK worker and warmup.

## Reruns (service=frontend, 25/s, 3 pairs, 10 s warmup + 60 s, mysql_tmpfs, sql_log_secs=60, fresh_build)
| Run | Runner | Go p99 ms | Rust p99 ms | Used |
|---|---|---:|---:|---|
| https://github.com/auenkr/cadence-rust/actions/runs/44783391659 | EPYC 9V74 | 115.5 | 113.5 | clean |
| https://github.com/auenkr/cadence-rust/actions/runs/44789861132 | EPYC 9V45 | 158.9 | 156.9 | clean |
| https://github.com/auenkr/cadence-rust/actions/runs/44783378514 | EPYC 7763 | 4690.5 | 3217.3 | no (both arms degraded; Rust/Go 0.69) |

Before (44768173605, Xeon 8573C, no fix): Go 189.6, Rust 805.8 (4.25x). After on clean runners: Rust/Go p99 0.98 and 0.99; p50 0.81 and 1.01; replaced CPU 0.59/0.70; MySQL q/s 1.00.
