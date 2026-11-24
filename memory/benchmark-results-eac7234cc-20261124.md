# Benchmark: Rust (Frontend+Matching+History) vs Go — master eac7234cc (2026-11-24)

Run: https://github.com/auenkr/cadence-rust/actions/runs/45408864439 (success). Paired, same runner, 5 interleaved reps, medians; 1000 workflows/rep, 1 KiB payload; Go Worker in both arms.
Includes: fat LTO, shared MySQL pool + maxConns split, History cache index, shard write-lock ordering, TCP_NODELAY, memory tuning, SQL blob move, borrowed append encode.

## Machine spec
- AMD EPYC 9V45 96-Core, 4 vCPU (2c x 2t), 15 GiB RAM, Ubuntu 26.04.1, kernel 7.0.0-1020-azure, runner image ubuntu26 20261116.202.1
- Each Cadence service: 1 CPU, 1400 MiB; mysql:8.0: 1.5 CPU, 1800 MiB

## Throughput and latency
| Concurrency | Go wf/s | Rust wf/s | Speedup | Go p50/p95/p99 ms | Rust p50/p95/p99 ms |
|---:|---:|---:|---:|---|---|
| 1 | 24.3 | 25.2 | 1.04x | 34 / 92 / 109 | 24 / 101 / 110 |
| 16 | 52.9 | 77.4 | 1.46x | 308 / 416 / 451 | 205 / 299 / 332 |
| 64 | 25.2 | 66.9 | 2.65x | 2853 / 3796 / 4021 | 955 / 1315 / 1451 |

## CPU seconds (Go -> Rust) and peak memory delta (Rust - Go)
| Concurrency | Frontend | Matching | History | History mem |
|---:|---|---|---|---:|
| 1 | 5.6 -> 2.9 | 3.4 -> 2.0 | 14.8 -> 6.1 (-59%) | +10.4 MiB |
| 16 | 4.9 -> 3.1 | 3.1 -> 2.6 | 14.3 -> 9.1 (-36%) | +1.5 MiB |
| 64 | 5.2 -> 3.5 | 3.6 -> 3.0 | 37.4 -> 10.8 (-71%) | +12.1 MiB |
Frontend and Matching peak memory are 8-11 MiB lower in Rust at every level.

## Notes
- c1 is bounded by the per-shard transfer-queue limiter (20 rps), shared with Go (perf-transfer-queue-latency-20261124.md); Rust p95 slightly higher there.
- Cross-run comparison is noisy (runner CPU varies); previous run 45366356133 was on EPYC 7763. Same-runner A/B of today's perf merges: three-arm run 45446890614 (control abc4942b2), pending.
