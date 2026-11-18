# Benchmark rerun 2026-11-19 (Rust vs Go)

Master SHA: `9592b349a5e64e66a886a99d2a9af4b37bb60671` (all runs: `source_ref` set to the full SHA, branch `bench-rerun-20261119` = master).
Baseline for comparison: CI 41767896292 (2026-11-04, head 316c28e4a2638e55cd52603a307fb6d7ffd09bd7, 5 reps).
Runner: GitHub `ubuntu-latest`, 4 vCPU AMD EPYC 7763. Repetitions: 3 alternating Go/Rust fresh-DB pairs in every lane.

| Lane | Workflow / inputs | Run | Result |
|---|---|---|---|
| Paired closed-loop (Rust Frontend+Matching vs Go; Go History) | frontend-matching-benchmark.yml `repetitions=3`, defaults 1000 req, 10 warmup, c=1,16,64, 1 KiB | https://github.com/auenkr/cadence-rust/actions/runs/44395943619 | success, artifact verification valid (3 pairs × 3 conc) |
| SDK open-loop suite, Rust Matching | `sdk_openloop sdk_suite service=matching sdk_repetitions=3` | https://github.com/auenkr/cadence-rust/actions/runs/44395950242 | success |
| SDK suite, Rust Frontend | `service=frontend` | https://github.com/auenkr/cadence-rust/actions/runs/44395956185 | success |
| SDK suite, Rust History | `service=history` | https://github.com/auenkr/cadence-rust/actions/runs/44395962649 | success |
| SDK suite, all Rust | `service=all` | https://github.com/auenkr/cadence-rust/actions/runs/44395970085 | success |

SDK suite defaults: MySQL `--innodb-flush-log-at-trx-commit=2 --sync-binlog=0` (relaxed commit), 4 history shards, capacity rates 25,100, one-activity workflow, 10 s warmup + 30 s measurement.
"All Go" is the Go arm of every lane (no separate run). The paired workflow has no MySQL-args input (uses its own fixed compose; same as 41767896292).
Binaries (paired run): Go `ce71c7bfa24acee1648edc8066fd0c23603a33243aa5ad2bb5e10ebd3e519980`, Rust `e5ad33b628c67cb65ddc45031df648a91a3b04a3e47e3ffff55c976a67b9ca6b`.

## 1. Paired closed-loop (Rust Frontend+Matching) vs 41767896292

Medians over runs. Throughput workflows/s; latency end-to-end ms; errors: 0 failed workflows in all runs (verifier valid).

| Conc | Run | Go thr | Rust thr | Go p50 | Rust p50 | Go p99 | Rust p99 |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | 41767896292 (5 reps) | 12.774 | 10.778 | 77.627 | 92.244 | 90.341 | 104.098 |
| 1 | **44395943619 (3 reps)** | 19.439 | 20.371 | 49.898 | 47.564 | 72.832 | 71.421 |
| 16 | 41767896292 (5 reps) | 33.575 | 29.821 | 484.454 | 541.928 | 671.856 | 769.264 |
| 16 | **44395943619 (3 reps)** | 35.104 | 35.620 | 467.248 | 460.624 | 705.940 | 679.662 |
| 64 | 41767896292 (5 reps) | 31.129 | 27.549 | 2294.921 | 2623.260 | 2884.152 | 3488.815 |
| 64 | **44395943619 (3 reps)** | 19.695 | 19.410 | 3351.995 | 3298.936 | 5570.488 | 6368.061 |

Resources, median per run (CPU seconds / peak cgroup memory MiB; harness reports cgroup memory, not RSS):

| Conc | Container | Go CPU | Rust CPU | Go mem | Rust mem | Old run Go→Rust CPU |
|---|---|---:|---:|---:|---:|---|
| 1 | frontend | 9.282 | 5.140 | 34.77 | 24.08 | 4.942 → 5.183 |
| 1 | history | 25.209 | 24.959 | 76.55 | 75.52 | 12.343 → 15.057 |
| 1 | matching | 5.505 | 4.357 | 33.06 | 26.43 | 2.926 → 4.723 |
| 1 | worker | 0.252 | 0.310 | 27.01 | 27.96 | 0.230 → 0.316 |
| 16 | frontend | 7.684 | 5.100 | 39.46 | 31.25 | 4.069 → 5.008 |
| 16 | history | 23.328 | 23.476 | 101.31 | 101.37 | 11.591 → 13.944 |
| 16 | matching | 5.295 | 5.284 | 35.69 | 32.23 | 2.687 → 5.045 |
| 16 | worker | 0.173 | 0.202 | 28.57 | 29.00 | 0.086 → 0.138 |
| 64 | frontend | 8.015 | 5.225 | 45.73 | 36.36 | 4.144 → 5.139 |
| 64 | history | 46.581 | 48.048 | 109.87 | 112.60 | 23.075 → 26.238 |
| 64 | matching | 5.731 | 5.494 | 35.98 | 33.40 | 2.780 → 5.148 |
| 64 | worker | 0.255 | 0.326 | 29.99 | 30.72 | 0.102 → 0.131 |

Takeaway: vs 41767896292 the Rust FE+Matching arm moved from 11–16% slower to parity/ahead: c=1 +4.8% throughput and lower p50/p99; c=16 +1.5% throughput, p99 −26 ms; c=64 −1.5% throughput, p50 −53 ms but p99 +798 ms (tail still worse at saturation, cf. memory/fe-soak-fix-deferred). Rust frontend CPU now 30–45% below Go (was +5–25% above); Rust matching CPU at/below Go (was ~+75%). Memory 3–11 MiB lower per Rust container. Absolute Go numbers differ from the old run (c=1 19.4 vs 12.8 wf/s, c=64 19.7 vs 31.1 wf/s) — runner variance between days; compare within-run deltas only.

## 2. SDK open-loop suite (each lane replaces one service; all-Rust replaces all four)

Medians over 3 runs per arm. completions/s, completion latency ms, errors = summed SDK/start errors over 3 runs, target = p99 ≤ 1000 ms & 0 errors.
"replaced-svc CPU s" = cgroup CPU seconds of the replaced container(s) during measurement; "peak RSS" = summed process RSS of those container(s) (all four cadence containers for all-Rust).


### Rust matching — run 44395950242

| Stage | Arm | completions/s | p50 ms | p99 ms | errors (sum) | target met | replaced-svc CPU s | replaced-svc peak RSS MiB | all cadence CPU s |
|---|---|---:|---:|---:|---:|---|---:|---:|---:|
| fixed 5/s | go (3) | 5.00 | 52.6 | 65.7 | 0 | 3/3 | 0.80 | 95 | 6.65 |
| fixed 5/s | rust (3) | 5.00 | 52.7 | 65.5 | 0 | 3/3 | 0.65 | 62 | 6.56 |
| capacity 25/s | go (3) | 24.97 | 71.2 | 139.7 | 0 | 3/3 | 2.89 | 97 | 25.06 |
| capacity 25/s | rust (3) | 24.97 | 71.1 | 126.7 | 0 | 3/3 | 2.53 | 61 | 24.86 |
| capacity 100/s (exit 124) | go (3) | 0.23 | 0.0 | 120002.1 | 8977 | 0/3 | 0.00 | 0 | 0.00 |
| capacity 100/s (exit 124) | rust (3) | 0.30 | 0.0 | 120002.1 | 5982 | 0/3 | 0.00 | 0 | 0.00 |
| recovery 5/s | go (3) | 5.00 | 52.7 | 3386.1 | 0 | 0/3 | 0.79 | 96 | 6.50 |
| recovery 5/s | rust (3) | 5.00 | 52.8 | 3372.1 | 0 | 0/3 | 0.64 | 61 | 6.48 |

### Rust frontend — run 44395956185

| Stage | Arm | completions/s | p50 ms | p99 ms | errors (sum) | target met | replaced-svc CPU s | replaced-svc peak RSS MiB | all cadence CPU s |
|---|---|---:|---:|---:|---:|---|---:|---:|---:|
| fixed 5/s | go (3) | 5.00 | 55.7 | 67.1 | 0 | 3/3 | 1.77 | 101 | 7.12 |
| fixed 5/s | rust (3) | 5.00 | 52.7 | 66.3 | 0 | 3/3 | 0.87 | 64 | 5.96 |
| capacity 25/s | go (3) | 24.97 | 75.2 | 169.4 | 0 | 3/3 | 5.91 | 103 | 26.26 |
| capacity 25/s | rust (3) | 24.97 | 70.4 | 153.5 | 0 | 3/3 | 3.72 | 82 | 23.50 |
| capacity 100/s (exit 124) | go (3) | 0.25 | 0.0 | 120002.1 | 5986 | 0/3 | 0.00 | 0 | 0.00 |
| capacity 100/s (exit 124) | rust (3) | 0.20 | 0.0 | 120002.0 | 5988 | 0/3 | 0.00 | 0 | 0.00 |
| recovery 5/s | go (3) | 5.00 | 54.7 | 3403.4 | 0 | 0/3 | 1.66 | 103 | 6.85 |
| recovery 5/s | rust (3) | 5.00 | 51.5 | 3371.0 | 0 | 0/3 | 0.84 | 66 | 5.77 |

### Rust history — run 44395962649

| Stage | Arm | completions/s | p50 ms | p99 ms | errors (sum) | target met | replaced-svc CPU s | replaced-svc peak RSS MiB | all cadence CPU s |
|---|---|---:|---:|---:|---:|---|---:|---:|---:|
| fixed 5/s | go (3) | 5.00 | 41.9 | 168.6 | 0 | 3/3 | 2.97 | 113 | 4.90 |
| fixed 5/s | rust (3) | 5.00 | 34.5 | 252.3 | 0 | 3/3 | 1.72 | 79 | 3.66 |
| capacity 25/s | go (3) | 25.00 | 43.5 | 119.7 | 0 | 3/3 | 12.43 | 143 | 19.20 |
| capacity 25/s | rust (3) | 24.97 | 32.1 | 417.6 | 0 | 3/3 | 6.87 | 118 | 14.00 |
| capacity 100/s (exit 1) | go (3) | 1.17 | 0.0 | 120001.9 | 8893 | 0/3 | 0.00 | 0 | 0.00 |
| capacity 100/s (exit 1) | rust (3) | 20.57 | 42191.1 | 59218.0 | 0 | 0/3 | 35.98 | 187 | 65.47 |
| recovery 5/s | go (3) | 5.00 | 41.5 | 3380.8 | 0 | 0/3 | 2.93 | 118 | 4.80 |
| recovery 5/s | rust (3) | 5.00 | 34.0 | 3640.1 | 0 | 0/3 | 1.67 | 84 | 3.49 |

### Rust all — run 44395970085

| Stage | Arm | completions/s | p50 ms | p99 ms | errors (sum) | target met | replaced-svc CPU s | replaced-svc peak RSS MiB | all cadence CPU s |
|---|---|---:|---:|---:|---:|---|---:|---:|---:|
| fixed 5/s | go (3) | 5.00 | 49.4 | 61.1 | 0 | 3/3 | 6.11 | 407 | 6.11 |
| fixed 5/s | rust (3) | 5.00 | 35.6 | 44.8 | 0 | 3/3 | 3.44 | 295 | 3.44 |
| capacity 25/s | go (3) | 24.97 | 66.7 | 126.6 | 0 | 3/3 | 23.18 | 435 | 23.18 |
| capacity 25/s | rust (3) | 25.00 | 34.6 | 194.0 | 0 | 3/3 | 13.26 | 339 | 13.26 |
| capacity 100/s (exit 1) | go (3) | 0.33 | 0.0 | 120002.2 | 8969 | 0/3 | 0.00 | 0 | 0.00 |
| capacity 100/s (exit 1) | rust (3) | 7.77 | 60397.6 | 62406.0 | 6827 | 0/3 | 0.00 | 0 | 0.00 |
| recovery 5/s | go (3) | 5.00 | 49.6 | 3379.7 | 0 | 0/3 | 6.04 | 409 | 6.04 |
| recovery 5/s | rust (3) | 5.00 | 35.5 | 3484.8 | 0 | 0/3 | 3.38 | 299 | 3.38 |

Notes:
- **capacity 100/s fails in both arms in all lanes** (generator timeouts exit 124/1, 0/3 target met) — over the 4-vCPU runner ceiling, see perf-capacity-limit.md. Not a Go/Rust difference; only History lane Rust (20.6/s, 0 errors) and all-Rust (7.8/s) completed partial work where Go collapsed to <1.2/s, so treat as overload behaviour, not throughput.
- **recovery** stage target is never met by design (3 s MySQL drop forces p99 ≈ 3.4 s); both arms recover with 0 errors; Rust History p99 3640 vs Go 3381 ms.
- **Regression to watch:** Rust History p99 at 5/s 252 vs 169 ms and at 25/s 418 vs 120 ms (p50 is 18–26% better); all-Rust 25/s p99 194 vs 127 ms. History tail latency is the remaining gap.
- All-Rust at 25/s uses 43% less cadence CPU (13.3 vs 23.2 s) and 22% less RSS (339 vs 435 MiB); p50 48% lower.
- Previous run 41767896292 had no SDK open-loop lanes (paired harness only), so section 2 has no older baseline in that run; earlier per-lane numbers are in perf-history-dispatch.md / perf-matching-*.md.

Artifacts were downloaded to /mnt/storage/service/tmp and deleted after this report (CI artifacts expire after 3 days).
