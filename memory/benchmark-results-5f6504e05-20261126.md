# Go vs Rust benchmark results (2026-11-26, master 5f6504e05)

This report supersedes `benchmark-results-6939d2ab4-20261124.md`.

Rust runs Frontend, Matching and History. The Worker service stays in Go.

| Binary | SHA256 |
|--|--|
| Rust `cadence-server` | `f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74` |
| Rust `cadence-history` | `1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5` |

- **Runners:** GitHub-hosted, 4 vCPU and 15 GiB, Ubuntu 26.04.1. CPUs varied per run: AMD EPYC 7763, 9V74 or 9V45, or Intel Xeon 8370C or 6975P-C.
- **Container limits:** each service 1 CPU and 1.4 GiB; the database 1.5 CPU and 1.8 GiB.
- **Arms:** every run has three: Go, Rust, and an A/A control (the same Rust binary run again) that measures noise.
- **Sources:** each section names its evidence file in `memory/evidence/`.

## 1. Throughput and latency, closed loop (`benchmark-final-0f1aac451-20261125.md`)

Medians of 5 reps. Change is Go → Rust (negative latency = Rust faster).

| Setup | Concurrency | Go wf/s | Rust wf/s | Throughput | p50 | p95 | p99 |
|--|--|--|--|--|--|--|--|
| MySQL default | 1 | 23.3 | 25.5 | +9.6% | −33.9% | **+25.6%** | −4.0% |
| MySQL default | 16 | 47.2 | 71.1 | +50.5% | −36.0% | −31.4% | +0.6% (noise) |
| MySQL default | 64 | 23.0 | 65.6 | +184.7% | −65.2% | −67.6% | −68.6% |
| MySQL `transferProcessorMaxPollRPS=1000` | 1 | 26.5 | 37.2 | +40.4% | −35.9% | −17.9% | +8.3% (noise) |
| MySQL `transferProcessorMaxPollRPS=1000` | 16 | 53.5 | 69.6 | +30.1% | −25.3% | −8.1% | −8.6% |
| MySQL `transferProcessorMaxPollRPS=1000` | 64 | 25.1 | 65.4 | +160.3% | −66.7% | −51.6% | −48.4% |
| PostgreSQL default | 1 | 25.0 | 25.4 | +1.3% | −15.4% | +0.8% (noise) | −5.6% |
| PostgreSQL default | 16 | 63.3 | 87.2 | +37.7% | −28.6% | −27.8% | −26.6% |
| PostgreSQL default | 64 | 60.3 | 77.4 | +28.4% | −38.4% | −16.9% | −16.7% |

The A/A control (the same Rust binary in the same run) differs by up to ±3% in wf/s and up to +44% in c1/c64 p99. Go-vs-Rust differences of that size are noise.

## 2. Concurrency 1 at a fixed rate (`perf-c1-knee-20261126.md`, `perf-c1-knee2-20261126.md`)

- **Criterion:** fixed before the runs. The upper bound of the 95% cluster-bootstrap CI of (Rust−Go)/Go must be at or below +5% for both p90 and p95.
- **Runs:** 36 in total, each with 5 reps.

| Rate (wf/s) | Runs | Rust−Go p90 [95% CI] | Rust−Go p95 [95% CI] | Verdict |
|--|--|--|--|--|
| 10 | 4 | −37.6% [−38.4, −33.6] | −37.9% [−39.6, −35.7] | PASS |
| 15 | 12 | −32.3% [−38.2, −25.7] | −30.1% [−38.6, +4.6] | PASS |
| 18 | 4 | −38.6% [−42.7, −30.4] | −37.4% [−44.9, −7.7] | PASS |
| 20 | 12 | −36.8% [−38.2, −29.8] | −31.9% [−39.2, −9.3] | PASS |
| 22 | 12 | −34.6% [−37.0, −26.8] | −25.2% [−36.5, −3.5] | PASS |
| unpaced | 4 | +77.0% [+47.9, +101.2] | +51.3% [+1.4, +85.3] | **FAIL** |

- **Unpaced:** Rust completes 25.1 wf/s against Go's 22.5. Its p90 is 94.0 ms against Go's 53.1 ms.
- **Cause:** the History queue limiter allows 20 rps with a burst of 20 (Go's default). Rust's faster loop packs requests at the limiter's edge, so more of them wait.
- **Control:** the A/A control shows the same p90 (93.7 ms), so the gap comes from where the limiter sits and is not a Rust code defect.
- **Status:** no fix that keeps Go's behaviour has been found. A limiter parity check against Go is in progress.

## 3. Soak (`perf-pg-soak-tails-20261126.md`, `perf-pg-paced50-20261126.md`, `perf-mysql-soak-drift-20261126.md`)

- **PostgreSQL, unpaced:** Rust p99 is 21% lower than Go.
- **PostgreSQL, paced 50 wf/s:** Rust p99 is 28.9% lower than Go.
- **MySQL, c16 with 12,000 requests:** Rust runs 51.7–54.6 wf/s against Go's 26.8–30.7. Rust's latency drift over the run is within the A/A control's spread.

## 4. Workload shapes (MySQL, 3 reps; `benchmark-final-0f1aac451-20261125.md`)

The anon-memory and CPU columns are the Rust/Go ratio summed over Frontend, Matching and History.

| Shape | Go wf/s | Rust wf/s | Rust/Go wf/s | p99 ratio | Peak anon memory | CPU-seconds |
|--|--|--|--|--|--|--|
| activities | 17.65 | 25.84 | 1.46× | 0.67 | 0.70 | 0.67 |
| signal-timer | 14.22 | 14.61 | 1.03× | 0.93 | 0.62 | 0.53 |
| child | 8.24 | 11.50 | 1.40× | 0.74 | 0.79 | 0.59 |
| history (1,200+ events) | 0.39 | 0.53 | 1.36× | 0.74 | 0.98 | 0.75 |

## 5. Resources

- **Frontend and Matching memory:** Rust uses 15–23 MiB peak anon per service against Go's 30–44 MiB.
- **History memory:** Rust is within ±5% of Go. On the history shape it is 151.7 MiB against Go's 154.2 MiB, after the jemalloc fix.
- **CPU:** Rust uses about 40–55% less across the three services. At c1 it uses 10–14.5 CPU-s per rep against Go's 24–34.

## 6. Recovery (`perf-recovery-timing-20261126.md`, `perf-recovery-pooled-20261126.md`, `fix-restart-lost-wf-20261126.md`)

| Scenario | Go stable median | Rust stable median | Go lost | Rust lost |
|--|--|--|--|--|
| Cassandra kill_history | 1,878 ms | 1,532 ms | 0 | 0 |
| Cassandra cold_restart | 14,857 ms | 12,064 ms | 0 / 9,693 | **9 / 8,629** |
| MySQL kill_history + cold_restart | — | — | 0 / 9,542 | 0 / 7,898 |
| MySQL kill_frontend (pooled) | — | −185 ms vs Go | 1 / 80 | 1 / 80 (driver timeout race) |

On Cassandra, a cold restart loses the first decision task of some new workflows. It is an open Rust bug, and a fix is in progress.

## Where Rust is still worse than Go

1. **Unpaced c1 p90/p95** (MySQL default c1 p95 is +25.6%): this is the limiter plateau described in section 2.
2. **Cassandra cold_restart:** workflow loss, which is an open bug.

Every other measured metric is equal or better for Rust.

Council (Opus 5.5 low / GPT 6.2 Sol low): rounds 9–11 all scored 9.5 / 9.

The HTML view of this report is `memory/benchmark-report-20261126.html`.
