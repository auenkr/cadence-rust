# Benchmark rerun on master 6939d2ab4 — 2026-11-24

Branch `benchmark-rerun-fa85672`, head SHA of the dispatched runs: `4fe81943bbcd746169682466a2782d1875e5c409` (= origin/master 6939d2ab4 merged with the harness changes: `rust_services` input, machine-spec capture). No Rust/Go service code changed. Same workload as 41767896292: 5 alternating paired repetitions x 3 concurrency levels = 30 runs x 1000 workflows, 10 warmup (prior run used 100), 1024-byte payload, Go SDK over TChannel, sticky off, fresh MySQL per arm, 1 CPU/1400 MiB per service, MySQL 1.5 CPU/1800 MiB.

## Runs
| Lane | Run | Result |
|---|---|---|
| (a) Go baseline + (b) Rust FE+Matching, Go History/Worker | https://github.com/auenkr/cadence-rust/actions/runs/45273706507 | success; strict artifact verification exit 0; 30,000 workflows, 0 failures |
| (a') Go baseline + (c) all-Rust FE+Matching+History, Go Worker | https://github.com/auenkr/cadence-rust/actions/runs/45273717881 | **FAILED**: Rust arm never completed a workflow (see below) |

Lane (a) is the Go arm inside each paired run (the Go arm is identical in both dispatches).

## Machine spec (per run; from `out/machine-spec.md`)
| | Run 45273706507 | Run 45273717881 |
|---|---|---|
| CPU | AMD EPYC 9V74 80-Core | AMD EPYC 7763 64-Core |
| cores/threads, nproc | 2 cores x 2 threads, 1 socket; nproc 4 | same |
| RAM | 15 GiB | 15 GiB |
| Disk (/) | 145G, 72G used, 74G free | same |
| Kernel / OS | 7.0.0-1020-azure / Ubuntu 26.04.1 LTS | same |
| Runner image | ubuntu26 20261116.202.1 (GitHub Actions 1000007920) | same image (GitHub Actions 1000007921) |
| Container limits | 4 services: 1 CPU, 1400 MiB; MySQL 1.5 CPU, 1800 MiB | same |
| DB | mysql:8.0 sha256:7dcddc01f13b… | same |

**Hardware identity:** within each run the Go and Rust arms ran on the same VM, so each Go-vs-Rust pair is a like-for-like comparison. The two *runs* landed on different CPU models (EPYC 9V74 vs 7763), so absolute numbers must not be compared across the two runs. Go-arm c1 of the failed run (22.70 wf/s) vs run 1 Go median 21.1 shows the offset is small, and the previous benchmark 41767896292 ran on an unrecorded runner, so absolute numbers vs it are indicative only.

## Lane (a) Go vs lane (b) Rust FE+Matching (run 45273706507; five-run medians)
| Conc | Go wf/s | Rust wf/s | Δ | Go p50/p95/p99 ms | Rust p50/p95/p99 ms |
|---:|---:|---:|---:|---|---|
| 1 | 21.11 | 21.44 | +1.58% | 36.6 / 100.7 / 232.5 | 36.2 / 105.1 / 174.9 |
| 16 | 42.90 | 43.25 | +0.81% | 356.2 / 662.5 / 905.6 | 346.1 / 582.8 / 715.8 |
| 64 | 20.83 | 20.30 | -2.52% | 3179 / 5086 / 5774 | 3116 / 5482 / 6369 |

Resources at c16 (median per run; CPU = cgroup CPU seconds for 1000 workflows; memory = peak sampled cgroup memory incl. page cache, not RSS):
| Container | Go CPU s | Rust CPU s | Go peak MiB | Rust peak MiB |
|---|---:|---:|---:|---:|
| frontend | 5.59 | 4.71 | 39.4 | 32.1 |
| matching | 3.64 | 3.58 | 35.5 | 27.5 |
| history (Go in both) | 16.14 | 16.65 | 97.4 | 95.7 |
| worker (Go in both) | 0.12 | 0.14 | 30.2 | 28.7 |
Full c1/c64 tables: artifact `frontend-matching-paired-benchmark` of run 45273706507 (`paired-benchmark/comparison-c*.md/json/html`; retention 3 days).

Binary SHA256: Go `6bc62f8f36662cb76e695ef7b578c06ef9487b468bae51b318bb252e514e7d3a`; Rust `2a9af5c319edd3e7806a5fc1c62cd12400ff29e11287614a02e661d6cf1c5187`.

## Lane (c) all-Rust FE+Matching+History, Go Worker — FAILED
Run 45273717881: the Go arm of rep 0 passed all three concurrencies (c1 22.70, c16 42.53, c64 20.89 wf/s, 0 failures). In the Rust arm (rep 0), the `cadence-benchmark` driver's first warmup workflow at c1 hit `warmup failed: code:deadline-exceeded message:timeout` after 20 minutes (`run-00-rust-c1.log`); the harness aborted, so there are no all-Rust throughput/latency/CPU/RSS numbers. Evidence in the run artifact (`paired-benchmark/final-compose-logs.log`): Rust History logs "listening on 0.0.0.0:7934" and then nothing further; Rust Matching ringpop JOIN reached only frontend and worker (`failures=["history:7934","matching:7935"]`), and the Go Worker's system-workflow starts timed out (`Failed to start workflow … deadline-exceeded`). So the all-Rust cluster (Rust FE+Matching+History in this 4-container compose, 4 shards, MySQL) starts but does not serve workflows. This is a service-level finding, not harness: the Rust History mixed gates pass with Go FE/Matching, and Rust FE/Matching pass with Go History, but Rust FE + Rust Matching + Rust History together was not covered. Needs investigation (likely ringpop membership/shard ownership between Rust History and Rust Matching/Frontend) before lane (c) can be benchmarked; then rerun with `-f rust_services=frontend,matching,history`.

## Comparison with 41767896292 (2026-11-04, source 316c28e4a)
| Conc | Old Go / Rust wf/s | Old Rust Δ | New Go / Rust wf/s | New Rust Δ |
|---:|---|---:|---|---:|
| 1 | 12.77 / 10.78 | -15.6% | 21.11 / 21.44 | +1.6% |
| 16 | 33.57 / 29.82 | -11.2% | 42.90 / 43.25 | +0.8% |
| 64 | 31.13 / 27.55 | -11.5% | 20.83 / 20.30 | -2.5% |
Old c16 p95: 619 / 709 ms; new 662 / 583 ms. Old c16 CPU (FE/Matching Go→Rust): 4.07→5.01 s / 2.69→5.05 s; new 5.59→4.71 s / 3.64→3.58 s. Memory: Rust still lower (-7 / -8 MiB).

## Conclusion
- Rust Frontend+Matching is now at throughput parity with Go (within ±3% at all three concurrencies) versus -11…-16% before; p50/p95 at c1 and c16 are equal or better, p99 better at c1/c16, worse at c64 (p95 +8%, p99 +10%) where the run is saturated.
- Rust now uses less or equal CPU than Go for both services (Frontend -16%, Matching -2%) versus up to +88% for Matching earlier, and less peak memory.
- Caveats: different runner CPU than the old run, 10 instead of 100 warmups, c64 differences are in the noise range of 5 reps; absolute numbers are not comparable to the old run.
- All-Rust cluster does not serve workflows (lane c); this must be fixed before an all-Rust benchmark. Raw artifacts for both runs are available for 3 days.


## Lane (c) all-Rust FE+Matching+History, Go Worker — PASSED on rerun (run 45291978847)
Run https://github.com/auenkr/cadence-rust/actions/runs/45291978847, head `500dec50a9ccc2660ad6a32ecfa8aa12e262dd74`, `-f rust_services=frontend,matching,history`, other inputs default (same as 45273706507). Conclusion success; strict `verify_artifacts.py` exit 0 (30,000 workflows, 0 failures). Rust binary sha256 cadence-server `3d39074f89e0…`, cadence-history `f549c3c9eb83…`. Earlier same-config run 45283282001 (head 6139aca03) also succeeded with identical numbers; its verify step was skipped by the workflow condition.

**Machine:** AMD EPYC 7763 64-Core, nproc 4, 15 GiB, ubuntu26 20261116.202.1. It does NOT match the lane (a)/(b) run 45273706507 (EPYC 9V74); it matches the failed run 45273717881 (7763). Compare Go vs Rust only inside this run (same VM). Limits: 1 CPU/1400 MiB per service, MySQL 1.5 CPU/1800 MiB.

**Root cause of the earlier failure (harness/packaging, not a service bug):** the Rust image shipped only `cadence-server`, which has no History service. `/start-cadence.sh` runs `cadence-server start --services=history`, which silently ran the matching/frontend launcher on the History port: it listened on 7934, did nothing else, and never joined ringpop. The History gates use the separate `cadence-history` binary. Fix: `rust/Dockerfile` builds and installs `cadence-history`; `rust/docker/cadence-server-dispatch.sh` (installed as `cadence-server`, real one `cadence-server-real`) execs `cadence-history` when `--services` includes history. `verify_artifacts.py` now honors `host.rust_services` and the workflow always runs strict verification and records both binary hashes.

### Median of 5 paired repetitions (Go vs all-Rust)
| Conc | Go wf/s | Rust wf/s | Go p50/p95/p99 ms | Rust p50/p95/p99 ms |
|---:|---:|---:|---|---|
| 1 | 22.67 | 25.17 | 42.6 / 53.0 / 75.0 | 29.2 / 99.7 / 107.1 |
| 16 | 41.90 | 60.62 | 386.7 / 509.8 / 563.9 | 265.9 / 354.4 / 395.0 |
| 64 | 20.67 | 57.08 | 3484 / 4593 / 5588 | 1281 / 1600 / 1719 |

### Median per-run CPU seconds / peak cgroup memory MiB (Go -> Rust; cgroup, includes cache, not RSS)
| Container | c1 CPU | c16 CPU | c64 CPU | c1 mem | c16 mem | c64 mem |
|---|---|---|---|---|---|---|
| frontend | 7.72 -> 4.58 | 6.57 -> 4.81 | 6.87 -> 4.87 | 34.9 -> 27.4 | 39.2 -> 31.0 | 45.9 -> 36.8 |
| history | 22.38 -> 9.15 | 20.22 -> 13.02 | 46.02 -> 14.04 | 77.4 -> 89.2 | 98.0 -> 114.8 | 111.9 -> 137.3 |
| matching | 4.66 -> 3.02 | 4.45 -> 4.10 | 4.92 -> 4.09 | 32.7 -> 22.5 | 35.5 -> 25.2 | 36.1 -> 28.1 |
| worker (Go both) | 0.20 -> 0.28 | 0.14 -> 0.10 | 0.21 -> 0.17 | 27.4 -> 28.1 | 29.4 -> 28.4 | 30.0 -> 28.6 |

Rust History uses ~12-25 MiB more cgroup memory than Go History but 2-3x less CPU. Rust p95/p99 at c1 is worse than Go (99.7/107 vs 53/75 ms).
