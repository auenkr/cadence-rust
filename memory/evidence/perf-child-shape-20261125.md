# Child-shape Go vs Rust (2026-11-25)

Follow-up to `perf-workload-shapes-20261125.md`, where `child` showed Rust at 0.84x Go wf/s (3 reps, large A/A spread).

Conclusion: **the gap is not real. No Rust code change was made.** Over 9 reps Rust is faster than Go on every step. The earlier gap came from noise and from a slow Go arm.

## Setup
- Workflow `frontend-matching-benchmark.yml`, ref `perf-child-shape` (head 4219d987ba, same tree as master), `shape=child`, `rust_services=frontend,matching,history`, `control_ref=origin/master`, `sdk_openloop=false`, 300 requests, 3 reps, c16, payload 256, `trace_decision=true`. Three dispatches give 9 reps per arm.
- Runs: [45649723786](https://github.com/auenkr/cadence-rust/actions/runs/45649723786), [45649739671](https://github.com/auenkr/cadence-rust/actions/runs/45649739671), [45649755820](https://github.com/auenkr/cadence-rust/actions/runs/45649755820). All succeeded.
- A `profile=1` run, [45649782161](https://github.com/auenkr/cadence-rust/actions/runs/45649782161), failed with `startup_failure` (not retried). No profile was captured, because the data shows no Rust-specific slowness to profile.
- Hardware (GitHub `ubuntu-latest`, ubuntu26 20261116.202.1): AMD EPYC 9V74 (the earlier evidence run drew an Intel Xeon 8573C, so the CPU model varies by dispatch), 2 cores x 2 threads = 4 vCPU, 15 GiB RAM, kernel 7.0.0-1020-azure. Services capped at 1 CPU and 1400 MiB each, MySQL 8.0 at 1.5 CPU and 1800 MiB.

## Throughput (9 reps, medians; per-rep wf/s in brackets)
| Arm | wf/s | p50 ms | p99 ms | per-rep wf/s |
|---|---|---|---|---|
| go | 8.62 | 1818 | 2783 | 8.6 9.0 9.0 8.9 6.9 8.9 8.3 7.7 7.5 |
| control (master) | 11.67 | 1363 | 1995 | 11.8 11.7 11.9 11.8 12.4 8.9 10.6 11.1 10.0 |
| rust | 11.11 | 1359 | 2108 | 11.8 11.7 12.0 9.8 12.2 10.7 11.1 10.5 9.8 |

Rust/Go: 1.29x wf/s, 0.75x p50. A/A (rust vs control, identical binaries): 0.95x wf/s, 1.00x p50, so the noise is about 5%. The Rust lead over Go is far outside it. Go is slow in this sample (8.6 wf/s versus 14 in the earlier run), so Go's own run-to-run variance is also large, and the earlier 0.84x was one such draw.

## Per-workflow gaps (ms, median; fast = 20 random fast workflows per rep, slow = slowest 5%)
Source: `result-*-tail-history.json`. The gaps are the generic decision-level gaps (dec1 = parent's decision that schedules the children, dec2 = decision after the children complete).

| Set | Gap | go | control | rust |
|---|---|---|---|---|
| fast | dec1 scheduled to started | 339 | 274 | 244 |
| fast | dec1 started to completed | 19 | 12 | 13 |
| fast | dec2 scheduled to started (children done, parent resumes) | 473 | 287 | 287 |
| fast | dec2 started to completed | 18 | 12 | 12 |
| fast | dec2 completed to workflow completed | 894 | 702 | 732 |
| fast | total history span | 1833 | 1326 | 1385 |
| slow | dec1 scheduled to started | 495 | 347 | 386 |
| slow | dec2 scheduled to started | 625 | 372 | 392 |
| slow | dec2 completed to workflow completed | 1347 | 1085 | 1076 |
| slow | total history span | 2612 | 1948 | 1960 |

Rust is equal to or better than Go at every step. The child-initiated, child-started and child-completed events sit inside the "dec2 completed to workflow completed" window (children run in parallel, so that window is dominated by child execution plus the parent resume), and there Rust is also below Go (732 vs 894 ms fast, 1076 vs 1347 ms slow). The tail-history files do not break out per-child event timestamps, so a finer split was not possible. No step shows a Rust-only delay, so no change was needed in `rust/crates/cadence-history`.

## Reproduction
```
for i in 1 2 3; do gh workflow run frontend-matching-benchmark.yml --ref perf-child-shape -f shape=child \
  -f rust_services=frontend,matching,history -f control_ref=origin/master -f sdk_openloop=false \
  -f requests=300 -f repetitions=3 -f concurrency=16 -f payload_bytes=256 -f trace_decision=true; done
```
`trace_decision` is boolean: pass `true`, not `1`. `profile` takes `1`. Dispatches returned HTTP 500/502 several times and had to be retried.
