# History and Matching throughput investigation

## Scope and baseline

Baseline report: `Benchmark/reports/final-master-20261115.md` on `t3/matching-benchmark-harness`.
The reported Rust History lane used musl System and measured p50 81.29 ms / p99 720.03 ms at 5 starts/s; the paired Go lane measured p99 559.90 ms. Rust Matching measured p99 71.95 ms versus Go 91.22 ms in the fixed-load row, but its p99 was 71.95 ms versus the common all-Go baseline 54.30 ms. Rust Matching passed 25 starts/s; 100/s failed for all lanes.

## Changes

- `rust/Dockerfile`: release Alpine image opts into the existing `cadence-server/jemalloc-default` feature. Normal GNU Cargo builds still use the default System allocator.
- `rust/crates/cadence-history`: added the same opt-in allocator feature to the standalone History binary, preserving System as its default.
- Harness benchmark Dockerfile: on temporary source branch `perf-history-throughput-bench`, both standalone History and all-Rust image builds opt into jemalloc. The harness workflow is pinned to `t3/matching-benchmark-harness`; its source checkout is the temp branch so the run tests the complete matched benchmark context.

No History or Matching workflow logic was changed. Static config review found Rust History `taskSchedulerWorkerCount` default 200, `transferProcessorMaxPollInterval` 60s, and `timerProcessorMaxPollInterval` 5m, consistent with Go config behavior. No clear per-request connection setup or lock-across-await mismatch was established from source review. These remain candidates for a CPU profile if jemalloc does not close the latency gap.

## Validation runs

- `rust-ci` for `cadence-server`, pre-History allocator addition: https://github.com/auenkr/cadence-rust/actions/runs/43735842833 (running at report creation; head `47f655573`).
- `rust-ci` for `cadence-history`: https://github.com/auenkr/cadence-rust/actions/runs/43736010933 (running at report creation; head `09d146110`).
- History benchmark suite: https://github.com/auenkr/cadence-rust/actions/runs/43736038261 (running at report creation; harness head `cb03e4d19e6562f94d272b0555f3e1f7be267890`, source `e2f780c2d`).
- All-Rust benchmark suite: https://github.com/auenkr/cadence-rust/actions/runs/43736041525 (running at report creation; harness head `cb03e4d19e6562f94d272b0555f3e1f7be267890`, source `e2f780c2d`).

The first two benchmark dispatches used source `47f655573`, which predates the harness context in that checkout and failed. Corrected runs above use the merged temporary harness branch. Replace this section with final run conclusions and p50/p99/capacity results when they complete.

## Before/after

| Lane | Baseline Rust p50 / p99 | Baseline Go p99 | Post-change Rust p50 / p99 | Post-change max passing rate |
|---|---:|---:|---:|---:|
| History | 81.29 / 720.03 ms at 5/s | 559.90 ms | pending | pending |
| Matching | 41.00 / 71.95 ms at 5/s | 91.22 ms (paired); 54.30 ms all-Go baseline | pending (all-Rust suite) | pending |
| All Rust | 66.31 / 72.02 ms at 5/s | 54.30 ms | pending | pending |

## Reproduction

On GitHub Actions only, from `perf-history-throughput`:

```sh
gh workflow run rust-ci.yml --ref perf-history-throughput -f package=cadence-server
gh workflow run rust-ci.yml --ref perf-history-throughput -f package=cadence-history
```

Benchmark dispatcher is `.github/workflows/frontend-matching-benchmark.yml` on `t3/matching-benchmark-harness`, with `sdk_openloop=true`, `sdk_suite=true`, `service=history` or `service=all`, and `source_ref=e2f780c2d`.
