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

- `rust-ci` for `cadence-server`: https://github.com/auenkr/cadence-rust/actions/runs/43736191723 — success; branch head `b2c10ea3f7f461e2e2af9dcb1a3f34e6f145c56e`.
- `rust-ci` for `cadence-history`: https://github.com/auenkr/cadence-rust/actions/runs/43736010933 — success; source head `09d14611064a90e52aff6f86c1d3c4a516d02b45`.
- System-allocator History comparison: https://github.com/auenkr/cadence-rust/actions/runs/43736172831 — success; `Benchmark/ci.sh` resolved the Dockerfile from the harness checkout, leaving the default System allocator enabled. This is the before measurement; it does not validate the jemalloc image configuration.
- Earlier all-Rust suite: https://github.com/auenkr/cadence-rust/actions/runs/43736175975 — canceled after the same Dockerfile resolution issue was found.
- Corrected History suite (workflow ref `perf-history-throughput-bench`): https://github.com/auenkr/cadence-rust/actions/runs/43743286537 — success; `ci.exit=0`; Alpine build log confirms both jemalloc feature flags/path; source `e2f780c2d80f66327bc5da5474a2a39b6e218814`.
- Corrected all-Rust suite (workflow ref `perf-history-throughput-bench`): https://github.com/auenkr/cadence-rust/actions/runs/43743289833 — completed with failure in recovery because the first Go repetition had 11 `TimeoutType: START_TO_CLOSE` errors; fixed load and 25/s capacity stages passed for Go and Rust; `ci.exit=1`.

History allocator comparison, each median across three Rust repetitions: at 5 starts/s, System measured p50 71.46 ms / p99 80.26 ms; jemalloc measured p50 63.04 ms / p99 66.55 ms (11.8% lower p50 and 17.1% lower p99). Paired Go p99 medians were 73.86 ms (System run) and 49.24 ms (jemalloc run), showing hosted-runner variation. At 25/s, System measured p50 181.06 / p99 283.03 ms; jemalloc measured p50 161.09 / p99 213.53 ms. Both Rust runs passed all repetitions at 5/s and 25/s; all three 100/s attempts failed their targets. Highest tested passing rate is 25 starts/s. The original 720 ms p99 observation is not reproduced by the System rerun, so the direct allocator comparison is more reliable than comparing with that historical point. Recovery p99 includes the planned MySQL outage and is not steady-state latency.

All-Rust suite summary: at 5 starts/s, median across three Rust repetitions was p50 66.93 ms / p99 74.27 ms; paired Go p99 median was 69.07 ms. Both Rust and Go passed all repetitions at 5/s and 25/s; neither passed 100/s. Highest tested passing rate is 25 starts/s. Rust recovery repetitions recovered; the workflow failed because the first Go recovery repetition reported 11 `START_TO_CLOSE` errors. Recovery completion p99 includes the MySQL fault and is not steady-state latency. The build log confirms both jemalloc feature flags were enabled.

Status checked 2026-11-15 19:13 UTC: corrected History completed successfully; corrected all-Rust completed with the Go recovery error noted above. The latest `cadence-server` and `cadence-history` Rust CI runs are green.

The first two benchmark dispatches using source `47f655573` failed because that source branch did not contain the benchmark Docker context. Two dispatches with the merged temporary branch's short SHA failed in `actions/checkout` (the workflow requires a full SHA). The full-SHA History run from the harness ref built the default System allocator and serves as the before measurement. Corrected runs use the merged temporary branch for both workflow and source checkout, enabling jemalloc in the image build.

## Before/after

| Lane | Baseline Rust p50 / p99 | Baseline Go p99 | Post-change Rust p50 / p99 | Post-change max passing rate |
|---|---:|---:|---:|---:|
| History | 81.29 / 720.03 ms at 5/s | 559.90 ms | System rerun 71.46 / 80.26 ms; jemalloc 63.04 / 66.55 ms at 5/s | 25 starts/s |
| Matching | 41.00 / 71.95 ms at 5/s | 91.22 ms (paired); 54.30 ms all-Go baseline | no isolated Matching-only rerun | 25 starts/s in the all-Rust lane |
| All Rust | 66.31 / 72.02 ms at 5/s | 54.30 ms | 66.93 / 74.27 ms at 5/s (paired Go p99 69.07 ms) | 25 starts/s |

## Reproduction

On GitHub Actions only, from `perf-history-throughput`:

```sh
gh workflow run rust-ci.yml --ref perf-history-throughput -f package=cadence-server
gh workflow run rust-ci.yml --ref perf-history-throughput -f package=cadence-history
```

Benchmark dispatcher is `.github/workflows/frontend-matching-benchmark.yml` on `perf-history-throughput-bench`, with `sdk_openloop=true`, `sdk_suite=true`, `service=history` or `service=all`, and `source_ref=e2f780c2d80f66327bc5da5474a2a39b6e218814`.
