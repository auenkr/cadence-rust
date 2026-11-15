# Matching append-copy latency work

## Scope and change

- Source base: `7d83dce4d86b72b70cbcb74f9db798ba78ac5dad` (`origin/master`).
- Implementation commit: `cf7bdb2ef` (`perf-matching-latency`).
- Changed the native Matching task writer and `cadence-persistence`'s native Matching queue API only. The writer now moves each request's task into the append batch and passes borrowed task references to persistence, removing one deep clone of every task payload. Persistence still makes the owned rows needed by the database operation; ordering, retries, and caller response handling are unchanged.
- No History or allocator/Docker implementation files were changed.

## Existing benchmark baseline

Source: `origin/t3/matching-benchmark-harness`, `Benchmark/reports/final-master-20261115.md`, run [43702926802](https://github.com/auenkr/cadence-rust/actions/runs/43702926802). The report uses the new source versus its previous comparison source, and is not a direct before/after of this commit.

| Lane / observation | Go | Rust | Difference |
|---|---:|---:|---:|
| Matching only, 5 starts/s, p50 (run 43702926802) | 49.26 ms | 41.00 ms | Rust -8.26 ms |
| Matching only, 5 starts/s, p99 (run 43702926802) | 59.85 ms | 71.95 ms | Rust +12.10 ms |
| Older closed-loop Matching CPU / 1,000 workflows (run 41767896292) | 2.687 s | 5.045 s | Rust +2.358 s |

The `43702926802` fixed-load table and its capacity results are different observations: its fixed-load percentile table is at 5 starts/s, and the tested capacity step reached 25 starts/s. Its `All Go` p99 of 54.30 ms is not the paired Go baseline for the `Rust Matching only` row; that row's paired Go p99 was 91.22 ms. Do not compare those rows as an isolated Matching effect.

The report's Matching lane passed the tested 25 starts/s capacity step; its fixed-load percentile table is at 5 starts/s. Those are separate observations.

## Validation

- `git diff --check`: passed before commit.
- Local Cargo, Go, and Docker commands: not run, per project CI-only validation rules.
- Matching CI: [43737179904](https://github.com/auenkr/cadence-rust/actions/runs/43737179904), package `cadence-matching`, conclusion `success` (head `cf7bdb2eff013fbcb32546b1e019658c81dcfa80`).
- Workspace CI: dispatched as [43737183856](https://github.com/auenkr/cadence-rust/actions/runs/43737183856) because `cadence-persistence` is shared; pending at time of this entry.
- Fixed-arrival benchmark workflow is not registered on the default branch. Direct dispatch attempts returned HTTP 404, so a branch-only push trigger was added to the temporary benchmark branch. It did not change the implementation branch. The temporary branch was created from `origin/t3/matching-benchmark-harness`, merged `perf-matching-latency`, and pushed as `bench-perf-matching-latency`.
- 5 starts/s lane run: [43737479740](https://github.com/auenkr/cadence-rust/actions/runs/43737479740), success. Configuration: 1 KiB payload, 10 ms activity, 30 s measurement, 10 s warmup, three alternating pairs, 150 workflows per arm. The runner report explicitly sets `performance_claims_allowed=false` and labels this an initial pipeline validation; the complete protocol calls for 600 s warmup and 1,800 s measurement.
- 5 starts/s median across the three pairs: Go p50 40.88 ms, p99 71.38 ms and Matching CPU 0.01505 cores; Rust p50 42.19 ms, p99 71.15 ms and Matching CPU 0.01781 cores. Rust p50 was 1.31 ms slower, p99 was 0.23 ms faster, and Matching CPU was 18.3% higher. All 450 workflows per arm completed correctly with zero errors and zero outstanding workflows.

| Metric | Before: report 43702926802 Rust Matching-only | After: run 43737479740 Rust, 5 starts/s | Change |
|---|---:|---:|---:|
| Completion p50 | 41.00 ms | 42.19 ms | +1.19 ms |
| Completion p99 | 71.95 ms | 71.15 ms | -0.80 ms |
| CPU, all three services | 0.12 cores | 0.1247 cores | +3.9% |

This is an indicative comparison, not a controlled before/after: the source snapshots differ and the benchmark reports use the short pipeline protocol. The current Go/Rust paired p99s differ by only 0.23 ms; current Matching-only CPU is 18.3% above Go. Both run reports reject performance claims for this abbreviated protocol.
- Rust binary SHA256: `b9613f224afffd5a62ea78e4fbf6f6995f4e49d3c1c457a807924bff2b394ff6`; benchmark source head `8fb5c70c7e84241297276a151e3ce11c121d17bd`, Rust tree `12f72853ddee5d08cb8c161c6f65657618d8a090`.
- 25 starts/s lane run: [43740549373](https://github.com/auenkr/cadence-rust/actions/runs/43740549373), success. Same payload, activity, duration, warmup, repetitions, and worker setup. Median across three pairs: Go p50 81.11 ms, p99 188.27 ms and Matching CPU 0.09330 cores; Rust p50 83.27 ms, p99 175.13 ms and Matching CPU 0.10675 cores. Rust p50 was 2.15 ms slower, p99 was 13.14 ms faster (7.0%), and Matching CPU was 14.4% higher. Each arm processed 750 workflows with zero errors; at arrival end Rust had 1/2/1 outstanding workflows in its three runs and Go had 3/2/1.
- At 25 starts/s, total service CPU medians were 0.0933 cores for Go and 0.1067 for Rust (+14.4%). Rust binary SHA256 remained `b9613f224afffd5a62ea78e4fbf6f6995f4e49d3c1c457a807924bff2b394ff6`; source head `880a71731a8def0961601f05381944eea78ab233`, Rust tree `12f72853ddee5d08cb8c161c6f65657618d8a090`.

## Before/after

The short 25 starts/s run has a lower p99 sample but higher Matching CPU. Both lane reports explicitly reject performance claims: the full protocol calls for 600 s warmup and 1,800 s measurement, and this change has no source-identical before arm. Treat the numbers as directional evidence only, not a demonstrated optimization result.
