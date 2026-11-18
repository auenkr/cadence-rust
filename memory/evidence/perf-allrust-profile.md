# All-Rust latency profile and fix (2026-11-18)

Branch `perf-allrust-profile`. Fix commit `242ceff3118fe973ce0e9bf979974fa23b38d012` (base master `36e8a1e3175ad76ed77bc9ec9bddadf3241a4d78`).

## Decomposition
Per-hop data comes from the harness samples, which hold schedule-to-start times read from workflow history. Baseline run 44321575675, medians over 3 pairs:

| hop | Go ms | Rust ms |
|---|---:|---:|
| first decision schedule-to-start | 13.7 | 9.7 |
| activity schedule-to-start | 0.02 | 0.55 |
| activity run (10 ms activity) | 16.5 | 14.1 |
| **second decision schedule-to-start** | **7.4** | **27.3** |

A trace run (temp harness branch `tmp/perf-allrust-trace`, `RUST_LOG=warn,cadence_history=debug`, run 44330788001) showed that History started a transfer notify and then waited 22–39 ms before the DB load. A load right after an idle period waited 0.6 ms.

## Root cause
`wiring/assembly/queue_exec.rs` `DynamicRateLimiter` (the queue processor `MaxPollRPS` limiter, default 20) spaced every load 1/rps apart, which is a burst of 1. So a load within 50 ms of the previous one slept. Go `quotas.NewDynamicRateLimiter` is a `rate.Limiter` token bucket. Its burst is `max(ceil(rps), 1)` and it starts full. The fix ports the token bucket, including its deadline check. Other behaviour is unchanged.

## Validation
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/44339708685 — success
- Benchmark: workflow frontend-matching-benchmark.yml, ref t3/matching-benchmark-harness, `sdk_openloop=true service=all sdk_repetitions=3`, 5 starts/s, 3 alternating fresh-MySQL pairs.
  - before (source 36e8a1e31): https://github.com/auenkr/cadence-rust/actions/runs/44321575675
  - after (source 242ceff31): https://github.com/auenkr/cadence-rust/actions/runs/44347363726 (clean). An earlier after-run, 44339714446, ran on a noisy runner (Go p99 440–890 ms). Rust still won every pair there.

Completion latency (ms) and CPU (FE+Matching+History average cores) per run:

| run | before p50 | before p99 | before CPU | after p50 | after p99 | after CPU |
|---|---:|---:|---:|---:|---:|---:|
| 00 Go | 61.53 | 80.23 | 0.227 | 60.37 | 76.05 | 0.216 |
| 00 Rust | 69.46 | 76.95 | 0.136 | **46.36** | **58.77** | 0.123 |
| 01 Go | 61.59 | 83.40 | 0.227 | 62.07 | 76.21 | 0.223 |
| 01 Rust | 69.49 | 74.09 | 0.138 | **47.18** | **60.39** | 0.123 |
| 02 Go | 62.30 | 77.67 | 0.229 | 63.33 | 80.50 | 0.232 |
| 02 Rust | 69.11 | 80.39 | 0.135 | **47.83** | **64.36** | 0.129 |

After the fix, all-Rust beats Go: p50 is about 15 ms lower (−24%), p99 about 16 ms lower (−20%), and CPU about 44% lower. There were 0 errors and 450/450 correct in every arm. The archiver client (`archiver_client.rs`) still uses the same pacing limiter, but it is not on the hot path and was left unchanged.
