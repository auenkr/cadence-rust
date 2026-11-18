# Rate limiter / pacing Go-parity audit (2026-11-18)

Branch `perf-ratelimiter-parity` (base master 451146825). Fix commit a300493e1; test fix 2nd commit.
New shared type: `cadence_common::dynamic_rate_limiter::DynamicRateLimiter`, a port of Go `quotas.NewDynamicRateLimiter`. It is an x/time/rate bucket that starts full. Burst is `max(ceil(rps),1)`, or 0 when rps is 0. The rps is re-read every 5s.

## Audit table

| Limiter (Rust) | Go counterpart / behavior | Rust before | Fixed? |
|---|---|---|---|
| cadence-common `rate_limiter::RateLimiter` | x/time/rate Limiter | faithful port (fixture-tested) | n/a (OK) |
| history `wiring/assembly/queue_exec.rs` DynamicRateLimiter (queue MaxPollRPS) | DynamicRateLimiter burst | fixed earlier in 242ceff31 | already |
| history `task/deps_exec.rs` DynamicRateLimiter | DynamicRateLimiter (ttl 5s, minBurst 1) | uses common bucket, correct | n/a (OK) |
| history `replication/deps.rs` SimpleRateLimiter (task_fetcher host QPS, task_processor shard QPS, task_store generation QPS) | `quotas.NewDynamicRateLimiter(cfg.X.AsFloat64())`, Wait errors ignored | **spaced every wait 1/qps (burst 1), qps snapshotted at construction** | yes: shared bucket + dynamic rps |
| history `wiring/assembly/archiver_client.rs` AllowLimiter (request / inline history / inline visibility) | `quotas.NewDynamicRateLimiter` (resource.go:164) `.Allow()` | **1 token per 1/rps interval (no burst)** | yes |
| history `service.rs` workflow-ID cache DynamicLimiterFactory | `NewSimpleDynamicRateLimiterFactory` → NewDynamicRateLimiter | burst max(rps,1); **allowed 1 at rps 0** (Go: burst 0 → deny) | yes |
| history `handler/handler_impl.rs` handler limiter | `quotas.NewDynamicRateLimiter(config.RPS.AsFloat64())` | **rps snapshotted, burst max(rps,1), rps≤0 allowed all** | yes (dynamic, Go burst rules) |
| history `wiring/server/limiter.rs` server limiter | none in Go (Rust-only, default rps 0 = off) | no-op by default | left unchanged (no Go counterpart) |
| frontend `global_ratelimit.rs` Bucket (FallbackLimiter primary/fallback, CountedLimiter) | `clock.NewRatelimiter` zero `last` → first use fills full burst. Primary is `(0,0)` until `Update`, and `SetLimit` accrues tokens at the old rate | **last=now at creation: the primary gave only 1 token after the first Update. Rate change had no accrual at the old rate** | yes |
| frontend `wrappers/quota_policy.rs` Bucket | DynamicRateLimiter burst max(ceil,1), 0 at 0 | starts full, correct burst | n/a (OK) |
| matching host/tasklist/persistence limiters, throttled logger, service-core matching_quota | DynamicRateLimiter / TaskListLimiter | all use common RateLimiter with Go limit_and_burst | n/a (OK) |
| history taskdlq host_limiter | semaphore (concurrency, not rate) | n/a | n/a |
| matching dispatch 100ms select sleep (`matching_dispatch.rs:1462`, native :1383) | notify-driven | notify wakes immediately, sleep is only a fallback | n/a |
| other `sleep(from_millis)` in matching/history queue/task | — | test-only code | n/a |

## Validation
- rust-ci cadence-common: https://github.com/auenkr/cadence-rust/actions/runs/44356805626 success
- rust-ci cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/44356813268 success
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/44356819885 failed. An old test asserted the rps-0 divergence. The test was fixed to match Go. Rerun https://github.com/auenkr/cadence-rust/actions/runs/44358157771 success
- All-Rust benchmark: frontend-matching-benchmark.yml on temp ref `tmp/perf-ratelimiter-bench` (= t3/matching-benchmark-harness), `sdk_openloop=true service=all sdk_repetitions=3 source_ref=a300493e1`: https://github.com/auenkr/cadence-rust/actions/runs/44356963329 success. 150/150 correct and 0 errors in every arm.

| run | completion p50 ms | p99 ms | CPU FE+M+H cores |
|---|---:|---:|---:|
| 00-go | 62.77 | 76.62 | 0.197 |
| 00-rust | 48.66 | 57.76 | 0.118 |
| 01-go | 61.03 | 85.81 | 0.190 |
| 01-rust | 48.08 | 55.74 | 0.114 |
| 02-go | 61.13 | 76.96 | 0.192 |
| 02-rust | 48.06 | 59.68 | 0.115 |

Rust vs Go: p50 is about 13 ms lower (−22%), p99 about 22 ms lower, and CPU about 40% lower. These match the 242ceff31 numbers (44347363726: Rust p50 46–48). The audited limiters are not on the steady-state hot path at 5 starts/s, so this lane shows no regression rather than a gain.
