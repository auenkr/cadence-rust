# History queue v2 limiter parity vs Go x/time/rate (2026-11-26)

Branch `perf-limiter-parity`, code head `0cadb6b057036bbc7fe4903ad284cab6995113d2` (not merged). Default config unchanged (History queue v2 `MaxPollRPS` 20 rps, burst `max(ceil(20),1)` = 20).

## Go call sites
- `go/service/history/queuev2/queue_base.go:159`: `quotas.NewDynamicRateLimiter(options.MaxPollRPS.AsFloat64())`, one limiter per queue, passed to `VirtualQueueManager` and every `VirtualQueue`.
- Used only at `virtual_queue.go:341`: `taskLoadRateLimiter.Wait(q.ctx)` before each task-load page.
- `quotas.DynamicRateLimiter` wraps `clock.Ratelimiter` (`go/common/clock/ratelimiter.go`), which wraps `x/time/rate`: bucket starts full, `maybeRefreshRps` re-reads rps at most every 5s (`SetLimitAndBurst` at `latestNow`), `Wait` = `ReserveN` then timer sleep, cancelled wait calls `res.CancelAt`.
- No per-shard/host limiter sits on this path.

## Audit of the Rust port
- `cadence-common/src/rate_limiter.rs` + `dynamic_rate_limiter.rs`: faithful (float tokens, full start, reserve then delay, CancelAt, 5s TTL, existing Go-generated fixture `tests/fixtures/clock-rate.json`).
- BUT the queue did NOT use it. `cadence-history/src/wiring/assembly/queue_exec.rs` had its own bucket, used by `V2RateLimiter` (`queue_v2_adapters.rs`). Mismatches found:
  1. rps/burst re-read on every call (Go: every 5s, only then `SetLimitAndBurst`).
  2. a `wait` interrupted by cancellation did not return its token (Go `CancelAt`), so cancelled waits could push later waiters out.
  3. separate float path for delay (`Duration::from_secs_f64`) vs Go's truncating `durationFromTokens`; sub-ns only.
- Not changed (cannot match): `tokio::time::sleep` rounds the delay up to the 1 ms timer wheel (Go timers are precise): up to +1 ms per wait. At 20 rps spacing of 50 ms this is <=2%.

## Fix
`queue_exec::DynamicRateLimiter` is now a thin wrapper over `cadence_common::dynamic_rate_limiter::DynamicRateLimiter` (new `reserve_at/reserve_now/cancel` there); `V2RateLimiter::wait` cancels its reservation when the context is cancelled. Unit tests: replay of a 400-request arrival series (bursts, 0-1 ms gaps, idle gaps) against an independent x/time/rate reference (delays equal within 1 ns), burst-then-50ms pacing, deadline consumes nothing, zero rps rejects, cancelled wait restores token.

## CI
- rust-ci `package=cadence-common`: https://github.com/auenkr/cadence-rust/actions/runs/45748917074 success.
- rust-ci `package=cadence-history` (after fixing a dead-code clippy error from run 45748913265): https://github.com/auenkr/cadence-rust/actions/runs/45749561726 success.
- rust-ci workspace, head 0cadb6b05: https://github.com/auenkr/cadence-rust/actions/runs/45750365729 success.

## Unpaced c1 benchmark, 4 runs (frontend-matching-benchmark.yml, master workflow, source_ref=0cadb6b05, rust_services=frontend,matching,history, mysql, c1, echo, 5 reps x 1000, warmup 10, target_rate empty)
Runs: 45750371177 45750374664 45750377978 45750381921 (all success). Rust built from `benchmark-source` = 0cadb6b05 (`source-head.txt` shows the workflow checkout, master 2039de427, not the source).
Rust `cadence-server` SHA256 `e778ea2e1a3a9fcbb9afb84a0d04896274bb5f3ec38c2d3777e61d87a821e725` (identical in all 4); `cadence-history` `d86114586c140074794f75d57f6ec28c23c1af4699a8cb63f7589dcff3badeb5`. Go `/usr/local/bin/cadence-server` `e44ee0f569cb118a236eb4076e19bddae5af8c9638c690e4a422741c76e47a1d` (run 45750371177).

Pooled (4 runs x 5 reps x 1000 = 20 000 per arm):
| arm | p50 | p90 | p95 | p99 |
|---|---|---|---|---|
| Go | 34.7 | 93.9 | 120.5 | 251.7 |
| Rust | 23.1 | 98.7 | 108.7 | 219.6 |

Rust-Go, run-cluster bootstrap (seed 1, 20 000 draws): p90 point +5.1%, 95% CI [-2.4%, +14.2%]; p95 point -9.8%, CI [-22.6%, +1.0%].
Per-run p90 go/rust: 102.4/99.5, 88.1/98.9, 84.1/98.0, 100.8/98.6.

## Verdict
- Limiter mismatches 1 and 2 were real parity bugs and are fixed; the arithmetic itself was already equal to x/time/rate (fixed rate, steady state), so the earlier p90 94 vs 53 ms was not explained by token accrual.
- In this series the unpaced gap is much smaller than the earlier +77% (Rust p90 98.7 vs Go 93.9; CI includes 0, upper bound +14%). Rust p90 is stable (98-99.5 per run) while Go p90 swings 84-102, so the earlier Go 53 ms looks like the low end of Go run-to-run variance; no A/B against the pre-fix binary was run here, so the improvement cannot be attributed to this change alone.
- Predeclared knee criterion (+5% upper bound on p90 and p95) is NOT met on p90 (+14.2% upper bound). Not a pass; more runs would be needed to narrow the interval.
