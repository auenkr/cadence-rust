# c1 permit timing: Go vs Rust queue v2 load path (2026-11-25)

Branch `perf-c1-permit-timing`. **No product code change.** Data: trace run
[45633827327](https://github.com/auenkr/cadence-rust/actions/runs/45633827327) (head `7a1a3ced4`, MySQL default c1, 5 reps, `trace_decision=true`,
artifact `frontend-matching-paired-benchmark`). Analysis script: `memory/evidence/perf-c1-permit-timing-replay.py`
(transfer queue only, measured window only, request time = log time - wait_ms, replayed through a 20 rps / burst 20 bucket per shard).
cadence-history CI, history-mixed-gate and the 5-rep benchmark were not run: no code changed.

## Code comparison (line by line) - no divergence found

| step | Go | Rust | same? |
|---|---|---|---|
| limiter | `quotas.NewDynamicRateLimiter(MaxPollRPS)` per queueBase, burst = ceil(rps), `Wait` = `ReserveN` + sleep (queue_base.go:159, dynamicratelimiter.go, clock/ratelimiter.go:357) | `DynamicRateLimiter` + `RateLimiter::reserve_raw` is a port of x/time/rate (cadence-common/dynamic_rate_limiter.rs, rate_limiter.rs) | yes |
| trigger | `immediateQueue.NotifyNewTask` -> `notifyCh` (cap 1, non-blocking) -> `processNewTasks` -> `AddNewVirtualSliceToRootQueue` -> `virtualQueue.notify` (cap 1) | same channels and coalescing (queue_immediate.rs:67,186; virtual_queue.rs:305) | yes |
| load | `run()` waits notifyCh, then `loadAndSubmitTasks`: limiter Wait first, then lock, GetTasks, `notify()` again only if slice `HasMoreTasks`/next slice (virtual_queue.go:327-441) | `run` + `load_and_submit_tasks` identical order (virtual_queue.rs:309-480) | yes |
| max read level | `getImmediateTaskMaxReadLevel` takes shard RLock, so it waits for in-flight task writes | `wait_for_task_writes` (shard write lock taken and dropped) before reading (queue_base.rs:288, commit dca628c49) | yes |
| poll timer fallback | `MaxPollInterval` jittered timer, acts only if `lastPollTime + PollBackoffInterval` elapsed | same (queue_immediate.rs:71) | yes |
| in-memory satisfaction | none: the default (non-cached) immediate queue reads the DB for every slice; no pending-queue shortcut | none | yes |

Go has no per-shard reader jitter, no extra backoff and no load without a notify; every load is one notify-coalesced `GetTasks`.

## Loads per workflow (trace)

| arm | transfer loads/wf | tasks/load | loads that waited |
|---|---|---|---|
| Go | 2.98-3.03 | ~1.3 (4 tasks/wf: 2 decision, 1 close, 1 record-started) | 18% (median of 5 reps) |
| Rust | 2.92-2.96 | ~1.4 (histogram of tasks/load: 0:8, 1:1851, 2:840, 3:170, rest batched system tasks) | 26% |

So the event mix, the number of loads per workflow and the number of tasks served per load are the same. Rust does not issue
loads that Go satisfies without one.

## Timing

Per-shard permit rate (median of 5 reps): Go 17.9/s, Rust 18.3/s (2% apart). Replaying the observed request times through an ideal
20 rps / burst 20 bucket reproduces the observed waited share (open loop, no feedback): Go 13.6% (observed 17.9%), Rust 21.0% (observed 25.9%),
mean wait/permit 5.6 vs 9.1 ms (observed 7.4 vs 11.5). Stretching the same request series by 15% (about 15.6/s/shard) removes every wait in both
arms. The limiter is therefore a pure function of the request series: waits appear when a shard receives a cluster of workflows
(3 permits each, all on the shard of that workflow) faster than the 20-token bucket refills, and Rust's faster commit/notify/read path
(non-limiter 5 ms vs Go 18 ms per wf) packs the permits of one workflow closer together (gap p5 5 ms vs 7-8 ms), so the same shard
random-walk exceeds the bucket more often. No component in the notify/load path delays Go's requests deliberately.

## Conclusion

No Rust-only divergence: same triggers, same coalescing, same loads per workflow, same limiter. The c1 p95 gap is the default
20 rps / burst 20 limiter reacting to Rust's faster closed loop (see perf-c1-burst-20261125.md, perf-default-tails-20261125.md). Making
Rust requests spread like Go would need an artificial delay, which is a behavioural deviation, so nothing was changed.
