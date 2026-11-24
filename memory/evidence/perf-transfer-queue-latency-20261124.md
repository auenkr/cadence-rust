# Transfer-queue latency at c1 (2026-11-24)

Branch `perf-transfer-queue-latency`. Outcome: root cause found, **no safe Rust fix exists**; Go has the same behaviour by design. Only env-gated tracing was added (off by default).

## Method
`CADENCE_HISTORY_TRACE_TRANSFER=1` (new workflow input `transfer_trace`, `BENCH_TRANSFER_TRACE` in paired-run.py) prints `TRANSFER_TRACE <step> now_us=...` lines in History. Steps: `v2_notify`, `v2_notify_recv`, `v2_process_new_tasks`, `v2_loaded` (task id, created_us, limiter/lock/read durations), `add_decision_sent` / `add_decision_acked`. The V1 processor is also instrumented, but `history.enableTransferQueueV2` defaults to true in Go and Rust, so only the V2 path (`queuev2/`) runs.

Paired benchmark, rust_services=frontend,matching,history, c1, warmup 100, 3 reps:
- Run 45356076561 (V1-only trace; no V1 lines because V2 is active).
- Run 45363735289 (V2 trace), success. rust-ci cadence-history: 45356082748 and 45363729770, both success.

## Result (Rust History, ms, p50/p75/p90/p99, 3 reps)
| step | rep0 | rep1 | rep2 |
|---|---|---|---|
| task created -> loaded | 1.9/2.2/32.8/43.3 | 1.8/2.2/37.8/43.8 | 1.8/2.1/38.4/43.9 |
| wait for task-load rate limiter | 0/0/39.5/50.2 | 0/0/41.7/50.5 | 0/0/41.3/50.4 |
| virtual queue lock wait | 0 | 0 | 0 |
| DB read (GetTasks) | 0/0.1/0.1/0.4 | same | same |
| loaded -> AddDecisionTask sent | 0.2/1.7/1.8/2.2 | same | same |
| AddDecisionTask sent -> acked | 3.9/4.2/4.7/6.2 | same | same |

About 12% of loads (586-724 of about 6100 per rep) take more than 35 ms, and all of that time is the limiter wait (708-819 loads over 35 ms). No other step ever exceeds 7 ms.

## Root cause
`virtualQueueImpl.loadAndSubmitTasks` first calls `taskLoadRateLimiter.Wait` (Go `queuev2/virtual_queue.go:341`). The limiter is `quotas.NewDynamicRateLimiter(TransferProcessorMaxPollRPS)` (`queuev2/queue_base.go:159`), default 20 rps with burst 20 (`dynamicproperties/constants.go:4272`). Each History shard owns one limiter, and every load, even for one new task, costs a token. At c1 each shard produces more than 20 transfer-task loads per second in bursts, so the bucket drains and the next load waits for a token, 1/20 s = 50 ms. That is the ~43 ms Scheduled->Started gap. It clusters per shard because a shard's bucket stays drained while its load rate exceeds 20/s, which explains the shard clustering seen in perf-c1-decision-gap-20261124.md.

Go has the same limiter, so Go shows the same gap (21.7% vs Rust 27.7%). Rust finishes each workflow step faster, so it hits the cap more often.

## Why other candidates are not the cause
- Notify path: V2 `notify` uses a 1-slot channel exactly like Go (`queue_immediate.go notify`); `process_new_tasks` wait is 0 ms.
- Max poll interval and jitter: identical to Go; not on this path (the notify arrives within ~2 ms).
- Rust limiter arithmetic (`wiring/assembly/queue_exec.rs` reserve, `queue_v2_adapters.rs`): identical to `x/time/rate` Wait (burst ceil(rps), refill at rps).
- Read-level lag, DB read and executor: under 7 ms.

## Fix assessment
No code fix. Skipping or bypassing the token wait would raise the DB read rate beyond `TransferProcessorMaxPollRPS` (an observable, operator-configurable cap), which is a semantic change. Limiter capacity is not available when the gap occurs, so "notify immediately when capacity exists" does not apply. Operators who want lower c1 latency can raise `history.transferProcessorMaxPollRPS` in dynamic config; that applies equally to Go and Rust. I did not run a benchmark with the raised value (the paired runner does not pass `dynamic_config_extra`). No gate runs or before/after benchmark runs were done because no behaviour changed.
