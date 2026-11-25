# perf-c1-budget (2026-11-25)

Goal: find the largest remaining per-workflow latency in Rust services at concurrency 1 (all-Rust cluster ~1.1x Go).

## Latency budget (run 45539135258, master 9ce0ba1a1, history=rust, c=1, 1x1000, trace on)
Mean workflow latency 38.8 ms, p50 24.4 ms (rust), so the mean is dominated by a ~40 ms tail on ~22% of workflows.
| step (History trace gap) | n | total s | p50 ms | p95 ms |
|---|---|---|---|---|
| **b notify -> c transfer task picked up** | 1010 | 8.8 | 0.1 | **38.9** |
| d2 AddDecision returned -> a next decision committed (activity complete + commit) | 1010 | 7.5 | 7.3 | 8.5 |
| f lock -> f2 RecordDecisionTaskStarted update (DB tx) | 2020 | 4.7 | 2.2 | 2.9 |
| d AddDecisionTask sent -> e RecordStarted received (Matching) | 2020 | 1.4 | 0.7 | 0.9 |
| c -> c2 transfer lock | 2020 | 1.2 | 0.0 | 1.6 |
| g -> d2 | 2020 | 0.8 | 0.4 | 0.5 |
The stall is only on the second decision (schedule_id=8, scheduled by activity completion): 228/1010; the first decision (workflow start) never stalls.

## Root cause (pump trace, run 45549276233, head 8e7a40598)
Opt-in `pump_trace!` (same `CADENCE_TRACE_DECISION` switch) added to queuev2 (`q1_notify_recv`, `q1b_after_gate`, `q2_process_new_tasks_done`,
`v1_vq_notified`, `v2_vq_locked`, `v3_got_tasks`) and the v1 pump. Stalled cases show everything within 0.1 ms up to `v1_vq_notified`,
then a 32-42 ms gap before `v2_vq_locked`: that is `VirtualQueue::load_and_submit_tasks` waiting on `task_load_rate_limiter`
(`quotas.NewDynamicRateLimiter(MaxPollRPS)`, default `history.transferProcessorMaxPollRPS` = 20, burst 20). A workflow causes ~4 transfer
notifications, i.e. ~100 loads/s at c1, so the 20 tokens/s limiter throttles. Rust `DynamicRateLimiter`/`V2RateLimiter` is a faithful port of Go
(burst `ceil(rps)`, reserve + sleep), and the Go arm shows the same ~100 ms p95 (Go p95 100.4 ms vs Rust 100.3 ms in run 45553106424), so
this is Go-parity behaviour, not a Rust inefficiency. Rust only looks relatively better than it is because it hits the limiter equally while
being faster elsewhere.

## Confirmation (run 45553865574, head ff5432824, both arms with `history.transferProcessorMaxPollRPS=1000`, c=1, 1x1000)
| arm | throughput wf/s | p50 | p95 | p99 |
|---|---|---|---|---|
| go | 26.15 | 37.3 | 45.5 | 53.9 |
| rust | 39.87 | 24.8 | 28.3 | 32.4 |
Rust vs Go +52% (was +5..+10% with the default). b->c p95 0.27 ms, total 0.19 s (was 13 s). The limiter explains the whole remaining stall.
(Run 45553106424 did not apply the override, because the paired harness ignored `dynamic_config_extra`; fixed by `BENCH_DYNAMIC_CONFIG_EXTRA` in paired-run.py.)

## Change / decision
- No behaviour change to Rust services: lowering/removing the limiter or raising its default would diverge from Go (program logic/config change), so nothing is optimised.
  Whether to raise the `transferProcessorMaxPollRPS` default is a product decision (same knob exists in Go); it would help Go equally.
- Kept (all opt-in, no effect when off): pump/queue traces (`pump_trace!`), benchmark `dynamic_config_extra` support in paired-run.py (applies identical dynamic config to every arm).
- Three-arm control/candidate benchmark not run: there is no optimisation to compare.

## Runs
- 45539135258 baseline trace; 45544330462 v1 pump trace (no output: queuev2 is active); 45549276233 queuev2 trace; 45553106424 (override ignored); 45553865574 confirmation.
- CI: rust-ci cadence-history 45554624630 head ff5432824041654e0b510277fe19f40ef8d0414d, conclusion success (https://github.com/auenkr/cadence-rust/actions/runs/45554624630).
- history-mixed-gate.yml not run: History changed only by opt-in trace macros that are inert when `CADENCE_TRACE_DECISION` is off.
