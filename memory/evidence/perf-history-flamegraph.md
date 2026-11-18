# perf-history-flamegraph (2026-11-18)

Branch `perf-history-flamegraph` (from master 469779f95).

## Profiling mode (Benchmark harness)
`gh workflow run sdk-matching-benchmark.yml --ref <branch> -f service=history|all -f rate=25 -f profile=1`
- Dockerfile `PROFILE=1`: `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`, `-C force-frame-pointers=yes`, same opt level.
- `Benchmark/profile.py`: `perf record -F 99 -g` on Rust service process trees between measurement start/end
  markers; `perf script --no-inline` (inline/addr2line resolution stalled >300 s) -> FlameGraph folded/SVG + top-30.
  perf.data kept in /tmp (root-owned file broke artifact upload).
- Profile runs: History lane https://github.com/auenkr/cadence-rust/actions/runs/44350064637 ,
  all-Rust https://github.com/auenkr/cadence-rust/actions/runs/44350073382 (Rust arm profiled OK;
  Go arm generator exited 1 with empty log at 25/s in both history attempts - unexplained, not investigated).

## History lane flamegraph summary (Rust History, 25 starts/s, 120 s, 4610 samples)
Self: memcpy 7.5%, kernel spin_unlock/irqrestore 4.7%, finish_task_switch 4.7%, memcmp 2.1%, jemalloc malloc/free/realloc ~4.6%.
Inclusive: syscalls 38% (mostly MySQL TCP send via `mysql::Conn::exec_iter`, 22%);
update_as_active/persist 24%; SqlBackend::transaction 21%; handle_decision_task 21%; load_state 15%;
get_workflow_execution 13%; queue task execute 15%; metrics registry 5.3%; mysql StmtCache 1.2%; Ring::new 0.6%.
Attribution: memcmp 75% and memcpy 11% / realloc 36% came from `MetricsRegistry::{add,key}`
(format!-built String key per observation + BTreeMap<String> string compares under a global Mutex).
Conclusion: Rust History CPU is dominated by MySQL round trips (kernel network path), same as Go's design;
largest pure-userland waste was the metrics registry.

## Fix
`rust/crates/cadence-history/src/wiring/resources/metrics.rs` (94f38087a): HashMap store, key built in a
thread-local buffer, allocation only on first insert; `snapshot()` still returns the sorted BTreeMap with
identical keys/values. No API/logic change (Go uses tally; this is the in-process registry only).

## Before/after (History lane, 15 starts/s, 30 s warmup, 120 s, 3 alternating pairs)
Rate 15 instead of 25 because the Go arm generator failed at 25/s on this runner.
Before = master 469779f95 https://github.com/auenkr/cadence-rust/actions/runs/44369094172 (success)
After = 94f38087a https://github.com/auenkr/cadence-rust/actions/runs/44359632339 (success)
rust-ci cadence-history on 94f38087a: https://github.com/auenkr/cadence-rust/actions/runs/44359206360 (success)
History container avg CPU cores; completion latency ms.

| run | before CPU | before p50 | before p99 | after CPU | after p50 | after p99 |
|---|---:|---:|---:|---:|---:|---:|
| 00-go | 0.437 | 68.9 | 116.8 | 0.435 | 68.5 | 119.2 |
| 00-rust | 0.218 | 87.6 | 220.5 | 0.221 | 88.2 | 223.6 |
| 01-go | 0.438 | 67.6 | 119.7 | 0.425 | 65.8 | 115.5 |
| 01-rust | 0.223 | 89.1 | 222.8 | 0.217 | 87.9 | 222.8 |
| 02-go | 0.434 | 67.5 | 119.8 | 0.433 | 68.3 | 116.7 |
| 02-rust | 0.224 | 89.3 | 223.9 | 0.219 | 89.0 | 225.2 |

Rust/Go History CPU ratio: before 0.50/0.51/0.52 (mean 0.508); after 0.51/0.51/0.51 (mean 0.508).
Result: no measurable change at 15/s (metrics share ~5% of an already ~0.22-core process is within noise).
Rust History already uses ~half Go's CPU. The real gap is latency: Rust completion p50 +20 ms, p99 ~1.9x Go,
with decision schedule-to-start p99 93 ms vs 33 ms and activity STS 1.0 ms vs 0.08 ms -> a waiting/latency
issue (queue/poll/notification path), not CPU. Next: latency profiling (off-CPU / tracing) of the decision
task dispatch path; kernel MySQL send is the largest CPU block (per-statement round trips).
