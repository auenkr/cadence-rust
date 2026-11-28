# perf-c1-hop-trace (2026-11-28): the unpaced c1 p90 gap is the history transfer-queue load limiter, not a divergence. No fix.

Branch `perf-c1-hop-trace` (base origin/master 1f3477295). Question: which hop holds the extra ~40-100 ms in the Rust unpaced c1 p90
(Rust ~94-98 ms vs Go ~53-77 ms, `perf-unpaced-c1-limiter-20261126.md`).

## Instrumentation (all off by default; gated by `CADENCE_TRACE_DECISION=1`, one once-initialized bool when off)
- history `decision_trace!`/`pump_trace!` now also print `epoch_ns` (wall clock, comparable across containers). Existing stages: a committed,
  b notified, c0 transfer task read, c1 limiter wait (>5 ms), c picked up, c2 lock, d AddDecision sent, d2 returned, e RecordStarted received,
  f/f2 mutable-state update, g RecordStarted returned; plus per-permit `DTRACE queuev2 load wait_ms` (Go: `go-queuev2-trace.patch`).
- frontend `hop_trace.rs`: `HOPTRACE svc=frontend` sent/returned around the gRPC outbound `StartWorkflowExecution`, `PollForDecisionTask`,
  `RespondDecisionTaskCompleted` (`grpc_outbound.rs::try_call`).
- matching: `adaptive_scaler_trace` logs all task lists when the flag is on (did not fire in this run, since the active path is native dispatch).
  The matching hop is still bracketed by history d → e (AddDecision sent → RecordDecisionTaskStarted received from matching).
- `paired-run.py`: with `trace_decision=true` the env goes to every service container, not only history.
- Analysis: `perf-c1-hop-trace-analysis.py` (cross-process join by epoch; at c1 one workflow is in flight). Model: `perf-c1-hop-trace-sim.py`.

## Run
- Traced benchmark https://github.com/auenkr/cadence-rust/actions/runs/46109053428 (success), ref `perf-c1-hop-trace` head
  `d65e5fa27e021424e2e6833a7f6a8ea2002de619`, mysql, rust_services=frontend,matching,history, unpaced, c1, 5 reps x 1000, warmup 10, trace_decision=true.
  (The benchmark's paired job builds the dispatched ref and ignores `source_ref`, so dispatch on the branch itself.)
- Rust `cadence-server` sha256 `8372b3571ed160de57622d37e08e92b4eefecfee88328eea4b6a150c6b87da5a`, `cadence-history` `6ee9506d1e2b3e14f4bcf82ad5eca3cc99d5a0d46e94ec2405d19dae1b323b41`.
- CI: rust-ci (workspace) https://github.com/auenkr/cadence-rust/actions/runs/46109714432 success on head `6334b09e644afeb451bd5bacdca31072bc64d58d` (rustfmt-only follow-up commit; same logic).
- SDK results (per rep): Go 21.6-23.6 wf/s, p50 39.7-44.5, p95 51.8-55.4; Rust 25.2-26.2 wf/s, p50 25.7-26.6, p95 99.6-100.0 ms.

## Per-hop table (Rust, 5050 workflows, ms; each workflow has 2 decision tasks)
| hop | p50 | p90 | p95 | share >=40 ms |
|--|--|--|--|--|
| FE start sent → returned (history start) | 3.26 | 3.86 | 4.26 | 0 |
| **FE start sent → transfer task1 read (commit+notify+queue load incl. limiter)** | 3.15 | **36.41** | **40.73** | 0.072 |
| task read → picked up | 0.21 / 0.07 | 0.29 / 0.09 | 0.34 / 0.11 | 0 |
| picked up → AddDecision sent | 1.50 / 0.11 | 1.68 / 0.13 | 1.78 / 0.14 | 0 |
| AddDecision sent → RecordStarted recv (matching match + dispatch) | 0.59 / 0.59 | 0.70 / 0.69 | 0.75 / 0.75 | 0 |
| RecordStarted handler | 2.60 / 2.48 | 3.11 / 2.88 | 3.47 / 3.16 | 0 |
| RecordStarted returned → FE poll returned | 0.41 / 0.41 | 0.48 / 0.47 | 0.51 / 0.50 | 0 |
| FE poll returned → FE respond sent (SDK worker) | 1.14 / 1.25 | 1.29 / 1.44 | 1.45 / 1.65 | 0 |
| FE respond sent → returned (history respond) | 3.17 / 3.26 | 3.68 / 3.73 | 4.08 / 4.05 | 0 |
| respond1 returned → decision2 committed | 3.31 | 3.77 | 4.06 | 0 |
| decision2 committed → notified | 0.03 | 0.03 | 0.03 | 0 |
| **decision2 notified → transfer task2 read (incl. limiter)** | 0.09 | **38.31** | **38.64** | 0 |
| whole traced span (FE start sent → respond2 returned) | 25.10 | 94.69 | 98.95 | 0.227 |

(x / y = decision task 1 / 2.) All hops except the two queue-load hops have p95 < 5 ms. The traced span p90 (94.7) matches the SDK p90.

Limiter permits (both queues, all reps): Rust 15384 permits, **21.0% waited** (>1 ms), wait p50 49.4 / p95 50.5 ms.
Go: about 3170 permits per rep (same count as Rust, about 3070), **3.2% waited**, p50 37.7 / p95 50.3. Scope is identical: one limiter per shard per queue
(8 limiter ptrs in Rust; Go `queue_base.go:159` one `NewDynamicRateLimiter(MaxPollRPS)` shared by the queue's virtual queues).

## Why Rust waits more (model)
Each workflow needs 3 transfer permits on one of 4 shards (20 rps, burst 20 per shard). Closed-loop c1 demand per shard = 3 / base latency / 4.
Rust's unthrottled base (~25-27 ms) requests ~28 permits/s per shard, above the 20/s refill, so buckets drain and waits of 1/20 s = 50 ms appear
(two per slow workflow = ~100 ms plateau). Go's ~40-42 ms base requests ~18/s and stays just under. `perf-c1-hop-trace-sim.py`:
base 42 ms → 2.5% waits, p90 42; base 27 ms → 16% waits, p90 114 (base 40/30 → 3.9% / 13%). This matches the measured 3.2% / 21%.
The earlier config-only confirmation (run 45553865574, both arms `history.transferProcessorMaxPollRPS=1000`) removes the wait:
Rust p95 28.3 vs Go 45.5 ms. The faster-CPU runners where Go's p90 rises to ~85 ms (limiter study) are the same effect hitting Go.

## Verdict
The extra ~40-100 ms is in history: notify → transfer task read, i.e. `VirtualQueue::load_and_submit_tasks` waiting on the per-shard
`task_load_rate_limiter` (default `history.transferProcessorMaxPollRPS` = 20). Matching, frontend, RecordStarted and respond hops are all < 5 ms.
The limiter, its scope and its permits per workflow match Go. Rust binds it more often only because every other hop is faster.
**Not a divergence → no program change and no rerun** (changing the limiter or its default would break Go parity; raising the default is a
product decision that helps both). The trace code stays as opt-in diagnostics (inert when the flag is off).
