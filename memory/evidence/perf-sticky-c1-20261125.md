# Sticky c1 tail investigation (2026-11-25)

Run: https://github.com/auenkr/cadence-rust/actions/runs/45563123553 (success).
Inputs: service=all, sticky=true, concurrency=1, repetitions=3, trace_decision=true,
rust_services=frontend,matching,history, source_ref=master, control_ref=02ab1e700 (head 02ab1e700fef481e3ab9b6f9709a72eda55bf34f).
Runner: AMD EPYC 7763, 4 vCPU, 15 GiB, Ubuntu 26.04 (see artifact machine-spec.md). Zero failures, verifier exit 0.
Three arms: Go, Rust control (master, all-Rust), Rust candidate (same tree as control).

| Arm | >60 ms per rep | Pooled >60 ms | p50 / p95 / p99 ms (per rep) |
|---|---|---|---|
| Go | 15, 57, 34 | 106/3000 (3.5%) | 45.3/54.9/62.0; 49.0/60.4/65.9; 46.2/57.1/96.1 |
| Rust control | 160, 145, 157 | 462/3000 (15.4%) | 29.7/97.9/102.7; 31.6/99.6/114.3; 31.0/99.5/112.2 |
| Rust candidate | 166, 166, 169 | 501/3000 (16.7%) | 29.8/99.3/110.8; 32.1/100.0/110.7; 31.1/100.1/108.0 |

## Hop analysis (Rust traces, all 3 reps, warmup included)
| Interval | Samples | p50 ms | p95 ms | p99 ms | >30 ms |
|---|---:|---:|---:|---:|---:|
| Matching resolve start to AddDecisionTask send | 6061 | 0.08 | 0.11 | 0.14 | 0 |
| AddDecisionTask send to return (incl. sticky dispatch) | 6060 | 4.19 | 6.44 | 8.91 | 2 |
| AddDecisionTask send to RecordDecisionTaskStarted ingress | 6060 | 0.75 | 0.96 | 1.13 | 2 |
| RecordDecisionTaskStarted handler | 6060 | 2.97 | 5.11 | 7.58 | 0 |
| Transfer task read to executor pickup | 6060 | 0.15 | 0.32 | 0.45 | 0 |
| c1_queue_rate_limiter_wait (>5 ms events) | 2228 | 41.2 | 50.3 | | |

Every slow (>60 ms) workflow (166/166/169) had a queue v2 task-load limiter wait >30 ms;
only 50/44/39 of the fast workflows did. Sticky Matching, Frontend forwarding and
RecordDecisionTaskStarted add no 40-50 ms hop.

## Conclusion
No Rust-only wait in the sticky path. The 40-50 ms stall is the same
`taskLoadRateLimiter.Wait` (default history.transferProcessorMaxPollRPS=20, burst 20, per
queue/shard) found in perf-c1-budget; sticky makes no difference to the cause. Go source
(queuev2/queue_base.go, virtual_queue.go) uses the identical limiter and Wait-before-load, and the Rust
DynamicRateLimiter matches Go's. Rust issues ~3.1 loads per workflow (one per transfer task notification) and
runs ~25 wf/s vs Go ~22, i.e. ~19 loads/s/shard against 20/s supply, so it saturates the
bucket while Go stays just below. Not established: Go's loads per workflow (Go has no trace).
No code change was made, hence no unit test or CI/gate/benchmark revalidation was needed; the
c1 >60 ms target remains unmet. A fix would need a parity-changing choice (limiter rate or load coalescing).
