# perf-c1-trace (2026-11-25)

Instrumentation: `CADENCE_TRACE_DECISION=1` (off by default; once-initialized bool) logs `DTRACE t_ms=.. stage=..` lines
(a committed, b notified, c transfer picked up, c2 lock, d AddDecisionTask sent/d2 returned, e RecordDecisionTaskStarted received,
f lock acquired (wait, cache hit), f2 update done, g returned). Benchmark input `trace_decision=true` passes it to Rust History (paired-run.py);
lines land in `services-*.log` of the artifact. Both `cadence-history` and `cadence-server` mains enable the `decision_trace=info` target when set.

## Trace (run 45514180239, head 503d89793, history=rust, c=1, 1 rep, 1010 decisions)
| gap | p50 | p95 | >30ms |
|---|---|---|---|
| c->c2 transfer lock | 0.0 | 1.4 | 0 |
| d->e AddDecision->RecordStarted (incl. Go Matching) | 0.7 | 1.0 | 1 |
| e->g RecordStarted handler | 2.1 | 4.3 | 0 |
| e->f lock wait | 0.0 | 0.1 | 0 |
| **b->c commit/notify -> transfer processor picks up** | 0.1 | **40.0** | **215/1010** |
Lock wait, cache misses, Matching RPC and handler are all fast; the 40ms is entirely notify -> transfer pickup.

## Root cause
Go: `notifyingExecutionManager` notifies, then `updateMaxReadLevelLocked`, but both under the shard write lock, and the queue reader's
`getImmediateTaskMaxReadLevel` takes `s.RLock()`, so it can never read the level before it advanced. Rust read the level with no such
exclusion, so the woken pump read a stale max read level, missed the new task and idled until the next poll (~40ms). Rust, being faster,
hit the race more often than Go.

## Fix
Advance `update_max_read_level` before `notify_tasks` in engine_cache.rs write_update, engine_start.rs create, ndc_context_hooks.rs.

## Result (run 45519128738, head 8ea8106ca, history=rust, c=1, 5 reps x 1000)
Median p50/p95/p99: Go 41.4/145.5/260.5, Rust 27.5/118.3/252.9 (Rust p95 per rep 125.6/116.5/120.4/102.5/118.3; Go 131.4/226.9/145.5/90.3/156.4).
Done condition (Rust median p95 <= Go) met; runner noise is high (Go p95 90-227).
CI: rust-ci cadence-history run 45519123402 success.
