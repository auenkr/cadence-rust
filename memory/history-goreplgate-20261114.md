# History Go<->Rust replication gates (2026-11-14)

Head SHA gated: b54d550ce146ee6f74f31c6058770c4501c85661 (branch history-goreplgate).
Workflow: frontend-mixed-gate.yml (`repl_rust_cluster=A|B`, `kafka=false`). Pass = run success and `GATE SUMMARY failed: []`.

| Direction | mysql | postgres | cassandra |
|---|---|---|---|
| A: Go active -> Rust standby | 43533386756 | 43533390058 | 43533393280 |
| B: Rust active -> Go standby | 43534506629 | 43534510318 | 43534514011 |

rust-ci package=cadence-history: 43533396763 (success).

## Bugs fixed (Rust matched to Go)
- query_matching yarpc headers.
- state_builder parent-domain fallback; domain-aware NDC state builder (child history on standby).
- StartTransaction current-version bump (`UpdateCurrentVersion(failoverVersion,false)`) in engine_cache `load_state` (post-failover DomainNotActiveError on A).
- CronOverlapPolicy serialized as integer in Go-JSON mode (Go FE decode).
- Timer visibility ts lost on NDC persist path: `timer_task_from_struct` returns visibility 0 (blob lacks it), so `allocate_timer_ids` moved retention `DeleteHistoryEvent` to now and deleted replicated workflows right after failover. Hook now copies the ts from the outer Task.

## Known divergences / notes
- Rust mutable-state JSON shape differs from Go; gate normalizes version histories.
- Decision-failover flush of `startTransactionHandleDecisionFailover` not ported in engine_cache.
- Temporary DIAG logs (timer executor, sql_workflow_store eprintln, task generator, engine_tasks) remain; remove later.
- No dedicated unit test for the timer visibility fix; covered by gate A.
