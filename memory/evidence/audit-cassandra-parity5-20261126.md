# Cassandra parity audit 5 (2026-11-26) — IN PROGRESS (CI pending)

## 1. Session config
| Item | Go | Rust | Result |
|---|---|---|---|
| Request timeout default | nosqlplugin/cassandra/plugin.go:43,130 `defaultSessionTimeout=10s` when cfg.Timeout==0 | cassandra.rs `duration_option(..,10s)` | **match** (parity4 "open" was wrong: Go applies 10s, not gocql 600ms) |
| Connect timeout default | plugin.go:44,134 2s | 2s | match |
| MaxConns default | gocql NumConns 2 | 2 | match |
| Retry policy | gocql ClusterConfig default (module not vendored locally, could not re-read source) | scylla default policy | **open**: unverified, no change |

## 2. Statement audit (Go cql vs Rust consts)
| File | Result |
|---|---|
| matching_domain_cassandra.rs queue/queue_metadata/domains/domains_by_name_v2 | text equivalent (whitespace/`COUNT(*)` vs `COUNT(1)` only) |
| matching_domain_cassandra.rs domain_audit_log SELECT | Rust omits `domain_id, operation_type` columns (known from WHERE); same rows |
| semaphores_cassandra_native*.rs (semaphore_tokens, semaphore_metadata) | identical statements |
| history_execution_cassandra.rs read paths | **not audited** this round |

## 3. Lost workflow in run 45759874474 (rep 1 rust; "rep 2" label = 3rd arm pair)
Artifacts: recovery-timing-manual/recovery/lost-01-rust/680ed893-....json, samples-01-rust.jsonl, services-01-rust.log.
- History: only events 1 WorkflowExecutionStarted, 2 DecisionTaskScheduled, 3 WorkflowExecutionTimedOut (90s). DecisionTaskStarted never happened → **first-decision loss**, not the activity StartToClose race.
- Workflow created 05:46:50.42 inside the cold_restart window (driver needed attempts=2).
- Same second, Rust history logged `potential severe mismatch between db and cache states` (cached-immediate-queue-reader, transfer, mode enabled): DB has tasks 4194340/4194341 (run 2eaf3348, also a start retried by the frontend, "WorkflowAlreadyStarted") that are absent from the cache. Go arms never log it.
- Root-cause hypothesis (consistent with all evidence): the request future is dropped by tonic when the frontend times out/retries (cold restart), between `create_workflow_execution(...).await` landing in Cassandra and `notify_tasks`. Go always runs the notifier (ctx timeout → `PersistenceError` notify → cache Clear). Rust never notified, so the cached queue reader (default "enabled") served without the task.
- Fix: `WriteCancelGuard` (engine_cache.rs) armed around task-bearing create/update writes; on drop before completion notifies with `persistence_error=true` (clears cache), as Go's timeout path does. Not proven by a reproducing test; verified only by CI recovery runs below.
