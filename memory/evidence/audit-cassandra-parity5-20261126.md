# Cassandra parity audit 5 (2026-11-26) — DONE (fix unproven by repro; 1 loss was rare)

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

## CI evidence (head 8f21569b54b0c1dec47553a006e83721fec2a0f2)
- rust-ci cadence-history: success https://github.com/auenkr/cadence-rust/actions/runs/45767478323
- rust-ci workspace: success https://github.com/auenkr/cadence-rust/actions/runs/45767482662 (cadence-persistence not run separately: no persistence code changed)
- recovery-timing cassandra kill_history,cold_restart 3 reps: success, lost=0 for all 6 arms (Go 319/361/342, Rust 309/312/323) https://github.com/auenkr/cadence-rust/actions/runs/45768696892
- Rust SHA256: cadence-server 104a9ddb6232babd44f3ece63654f439c068f5bf471882a7686717d5827d020e, cadence-history bcc6e9be02e0f34d7a25886ffd1e372bd788e05664927136dd4cd2a4e5493a0c
- Caveat: loss was ~1/895, so 3 clean reps do not prove the fix; the cancellation hypothesis is unconfirmed. Open: gocql retry policy, history_execution_cassandra.rs read-path audit.
