# Matching v6 lifecycle/persistence source audit (read-only)

Candidate: `matching-acceptance-20261027-v6`, 3055d0c6c. Source reading only; no
Cargo/Go/runtime executed, no CI inspected. Prior Luna thread (6cda5c04) failed
after ~2 min with only a plan-level conclusion; nothing recoverable beyond that.

## Source parity checklist

| Item | Go | Rust | Result |
| --- | --- | --- | --- |
| Stop order: evict self, sleep ShutdownDrainDuration, then close stopC, handler.Stop, Resource.Stop | go/service/matching/service.go:138-160 | launcher.rs:1699-1735 (withdraw, then `runtime.shutdown()`, then `stop_after_drain`); matching_runtime.rs:651-690 | Match. Listeners stay live during drain (launcher.rs:1697 comment). |
| Drain duration read live from dynamic config | config.go:216 `MatchingShutdownDrainDuration` | matching_runtime.rs:794 `live_shutdown_drain_duration` | Match. |
| EvictSelf error discarded | service.go:144 | launcher.rs:1767-1770; matching_runtime.rs:672-675 | Match. |
| Engine shutdown flag set only after drain; executor and all task-list managers stopped, semaphore buckets stopped, domain callback unregistered, wait for completion | handler/engine.go:206-219 | matching_runtime.rs:668-675, 700-760 (`begin_shutdown`, registry shutdown, `wait_for_writers`, `stop_semaphores`, `unregister_callback`) | Match. |
| Writer fatal not lost during shutdown | n/a (Rust-only SQL writer actors) | launcher.rs:1748-1755 | Sampled after joining the writers. |
| Task-list ownership fencing (range_id lease, conditional update/delete) | sql_task_store.go:~100-140; nosql_task_store.go:139 | MySQL/Postgres: `LOCK_TASK_LIST ... FOR UPDATE`, `UPDATE`, `DELETE ... AND range_id=?` (matching_mysql.rs:453-457, matching_postgres.rs:284-288, 528-540). Cassandra: `IF range_id=?` LWT (matching_cassandra.rs:36-40, 458-459). SQLite: matching_sqlite.rs:338-394. | Match for all four backends. Stale-lease message mirrors Go's `leaseTaskList:renew failed:...`. SQL backends hard-code `haveRangeID:0`, as Go's SQL path does (`rangeID` var, sql_task_store.go:137). |

## Confirmed blockers

None found in lifecycle, drain or ownership persistence.

## Uncertainties and notes (not confirmed bugs)

1. matching_runtime.rs comment at `with_membership_evictor` ("remains absent in
   the current main assembly") is stale. The launcher performs
   `lifecycle.withdraw()` before `runtime.shutdown()` instead of installing the
   evictor (launcher.rs:1699), so the observable order is equivalent. Check at
   runtime that withdraw does not skip the drain sleep. `evict_self` is `None`,
   so the sleep still runs via `drain_service`.
2. `serving.abort_all()` (launcher.rs:1738) aborts listeners after the runtime
   drain, so in-flight RPCs are cancelled and not gracefully finished. Go's
   dispatcher stop is likewise immediate. This is a parity note and not a gap.
3. I did not trace the Go task-GC, reader ACK or writer batching paths in this
   pass. Earlier notes (matching-peer-lifecycle.md, the v5 ledger) cover them
   only at source-map level. The restart/crash/idle-recovery behaviour
   remains an exact-pin runtime gate. The v5 ledger shows those lanes
   pending, and this audit does not credit any of them.

## Closure scope

Source closure is bounded to the shutdown/drain ordering and range-id lease
fencing across the four backends. The Matching service is not declared
complete. Runtime gates (restart, drain, crash recovery with Go Frontend,
History and Worker) remain the parent's responsibility.
