# Frontend Cassandra mixed lane (2026-11-03)
Branch t3/cassandra-lane (= origin/t3/cassandra-runtime-boundary + master merge + fixes).
Fixes: (1) CassandraBlockingSession::run uses block_in_place on multi-thread runtime (panic "Cannot start a runtime within a runtime" from Matching domain-cache list_domains);
(2) domain-by-id lookup uses `SELECT domain.name FROM domains WHERE id=?` like Go (domains table UDT only has name);
(3) Cassandra domain decode: failover_end_time 0 = unset (else global domains looked failing-over, ADD-TRACE active=false, all tasks timed out).
Run 41431874094 (old head) failed with the runtime panic (real bug, not flake).
Final gate run 41511146717, head 5b36e7bb42eb103849867ac0a6f5c7a820448f9f, conclusion failure; Rust binary sha256 125edcc58cf6697980343428f451e5b7b9eb6a79090cdd78bdb1ef863d8499b0.
Passing now: all SDK start/complete/child/cancel/terminate/signal/query/task-list/schedule-create steps.
Remaining failing (only): sdk-list-open, sdk-list-closed, sdk-sched-backfill-completed, cli-list-workflows-grpc/tchannel -> "Operation is not supported".
BLOCKER: no Cassandra VisibilityStore in Rust (cadence-persistence/src/visibility_reader.rs has only SQL dialects; frontend visibility is None -> workflow_visibility::not_supported). Need port of Go nosql visibility (open_executions/closed_executions in keyspace cadence_visibility; go/common/persistence/nosql/nosqlplugin/cassandra/visibility*.go) and wiring in workflow_backend/with_visibility_store.

## Cassandra visibility store (branch t3/cassandra-visibility-store)
Ported Go nosql visibility reads: `cadence-persistence/src/visibility_cassandra.rs` (open_executions / closed_executions / closed_executions_v2 in the visibility datastore keyspace; Go CQL templates, native Cassandra paging state as page token, LocalOne, partition IN(?) for all-lists, ALLOW FILTERING closed lookup by workflow_id+run_id). Closed ordering by close_time is off by default (Go `system.enableReadFromClosedExecutionV2` default false; not read dynamically yet). Trait `VisibilityStore` gained `list_native` / `get_closed_by_workflow`; Frontend uses `list_page`. List/Scan/Count keep "not supported". Wired in matching_task_store.rs Cassandra branch (separate visibilityStore datastore gets its own session).
Gotchas: scylla tuple rows max 16 columns -> by-name derive needs ALL selected columns as fields (first gate run failed with timeouts for that reason: run 41514859344).
Validation (head 09d3f524e8bb3e1f83c28a8f3dfe8b595a647d31):
- cadence-persistence CI success: https://github.com/auenkr/cadence-rust/actions/runs/41517849226
- Cassandra mixed gate success, RESULT failures=0, GATE SUMMARY failed []: https://github.com/auenkr/cadence-rust/actions/runs/41517853946 ; Rust binary sha256 065da6a0e0902f5d318ba571fba14e112967cc05d0d08aeff92591e9700c3804
Not done: unit tests of live row decode, dynamic V2 ordering flag, workspace-wide clippy/test run.
