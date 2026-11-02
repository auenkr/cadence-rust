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
