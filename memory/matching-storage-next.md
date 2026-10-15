# Matching durable storage and next boundary (2026-10-15)

Active service remains Matching. Do not move to History/Frontend/Worker yet.

## Current storage port

`cadence-persistence::matching_sqlite::SqliteTaskStore` accesses Go's exact
SQLite `task_lists` and `tasks` tables, copied into rust/sql/matching-sqlite.sql
and checked against the pinned Go schema. Open existing databases without DDL;
create() only bootstraps these two tables, not a full Cadence production schema.
Single SQLite database => SQL shard_id 0 (Go total DB shards 1).

`matching_blob` ports sqlblobs.thrift TaskInfo/TaskListInfo/partition configs,
including nil/empty containers, all timestamp nanoseconds, opaque run UUID bytes,
and deprecated counts. SQL blobs have NO 0x59 version preamble. Both `thriftrw`
and raw-block `thriftrw_snappy` are supported, using bundled native SQLite and
Rust Snappy; no Go compiler/server is needed for Rust runtime.

Store supports lease/renew/steal, conditional range fencing, metadata updates,
atomic batch inserts, ordered (read,max] task reads, counts, single/bounded batch
deletes and fenced task-list deletion. Sticky/ephemeral metadata expiry is 24h;
normal list expiry is Unix epoch. Task expiry belongs to the reader, so reads
return expired rows. No-timeout task expiry uses Go zero time nanos
-6795364578871345152, NOT Unix epoch. Lease error's haveRangeID:0 is a Go quirk.
Recreating a task-list lease does not delete orphan task rows.

## Validation

Run `python3 tests/run-matching-storage-compat.py` in tmux cadence-rust.
Eight real Go serializer fixtures cover raw bytes plus compressed decode, UUIDs,
UTF-8, zero/negative times, scalar zero fields, nil/empty maps/lists and configs.
Focused Rust tests passed for schema, metadata expiry, invalid blobs, recovery,
namespace/type isolation, batch rollback, read bounds, deletes and range fencing.

Bidirectional actual-Go store interop PASSED:
Go sql.NewFactory/NewTaskStore on SQLite writes uncompressed AND compressed tasks
and metadata; Rust opens the same file, reads them, steals lease, rejects stale
append, writes task/config metadata; original Go store reads Rust rows, checks
its own stale-write rollback and bounded delete; Rust verifies the final rows.
Go test uses overlay in external sql_test package with real SQLite plugin and
serializer, no mocked DB and no Go source-tree edits. Driver cleans up its DB.
Evidence /tmp/cadence-matching-storage-interop2.log, exit 0.

Final full runner PASSED 19 stages, 303 workspace tests and strict clippy:
/tmp/cadence-matching-storage-final/results.json and outer exit 0. Includes
bidirectional SQLite interop, all existing differential checks, isolated
listeners/config/startup, public/internal gRPC and Go SDK TChannel. Test server
was cleaned up. Initial clippy flagged only a test helper style lint, corrected
before the final run. The Go-seeded Rust interop test is ignored in plain Cargo
runs and explicitly executed twice by the differential runner.

## Not yet ported/integrated

This is a standalone persistence component. grpc_matching still delegates to
embedded frontend/execution state. Configured production persistence selection
still does not use this backend. No mixed-service replacement claim is valid.
Other backends, full schema migration/version tooling, list-task-list Gob cursors,
orphans/scanners, storage error transport mappings and broad failure injection
are unfinished. Do not mark the whole SQL store file completely ported.

Next: matching-owned queue writer/reader and History RPC delivery. Port Go
service/matching/tasklist/task_writer.go, task_reader.go, task_gc.go and manager
lifecycle: lease range => [(range-1)*rangeSize+1, range*rangeSize]; preserve
reserved IDs on append failure, maxReadLevel, read/ack recovery and fencing.
Reader must not lose a task when polling is cancelled or History fails. Expired
and already-started tasks may be acknowledged only under Go's rules. Requeue
before acknowledging original rows on recoverable delivery failures.

History calls: service/matching/handler/engine.go recordDecisionTaskStarted
(1471), recordActivityTaskStarted and createPollFor*Response. Request UUID stays
constant across RecordTaskStarted retries. Use YARPC metadata rpc-service:
cadence-history, rpc-caller: cadence-matching, rpc-encoding: proto.

Decision token must use History's actual scheduledEventID and attempt. Public
frontend fetches history after Matching returns metadata/branch token. Activity
responses come from History's scheduled event and include attempt timestamps,
heartbeat details, workflow type/domain and opaque headers/payloads. Error paths
include already-started/not-found/completed drops, transient retries and inactive
domain RefreshWorkflowTasks before drop. Passive matching, queries, forwarding,
isolation groups, rate limits, sticky availability and semaphore ownership remain.

Mixed topology also needs domain cache, membership/discovery and routing. The
all-Go baseline harness currently boots all four original roles; it cannot yet
substitute Rust Matching. Do not confuse shared Go/Rust persistence interop with
mixed-service workflow validation.
