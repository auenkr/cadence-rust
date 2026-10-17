# Matching maintenance live configuration (2026-10-17)

This patch wires the existing acknowledgement and idle interval settings into
Matching's durable queue owner. It does not complete Matching.

- `matching.updateAckInterval` uses task-list dynamic-config filters. The Rust
timer samples when the owner is created and after each maintenance attempt,
  matching Go's `task_reader.go` `NewTimer`/`Reset` read points. A background
  pump regression uses a 20 ms live interval and 60 s fallback.
- `matching.idleTasklistCheckInterval` is sampled once when each owner is
  constructed and stored with that owner. Its idle sweep reuses the sampled TTL;
  a changed config only applies to newly created owners. This follows
  `task_list_manager.go`, which reads the value once before constructing
  `liveness.NewLiveness`. `matching.maxTasklistIdleTime` remains unused because
  the inspected Go manager path does not consume it.
- `matching.maxTaskDeleteBatchSize` remains explicitly unwired. Do not coerce
it to `NonZeroU32`: Go samples its signed `int` once inside `tryDeleteNextBatch`
after acquiring the lock, then passes it to storage and loops while `n >=
batchSize`. The Rust collector currently accepts `NonZeroU32`, and its SQLite
store accepts `i64` but rejects a zero limit. A correct port must retain signed
batch values through the GC/store boundary and test the loop using a bounded
injected-store oracle (never invoke a zero-sized GC loop without a bound or
cancellation). Go's SQL store passes the `int` limit to the SQL plugin and
reports `RowsAffected`; precise zero, negative, and oversized effects therefore
need backend-specific oracle evidence before wiring the live setting. On a
64-bit Go build the dynamic setting is a signed 64-bit `int`; values above the
Rust store's `i64` range cannot originate from it, while values above `u32::MAX`
are still valid and must not saturate if Rust is to preserve the signed range.

Go sources checked: `go/service/matching/config/config.go`,
`go/service/matching/tasklist/task_list_manager.go`, `task_reader.go`,
`tasklist/task_gc.go`, `tasklist/db.go`, and
`go/common/persistence/sql/sql_task_store.go`. Config providers use task-list
filters. The prior `recordTaskStartedTimeout` draft was retained and remains
wired. Its test compile errors (metadata type comparison and a shadowed helper)
were corrected without removing the timeout behavior.

Implementation: `rust/crates/cadence-server/src/grpc_matching.rs`,
`matching_registry.rs`, and `rust/crates/cadence-persistence/src/matching_queue.rs`.
The current GC default remains unchanged (100 rows, one second between small
drains); the live dynamic setting is unfinished.

No Matching completion claim is made; see `memory/service-migration-plan.md`
for the remaining gates.
