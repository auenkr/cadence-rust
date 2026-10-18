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
- `matching.maxTaskDeleteBatchSize` is wired through the task-list filtered
  admin dynamic-config provider to the durable queue's GC. The value stays
  signed `i64` through the lock-protected GC read and SQLite `LIMIT`. Zero
  reaches storage, where the Go SQLite plugin rejects it; the collector breaks
  on that error without advancing its cursor. Negative values reach SQLite,
  whose negative `LIMIT` deletes all eligible rows; Go then repeats because a
  nonnegative row count cannot be less than a negative batch. Keep that loop
  behavior covered only with a bounded injected error after observed calls.
  Positive values above `u32::MAX` remain intact. See
  `memory/evidence/matching-live-config-2026-10-18.md` for the Go backend and
  collector oracle results.

Go sources checked: `go/service/matching/config/config.go`,
`go/service/matching/tasklist/task_list_manager.go`, `task_reader.go`,
`tasklist/task_gc.go`, `tasklist/db.go`, and
`go/common/persistence/sql/sql_task_store.go`. Config providers use task-list
filters. The prior `recordTaskStartedTimeout` draft was retained and remains
wired. Its test compile errors (metadata type comparison and a shadowed helper)
were corrected without removing the timeout behavior.

Implementation: `rust/crates/cadence-server/src/grpc_matching.rs`,
`matching_registry.rs`, and `rust/crates/cadence-persistence/src/matching_queue.rs`.
The GC fallback remains 100 rows with the existing one-second interval. Every
default durable queue GC invocation reads the live setting; its value is
sampled after the collector lock is acquired. The explicit batch override is
retained for focused collector tests.

No Matching completion claim is made; see `memory/service-migration-plan.md`
for the remaining gates.
