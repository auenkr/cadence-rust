# Matching Cassandra task TTL/expiry audit (2026-10-21)

Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/matching-semaphore-durable`
Audited source HEAD: `58fb1a17b68347d23a8fa37484e77eed1f1c1d72`

## Finding

The suspected `Utc::now()` versus Matching `now_nanos` discrepancy is not a
wall-clock parity defect by itself. Go's `go/common/persistence/nosql/nosqlplugin/cassandra/tasks.go`
`SelectTasks` reads `TTL(task)` and sets the plugin row's `Expiry` to
`time.Now().Add(ttl seconds)`. Rust's corresponding code did the same using
`Utc::now()`. The persistence interface `MatchingTaskStore::get_tasks` has no
clock argument; the queue's injected clock is consulted after the fetch in
`matching_queue.rs::register_read`.

The actual parity defect is one adapter layer later. Go's
`go/common/persistence/nosql/nosql_task_store.go::toTaskInfo` copies domain,
workflow/run IDs, task ID, schedule ID, created time, and partition config,
but deliberately omits `nosqlplugin.TaskRow.Expiry`. Consequently Cassandra
TTL is enforced by Cassandra deleting the row; Go Matching does not locally
drop a returned task by the reconstructed TTL deadline. Rust had propagated
that deadline into `TaskData.expiry_nanos`, and `DurableTaskQueue::register_read`
could drop the returned task when its clock advanced past that deadline.

The narrow fix makes Cassandra-decoded `expiry_nanos` zero regardless of the
reported TTL. For example, a returned row with `TTL(task)=1` has a decoded
`created_nanos` matching its UDT `created_time` (e.g. `1_700_000_000_123_000_000`)
and `expiry_nanos=0`; it remains eligible for Matching admission while present.
The Cassandra row TTL continues expiring the row. Existing TTL write behavior
is unchanged: whole-second truncation and the `157_680_000` second cap remain
in `task_ttl`; bounded `(read_level, max_read_level]` query, result batch limit,
and lease range-fence CQL are unchanged.

## Source evidence

- Go Cassandra `SelectTasks`: `go/common/persistence/nosql/nosqlplugin/cassandra/tasks.go`,
  `TTL(task)` decode and `time.Now().Add(...)` assignment.
- Go adapter mapping: `go/common/persistence/nosql/nosql_task_store.go`,
  `toTaskInfo` omits `Expiry`.
- Go Matching expiration check: `go/service/matching/tasklist/task_reader.go`,
  `isTaskExpired`; Cassandra persistence tests also state that Cassandra uses
  TTL and expiry is not stored as task state in
  `go/common/persistence/persistence-tests/matchingPersistenceTest.go`.
- Rust persistence boundary: `rust/crates/cadence-persistence/src/matching_task_store.rs`,
  `get_tasks` has no `now_nanos` argument.
- Rust post-fetch clock use: `rust/crates/cadence-persistence/src/matching_queue.rs`,
  `register_read`.

## Validation

- Added deterministic source test `cassandra_task_ttl_is_not_promoted_to_matching_task_expiry`,
  checking `Some(1)`, `Some(0)`, and `None` TTL values yield the same decoded
  created timestamp and zero task expiry.
- Source-only `git diff --check` is the only validation command run. No build,
  tests, Go processes, services, or containers were run, as requested for this
  worktree/lane.
