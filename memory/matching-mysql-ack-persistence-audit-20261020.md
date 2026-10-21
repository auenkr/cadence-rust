# Matching acknowledgement persistence audit (2026-10-20)

Audit of the claim that "Go persists the acknowledgement inside
`taskListDB.CompleteTask` while Rust Matching defers it to the timer". The
claim was wrong: Go's `taskListDB` has no `CompleteTask` method. Go and Rust
Matching persist the acknowledgement in the same way, and there is no
behaviour gap.

## What Go does

- `go/common/messaging/ackManager.go:62` — a new acknowledgement manager
  starts at `ackLevel: -1`.
- `go/common/messaging/ackManager.go:80-82` — `ReadItem` sets
  `m.ackLevel = itemID - 1` while the acknowledgement is still `-1`.
- `go/common/messaging/ackManager.go:91-117` — `AckItem` advances the
  in-memory acknowledgement to the highest continuously completed id.
- `go/common/messaging/ackManager.go:138-146` — `SetAckLevel` only ever
  advances the acknowledgement.
- `go/service/matching/tasklist/task_reader.go:430` — the completion of a
  delivered task calls `tr.taskAckManager.AckItem(task.TaskID)`. This is an
  in-memory update only; no statement is written here.
- `go/service/matching/tasklist/task_reader.go:392-397` — `persistAckLevel`
  reads `tr.taskAckManager.GetAckLevel()` and calls `tr.db.UpdateState(ackLevel)`.
- `go/service/matching/tasklist/db.go:117-136` — `taskListDB.UpdateState` is
  the only writer of the acknowledgement. It calls
  `store.UpdateTaskList(...)` and only then updates `db.ackLevel`.
- `go/service/matching/tasklist/task_reader.go:227,269,290` — `UpdateState` is
  reached only from the reader's acknowledgement timer, which is created with
  `tr.config.UpdateAckInterval()` and reset after every tick.
- `go/service/matching/tasklist/testing.go:155` — the only `CompleteTask` in
  the matching package is the `TestTaskManager` mock. There is no
  `taskListDB.CompleteTask`.
- `go/common/persistence/sql/sql_task_store.go:146-196` — the lease reads the
  persisted blob, keeps `ackLevel = tlInfo.GetAckLevel()`, bumps the range by
  one and writes the row back. The lease publishes the persisted
  acknowledgement, not an in-memory one.
- `go/service/matching/tasklist/task_reader.go:430-431` — the completion
  updates the in-memory acknowledgement and then runs the garbage collector
  with that in-memory value, `tr.taskGC.Run(ackLevel)`. The collector deletes
  the rows with `task_id <= ack` from the database, but the acknowledgement
  itself is not written here.
- `go/service/matching/tasklist/task_reader.go:172-177` — when the reader
  stops, Go first persists the acknowledgement with `tr.persistAckLevel()`
  and then runs the collector with `tr.taskGC.RunNow(...)`.

## What Rust does

- `rust/crates/cadence-common/src/ack_manager.rs:14-24` — the manager starts
  at `ack_level: -1`, like Go.
- `rust/crates/cadence-common/src/ack_manager.rs:29-45` — `read_item` sets
  the acknowledgement to `id - 1` while it is still `-1`, like Go.
- `rust/crates/cadence-common/src/ack_manager.rs:47-70` — `ack_item` advances
  the in-memory acknowledgement to the highest continuously completed id,
  like Go.
- `rust/crates/cadence-common/src/ack_manager.rs:72-80` — `set_ack_level`
  only ever advances, like Go.
- `rust/crates/cadence-persistence/src/matching_queue.rs:465-471` —
  `complete()` calls `self.ack.ack_item(task_id)`, an in-memory update only,
  like Go's `task_reader.go:430`.
- `rust/crates/cadence-persistence/src/matching_queue.rs:487-489` and
  `521-545` — `persist_ack` and `persist_final_ack` write the acknowledgement
  with `update_task_list`, the database write of the timer path, like Go's
  `persistAckLevel` and `UpdateState`.
- `rust/crates/cadence-server/src/matching_registry.rs:1549-1550` and
  `1688-1770` — the registry's maintenance loop persists the acknowledgement
  of every owner whose deadline passed. The interval comes from the dynamic
  configuration `matching.updateAckInterval`
  (`rust/crates/cadence-server/src/grpc_matching.rs:565-577`, default 60
  seconds) and the maintenance tick is 10 seconds
  (`rust/crates/cadence-server/src/matching_runtime.rs:163`).
- `rust/crates/cadence-persistence/src/matching_mysql.rs:647-710` — the lease
  decodes the persisted row, bumps the range and writes the decoded metadata
  back. It publishes the persisted acknowledgement, like Go's lease.
- `rust/crates/cadence-server/src/matching_dispatch.rs:389-393` — the
  completion updates the in-memory acknowledgement with `queue.complete(task)`
  and then runs the collector with that in-memory value,
  `queue.run_garbage_collection(false)`.
- `rust/crates/cadence-server/src/matching_registry.rs:88-99` — when an owner
  is finished, Rust first persists the acknowledgement with
  `queue.persist_final_ack(now)` and then runs the collector with
  `queue.run_garbage_collection(true)`.

## Conclusion

Both implementations persist the acknowledgement on the acknowledgement timer
and on the unload, both keep the completion in memory, both run the garbage
collector with the in-memory acknowledgement right after a completion, and
both persist the acknowledgement before the forced collector run of an
unloaded owner. The only
difference is the worst case latency of the timer: Go's reader timer fires
exactly every `UpdateAckInterval`, while Rust Matching's registry ticks
every 10 seconds and persists an owner when its own 60 second deadline
passes, so an owner can be persisted up to one maintenance tick later. That
is a timing nuance of the same timer driven behaviour, not a semantic
divergence.

The earlier claim in the evidence notes of the runs
`20261020T183524Z` and `20261020T184311Z` is corrected: there is no
acknowledgement persistence parity gap, and the gate keeps the persisted
acknowledgement assertions strict for every queue with an observed durable
task id.
