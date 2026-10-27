# Matching native queue source review (2026-10-28)

Scope: read-only review of `rust/crates/cadence-persistence/src/matching_native_queue.rs` (`DurableTaskQueueBytes`) against `matching_queue.rs` (`DurableTaskQueue`) and Go `service/matching/tasklist/{task_reader,task_writer,db,task_gc}.go`. No source, Cargo, Go or CI commands were run. Base: master f49a3339a (CI 40586873102 checks a04f6ab98 and excludes this work).

## Verdict
No confirmed logic mismatch against the legacy queue or Go. The native file is a type-level port of the legacy file; every non-test function body is identical apart from the types (`TaskDataBytes`, `AllocatedTaskBytes`, `TaskListKeyBytes`, `TaskListDataBytes`) and the error adapter. No `String`/lossy conversion appears on the key or task path.

## Function-by-function (native vs Go)
- `open` / `block` / `max_read_level = next_id-1`: Go `task_writer.go:112` (`block.start-1`). Same. Lease failure in `open` does not stop or observe, same as legacy.
- `append_serialized` rollover: lease renewal under the `database` gate per attempt, with the sleep outside the gate (Go taskListDB mutex). It sends the previous `range_id` and `metadata.kind`, then recomputes the block. Observer is called with success/failure, and a failure stops the queue. Same as legacy.
- Write failure: `max_read_level` advances to the last allocated ID (when > 0) before `check(result)`. Matches Go `task_writer.go:286-303`. Allocated IDs are never reused.
- Source-domain keys: `create_tasks_with_domains` gets `(source_domain, &task)` per task. `append` defaults to `key.domain_id`. `rewrite_failed` uses `key.domain_id`, which is correct: Go re-appends the read-back `TaskInfo`, and the reader only sees rows under its own domain key.
- Retry and error adaptation (`adapt`/`restore`/`retry`/`retry_lease`): `Legacy` and `Backend` map to the legacy variants, so transient classification is preserved. Other native errors (`ConditionFailed{message}`, `MalformedSqlRunId`) are non-transient, so they are stashed under a sentinel `ConditionFailed` and restored unchanged. When a previous transient error exists, the shared policy returns `previous` and the stashed native error is dropped. This matches Go `ThrottleRetry.Do` (`retry.go:233-240`, returns prevErr). The stash is local per call, so there is no cross-call leak.
- `is_condition_failed` covers both the native `ConditionFailed` and the legacy variant. `check_with(report)`: the writer and periodic ack report, while the partition update and the final ack do not. All callers stop the queue. Same as legacy.
- Reader (`ReaderGate`, `load_reader_turn`, `scan_batch`): the gate serialises turns. Read starts from the ack manager's read level up to `max_read_level`, over at most 10 ranges, and a non-empty range returns with `read_level = upper`. An empty scan sets the read level and, if the ack level is unchanged, also the ack level. Matches Go `task_reader.go:328-347`. `register_read` re-checks `active` and expires with `expiry > 0 && now > expiry`, which is Go `!IsZero && After(epoch) && Now.After(expiry)`. `GO_ZERO_TIME_NANOS` is negative, so it is treated as unexpired. An expired row advances the read level only and is never put in flight. `read_batch_into` stops at the first rejected admission, leaving later rows unread, as legacy does.
- Ack: `complete` removes the row from `in_flight` and acks it. `persist_ack_impl` takes the `database` gate, so ack and lease renewal do not interleave. It fences on `range_id` and publishes `metadata` only after success. `persist_final_ack` skips the `active` check, so it works after stop. Partition config follows the same pattern.
- GC: `run_garbage_collection*` uses the shared `TaskGarbageCollector`, with the live batch provider and default 100/1s. `collect_garbage` deletes `< ack` with the limit. Same as legacy.
- Stop and observers: `stop` is idempotent via swap and calls the waker once, and `set_stop_waker` fires immediately if already stopped. No lock is held across the callbacks. Same.
- Sticky expiry and metadata: `at_creation` is a copy of `TaskData::at_creation`. A non-positive timeout gives the Go zero time, and `append_pending` stamps the creation time from `SystemTime`. `rewrite_failed` uses timeout 0 and a fresh `created`. Same.

## Not a defect: native queue is not adopted yet
Only `lib.rs` references `matching_native_queue`. `cadence-matching` (`matching_reader.rs:1091`, writer and registry) still use the legacy `DurableTaskQueue`. Until the writer, reader and registry switch, the raw-byte guarantee holds only in this module and its tests. Production still goes through the UTF-8 model. The next adoption step is to convert the writer, reader and registry call sites.

## Raw-byte invariants
Identifiers (`GoString` name, workflow_id, raw run_id, `ConditionFailed.message`) are never converted to `String`. The only `String` in the path is the legacy-adapter sentinel and diagnostic text on `Backend`. Pass: confirmed by reading; no `to_string`, `from_utf8` or `lossy` use.

## Test substantiation and missing regressions
The native test module has 7 tests against SQLite only. The legacy file has about 30. Tests that exist and are meaningful: raw-name isolation (`\xff`, `\xfe`, U+FFFD), stale-lease fencing plus observer, rollover (range 2 after 3 tasks with block size 2), ack persist and reopen, GC row counts, expired skip, cross-domain row key.
Gaps and weak claims:
1. `range_rollover...never_reuses_ids` only checks IDs 1,2,3 and range 2. It never exercises reuse after a failed write.
2. No test for write failure advancing `max_read_level` (a failing store with a following reader). Port the legacy fixtures at `matching_queue.rs:1109,1295`.
3. No test of the `adapt`/`restore` stash path: a native `ConditionFailed` or `MalformedSqlRunId` must survive `retry` unchanged, and a transient `Backend` must retry. No test that the message bytes (non-UTF-8 name) round-trip in a native `ConditionFailed`.
4. No `rewrite_failed` test (durable-before-ack, `key.domain_id`, timeout 0).
5. No reader tests: `reader_turn` gate exclusivity across threads, partial admission leaving rows unread, per-row expiry clock, 10-range scan budget, empty reads advancing the ack level across renewed ranges (legacy 1136-1300).
6. The expired test does not assert that the expired row stays out of `in_flight` or that the read level advanced.
7. No partition-config test (raw `PartitionConfigBytes` persistence and rollback on failure), no `persist_final_ack`/observer-suppression test, no GC batch provider or `stop_waker` test.
8. The cross-domain test only asserts 0 rows under the target key. It should also assert the row exists under the source-domain key.
9. No Cassandra or MySQL/Postgres bytes-store coverage, only SQLite.

Minimal fixes: none to production code. Add the tests above (mostly mechanical ports of the legacy fixtures with `\xff` names) before any adoption.

Parent follow-up: strengthened cross_domain_append_keeps_source_domain_row_key to read the source-domain row and assert its workflow bytes. Other missing regressions listed above remain open; source review remains distinct from runtime evidence.
