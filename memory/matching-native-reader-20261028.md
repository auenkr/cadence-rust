# Matching native reader (2026-10-28) — UNVERIFIED source only

New file: `rust/crates/cadence-matching/src/matching_reader_native.rs`. No Cargo, rustfmt, Go or tests ran; parent owns CI. Legacy `matching_reader.rs`, limiter and `lib.rs` are untouched.

## Integration requirements
- Register `pub mod matching_reader_native;` in `cadence-matching/src/lib.rs` and add the file to the CI rustfmt list.
- Needs `cadence_persistence::matching_native_queue` (and its dependencies) to be exported. It also uses `tempfile` and `SqliteTaskStore: MatchingTaskStoreBytes` in tests, both already present.
- Production adoption (registry/manager) must construct `NativeTaskReader::with_fatal_invariant_sink(...)` with a `NativeIsolationConfig` whose `groups`/`select` are `GoString`, and a `NativeBacklogForward`/`NativeDispatchCheck` taking `AllocatedTaskBytes`.

## What it is
`NativeTaskReader` is a line-for-line port of `TaskReader`:
- Store pump: scan budget via `reader_turn`, retry through the native queue, expiry skip, wake and ack-timer re-signal, and backlog gauges.
- Dispatch loops: one per buffer group; expiry recheck, dispatch check, limiter wait, dispatch deadline, local offer, forward with token, throttled/timeout retry, and completion GC.
- Poll delivery: `poll` and `poll_group(&[u8])`, plus `wait_stopped`.
- Fatal invariant: the sink is called in the pump before its completion guard drops, so `wait_stopped` cannot finish before the host-fatal callback returns.
- Reuses public `ReaderConfig`, `ReaderLive`, `ReaderFatalSink`, the Go default constants and `TaskListLimiter` unchanged.

## Raw-byte handling
- Isolation groups are `GoString` keys in the buffer and rendezvous maps, looked up by `&[u8]`. Selection returns a `GoString`. The limiter is not keyed by group.
- `dispatch_partition_config` is a byte-keyed copy of the `cadence_common` helper.
- Lossy text is limited to `diag()` (256-byte prefix) for traces and the isolation-leak metric tag. It is never used for routing.

## Copied private helpers
`Rendezvous`, `Pending`, `OfferGuard`, `Completion`/`Completed`, the trace helpers, and mirrors of `ReaderConfig`'s private accessors (`buffer_batch_size`, `read_batch_size`, `read_range_size`, `dispatch_timeout_nanos`, `update_ack_interval`, `now`).

## Tests (SQLite and a scripted fake store)
- Distinct invalid-byte tasks and groups (`\xff`, `\xfe`, U+FFFD) each deliver to their own group.
- Plain delivery through the shared channel.
- Duplicate (`[1,1]`) and out-of-order (`[2,1]`) task IDs fire the fatal sink exactly once. A 200 ms sink must finish before `wait_stopped` returns.

## Risks to check in CI
- Rejection of out-of-order ids depends on `AckManager::read_item`; if it only rejects duplicates, drop or adjust that case.
- Closure borrows around `target`/`isolation_duration` are copied from the legacy code.
- Task-ID dedupe in the scripted fake relies on the queue's `max_read_level` after `append`.

First CI compiled/clippy passed, but the fatal fixtures failed to call reader.stop after the callback, unlike the legacy registry-owner fixture. Parent corrected the fixture lifecycle and replaced the sleep with a controlled condition-variable sink, checking wait_stopped cannot outrun callback completion. Production pump logic unchanged. Validation rerun pending.
