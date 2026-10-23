# Matching missing Go tests: TestNewInternalTask, Test_pollNonBlocking_NoTasks, TestIsolationPollFailure

Branch: `matching-missing-tests-20261023`, based on `origin/matching-ci-merge7-20261023` (base sha 8a4590d12).
Source of the MISSING list: `memory/matching-go-test-map-20261023.md` (on `origin/matching-test-inventory-final-20261023`).
Scope: tests only. No Rust program logic was changed. Cargo was not run locally; verification is CI only.

## 1. TestNewInternalTask (go/service/matching/tasklist/task_test.go:36) — PORTED (with one structural exception)

Rust test: `sync_match_handoff_matches_go_new_internal_task` in `rust/crates/cadence-matching/src/matching_sync.rs`.
Production path exercised: `SyncMatcher::offer` (the sync handoff used by dispatch) then `SyncMatcher::poll`.

Go's `newInternalTask` has no Rust counterpart type (`InternalTask`). Its behavior is spread across the sync
handoff (`SyncTask.forwarded`, `cadence_common::task_isolation::dispatch_partition_config`). The test
drives the Go case table through that path:

| Go case | Input (partition config, group, forwarded) | Expected (Go) | Rust assertion |
|---|---|---|---|
| sync match | none, "", false | config nil, not forwarded | same |
| forwarded from history | none, "", true | forwarded | same |
| tasklist isolation | {isolation-group:a, wf-id}, "a" | {group a, original a, wf-id} | same |
| tasklist isolation - leaked | {isolation-group:a, wf-id}, "" | {group "", original a, wf-id} | same |
| tasklist isolation - forwarded | {group "", original a, wf-id}, "" | unchanged | same |

Not asserted, structural: Go `async match` (DbBacklog, not sync) asserts `ResponseC == nil` and `IsSyncMatch() == false`.
Rust async tasks never enter `SyncMatcher` (they go through `DurableTaskQueue`), so there is no sync-matcher value
to assert. Not a behavior gap, because the sync-only matcher never sees an async task. Go's `forwarded from backlog`
is the same Rust path as `forwarded from history` (forwarded bit only), so it is not repeated.

Verdict: ported. The forwarded-bit and partition-config rewrite match Go in every asserted case.

## 2. Test_pollNonBlocking_NoTasks (go/service/matching/tasklist/matcher_test.go:1224) — ALREADY PORTED, not duplicated

The memory map row (MISSING) is stale. Existing un-ignored test:
`rust/crates/cadence-matching/src/matching_dispatch.rs`, `matching_dispatch_non_blocking_poll_with_no_tasks_returns_immediately`
(around line 1840). It calls `TaskDispatcher::poll` with `Duration::ZERO` and no task, and asserts an empty response,
zero handler calls, and a return in under 1 second. Go asserts `ErrNoTasks` with a nil task. At the handler boundary
`ErrNoTasks` becomes an empty poll (`handler/engine.go`), so the observable is the same.

Verdict: ported (existing test, production path `TaskDispatcher::poll`). No new test added.

## 3. TestIsolationPollFailure (go/service/matching/tasklist/matcher_test.go:673) — PORTED

Rust test: `sync_match_invalid_isolation_group_poll_returns_no_task_at_deadline` in `matching_sync.rs`.
Go: `Poll(ctx 1s, "invalid-group")` falls back to the default channel; with no default task the poll returns `(nil, error)`
when the context ends.
Rust: `SyncMatcher::poll("invalid-group")` falls back to the default group (`poll_group`) and parks. With no
default-group task, the caller's 1 second deadline ends the poll with no task.

Difference in shape, not behavior: Go's sync `Poll` returns the timeout as an error. Rust's `SyncMatcher::poll` returns
`Option` and has no deadline of its own. The caller's deadline (`poll_with_lifecycle` timeout) turns it into an empty
response, the same as Go's handler mapping of the error. The production invalid-group metric is recorded in
`delivery_worker` (`matching_dispatch.rs` around line 1100), which this test does not cover.

Verdict: ported at the matcher level. A dispatch-level test would need a reader with isolation enabled, so it was not
written in this task.

## CI

Run: _pending_
