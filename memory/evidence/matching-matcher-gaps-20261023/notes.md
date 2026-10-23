# Matching matcher parity gaps (2 ignored tests) — 2026-10-23

Branch: `matching-matcher-gaps-20261023` (from `origin/matching-go-tests-port3-20261023`).
Scope: two `#[ignore]`d tests in `rust/crates/cadence-matching/src/matching_sync.rs`.

## Verdict

Neither gap is a production divergence. Both gaps were in the test expectations.
Production Rust already matches Go in both cases. Both tests are rewritten to
check the Go behaviour and un-ignored. No program logic was changed.

## Gap 1: `sync_match_non_blocking_poll_with_no_tasks_returns_immediately`

Go reference: `go/service/matching/tasklist/matcher_test.go:1224`
(`Test_pollNonBlocking_NoTasks`). It calls the private `pollNonBlocking` directly
with `LocalPollWaitTime = 0` and no tasks, and expects `ErrNoTasks`.

- Go `Poll` itself never returns "no task" immediately. It tries `pollNonBlocking`
  first (`matcher.go:464`), then falls into `pollOrForward`, which blocks until a
  task arrives or the context ends (`matcher.go:569`, `:577` returns `ErrNoTasks`).
- The `ErrNoTasks` becomes an empty poll response in `handler/engine.go:722` and
  `:931`.
- Rust production: `TaskDispatcher::poll_with_lifecycle`
  (`matching_dispatch.rs:1000-1034`) bounds the poll by `expiration` with
  `tokio::time::timeout`. On expiry it returns `Ok(R::default())` (`:1028-1032`),
  which is the empty response. The deliver closure is never called. The worker is
  cancelled through `sender.closed()` (`:1124`).
- Rust `SyncMatcher::poll` (`matching_sync.rs:90-123`) has no deadline of its own.
  It is only the parked-waiter half of Go's `pollOrForward`. That is why the
  standalone test timed out. The no-task, zero-budget outcome is a
  dispatcher-level property.

Resolution: the test moved to the production dispatcher path and un-ignored.
- New: `rust/crates/cadence-matching/src/matching_dispatch.rs:1786`
  `matching_dispatch_non_blocking_poll_with_no_tasks_returns_immediately`.
  It calls `dispatcher.poll((), Duration::ZERO, …)` on an empty fixture. It asserts
  an empty `task_token`, zero deliveries, and elapsed < 1s.
- The ignored stub was deleted from `matching_sync.rs`.

## Gap 2: `sync_match_invalid_isolation_group_poll_gets_no_task` (renamed)

Go reference: `matcher_test.go:673` (`TestIsolationPollFailure`). It polls with
`"invalid-group"` and a 1s context, and expects an error and a nil task.

- Go `Poll` (`matcher.go:438-445`) does NOT reject an unknown group. It falls back
  to the default `tm.taskC` channel and increments
  `PollerInvalidIsolationGroupCounter`. The test's error is only the context
  timeout (`ErrNoTasks`).
- Offers to unknown groups also go to the default channel (`matcher.go:828-833`).
- Rust production matches:
  - `matching_reader.rs:1045-1052` `poll_group`: an unknown group falls through to
    `self.poll()`, the default queue.
  - `matching_sync.rs:80-86` `SyncMatcher::poll_group` and `:137` `offer`: an
    unknown group maps to `""`.
  - `matching_dispatch.rs:1050-1058` `delivery_worker`: records the invalid-group
    metric and continues. It does not reject.

Resolution: renamed to `sync_match_invalid_isolation_group_poll_falls_back_to_default_group`
(`matching_sync.rs:799`) and un-ignored. It asserts:
- an isolated offer for `"a"` is not taken by `poll("invalid-group")` (times out),
- a default-group offer `""` is taken by it.

## Gates (worktree `rust/`, shared target dir, `cargo-safe`)

- `cargo test -p cadence-matching --lib`: exit 0.
  `449 passed; 0 failed; 3 ignored`. The 3 remaining ignores are pre-existing and
  unrelated (`matching_forward_activity_task_fields_match_go_forwarder`, two
  tchannel mixed-wire gates). Log: `memory/evidence/matching-matcher-gaps-20261023/`.
- `cargo clippy -p cadence-matching --all-targets -- -D warnings`: see result below.
- `rustfmt --check` on the two files reports diffs only in pre-existing code
  (`matching_sync.rs` ~685, ~757; `matching_dispatch.rs` ~94, ~148). The lines
  added by this change are formatted.

## Clippy result

CLIPPY_RESULT_PLACEHOLDER
