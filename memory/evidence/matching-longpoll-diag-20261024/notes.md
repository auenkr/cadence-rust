# Lane 9 `workflow-task-retry` with `matching.longPollExpirationInterval=2s` — Go vs Rust long-poll expiry (2026-10-24)

Branch: `matching-longpoll-diag-20261024`, base `origin/master` e7285da4f.
Question (from `matching-gate-controls-20261024/NOTES.md`): with the 2s seed, C2 fails
(`workflow-task-retry: timed out`, `mixed_matching.go:84`) and C1 (seed removed) passes. Does Rust diverge
from Go for a short long-poll expiry?

Method: code reading only. No cargo, no Docker, no gates. No production code changed.

## Verdict

**Rust divergence (real, confirmed by reading). It is a plausible contributor to lane 9, not a proven sole cause.**

The divergence: Rust ends the whole decision poll, including an in-flight `RecordDecisionTaskStarted`, at the
long-poll window. Go bounds the start by the RPC context and its own per-attempt cap, not by the long-poll interval.
With a 2s window, any match that lands late in the window has its history start cut, the task is rewritten, and
the decision can be started in history with no poller. That state is the "delivered to a dead poller" case.

Not a divergence (checked, same behavior on both sides): the pre-delivery race, sync-match hand-off, backlog reader
hand-off, and the poll-expiry formula.

## Mechanism (Rust side, file:line)

1. `rust/crates/cadence-matching/src/grpc_matching.rs:1247-1277` `poll_timeout`: window =
   `min(matching.longPollExpirationInterval, deadline - 1s)`. This matches Go `newChildContext` at
   `go/service/matching/tasklist/task_list_manager.go:864-881` (the formula itself is equal).
2. `rust/crates/cadence-matching/src/matching_dispatch.rs:892` `poll_decision_with_poller`:
   `caller_deadline = now + expiration`. It is passed to the history start (`:901`).
3. `rust/crates/cadence-matching/src/matching_history.rs` `retry` (`:327`):
   `overall_deadline = min(caller_deadline, now + record_started_timeout)`. The record-started timeout defaults to
   1s (`DEFAULT_RECORD_TASK_STARTED_TIMEOUT_NANOS`, `matching_history.rs:154`). So a start that begins with
   `w` seconds of window left gets at most `min(w, 1s)`.
4. `rust/crates/cadence-matching/src/matching_dispatch.rs:1075`
   `let response = tokio::time::timeout(expiration, receiver);` ends the poll at the window even while
   `deliver` (the history start, `:1336-1347`) is still running.
5. On that timeout the receiver drops. The worker's biased select (`:1344-1346`) cancels the start with
   `Status::cancelled("Poll cancelled during start.")`, and `finish_delivery(Err)` rewrites the task (`:1346`,
   `:627-672`).
6. If history already committed the start before the cancel, the rewritten task returns `EventAlreadyStarted`.
   Rust discards it (`:1359`, Go `handler/engine.go` also drops it with `task.Finish(nil)`). The decision is
   now started in history with no poller holding its token, and it waits for its start-to-close timeout.

## Mechanism (Go side, file:line)

- `go/service/matching/handler/engine.go:808` `resp, err := e.recordDecisionTaskStarted(hCtx.Context, request, task)`.
  The start runs under the RPC context, not the long-poll window.
- `go/service/matching/handler/engine.go:1484-1505` `recordDecisionTaskStarted`: `throttleRetry` with
  `WithOperationTimeout(RecordTaskStartedTimeout)` (default 1s, `dynamicproperties/constants.go` `MatchingRecordTaskStartedTimeout`)
  and `WithContextExpiration()`.
- `go/common/backoff/retry.go:268-291` `attempt`: `WithOperationTimeout` caps each attempt at 1s, and retries
  continue until the RPC context ends. There is no overall 1s budget.
- `go/service/matching/tasklist/task_list_manager.go:708` `getTask`: the long-poll child context bounds only the
  match (`c.matcher.Poll(childCtx, ...)`). Once a task is matched, nothing in `getTask` cuts it.
- `go/service/matching/handler/engine.go:719` `tlMgr.GetTask(pollerCtx, nil)` runs inside `for pollLoop` (the loop
  starts at `:698`). After a non-empty discard or a retryable start error, Go calls `GetTask` again, and each
  call gets a fresh long-poll window (`newChildContext`) bounded by the RPC context. Rust has one window for the
  whole RPC (secondary divergence, see below).

## Other points checked

- **Task handed to an expired poll waiter (the suspected loss).** Rust does not lose it before delivery:
  - `matching_dispatch.rs:1331` checks `sender.is_closed()` after the claim and rewrites.
  - `matching_dispatch.rs:1344-1347` biased-cancels the start when the receiver closes.
  - The claim is raced against `sender.closed()` (`:1178-1188`), so a dead poll does not claim.
  - Sync hand-off (`matching_sync.rs:90-122`, `:124-200`): if the poller drops after the send, the producer's
    `response` sees a dropped sender and returns false, so the producer persists the task (no loss). This matches
    Go `Offer` returning `false` on a dropped response.
  - The backlog reader `Rendezvous::poll` (`matching_reader.rs:239-262`) moves ownership synchronously, so a
    cancelled waiter cannot own a task hidden in a oneshot.
  - The loss vector is the post-commit race in mechanism 6 above, which Go cannot reach because Go never cuts a
    started decision at 2s.
- **Empty-poll return.** `grpc_matching.rs` `poll_for_decision_task` returns `PollForDecisionTaskResponse::default()`
  on expiry, which matches Go's empty response on `ErrNoTasks` (`engine.go:722-746`).
- **check_poll_context.** `grpc_matching.rs:118-124` rejects a poll with < 10ms left (`common.IsValidContext`,
  10ms threshold). Same as Go. Not related to the 2s seed.
- **Sticky.** Constant `STICKY_POLLER_UNAVAILABLE_WINDOW` = 10s (`grpc_matching.rs:130`) equals Go
  `_stickyPollerUnavailableWindow`. The sticky fallback path was not traced beyond that constant. The lane-9
  retry is on the normal path in the harness output (`mixed_matching.go:84`), so sticky is not the first suspect.
- **Local wait.** `matching.localTaskWaitTime` defaults to 10ms in Rust (`grpc_matching.rs:447-460`). It is not
  affected by the 2s seed.

## Secondary divergence (not required for lane 9)

Go gives each `GetTask` a fresh long-poll window inside `pollLoop` (`engine.go:719`). Rust gives one window per RPC
(`matching_dispatch.rs:1075`). A poll that discards or retries a start can therefore return empty in Rust at
2s, where Go would keep polling for another 2s.

## Why this may explain the stall but may not be the whole story

- A single cut at the window only costs one poll cycle (about 2s) if history did not commit. The 40s stall in C2
  needs more than one cut-and-discard cycle, or a decision-timeout wait after a committed-but-discarded start.
- The C2 Rust server logs (`rust-server-logs/rust-server-before-restart.log`, 10 lines) have no poll-level events,
  so they can neither confirm nor rule out the cut. The SDK log shows two workflow panics 102 ms apart at
  05:03:31.625 and 05:03:31.727, then nothing until the 60s context expired (05:04:11). That fits a third decision
  task that is created after the second failure and never reaches a poller.
- Not reproduced. Not run here.

## Parity test

`rust/crates/cadence-matching/tests/matching_longpoll_expiry_parity.rs`:
`decision_started_after_long_poll_window_still_returns_task`, `#[ignore = "parity gap: ..."]`.

Scenario: window 2s (via `with_durable_queues` fallback, same value as the seed). Poll parked. Match at ~1.3s.
RecordDecisionTaskStarted answers after 900ms, so the start ends at ~2.2s. Go's 1s per-attempt cap is not hit, so Go
should return started_event_id 92. Rust returns an empty poll at 2.0s, so the assertion should fail.

Compile-verified via GitHub Actions only (no local cargo). Run: see the CI result recorded below.

**Caveat (Go expectation not run):** the Go-side expectation is from reading (`engine.go:808`, `retry.go:268`).
No Go control was run. A Go-history control would settle it: run the same schedule against Go matching with a
2s window and a 900ms start delay.

## Proposed fix (Rust, not applied)

Match Go's scope: the long-poll window must bound only the wait for a task, not the start or the response after a
task is matched.

1. `matching_dispatch.rs` `poll_with_lifecycle`: keep `tokio::time::timeout(expiration, ...)` for the pre-claim wait.
   Once the worker reports a claim (e.g. a `claimed` oneshot or a state flag), stop the window timer and wait on
   the caller's deadline (the request's `grpc-timeout`, i.e. `incoming_deadline`), not `deadline = now + expiration`.
2. `matching_dispatch.rs:892` `poll_decision_with_poller` and `:1005` poll_activity: pass the caller deadline (from
   the RPC context) to `start_decision_with_deadline`, not `now + expiration`. Keep the 1s
   `record_started_timeout` as the per-attempt cap (Go `WithOperationTimeout`). Do not make it an overall budget.
3. Keep the biased cancel on a dropped *caller* (RPC cancel), so a started task is not left orphaned.
4. Optional, to match Go: re-enter the window per discard (secondary divergence).

Parity test: remove `#[ignore]` once the fix lands; it should then pass.

## Commands and evidence

- Worktree: `git worktree add /mnt/storage/service/github/wt/longpoll -b matching-longpoll-diag-20261024 origin/master`.
- Evidence read: `origin/matching-gate-controls-20261024:memory/evidence/matching-gate-controls-20261024/C2/evidence/`
  (`rust-server-logs/rust-server-before-restart.log`, `go-sdk.log`).
- No cargo, Docker, or gate was run. No production code changed.
