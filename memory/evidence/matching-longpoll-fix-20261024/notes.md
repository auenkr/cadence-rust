# Matching long-poll expiry fix (2026-10-24)

Branch: `matching-longpoll-fix-20261024`, based on `origin/matching-longpoll-diag-20261024` (head `63f1dd430`).
Diagnosis: `memory/evidence/matching-longpoll-diag-20261024/notes.md`.
Fix commit: `cc147f32f` (code). Notes commit follows it.

No cargo ran locally. All compile and test evidence is from GitHub Actions (`rust-ci.yml`).

## Go reference (verified against `go/` in this tree)

- `go/service/matching/handler/engine.go:808`: `recordDecisionTaskStarted(hCtx.Context, ...)`. The start runs under the RPC context.
- `go/service/matching/handler/engine.go:1490-1494` and `:1520-1524`: `WithOperationTimeout(RecordTaskStartedTimeout)` (1s default) for each attempt.
- `go/common/backoff/retry.go:268-272` `attempt`: per-attempt cap, no overall 1s budget.
- `go/service/matching/tasklist/task_list_manager.go:708`: `getTask` bounds only the match by the `newChildContext` window.
- `go/service/matching/handler/engine.go:719`: `tlMgr.GetTask(pollerCtx, nil)` re-runs inside `pollLoop` after a discard. Not ported (see below).

## Change

File: `rust/crates/cadence-matching/src/matching_dispatch.rs`

1. `poll_with_lifecycle` takes `caller_deadline`. It waits for a task under the long-poll window.
   An `AtomicBool` `claimed` is set when the worker claims a task, before any history start. If the window expires while
   `claimed` is set, the poll waits for the delivery until `caller_deadline`. Otherwise it returns empty, as before.
2. `delivery_loop` sets `claimed` on a successful claim and clears it when it re-enters the wait. It also emits
   `matching::poll` debug events for claim and delivery.
3. `poll_decision_with_poller` and `poll_activity_with_poller` take `caller_deadline: Option<Instant>`. The start uses the
   RPC deadline, not `now + expiration`. `None` keeps the window, so non-RPC callers and tests behave as before.
4. Dropping the caller still cancels a claimed start through the existing `closed` branch, so the task is rewritten
   and not orphaned.
5. The forward path (`forward_poll` into the parent `delivery_loop`) passes `claimed = None`, so forwarded delivery
   stays bounded by the window. That matches Go's `ForwardPoll`.

File: `rust/crates/cadence-matching/src/grpc_matching.rs`
- Both gRPC poll sites pass `incoming_deadline` (always `Some` on this path, since a missing deadline is rejected at `:1659`).

File: `rust/crates/cadence-matching/src/matching_history.rs`
- No change. `retry` already caps each call at `min(caller_deadline, now + 1s)`, and `retry_record_started` calls it once per
  attempt, so the 1s cap is per attempt as Go's is.

Other:
- `rust/crates/cadence-matching/Cargo.toml`: `tracing = "0.1"`. `rust/Cargo.lock`: `"tracing"` added to `cadence-matching`.
  `tracing` 0.1.44 was already in the lock, so no new package is resolved.
- Five test call sites updated for the new argument. The `#[allow(clippy::too_many_arguments)]` on `delivery_worker` and
  `delivery_loop` follows existing precedent in the crate.

## Tests

File: `rust/crates/cadence-matching/tests/matching_longpoll_expiry_parity.rs`
- `decision_started_after_long_poll_window_still_returns_task`: `#[ignore]` removed. Expected: `started_event_id == 92`.
- `activity_started_after_long_poll_window_still_returns_task`: new twin. Same schedule, `RecordActivityTaskStarted` answers
  after 900ms. Expected: non-empty `task_token`.

## Known gaps (not fixed here)

- `tracing` events are inert in the binary. No subscriber is installed anywhere in the workspace. Wiring
  `tracing-subscriber` is a separate change, because it would add a dependency tree that cannot be resolved locally.
- Optional item 4 (re-enter the window after a discarded start, `engine.go:719`) is not implemented. After a discard that
  happens past the window, the poll waits for the next task up to the caller deadline, not an empty return at the window.
- The Go side of the expectation was verified by reading only. No Go control run was done.
- The lane-9 Go/Rust gate was not run. Only the unit-level parity test covers this.

## CI runs

- Package run (`-f package=cadence-matching`), head `cc147f32f`: https://github.com/auenkr/cadence-rust/actions/runs/40066163723
  — success. Clippy `--all-targets -D warnings` passed. `matching_longpoll_expiry_parity.rs`: `2 passed; 0 ignored`
  (decision test un-ignored and the activity twin both pass).
- Whole-workspace run, head `4baa03060` (notes-only commit on top of `cc147f32f`; code identical):
  https://github.com/auenkr/cadence-rust/actions/runs/40066410068 — success. Clippy and tests passed across the
  workspace with no failures. Both parity tests ran `ok`. The flaky reader test passed, so no rerun was needed.
