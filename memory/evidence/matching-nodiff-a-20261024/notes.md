# matching-nodiff-a-20261024: four "accepted difference" candidates made Go-identical

Source list: `memory/matching-open-gaps-20261024-v2.md` section 4b on
`origin/matching-open-gaps-v2-20261024`. User directive: no accepted differences; Rust
must match Go Cadence exactly. These are the first four rows, each un-ignored.

Base: `origin/master` at `e7285da4f`. No local cargo run; verification is GitHub Actions only.

## 1. `matching_rpc_semaphore_cancelled_context_is_internal_error` (`src/matching_rpc_tests.rs`)

- Go: `go/service/matching/handler/handler.go:550` (`AcquireSemaphore`). When the engine returns an
  error and `hCtx.Err() != nil`, it counts `CadenceErrSemaphoreContextTimeoutCounter` and returns
  `InternalServiceError{Message: ctx.Err().Error()}` = `"context canceled"`.
- Go grant: `go/service/matching/semaphore/semaphore_manager.go:322` checks `ctx.Err()` before the
  first write, so a cancelled caller claims no slot.
- Rust before: only the deadline was observed. tonic drops a cancelled handler future, so no
  cancellation reached the handler.
- Rust after: `grpc_matching.rs` `caller_ended()` returns `"context canceled"` (cancel wins) or
  `"context deadline exceeded"`. The pre-write check and the post-error mapping both use it, and both
  count the metric exactly as before.
- Seam: `CallerCancel(watch::Receiver<bool>)` request extension, `pub(crate)`, not settable over the
  wire. The test inserts it already cancelled.
- Why this is the closest observable level: a client that cancels never reads a response, in Go or
  Rust. The in-process observable effects are the error value, the slot state, and the metric.
  Rust cancels an in-flight write by dropping the acquire future at the same await point where Go's
  context aborts persistence.

## 2. `get_tasklists_not_owned_returns_only_non_owned_managers` (`tests/matching_engine_go_tests_b.rs`)

- Go: `TestGetTasklistsNotOwned` expects `getNonOwnedTasklistsLocked` to return `[tl2m]`.
- Rust before: `QueueRegistry::unload_non_owned` returned `Result<(), Status>`.
- Rust after: returns `Result<Vec<String>, Status>` of the non-owned task-list names. `grpc_matching.rs`
  discards the value with `.map(|_| ())`. Registry tests are unchanged apart from the value being
  unused.
- Test: same ring setup as `shut_down_tasklists_not_owned_unloads_only_moved_owner`. Two names stay
  with self, one moves, and the returned list must equal exactly the moved name.

## 3. `refresh_workflow_tasks_canceled_context_is_propagated` (`tests/matching_engine_go_tests_c.rs`)

- Go: `engine.go:1531 refreshWorkflowTasks` returns the History error unchanged. The
  `context canceled` case in `TestRefreshWorkflowTasks` asserts `context.Canceled` is returned as-is.
- Rust: History errors were already passed through. Cancellation was not wired.
- Rust after: `MatchingHistoryClient::refresh_workflow_tasks_until(domain, task, deadline, caller_gone)`.
  It races `caller_gone` (biased, first) against the existing refresh call and returns
  `Status::cancelled("context canceled")` when the caller is gone.
- Production wiring: the delivery loop in `matching_dispatch.rs` passes `sender.closed()`. A poller
  that disconnects drops its `oneshot` receiver, which is the wire-level equivalent of the Go poll
  context ending. This is the same `closed()` signal the loop already races against claims.
- Tests: (a) History returns `Cancelled "context canceled"`, which is passed through unchanged;
  this path retries under the policy (`Cancelled` is retryable), like the existing internal case.
  (b) An already-done caller gets `Cancelled "context canceled"` with no History request recorded.
- Divergence to check: Go's mock records the RefreshWorkflowTasks call even for a done context. Rust
  returns before dialing. Whether the real Go client also skips the send for a done context was not
  verified in this change. It affects only History traffic for a caller that is already gone, not the
  caller's result.

## 4. `refresh_workflow_tasks_nil_execution_is_forwarded` (`tests/matching_engine_go_tests_c.rs`)

- Go: `TestRefreshWorkflowTasks` "nil workflow execution" forwards `Execution: nil`.
- Go production call sites: `engine.go:825` and `:977` run inside the `task.Event != nil` path, where
  `task.WorkflowExecution()` (`tasklist/task.go:148`) returns `{Event.WorkflowID, Event.RunID}`, never
  nil. Empty RunID is stored as an absent run. So nil cannot be reached through either production
  path in Go.
- Rust before: `MatchingHistoryClient::execution()` rejected `run_id: None` with
  `Internal "Missing run ID in persisted matching task."`. That diverges from Go's empty-RunID forwarding.
- Rust after: an absent (or empty) run is forwarded as `""`, and an empty workflow id is forwarded
  unchanged. A malformed non-empty run id still returns the existing internal error.
- Test: sends `run_id: None` with a workflow id, and an empty workflow id with no run; both reach the
  scripted History with the Go execution shape.

## Verification plan

1. Push `matching-nodiff-a-20261024`, run `rust-ci.yml` with `package=cadence-matching`, watch it to
   completion.
2. Once the package run is green, run the whole-workspace gate on the final code head.
3. If the only failure is the known flaky reader test, rerun.

Result: see the "Results" section appended below as CI completes.
