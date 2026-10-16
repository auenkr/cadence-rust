# Matching sticky task-list availability (2026-10-16)

Closes the "sticky availability" item of Matching gate 4 for the durable
matcher, on top of `matching-query-next.md`.

## Go semantics implemented

Go `AddDecisionTask` (engine.go:560) and `QueryWorkflow` (engine.go:1031) check,
for a **sticky** task list, that the sticky task-list manager saw a poller
recently: `tlMgr.HasPollerAfter(now - _stickyPollerUnavailableWindow)` with
`_stickyPollerUnavailableWindow = 10s`. If not, they fail fast with
`_stickyPollerUnavailableError = &types.StickyWorkerUnavailableError{Message:
"sticky worker is unavailable, please use non-sticky task list."}`. History then
clears stickiness and retries on the non-sticky list instead of leaving the task
(or the query) stranded on a sticky list nobody polls.

`AddActivityTask` has **no** such check in Go, and neither
`PollForDecisionTask`/`PollForActivityTask` (polling the sticky list is what makes
it available), so the Rust durable matcher adds it only to
`add_decision_task` and `query_workflow`.

Wire error: `StickyWorkerUnavailableError` maps to YARPC/gRPC `UNAVAILABLE` with a
`google.rpc.Status` detail of type
`type.googleapis.com/uber.cadence.api.v1.StickyWorkerUnavailableError` (the
message is empty; the text lives in the status message). Rust builds it with the
existing `grpc::detailed_status`, so the Go frontend's protobuf error mapper
recovers `types.StickyWorkerUnavailableError`.

## Files

- `rust/crates/cadence-server/src/matching_registry.rs`:
  `QueueHandle::has_poller_after(after)` -> `cadence_common::poller::Manager::has_after`.
- `rust/crates/cadence-server/src/grpc_matching.rs`:
  `STICKY_POLLER_UNAVAILABLE_WINDOW`, `sticky_task_list` (proto kind 2),
  `sticky_unavailable`, `check_sticky_poller`; called in `add_decision_task` and
  `query_workflow` after the durable queue is resolved and before enqueue/offer.
- `rust/crates/cadence-server/src/matching_rpc_tests.rs`:
  `matching_rpc_sticky_task_list_requires_a_recent_poller`.

## Validation

- `cargo test -p cadence-server --lib`: **80 passed** (the new test covers: no
  sticky poller -> `Unavailable` + the sticky message for both
  `AddDecisionTask` and `QueryWorkflow`; a waiting sticky poller makes the list
  available again — the delivery worker registers the outstanding poll
  asynchronously, so the test retries briefly).
- strict all-target Clippy clean.
- The mixed harness already contains a **Go SDK sticky scenario**
  (`tests/go-sdk/main.go`: `stickyWorker` with `StickyScheduleToStartTimeout:
  time.Second`, asserting the decision task was scheduled on the sticky owner
  queue), so the `go-services-rust-matching` stage is a real regression guard for
  this check.
- **Full 29/29-stage runner PASSED** (exit 0):
  `/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-sticky-full/results.json`
  (the `go-services-rust-matching` stage, which drives the Go SDK sticky
  scenario against the Rust matcher, passed).

## Remaining limits

1. The sticky check uses the owner's single poller manager, same as Go, but the
   `_stickyPollerUnavailableWindow` is a constant here while Go reads it from the
   engine constant `_stickyPollerUnavailableWindow`; Go does not make it
   dynamic-configurable either.
2. Sticky **query** dispatch (History trying the sticky list before the
   non-sticky one) is History's behavior, not matching's; the Rust matcher only
   serves whichever task list it is asked for.
3. Forwarding (`forwarded_from`), partition routing/scaling, isolation groups,
   semaphores, membership/ownership and standalone lifecycle remain open.
