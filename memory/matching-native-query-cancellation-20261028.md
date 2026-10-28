# Native query handoff and cancellation, 2026-10-28

Scope: `rust/crates/cadence-matching/src/matching_query_native.rs` and `matching_engine_poll_native.rs`. `matching_engine_query_native.rs` was inspected and left unchanged; its `response_config_from_cache` API partition-config behavior is preserved. Parent owns registration, CI and production listener changes. No local Cargo, Go, rustfmt, build, test or check commands, CI dispatch, staging, commit or push were run.

## Go behavior traced

- `go/service/matching/tasklist/matcher.go:281-306`, `taskMatcherImpl.OfferQuery`: first tries the unbuffered local `queryTaskC` send; then races local send, forwarder token, and caller context. Local send waits on `task.ResponseC`. A successful forward returns the forwarder's result. Caller cancellation returns `ctx.Err()`.
- `go/service/matching/handler/engine.go:670-804`, `matchingEngineImpl.PollForDecisionTask`: after `GetTask` returns a query, calls `task.Finish(nil)` at line 773 before `historyService.GetMutableState` at line 777. If mutable state fails (including request-context cancellation), it calls `deliverQueryResult(... internalError: err)` and returns an empty decision poll response with partition/load hints. There is no requeue path after `Finish(nil)`; the local synchronous handoff has completed and the query is terminal.
- `go/service/matching/handler/engine.go:1367-1403`: successful query response construction preserves task token/query request and mutable-state-derived decision fields.

## Changes and bounded evidence

`matching_engine_poll_native.rs` now races mutable-state lookup against the registered poll cancellation and deadline. Cancellation becomes `Status::cancelled`, flows through the existing `QueryOutcome::Internal` completion, and the poll returns its empty response. This matches the Go terminal error path after query handoff instead of allowing a cancelled poll to leave the query caller waiting indefinitely. It does not requeue.

Added deterministic tests:

- `committed_query_lookup_cancellation_is_terminal_error` starts a pending state lookup, waits for its start handshake, then notifies cancellation and asserts a Cancelled result. No sleep or scheduler yield defines the protocol.
- `committed_query_wins_against_ready_poll_backlog` publishes on the real native query channel and polls a biased query-vs-ready-backlog select, asserting the committed local query wins. Existing `cancelled_waiting_poll_releases_ready_forward_token` covers outer poll cancellation dropping `next()` and releasing a ready forwarding token; existing `waiting_poll_commits_before_ready_forward_token` covers local query precedence over a ready token.
- `ready_backlog_winner_releases_deferred_forward_token` chooses an already-ready backlog branch while `next()` is pending, drops the losing future, and verifies the ready forwarding token immediately wins. This checks the deferral release without sleeps/yields.

`git diff --check` passed for all owned paths after these edits. Cargo tests and compile validation are unverified by instruction; parent CI must validate. No mixed-cluster evidence is claimed. The new cancellation unit test exercises the cancellation-aware state-lookup helper and error result; it does not run a History peer or the full RPC stack.
