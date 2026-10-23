# Matching query-forward family: Go → Rust mapping (2026-10-24)

Branch: `matching-query-forward-family-20261024` (head `ffdeef542`, based on `origin/master` b43ab8f5b).
CI: rust-ci.yml `package=cadence-matching`, run https://github.com/auenkr/cadence-rust/actions/runs/40035777126 — **success**, `621 passed; 0 failed; 11 ignored` (crate-wide; the ignored count is pre-existing, no new `#[ignore]` was added).

Go sources: `go/service/matching/tasklist/forwarder_test.go`, `matcher_test.go`.
New Rust file: `rust/crates/cadence-matching/src/matching_query_forward_family_tests.rs` (mounted from `grpc_matching.rs`). Test-only visibility change in `matching_rpc_tests.rs` (`pub(super)` on five fixture helpers). No production change.

## Table

| Go test | Rust test | Status | Run URL |
|---|---|---|---|
| `forwarder_test.go:TestForwardQueryTaskError` (root branch) | `matching_rpc_query_without_poller_waits_for_deadline` (existing) | PASS (existing) | run 40035777126 |
| `forwarder_test.go:TestForwardQueryTaskError` (sticky branch) | `query_forward_family_sticky_list_never_forwards_and_waits_for_local_poller` | PASS | run 40035777126 |
| `forwarder_test.go:TestForwardQueryTask` | `query_forward_family_forwards_go_forwarder_fields_to_parent` | PASS | run 40035777126 |
| `forwarder_test.go:TestForwardQueryTaskRateNotEnforced` | `query_forward_family_repeated_forwards_are_not_rate_limited` | PASS | run 40035777126 |
| `matcher_test.go:TestQueryRemoteSyncMatch` | `matching_rpc_remote_query_sync_match_forwards_to_parent_and_returns_answer` (existing; not duplicated) | PASS (existing) | run 40035777126 |
| `matcher_test.go:TestQueryRemoteSyncMatchError` | `query_forward_family_busy_parent_keeps_query_for_local_poller` (overlaps existing `matching_rpc_remote_query_sync_match_error_falls_back_to_local_poller`) | PASS | run 40035777126 |
| `matcher_test.go:TestOfferQuery_ForwardError` | `query_forward_family_parent_error_is_returned_without_local_wait` | PASS | run 40035777126 |
| `matcher_test.go:TestOfferQuery_ContextExpired` | `query_forward_family_expired_deadline_never_forwards` | PASS | run 40035777126 |
| `matcher_test.go:TestQueryLocalSyncMatch` | `matching_rpc_delivers_query_to_decision_poller` (existing) | PASS (existing) | run 40035777126 |

Out of scope: `matcher_test.go:TestRemotePollForQuery` (a forwarded poll, not a query forward; already covered by `matching_rpc_child_poll_receives_parent_query_and_preserves_completion_partition`).

## Notes

- **Go error sentinels are not RPC-observable.** `ErrNoParent` and `ErrTaskListKind` are returned only by `forwarderImpl.ForwardQueryTask`. In production the matcher never builds a forwarder for root lists or sticky lists (`task_list_manager.go:238`, `isFowardingAllowed` at `:891`), so `OfferQuery` never reaches those errors. The RPC-level assertion is the Go-visible outcome: no parent call and a wait until the deadline.
- **Correction to memo `matching-cannot-port-review-20261024` row 11 (sticky forwarding "to verify").** Not a gap. Go's `isFowardingAllowed` returns false for sticky lists, so `fwdrAddReqTokenC()` is nil and `OfferQuery` blocks on the local queue, same as Rust `grpc_matching.rs` (`list.kind == 2` → force local). Verified by test above.
- **Correction to memo claim "no fake implements `query_workflow`".** `QueryParent` (recording + busy) already exists at `matching_rpc_tests.rs:2318`. The new `FamilyQueryParent` adds the failing mode.
- `ErrNoParent` from `OfferQuery` (`TestOfferQuery_ForwardError`) is reachable in Rust only through `parent.is_empty()` (`grpc_matching.rs` ~2664), which is unreachable for a partition name. The test asserts the parent-error passthrough instead.
- Go's `TestOfferQuery_ContextExpired` cancels the context (`context canceled`). Rust's RPC layer only sees a deadline, so the test uses a zero grpc-timeout and asserts `context deadline exceeded`.
- The busy-parent test answers the local poller with `"local"`; Go's `Finish(nil)` returns a nil result. The result text differs; the forward/fallback behaviour is the same.
