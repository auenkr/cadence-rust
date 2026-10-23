# Matching Go unit-test port, pass 3 (2026-10-23)

Branch: `matching-go-tests-port3-20261023` (from origin/master `92668eb2f`).
Builds on pass 1 and pass 2 (`matching-go-test-inventory-port2-20261023.md`).
Evidence: `memory/evidence/matching-go-tests-port3-20261023/`.

Scope: remaining tests in `go/service/matching/tasklist/matcher_test.go`
(and `forwarder_test.go`, which has no remaining portable cases; see below).
No production code changed.

## Counts: matcher_test.go (53 Test functions + suite runner = 54)

| Status | Count | Notes |
|---|---|---|
| PORTED (pass 1 + 2) | 15 | unchanged |
| PORTED (pass 3) | 7 | see group table |
| PARITY GAP (`#[ignore]`) | 2 | see parity table |
| CANNOT PORT | 29 | Rust has no matching API for them (listed below) |

Check: 15 + 7 + 2 + 29 = 53. The 54th function is the suite runner `TestMatcherSuite`.

forwarder_test.go: 4 ported (pass 1), 4 cannot port (pass 2). Pass 3 adds no forwarder cases.

## Pass 3 ported (group 1, one cargo run)

Cargo run: `cargo-safe test -p cadence-matching --lib sync_match -- --include-ignored`
(covers both the sync and query groups, since all names match `sync_match`).
Result: exit 101. 29 passed, 2 failed. The 2 failures are the expected parity gaps (below).
Log: `evidence/matching-go-tests-port3-20261023/group-sync-match-include-ignored.log`.

| Go test (matcher_test.go line) | Rust test (file) | Notes |
|---|---|---|
| TestOffer_NoTimeoutSyncMatchedNoError (694) | matching_sync.rs::sync_match_zero_local_wait_hands_off_to_waiting_poller | zero local wait, parked poller still matches |
| TestOffer_NoTimeoutAsyncMatchedNoError (746) | matching_sync.rs::sync_match_offer_without_waiting_poller_is_not_queued | shared with 771; zero wait, no poller → not matched, not queued |
| TestOffer_AsyncMatchedNoError (771) | same as above | |
| TestOfferOrTimeout_SyncMatchTimedOut (794) | matching_sync.rs::sync_match_offer_deadline_before_poll_withdraws_offer | 1ms budget, poller 100ms late → no match, offer withdrawn |
| TestPollersDisconnectedAfterDisconnectBlockedPollers (592) | matching_sync.rs::sync_match_poll_after_stop_returns_no_task | stop signal stands in for DisconnectBlockedPollers. Go's "parent ctx not cancelled" check has no Rust analog |
| Test_poll_ContextExpired (1169) | matching_sync.rs::sync_match_stop_while_parked_releases_poller_without_task | stop signal is the analog of the expired ctx |
| Test_pollNonBlocking_QueryTask (1210) | matching_query.rs::sync_match_non_blocking_query_poll_returns_queued_query_once | queued query, no wait, then empty |

## Parity gaps (`#[ignore]`, run with `--include-ignored` to see the failure)

| # | Go (file:line) | Rust (file:line) | What differs | Test |
|---|---|---|---|---|
| PG-2 | matcher_test.go:1224 (Test_pollNonBlocking_NoTasks) | matching_sync.rs:90-123 (`SyncMatcher::poll`) | Go non-blocking poll with no tasks returns ErrNoTasks at once. Rust poll has no non-blocking mode and parks until a task arrives or stop fires. Failure: poll with 100ms timeout gets Elapsed, not None. | matching_sync.rs::sync_match_non_blocking_poll_with_no_tasks_returns_immediately |
| PG-3 | matcher_test.go:673 (TestIsolationPollFailure) | matching_sync.rs:80-86 (`poll_group`) | Go rejects an unknown isolation group with an error. Rust maps unknown groups to the default group and parks. Failure: poll("invalid-group") gets no result in 100ms. | matching_sync.rs::sync_match_invalid_isolation_group_poll_gets_no_task |

PG-1 (activity forward source domain) is unchanged from pass 2.

## Cannot port (pass 3 audit, 29 Go tests)

Each has a reason. None needs production code changes to port, but each needs an API Rust does not expose at unit level.

- Rate limiter at the matcher (matcher_test.go): TestRateLimitHandling (262), TestOffer_RateLimited (682), TestRatelimitBehavior (1264). Rust limiter is covered only in matching_limiter.rs; the matcher has no limiter field. TestRatelimitBehavior pins Go limiter cancel semantics.
- Remote query forward: TestQueryRemoteSyncMatch (317), TestQueryRemoteSyncMatchError (368), TestOfferQuery_ForwardError (841). Query forward is inline in grpc_matching.rs:2495-2740. No `ForwardQueryTask` API.
- Query offer with ctx: TestOfferQuery_ContextExpired (865). `QueryChannel` has no ctx. The Go test asserts `context.Canceled`.
- MustOffer family: TestMustOfferLocalMatch (400), TestIsolationMustOfferLocalMatch (418), TestMustOfferRemoteMatch (437), TestMustOfferRemoteRateLimit (505), TestIsolationMustOfferRemoteMatch (524), TestMustOffer_ContextExpiredFirstAttempt (882), TestMustOffer_ContextExpiredAfterFirstAttempt (893), TestMustOffer_LocalMatchAfterChildCtxExpired (909), TestMustOffer_LocalMatchAfterForwardError (937), TestMustOffer_ContextExpiredAfterForwardError (971). No Rust MustOffer. The retry loop is in matching_reader.rs. Reader tests cover the same semantics at reader level (matching_reader.rs:1532, 1579, 1554), but they are not 1:1 ports.
- Offer with error return: TestOffer_NoTimeoutSyncMatchedError (719). `SyncMatcher::offer` returns bool, so the poller's error is collapsed to not-matched. Caller-level behavior is covered by matching_dispatch.rs::matching_sync_match_failure_reports_busy_without_matching.
- OfferOrTimeout async: TestOfferOrTimeout_AsyncMatchNotMatched (818). No OfferOrTimeout API. Covered by the 746/771 port.
- Remote poll: TestRemotePoll (621), TestRemotePollForQuery (647), TestIsolationPollFailure is a parity gap (above). TestRemotePoll and TestRemotePollForQuery need a per-poll forwarder token channel (`PollReqTokenC`), which Rust does not have.
- Disconnect with refresh: TestPollersConnectedAfterDisconnectBlockedPollersSetCancelContext (606). Rust has no per-poll cancel context and no deadline return from poll; callers wrap in timeout.
- Private Go helpers: Test_pollOrForward_PollIsolatedTask (1002), _PollTask (1026), _PollQueryTask (1049), _ForwardTask (1069), _ForwardTaskThenPoll (1094), Test_fwdrPollReqTokenC (1235). No Rust equivalent functions.
- AutoConfigHint: Test_LocalSyncMatch_AutoConfigHint (1605). The Rust poll result has no AutoConfigHint field.

## forwarder_test.go

No remaining portable cases. The 4 CANNOT PORT entries from pass 2 are unchanged (TestForwardQueryTaskError, TestForwardQueryTask, TestForwardQueryTaskRateNotEnforced, TestForwardPollError).

## Build note

A fresh worktree has an empty `go/idls` submodule, and the build fails with "Cadence IDL missing". Fix: `git submodule update --init go/idls` in the worktree. This does not touch the main checkout.
