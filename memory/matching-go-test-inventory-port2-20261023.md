# Matching Go unit-test port, pass 2 (2026-10-23)

Branch: `matching-go-tests-port2-20261023` (from origin/master `ff6d4b9f1`).
Supersedes the MISSING rows for the tests listed here in
`matching-go-test-inventory-20261023.md` (that file is unchanged on master).
Evidence: `memory/evidence/matching-go-tests-port2-20261023/`.

Run convention: `cargo-safe test -p cadence-matching --lib <filter> -- --include-ignored`
so that `#[ignore]` parity-gap tests execute and their real failure is recorded.

## Group 1: forwarder_test.go (8 MISSING → 4 ported, 4 cannot port)

| Go test (forwarder_test.go line) | Result | Rust test (file) | Notes |
|---|---|---|---|
| TestForwardDecisionTask (86) | PORTED | matching_dispatch.rs::matching_forward_decision_task_fields_match_go_forwarder | Passes. ScheduleToStartTimeoutSeconds asserted 0: Go's random TaskInfo value has no Rust input (TaskData has no field; Go persistence reads zero, matching_dispatch.rs forward_backlog_task). Go DomainUUID = task domain is passed as the `domain` argument. |
| TestForwardActivityTask (110) | PORTED, PARITY GAP | matching_dispatch.rs::matching_forward_activity_task_fields_match_go_forwarder (`#[ignore]`) | Fails as predicted, matching_dispatch.rs:2276: left `…0001` (list domain) vs right `…0005` (task domain). See parity gaps. |
| TestForwardQueryTaskError (148) | CANNOT PORT | — | No Rust ForwardQueryTask API. Query forwarding is inline in the RPC handler (grpc_matching.rs:2495-2740), which returns no ErrNoParent/ErrTaskListKind; sticky and root lists wait locally (grpc_matching.rs:2627-2633). |
| TestForwardQueryTask (159) | CANNOT PORT | — | Same location (grpc_matching.rs:2668-2697). Needs an RPC fixture with a remote parent; closest existing RPC test is matching_rpc_tests.rs::matching_rpc_child_query_forwards_to_parent_poller. |
| TestForwardQueryTaskRateNotEnforced (178) | CANNOT PORT | — | Query path takes only `try_forward_query` permits (grpc_matching.rs:2651, matching_registry.rs:325), no ForwardRate. Asserting "no rate limit" needs the RPC fixture. |
| TestForwardPollError (192) | CANNOT PORT | — | Rust ForwardPollRequest::forward (matching_dispatch.rs:136) returns a Result from the parent call, with no ErrNoParent/ErrTaskListKind path. |
| TestForwardPollForDecision (203) | PORTED | matching_dispatch.rs::matching_forward_poll_for_decision_fields_match_go_forwarder | Passes. Go's "decision response only" check is by return type in Rust. |
| TestForwardPollForActivity (231) | PORTED | matching_dispatch.rs::matching_forward_poll_for_activity_fields_match_go_forwarder | Passes. Same routing note. |

Group 1 cargo result: `forwarder-cargo-test.exit` = 101. 10 passed, 1 failed (the ignored activity parity-gap test, run via --include-ignored). The 1 failure is the expected parity gap, not a regression.

## Parity gaps

| # | Go (file:line) | Rust (file:line) | What differs | Status |
|---|---|---|---|---|
| PG-1 | go/service/matching/tasklist/forwarder.go:131-135 (activity `SourceDomainUUID = task.Event.DomainID`, `DomainUUID = fwdr.taskListID.GetDomainID()`); forwarder_test.go:126-127 | matching_dispatch.rs:378-380 (`domain_id` and `source_domain_id` both set from the single `domain_id` argument, the dispatcher's list domain) | Rust `TaskData` (cadence-persistence/src/matching_blob.rs:8-15) stores no source domain, so a forwarded activity row's workflow/source domain is replaced by the task-list domain. Affects cross-domain activities forwarded to a parent. | OPEN. Test `#[ignore]`d. Production impact not yet measured. |

## Cannot port (reason)

- forwarder_test.go TestForwardQueryTaskError, TestForwardQueryTask, TestForwardQueryTaskRateNotEnforced, TestForwardPollError: see Group 1 table.

## Group 2: remote-forward and sync-failure (matcher_test.go) — 5 ported, all pass

Run: `sync_match` filter, `remote-cargo-test.exit` = 0 (16 passed incl. existing). First attempt was a compile error
(missing SyncMatcher import), kept as `remote-cargo-test-attempt1-compile-error.log`.

| Go test (matcher_test.go line) | Rust test | Notes |
|---|---|---|
| TestRemoteSyncMatch (163) | matching_dispatch.rs::matching_remote_sync_match_forwards_to_root_poller | Composes forward_backlog_task with in-process root (Parent::Dispatcher → accept_forwarded_backlog). Child local-offer-then-forward is not a Rust matcher step, so not asserted. |
| TestIsolationRemoteSyncMatch (167) | matching_dispatch.rs::matching_isolation_remote_sync_match_forwards_to_root_poller | dca1 group |
| TestRemoteSyncMatchBlocking (171) | matching_dispatch.rs::matching_remote_sync_match_blocking_waits_for_root_poller | 10ms delayed root poll; Rust has no TaskSource distinction, so this is a delay variant only |
| TestIsolationRemoteSyncMatchBlocking (175) | matching_dispatch.rs::matching_isolation_remote_sync_match_blocking_waits_for_root_poller | |
| TestSyncMatchFailure (244) | matching_dispatch.rs::matching_sync_match_failure_reports_busy_without_matching | Go Offer returns no error; Rust forward returns SlowDown, treated as not matched by the caller |

## Group 3: poll and query cases — 7 ported, all pass

Run: `sync_match` filter, `poll-query-cargo-test.exit` = 0 (23 passed incl. existing).

| Go test (matcher_test.go line) | Rust test | Notes |
|---|---|---|
| Test_poll_Task (1143) | matching_sync.rs::sync_match_queued_offer_is_returned_to_poll | queued offer returned by poll |
| Test_poll_IsolatedTask (1129) | matching_sync.rs::sync_match_isolation_queued_offer_is_returned_to_its_group | |
| Test_pollNonBlocking_Task (1196) | matching_sync.rs::sync_match_non_blocking_poll_returns_queued_offer_without_waiting | 100ms timeout guard; Rust poll has no non-blocking flag |
| Test_pollNonBlocking_IsolatedTask (1181) | matching_sync.rs::sync_match_non_blocking_isolated_poll_returns_queued_offer_without_waiting | |
| TestIsolationSyncMatchFailure (278) | matching_sync.rs::sync_match_isolation_group_mismatch_does_not_match | group b poller never takes group a offer |
| Test_poll_QueryTask (1156) | matching_query.rs::sync_match_queued_query_is_returned_to_next | |
| TestQueryLocalSyncMatch (298) | matching_query.rs::sync_match_query_hands_off_to_parked_poller | Rust checks try_offer + complete; the Go OfferQuery nil-return contract is mapped to receiver Ok |

## Matcher totals after pass 2

- matcher_test.go: 54 functions. Ported in pass 1: 3. Ported in pass 2: 12 (group 2: 5, group 3: 7, of which TestIsolationSyncMatchFailure and TestQueryLocalSyncMatch are counted in group 3 above). Total ported: 15 of 54 (incl. suite runner and helper rows not counted).
- Not ported in pass 2: Test_pollNonBlocking_QueryTask (1210) (same queued-query scenario as Test_poll_QueryTask; not written, omitted this pass).

## Cannot port (reason, unit level) — matcher_test.go

Still MISSING after pass 2 (not individually audited in this pass; grouped by the Go API they need):
- MustOffer family (TestMustOfferLocalMatch 400, TestIsolationMustOfferLocalMatch 418, TestMustOfferRemoteMatch 437, TestMustOfferRemoteRateLimit 505, TestIsolationMustOfferRemoteMatch 524, TestMustOffer_* 882-971): no Rust MustOffer; retry loop lives in reader/dispatch.
- offerOrTimeout / Offer without timeout (TestOffer_NoTimeout*, TestOfferOrTimeout_*): Rust offer API always takes a deadline.
- Remote query forward (TestQueryRemoteSyncMatch 317, TestQueryRemoteSyncMatchError 368, TestOfferQuery_ForwardError 841, TestRemotePollForQuery 647): query forward is inline in grpc_matching.rs:2495-2740, RPC-level only.
- Remote poll (TestRemotePoll 621, TestIsolationPollFailure 673): ForwardPollRequest::forward is covered at field level by matching_forward_poll_for_* tests; matcher-level needs the RPC fixture.
- Private poll helpers with ctx/non-blocking-NoTasks (Test_poll_ContextExpired 1169, Test_pollNonBlocking_NoTasks 1224, Test_pollNonBlocking_QueryTask 1210): Rust poll takes a stop signal, not a ctx; with nothing queued it blocks rather than returning ErrNoTasks.
- Test_pollOrForward_* (1002-1094), Test_fwdrPollReqTokenC (1235): Go-private helpers with no Rust function.
- AutoConfigHint (Test_LocalSyncMatch_AutoConfigHint 1605): Rust poll result has no AutoConfigHint field.
- Rate-limit subtests (TestRatelimitBehavior 1264, TestRateLimitHandling 262, TestOffer_RateLimited 682, TestMustOfferRemoteRateLimit): Go matcher limiter cancel behavior is not represented in Rust matcher; limiter covered only in matching_limiter.rs.
- Disconnect blocked pollers (TestPollersDisconnectedAfterDisconnectBlockedPollers 592, TestPollersConnectedAfterDisconnectBlockedPollersSetCancelContext 606): no per-poll cancel context in Rust.
- TestOffer_NoTimeoutAsyncMatchedNoError (746), TestOfferOrTimeout_AsyncMatchNotMatched (818): async match with no timeout variant, no Rust counterpart.
- TestRemoteSyncMatch-family is ported above; TestQueryRemoteSyncMatch* listed here.

## Parity gaps (this pass)

- PG-1 (see group 1 table): activity forward SourceDomainUUID is the list domain in Rust; Go sends the task domain.
  Go: go/service/matching/tasklist/forwarder.go:131-135, forwarder_test.go:126-127.
  Rust: rust/crates/cadence-matching/src/matching_dispatch.rs:378-380 (domain_id and source_domain_id from one argument);
  cause: cadence-persistence/src/matching_blob.rs:8-15 TaskData has no source domain.
  Test: matching_dispatch.rs::matching_forward_activity_task_fields_match_go_forwarder, `#[ignore]`d.
