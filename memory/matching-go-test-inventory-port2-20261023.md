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
