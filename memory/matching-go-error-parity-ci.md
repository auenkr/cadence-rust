
## CI dispatch 2026-10-25
- rust-ci.yml package=cadence-matching on t3/matching-go-error-parity head 1b1b37da6a0c81bfe04a0a92191c3e942b08799f: run 40185780201 https://github.com/auenkr/cadence-rust/actions/runs/40185780201 (pending at record time).
- Prior: 40176622622 on 70a7eebb failed (stale routing fixture); 40176252648 on 416d619 passed.
- No mixed gate was active at dispatch.

## Source review of 1b1b37da (2026-10-25, no code change)
- 1b1b37da is docs-only; the fixture change is 45aa5223c (`tests/matching_partial_upgrade_e.rs` ~L493-520). It sends a valid inner PollRequest only for PollForActivityTask: outer field 20 -> {10 domain, 20 task list{10 name}}. This matches `from_thrift` ids (cadence-thrift convert.rs:3056) and `thrift_matching.rs:340`. The routing assertion is unchanged, so routing intent is preserved.
- Nil-request tests are untouched: `matching_rpc_tests.rs` L8900-8970 (`go_oracle_activity_poll_absent_request_is_captured_nil_dereference`, `..._deadline_precedence_and_wire_code`, `..._precedes_domain_lookup`) and `thrift_matching.rs` ~L1098-1111 (0x05 vs 0x06). Both cover no-deadline -> InvalidArgument, expired -> Internal, nil -> Unknown, and TChannel 0x05.
- Check order in `grpc_matching.rs:2009+`: quota -> missing deadline -> nil request (context check, then Unknown) -> identifier -> domain lookup -> context check. This matches the cited order.
- Verdict: no concrete defect found. CI 40185780201 is still pending for 1b1b37da.

## CI 40185780201 failure on 1b1b37da (2026-10-25)
- Conclusion failure. Only failing test: `go_thrift_handler_routes_each_tested_method` at matching_partial_upgrade_e.rs:527, PollForActivityTask "nil pointer dereference". 696 lib tests and the other integration tests passed.
- Root cause: `dispatch_inner` (thrift_matching.rs:736) reads the request from args field 1; the fixture put the inner request at the top level, so the request was still absent. The production nil contract is correct and unchanged.
- Fix: wrap the valid PollRequest in args field 1. The assertion is unchanged.
