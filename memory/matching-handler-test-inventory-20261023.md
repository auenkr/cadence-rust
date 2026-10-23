# Matching handler test inventory (go/service/matching/handler)

## engine_test.go: first six test functions (file order)

Branch: `matching-engine-tests2-20261024` (from origin/master c5f95ea13).
Rust file: `rust/crates/cadence-matching/tests/matching_engine_go_tests.rs`.
CI: https://github.com/auenkr/cadence-rust/actions/runs/40017937402 (head e25e5558b, `package=cadence-matching`, success).
The earlier run https://github.com/auenkr/cadence-rust/actions/runs/40017742409 failed on clippy `non-snake-case` only, and was fixed by e25e5558b.

Result on CI: `matching_engine_go_tests` 11 passed, 3 ignored.

- Rust has no `matchingEngineImpl`. Tests call the public `MatchingApi` methods on `Matching`, backed by the real SQLite `QueueRegistry`, the in-memory domain store, and dynamic config.
- Go gomock managers, membership resolver, and executor are not reproduced. Where a Go expectation depends on them, the test is `#[ignore]` with the gap named in the attribute.
- Ignored tests are not verified against Rust behaviour. Their reasons describe the missing public seam, not a measured behaviour difference.

| Go test (line) | Go subtest | Rust test | Status |
| --- | --- | --- | --- |
| TestGetTaskListsByDomain (73) | domain cache error | `get_task_lists_by_domain_domain_cache_error_is_not_found` | PASS |
| TestGetTaskListsByDomain (73) | success | `get_task_lists_by_domain_success_lists_normal_kinds` (keys only) | PASS |
| TestGetTaskListsByDomain (73) | success - all kinds | `get_task_lists_by_domain_success_all_kinds_includes_sticky` (keys only) | PASS |
| TestGetTaskListsByDomain (73) | success poller identity (Go mock pollers) | `get_task_lists_by_domain_success_reports_poller_identity` | IGNORED: no public seam to inject poller identities |
| TestListTaskListPartitions (240) | domain cache error | `list_task_list_partitions_domain_cache_error_is_not_found` | PASS |
| TestListTaskListPartitions (240) | invalid tasklist name | `list_task_list_partitions_invalid_tasklist_name_is_error` | PASS |
| TestListTaskListPartitions (240) | success (keys: root, /1, /2 for both kinds) | `list_task_list_partitions_success_returns_root_then_write_partitions` | PASS |
| TestListTaskListPartitions (240) | success owner hosts (Go scripted Lookup) | `list_task_list_partitions_success_owner_hosts_match_go_lookups` | IGNORED: owners come from the real hash ring, not scripted lookups |
| TestCancelOutstandingPoll (353) | invalid tasklist name | `cancel_outstanding_poll_invalid_tasklist_name_is_error` | PASS |
| TestCancelOutstandingPoll (353) | success | `cancel_outstanding_poll_success` | PASS |
| TestErrIfShardOwnershipLost (421) | 8 subtests (SD lookup error, ProcessNotFound, nil process, still-owned, shutdown, excluded owner changed/same, non-excluded uuid name) | `err_if_shard_ownership_lost_go_subtests_not_reproduced` | IGNORED: ownership is the private `shard_task_list_ownership_check` (matching_runtime.rs), reachable only with a live shard distributor |
| TestIsExcludedFromShardDistributor (530) | 5 cases (flag x uuid table) | `is_excluded_from_shard_distributor_go_cases` | PASS |
| TestRespondQueryTaskCompleted (585) | success | `respond_query_task_completed_success_delivers_answer_to_waiter` | PASS |
| TestRespondQueryTaskCompleted (585) | query task not found | `respond_query_task_completed_query_task_not_found_is_error` | PASS |

Totals for the six Go functions: 23 Go subtests/cases (3 + 3 + 2 + 8 + 5 + 2).
- 15 have a passing Rust test. Three of those (GetTaskListsByDomain success, GetTaskListsByDomain all kinds, ListTaskListPartitions success) assert only the key sets, not the Go poller identities or owner hosts.
- Poller identities are asserted only for the Go `success` case (decision0), and only in the ignored test. The Go all-kinds poller check (`sticky-poller-sticky0`) is not ported.
- Owner hosts are asserted only in the ignored test. The ListTaskListPartitions success Go expectations (addr0..addr3) are not passing.
- 8 subtests (TestErrIfShardOwnershipLost) are covered by one ignored placeholder function.

Related: [[matching-go-test-inventory-20261023]] for the wider Go-to-Rust inventory.
