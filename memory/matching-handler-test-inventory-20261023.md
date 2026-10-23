# Matching handler test inventory (2026-10-23)

Branch `matching-handler-tests-20261023`. Go spec: go/service/matching/handler/*_test.go.
Earlier per-test rows (context, engine, engine_integration, handler, membership) are in
`matching-go-test-inventory-20261023.md`; this file lists what this pass added or re-classified.

| Go test | Rust test | Status |
|---|---|---|
| handler_test.go TestDescribeTaskList / rate limiter not allowed (598) | matching_rpc_tests.rs::go_handler_describe_task_list_rate_limited_returns_service_busy | PORTED, CI pass |
| handler_test.go TestListTaskListPartitions / rate limited (656) | go_handler_list_task_list_partitions_rate_limited_returns_service_busy | PORTED, CI pass |
| handler_test.go TestUpdateTaskListPartitionConfig / rate limited (854) | go_handler_update_task_list_partition_config_rate_limited_returns_service_busy | PORTED, CI pass |
| handler_test.go TestRefreshTaskListPartitionConfig validation (804) | go_handler_refresh_partition_config_rejects_missing_tasklist_and_non_normal_kinds | PORTED, CI pass |
| handler_test.go TestGetTaskListsByDomain / rate limited (712) | matching_rpc_quotas_classify_shared_protocol_handlers_and_preserve_count_only_calls | COVERED (existing) |
| handler_test.go TestDomainName (771) | none | CANNOT-PORT: Go swallows domain-cache errors to "" in a private helper; Rust `domain_name` returns Err(Status) |
| handler_test.go TestNewHandlerContext (160) | none | CANNOT-PORT: no Rust handler-context type |
| context_test.go TestHandleErr* (38,48,129) | none | CANNOT-PORT: no generic handleErr; see matching-test-parity-gaps-20261023.md |
| membership_test.go panic tests (169,423) | none | CANNOT-PORT: Go panic recovery |
| remaining engine*/handler* rows | see matching-go-test-inventory-20261023.md | unchanged (PARTIAL/COVERED) |

Not done: the engine_integration_test.go PARTIAL rows (about 30) were not re-examined or extended in this pass.

## CI
- rust-ci run 40015060031 (package=cadence-matching), commit on branch matching-handler-tests-20261023:
  success. https://github.com/auenkr/cadence-rust/actions/runs/40015060031

## engine_integration_test.go pass (branch matching-engine-integration-tests-20261023)

INCOMPLETE. Only group 1 (add/add-then-poll) was examined and ported. All other rows remain PARTIAL as in
matching-go-test-inventory-20261023.md and have NOT been re-examined. engine_test.go (22 tests) was not started.

| Go test | Rust test | Status | CI run |
|---|---|---|---|
| TestAddActivityTasks / TestAddDecisionTasks (592,596; 111 adds all persisted) | matching_rpc_tests.rs::go_engine_add_decision_and_activity_tasks_persist_every_task | PORTED | 40016294676 success |
| TestAddActivityTasksForwarded / TestAddDecisionTasksForwarded (600,604; expects errRemoteSyncMatchFailed, 0 stored) | matching_rpc_forwarded_add_never_persists_and_reports_remote_failure | COVERED (existing) | - |
| TestAddAndPollDecisionTasks (709) | matching_rpc_tests.rs::go_engine_add_then_poll_decision_tasks_delivers_each_task_and_drains_store | PORTED, PARTIAL: FIFO delivery + no redelivery asserted; Go's `GetTaskCount==0` store-drain and rangeID checks NOT asserted (Rust deletes acked rows via batched GC; trigger not verified) | 40016294676 success |
| TestAddAndPollActivityTasks (713) | none | NOT PORTED: Go body actually calls the decision path (bug at engine_integration_test.go:714), so the decision port covers it; activity add-then-poll needs a RecordActivityTaskStarted history stub that does not exist in this file | - |
| TestAddAndPoll*Isolation (717,721) | none | NOT PORTED (also run decision path in Go, :718/:722) | - |
| TestQueryWorkflow (438) | matching_rpc_delivers_query_to_decision_poller | COVERED (existing) | - |
| all others (empty poll, expired ctx, sync match, concurrent, range stealing, unload on isolation change, drain backlog, TestOnlyUnloadMatchingInstance, TestAddTaskAfterStartFailure, metric oracles, TestConfigDefaultHostName) | see earlier inventory | NOT RE-EXAMINED, still PARTIAL | - |

Notes for the next pass: `SqliteTaskStore::task_list_size(key, ack_level)` counts task_id > ack_level, so use -1 to count all rows;
its second argument is not the task type (the key carries it).

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
