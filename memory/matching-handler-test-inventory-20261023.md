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
