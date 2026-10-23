# Matching persistence test port inventory (2026-10-23)

Branch `matching-persistence-tests-20261023`. Not run locally (no cargo); CI result at bottom.

| Go test | Rust test | Status |
|---|---|---|
| persistence-tests TestGetTasksWithNoMaxReadLevel (:170) | matching_sqlite.rs go_suite_get_tasks_paging_is_ascending_and_exclusive | PORTED |
| TestGetTaskListSize (:143) | go_suite_task_list_size_counts_above_ack_level | PORTED |
| TestCompleteTasksLessThan (:260) | go_suite_complete_tasks_less_than_limits_delete_lowest_first | PORTED |
| TestLeaseAndUpdateTaskList (:333) | go_suite_lease_and_update_task_list_range_conditions | PORTED (partition config round-trip covered by cassandra udt test, not here) |
| TestLeaseAndUpdateTaskListSticky/Ephemeral (:433,:467) | go_suite_sticky_and_ephemeral_kinds_lease_and_update | PORTED |
| TestCompleteDecisionTask (:219) | go_suite_complete_task_removes_each_row | PORTED |
| cassandra tasks_test.go TestInsertTasks (:741) | matching_cassandra.rs go_insert_tasks_batch_ttl_variants_and_fence | PORTED as statement-builder asserts (TTL 0 plain, TTL cap 157680000, fence) |
| tasks_test.go TestSelectTasks/RangeDelete/GetTasksCount | cassandra_cql_matches_go_tasks_cql (pre-existing) + cassandra_task_row_constants_match_go | PORTED (template text only, not bound values) |
| tasks_test.go TestInsertTaskList/UpdateTaskList(+TTL)/DeleteTaskList/SelectTaskList | cassandra_cql_matches_go_tasks_cql (templates) | PARTIAL: no bound-value assertions |
| TestCreateTask, TestGetDecisionTasks, TestListWithOneTaskList/Multiple, TestGetOrphanTasks | none | NOT PORTED (Rust store trait has no list-task-lists / orphan-scan API) |
| sqlplugin/{mysql,postgres}/task.go tests | existing go_sql_pin template tests in matching_mysql/postgres.rs | NOT EXTENDED |

## Recording executor (branch matching-persistence-recording-20261023)
Cassandra store now sits on `CqlExecutor` (`matching_cassandra_exec.rs`): `ScyllaExecutor` sends the identical statement text, per-statement consistency and ordered values through the Scylla session; tests use a `Recorder`. Module `recording_tests` in `matching_cassandra.rs` asserts whitespace/case-normalised statement text and Debug-rendered bound values in order:

| Go test (cassandra/tasks_test.go) | Rust recording test | Status |
|---|---|---|
| TestSelectTaskList (:109) | go_select_task_list_statement_and_values, ..._missing_row_is_not_found | PORTED |
| TestInsertTaskList (:244) | go_insert_task_list_statement_and_values, ..._not_applied_is_condition_failure | PORTED |
| TestUpdateTaskList (:394) + LeaseTaskList update | go_update_task_list_statement_and_values, go_lease_task_list_update_statement_and_values, ..._not_applied_... | PORTED |
| TestUpdateTaskListWithTTL (:505-506) | go_update_task_list_with_ttl_batch_statements_and_values (kinds 1,2) | PORTED; Go binds TTL=taskListTTL 86400 (nosql_task_store.go:49,234), Rust inlines 86400 |
| TestInsertTasks (:790-792) | go_insert_tasks_batch_members_and_fence, ..._not_applied_... | PORTED (task rows are CQL literals, so no bound values: pre-existing Rust design) |
| TestGetTasksCount (:881) | go_get_tasks_count_statement_and_values | PORTED |
| TestSelectTasks (:1073) | go_select_tasks_statement_and_values | PORTED (Rust binds trailing LIMIT; Go uses page size) |
| TestRangeDeleteTasks (:1149), CompleteTask(s) | go_range_delete_tasks_statement_and_values, ..._all_consistency_retries_at_local_quorum, ..._non_unavailable_error_is_not_retried | PORTED |
| TestDeleteTaskList (:617-700) | go_delete_task_list_* (ALL, fallback LOCAL_QUORUM, failed retry, non-consistency error, not applied) | PORTED |
| TestListTaskList (:572) | none | OUT OF SCOPE for Matching (below) |

## ListTaskList / GetOrphanTasks
Go Matching never calls them at runtime. Non-test callers are only `go/service/worker/scanner/tasklist/db.go:62` (GetOrphanTasks) and `:112` (ListTaskList), i.e. the Worker scanner, plus persistence wrappers (`common/persistence/task_manager.go:72,101`, metered/ratelimited/errorinjectors). `service/matching` only has a TestTaskManager stub (`tasklist/testing.go:196,316`). They belong to the Worker migration; not implemented in the Rust Matching store.

## Gaps
- MySQL/PostgreSQL: NOT done. Go has no per-plugin task tests under `sqlplugin/mysql` or `sqlplugin/postgres` (only `sql/sql_task_store_test.go`, store-level over a mock plugin). The Rust SQL stores run statements through pooled `mysql::Conn`/`Transaction` and postgres leases (lease+transaction code paths), so an executor seam there is a larger refactor; it was not attempted without a local build. Existing go_sql_pin template tests remain the SQL coverage.
- Rust-vs-Go difference found: none requiring a fix in the Cassandra statements/values.
- Not ported: TestCreateTask, TestGetDecisionTasks, TestListWithOneTaskList/Multiple, TestGetOrphanTasks (Worker-owned, above).

## CI
(see below)
- Run https://github.com/auenkr/cadence-rust/actions/runs/40015106530, commit d40c20e94b6b8058da4570a815e94bbfefcd80e6, clippy+test (cadence-persistence): success
