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

## Gaps
- A recording-session harness (statement + bound values) was NOT built: `CassandraMatchingTaskStore` takes a concrete scylla `Session` and mysql/postgres adapters take concrete drivers; recording needs a session trait refactor (production change), which was out of scope without a local build.
- Update/Lease TTL variants and ListTaskList/GetOrphanTasks unported.

## CI
(see below)
