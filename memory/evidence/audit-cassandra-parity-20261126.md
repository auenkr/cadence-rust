# Cassandra persistence parity audit (2026-11-26) — IN PROGRESS

Branch `audit-cassandra-parity`. No cargo/go/docker run locally. Status: matching task store (`matching_cassandra.rs`)
audited; remaining files listed under "Not yet audited". No CI runs have been dispatched yet, so no run URLs or SHA256 exist.

Go: `go/common/persistence/nosql/nosqlplugin/cassandra/tasks.go`, `tasks_cql.go`, `go/common/persistence/nosql/nosql_task_store.go`.
Rust: `rust/crates/cadence-persistence/src/matching_cassandra.rs`.

| Statement | Go | Rust | Result |
|---|---|---|---|
| SelectTaskList | tasks.go:46, tasks_cql.go templateGetTaskList | matching_cassandra.rs:32 | match (default consistency) |
| InsertTaskList `IF NOT EXISTS`, range 1 | tasks.go:~170, store :127 | :41, lease :496 | match; not-applied -> ConditionFailed, no retry |
| UpdateTaskList `IF range_id=?` | tasks.go:~200 | :37 | match; not-applied -> ConditionFailed with columns text |
| UpdateTaskListWithTTL (logged batch, part1 INSERT USING TTL + part2 UPDATE USING TTL, 86400) | tasks.go:~250 | :36,:38, update_task_list | match (TTL literal vs bound `?`, same value); batch type not asserted in Rust test |
| DeleteTaskList `IF range_id=?`, consistency ALL, fallback to LOCAL_QUORUM on consistency error | tasks.go:~290 | :39, delete_task_list | match |
| InsertTasks batch: plain insert if ttl<=0, TTL capped at maxCassandraTTL, plus fence `UPDATE ... IF range_id` | tasks.go:~330 | create_tasks_with_domains, insert_task_statement, range_fence_statement | match on text/TTL/fence; Rust inlines literals instead of binding (documented design) |
| Expired re-added task skipped (ttl<=0 when Expiry set) | nosql_task_store.go:310-324 | task_not_expired | match |
| GetTasksCount | tasks.go:~410 | :42 | match |
| SelectTasks | tasks_cql.go templateGetTasksQuery + `PageSize(batch)` | :43 `... LIMIT ?` | MISMATCH (query text): Go has no LIMIT and uses driver page size, breaking at batch size; Rust binds LIMIT. Row sets are equal. Go also aliases `TTL(task) AS ttl`. Left unchanged: the fix needs an executor page-size path and was not verifiable without CI. Existing Rust test comment documents the deviation. |
| RangeDeleteTasks | tasks.go:~500 | :40, range_delete_tasks | match (ALL consistency, flag-gated, fallback to default) |
| ListTaskList | tasks.go:~270 | n/a | Go returns "unsupported operation" |

## Not yet audited
history_cassandra, history_v2_cassandra, history_execution_cassandra{,_tasks,_write}, history_shard_cassandra,
history_visibility_cassandra, visibility_cassandra, matching_domain_cassandra, semaphores_cassandra_native,
cassandra.rs (session defaults: Rust LOCAL_QUORUM/LOCAL_SERIAL), error mapping (timeout -> Cadence error).

## CI
None run yet (rust-ci cadence-persistence, workspace, recovery-timing kill_history,kill_matching x5 are pending).
