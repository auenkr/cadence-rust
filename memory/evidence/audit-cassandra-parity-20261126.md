# Cassandra persistence parity audit (2026-11-26) — IN PROGRESS

Branch `audit-cassandra-parity`. No cargo/go/docker run locally. Status: matching task store (`matching_cassandra.rs`)
audited; remaining files listed under "Not yet audited". CI results are in the "CI" section.

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

## History execution tasks (history_execution_cassandra_tasks.rs vs workflow.go / workflow_cql.go)
| Statement | Result |
|---|---|
| Get transfer/replication tasks (`task_id >= ? and task_id < ?`, visibility_ts = default, PageSize+PageState) | match |
| Get timer tasks (`visibility_ts >= ? and < ?`, ms truncation) | match (Go template lacks spaces e.g. `type = ?and`, CQL-equivalent) |
| Complete transfer/timer/replication (logged batch chunks of 100, consistency ALL) | match |
| Range delete transfer (`task_id >= min and < max`), replication (`task_id < max`), timer (visibility range) with ALL | match (returns UnknownNumRowsAffected) |
| DLQ size/insert/delete/range-delete, cross-cluster delete | match |
No mismatches found.

## History shard (history_shard_cassandra.rs vs shard.go / shard_cql.go / nosql_shard_store.go)
| Item | Result |
|---|---|
| Create `IF NOT EXISTS`, Get, Update `IF range_id = ?`, UpdateRangeID `IF range_id = ?` text/keys | match (UDT bound whole) |
| GetShard repairs range_id column when UDT range is ahead | match |
| Create not-applied error | MISMATCH, FIXED: Go returns ShardAlreadyExistError; Rust returned ShardOwnershipLost, so History's create-race re-read (`shard_events.rs` AlreadyExists arm) never ran on Cassandra. Now `E::AlreadyExists`; unit test `lost_create_race_is_already_exists`. |
| Shard UDT contents | MISMATCH, NOT FIXED: Go writes all 19 UDT fields (owner, stolen_since_renew, ack levels, cluster maps, queue-state blobs, markers); Rust writes only shard_id, range_id, updated_at. The thriftrw `data` blob is authoritative on read in both, so Go<->Rust interop is unaffected; fixing needs decoding `ShardInfoRecord` into typed UDT fields and CI iteration. |

## History execution write (history_execution_cassandra_write.rs) — spot check only
All batches are LOGGED (matches Go workflow.go). Current-row insert `IF NOT EXISTS USING TTL 0`, current update `USING TTL 0 ... IF current_run_id = ?`,
request rows `IF NOT EXISTS USING TTL ?` / `USING TTL ?`, policy `IF NOT EXISTS`, lease `IF range_id = ?`, update `IF next_event_id = ?` match workflow_cql.go lines 243-386.
Full per-column UDT and cas_error mapping review not completed.

## Not yet audited
history_cassandra, history_v2_cassandra, history_execution_cassandra, (write: UDT columns + cas_error),
history_visibility_cassandra, visibility_cassandra, matching_domain_cassandra, semaphores_cassandra_native,
cassandra.rs (session defaults: Rust LOCAL_QUORUM/LOCAL_SERIAL), error mapping (timeout -> Cadence error).

## CI
Head SHA of the CreateShard fix: 25d0dee92da7f5bb1facb5400e36f6ed14a0083e.
- rust-ci package=cadence-persistence: success, https://github.com/auenkr/cadence-rust/actions/runs/45749245771
- rust-ci workspace: success, https://github.com/auenkr/cadence-rust/actions/runs/45749928949
- recovery-timing database=cassandra scenarios=kill_history,kill_matching repetitions=5: success, https://github.com/auenkr/cadence-rust/actions/runs/45751428633
  - Rust cadence-server SHA256 f1322cc8b503a700765843c7f3f331dab015343562055a20c3db3b1091e13ecc; cadence-history c8f87c03eb41437fa26b10ee8beeb2ffcb95e1d87e953d6f56d107141f093e4a; Go cadence-server f04c1972a31588e28ccf0dbf92c8f928a80d0dc48375e4bc3e62a0642863d755.
  - Workflows started/completed/lost: go 1049/1049/0, rust 942/942/0.
  - Median first completion (ms): kill_history go 37 / rust 26; kill_matching go 2222 / rust 32. Stable median: kill_history go 2108 / rust 1809; kill_matching go 2573 / rust 2641. No unstable reps.


## Follow-up fixes: branch fix-cassandra-parity3 (2026-11-26)
Head SHA tested: b0976006962df792ecac67762117fde35441ca3e.
- SELECT_TASKS: now Go's verbatim `templateGetTasksQuery` (no LIMIT, `TTL(task) AS ttl`); `CqlStatement::with_page_size(batch)` sets the driver page size and the Scylla executor reads one page (`query_single_page`), matching Go's `PageSize(batch)` + stop at batch size. Unit test asserts page size; Go-source oracle test now compares the statement verbatim. Native (bytes) store uses the same path.
- Shard UDT: `build_udt` decodes the thriftrw `data` blob (ShardInfoRecord + raw queue-state/marker blobs) and writes all 19 UDT fields on create and update like Go (`replication_dlq_ack_level` not carried by the Rust record, written null; nil maps/blobs null). Unit test `udt_carries_all_go_fields_from_blob`.
- history_execution_cassandra_write.rs `cas_error` (update path) vs `workflow_utils.go`: FIXED 3 mismatches: (1) current-run mismatch only when the conditional run id is non-empty and differs; (2) next_event_id mismatch only when actual != previousNextEventIDCondition; (3) fallback message now includes requestConditionalRunID. Unit tests added. Create path, shard range mismatch, duplicate-request mapping match Go. Column list formatting in the "unknown" messages differs (`{row:?}` vs Go `i: k=v`, Go's order is map-random anyway) - not fixed, message text only.
CI:
- rust-ci package=cadence-persistence: success, https://github.com/auenkr/cadence-rust/actions/runs/45758808827 (earlier runs 45757889713 clippy bound, 45758345846 oracle test: failed, fixed).
- rust-ci workspace: success, https://github.com/auenkr/cadence-rust/actions/runs/45759278188
- recovery-timing database=cassandra scenarios=cold_restart,kill_history repetitions=3: success, https://github.com/auenkr/cadence-rust/actions/runs/45759281887
  - Rust cadence-server SHA256 7c22d05635b496a6dbfafd4ebf336b6539c411ddc703fe1a5222f668ade8ada9; cadence-history 789e292a805130c51a19fa3dbd2a17bb9babcc1d66194ce9a3dd86ca3a5f9ed3; Go cadence-server 5ed8be2d0506003a1ccefe455a89e7c513780c8bf6c767ec144a388bd9e8572b.
  - Workflows started/completed/lost: go 1143/1143/0, rust 993/993/0.
  - First completion median (ms): cold_restart go 11714 / rust 10238; kill_history go 58 / rust 1590. Stable median: cold_restart go 14971 / rust 14013; kill_history go 2476 / rust 1905. No unstable reps.
