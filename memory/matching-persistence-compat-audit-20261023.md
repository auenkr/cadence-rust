# Matching persistence wire-compat audit (Go vs Rust) — 2026-10-23

Branch `matching-persistence-compat-20261023` (from origin/master a028331fb).
Scope: what the Matching service reads/writes. Go paths are relative to `go/common/persistence`,
Rust paths to `rust/crates/cadence-persistence/src` (`mysql`=matching_mysql.rs, `pg`=matching_postgres.rs,
`cass`=matching_cassandra.rs, `blob`=matching_blob.rs).
Status legend: OK = identical/equivalent; FIXED = difference proven from source and fixed here;
GATE = needs a live DB to confirm; N/A = not used by Matching.

## 1. SQL (MySQL + PostgreSQL) tasks / task_lists

| Area | Go | Rust | Status |
|---|---|---|---|
| Statement text, MySQL | `sql/sqlplugin/mysql/task.go:32-104` | `mysql` consts `SELECT_TASK_LIST`..`RANGE_DELETE_TASKS` | OK; now pinned by `matching_mysql_sql_matches_go_task_go` (string-for-string against the Go file) |
| Statement text, PostgreSQL | `sql/sqlplugin/postgres/task.go:32-105` (range delete uses `task_id IN (SELECT ... LIMIT $5)`) | `pg` consts | OK; pinned by `matching_postgres_sql_matches_go_task_go`. The PG lease INSERT bound `range_id` as literal `0`; now binds `$5` like Go (FIXED, cosmetic). `count(1)` now has Go's `as count` alias |
| Column order | `task_lists(shard_id, domain_id, name, task_type, range_id, data, data_encoding)`, `tasks(domain_id, task_list_name, task_type, task_id, data, data_encoding)` (no shard column in tasks) | same | OK (schema tests already exist) |
| Shard id | `sqlplugin/dbSharding.go:45` `farm.Hash32(domainID+"_"+name) % nShards` (go-farm 20240924) | `matching_task_store.rs::go_db_shard_id` (farmhash 1.1.5 `hash32`) | OK; vectors from the real go-farm pinned in `db_shard_id_matches_go_farm_hash32_vectors`. Note Go's `Hash32`==`Fingerprint32` and the Rust crate's `hash32` is the same `mk_hash32` |
| nShards for singleton DB | `sqldriver/connections.go:39` singleton ignores shard for routing, but `plugin.go:63` still passes `cfg.NumShards` to `numDBShards`, so `task_lists.shard_id` = hash % nShards even with one DB | `n_shards` used the same way | OK |
| Multi-DB routing | `sqldriver/sharded.go:99` `dbs[dbShardID]`; requires nShards>1 and len(multipleDatabasesConfig)==nShards | `mysql_database_index` / `database_index` | OK |
| Tasks insert domain | `sql_task_store.go:456` uses the task's own `Data.DomainID`; shard from the task-list's domain | `create_tasks_with_domains` (per-task domain) | OK |
| Lease flow | `sql_task_store.go:81-205`: select; on ErrNoRows insert (range 0) outside tx; renew check before decoding; `FOR UPDATE` lock; `UPDATE` rangeID+1; stored kind kept, request kind returned | mysql/pg `lease` | OK (messages `haveRangeID:0` identical) |
| UpdateTaskList | `:242-303` expiry=now+24h only for kinds 1,2 else Unix(0,0); lock with request range; `rowsAffected != 1` error | `update_task_list` | OK |
| CreateTasks | `:412-501` insert rows then lock+check; CreatedTimestamp = request time | pg already set `created_nanos=now`; **mysql ignored `now_nanos`** | FIXED (mysql now stamps created time with the request time) |
| CompleteTask / CompleteTasksLessThan / DeleteTaskList / GetTaskListSize | `:386-410, 542-584, 65-79` (limit 0 => "missing limit parameter"; delete count must be 1) | same | OK |
| ListTaskList, GetOrphanTasks | `:312-384, 589-608` | not implemented in Rust store | N/A for Matching (admin/scavenger only) |
| TTL / `*WithTTL` | `SupportsTTL()==false` for MySQL/PG (`task.go:224-237` returns ErrTTLNotSupported) | not used | OK |
| Blob thrift layout | `idls/thrift/sqlblobs.thrift:236-263` TaskInfo ids 10,12,13,14,15,17; TaskListInfo 10,12,14,16,18 | `blob` encode/decode | OK; golden bytes in `tests/fixtures/matching-blobs.json` (generated from the Go encoder incl. `time.Time{}.UnixNano()` = -6795364578871345152) already cover zero time, empty map vs nil, partition config |
| Expiry on create | `sql_task_store.go:427-441` now+ScheduleToStartTimeoutSeconds else zero time | `TaskData::at_creation` | OK (unit tested) |
| **Write encoding** | `serialization/parser.go:224,241` encodes with dynamic config `history.serializationEncoding` (default `thriftrw`, `dynamicproperties/constants.go:5641`); the SQL yaml `encodingType` is declared (`config/config.go:337`) but never read by any persistence code | Rust used `sql.encodingType` (parsed at startup, error on unknown) | **FIXED**: `EncodingSource::dynamic` reads `history.serializationEncoding` per write; `encodingType` ignored like Go. Tests: `dynamic_encoding_source_is_sampled_per_write_like_go_parser` |
| Read encoding | `getDecoder` accepts `thriftrw`, `thriftrw_snappy` | `BlobEncoding::parse` | OK |
| Domain id string for hashing | Go hashes the string it receives (lowercase UUID from History) | Rust hashes `Uuid` Display (lowercase) | OK for lowercase IDs; an upper-case domain ID string sent by a client would hash differently (edge, History always sends lowercase) |
| Isolation / DSN attrs | `mysql/plugin.go` forces `parseTime`, `clientFoundRows`, `multiStatements`; isolation default READ-COMMITTED | `mysql_options`, `configured_isolation` | OK (`CLIENT_FOUND_ROWS`); runtime confirmation = GATE |

## 2. Cassandra (tasks table)

| Area | Go | Rust | Status |
|---|---|---|---|
| Statements | `nosqlplugin/cassandra/tasks_cql.go:24-144` | `cass` consts + `insert_task_statement`, `range_fence_statement` | OK; pinned by `cassandra_cql_matches_go_tasks_cql` and `cassandra_task_statements_have_go_shape` (bound-UDT form equals Go's inline UDT template; TTL inlined as 86400) |
| Row types | `rowTypeTask=0`, `rowTypeTaskList=1`, sentinel `-12345`, initial range 1 | same | OK |
| TTL | ttl<=0 none, cap `maxCassandraTTL=157680000`; re-added tasks use `Expiry-now`, expired skipped (`nosql_task_store.go:342-381`) | `task_ttl`, `task_not_expired` | OK |
| Task UDT domain | Go writes the **task-list** domain into the UDT/partition (`nosql_task_store.go:329`) | uses `key.domain_id` | OK |
| Update with TTL (sticky/ephemeral) | `tasks.go:263-306` logged batch: INSERT USING TTL + conditional UPDATE USING TTL | `update_task_list` | OK |
| Lease, lost `IF NOT EXISTS` race | `nosql_task_store.go:127-135` returns ConditionFailed `leaseTaskList: taskList:..., haveRangeID:1, gotRangeID:<existing>`; no retry | Rust re-read and **leased on top of the competitor's row** | **FIXED** |
| Lease renew mismatch message | `"leaseTaskList:renew failed: taskList:%v, taskListType:%v, haveRangeID:%v, gotRangeID:%v"` (space after colon, real requested range) | copied the SQL variant (`failed:taskList`, `haveRangeID:0`) | **FIXED** |
| LWT rejected messages | `tasks.go:243-257` reads conflicting `range_id`; messages for lease/update/insert/delete at `nosql_task_store.go:140,248,331,354` | generic text, no conflicting range | **FIXED** (conflicting `range_id` is parsed from the LWT result row; same message templates; `columns:` shows only `range_id=`, Go prints the whole conflicting row in map order) |
| DeleteTaskList rejected | ConditionFailedError | `DeleteCount(0)` (SQL-style internal error) | **FIXED** |
| CompleteTask | range delete `task_id > id-1 AND task_id <= id` through `executeWithConsistencyAll` (honours `system.enableCassandraAllConsistencyLevelDelete`) | plain `task_id = ?`, ignored the flag | **FIXED** |
| CompleteTasksLessThan | single range delete via `executeWithConsistencyAll`, **no COUNT**, returns `UnknownNumRowsAffected` (-1) (`tasks.go:521-531`) | extra unbounded `SELECT count(1)` over the range first, returned the count | **FIXED** (no COUNT; returns 0 so `task_gc` stops after one delete, as -1 does) |
| GetTasks | page size = batch size; skip rows without `task_id` (static-column row) | `LIMIT ?`; selects only regular columns so Cassandra never returns a static-only row | OK (no gate-visible difference) |
| Count | `count(1) as count` | alias added | OK |
| Sharded NoSQL (`shardedNosql` tasklist policy) | `nosql_task_store.go` `GetStoreShardByTaskList` | not supported: `open_configured_default_store` fails closed when only `shardedNosql` is configured | GAP (documented, fail-closed) — GATE/feature |
| Consistency / serial consistency, TLS, DC aware | `cassandra/configuration.go` | `CassandraSessionOptions` | OK on paper; GATE for LWT serial consistency behaviour |
| Partition-config normalisation | `tasks.go:81-107,145-176` len/num mismatch -> default partitions; num = len(map); empty map -> NULL | `matching_partition.rs::from_storage/to_storage` | OK |

## 3. Domain metadata, config store, semaphores

| Area | Go | Rust | Status |
|---|---|---|---|
| MySQL domain reads | `sqlplugin/mysql/domain.go:40-58` shard 54321, `ORDER BY id LIMIT`, `id > ?` paging, `domain_metadata.notification_version` | `matching_domain_mysql.rs` (adds `MAX_EXECUTION_TIME` hint only) | OK |
| PostgreSQL domain reads | `sqlplugin/postgres/domain.go` | `matching_domain_postgres.rs` | OK (same shape; runtime = GATE) |
| Cassandra domain reads | `cassandra/domain.go` | `matching_domain_metadata.rs` | OK (existing fixtures/tests) |
| Domain on multi-DB | Go domain store uses DB shard 0 | `pools[0]` | OK |
| Dynamic config / config store | `common/dynamicconfig/configstore`, `sql_config_store.go` | `dynamic_config.rs` typed reads; **string reader added** for `history.serializationEncoding` | OK; reading the SQL/Cassandra config-store tables is performed by the admin/dynamic-config layer, outside this audit slice |
| Semaphore tables | `sql_semaphore_*`: SQL returns unsupported; Cassandra `semaphore_tasks.go` | `SqlUnsupportedSemaphorePersistence`; `CassandraSemaphorePersistence` | OK for SQL; Cassandra statements not re-audited in this slice -> GATE |

## Needs gate (live DB)

1. MySQL/PG `affected_rows` semantics for the lease/update statements with identical payload (needs `CLIENT_FOUND_ROWS` on the Rust connection; wired but unconfirmed live).
2. Cassandra LWT rejected-result column order: `range_id` is located by name in `QueryRowsResult::column_specs()`; confirm against a real conflicting lease.
3. Cassandra `UPDATE ... USING TTL 86400` inline vs Go's bound TTL parameter (semantically equal).
4. Rust-written rows read back by Go for both thriftrw and thriftrw_snappy with `history.serializationEncoding` toggled.
5. Cassandra tasks UDT `adaptive_partition_config` round trip from Go rows.
6. Cassandra semaphore tables.

## Residual / not done

* `shardedNosql` task persistence (fail-closed).
* Cassandra conflict `columns:` detail prints only `range_id`.
* SQLite store (Rust-only dev backend) still uses a fixed encoding.
