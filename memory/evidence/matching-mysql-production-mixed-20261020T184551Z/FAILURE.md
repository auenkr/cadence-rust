# Failed production MySQL gate run (2026-10-20T184551Z)

Exit code `1`. The run itself was healthy: the Go services, the Rust Matching
production binary, the SDK workflows, the SIGKILL restart, the graceful drain,
the SDK recovery and the dedicated SDK drained backlog all completed. The
failure is a defect of the gate's own query.

## Failure

```
Error: Matching task garbage collection did not converge: {"state":{"backlogDecision":{"rows":[],"taskRows":0,"live":{"startId":1,"endId":100000}}, ... }}
```

Every queue reported an empty metadata row set, a null acknowledgement and a
null persisted range, while the live task-list block was present and every
acknowledged task row was already removed.

## The defect was the observation, not the persistence

The gate read the task-list metadata with

```sql
SELECT task_type, range_id, HEX(data), data_encoding
FROM task_lists WHERE shard_id=54321 AND domain_id=UNHEX('<domain hex>') AND name='<list>'
```

`54321` is Go's *domain* shard, the shard of the `domains` table. The
`task_lists` table is keyed by the *task list* shard, which Go computes with

```go
// go/common/persistence/sql/sqlplugin/dbSharding.go
func GetDBShardIDFromDomainIDAndTasklist(domainID, tasklistName string, numDBShards int) int {
	hash := farm.Hash32([]byte(domainID+"_"+tasklistName)) % uint32(numDBShards)
	return int(hash) % numDBShards
}
```

and the Rust task store computes the same value
(`matching_mysql.rs`, `database_shard`). With the single database
configuration of this gate (`nShards: 1`) every task list is stored in shard
0, and the raw observation confirms it:

```sql
SELECT shard_id, COUNT(*) FROM task_lists GROUP BY shard_id;
-- 0    55
```

The same rows are returned with `shard_id=0` (22 rows for the probed domain)
and none with `shard_id=54321`. The full raw statements and their output are
in `db-observations-raw.json`.

The service persisted the metadata correctly: every queue of the run has its
`task_lists` row with a `thriftrw` blob, a leased range and an
acknowledgement.

## What the run already proved

- The shared database holds Go's schema only, with no private Rust table
  (`results.json`, `goSchemaOnly`).
- Matching's task garbage collector removed every acknowledged row of every
  drained queue: only the never polled control list, the two queues of the
  isolation and pacing scenarios and the Go services' own activity timeout
  queue still hold a row (`db-observations-raw.json`).
- Go's `domains` table holds exactly one row for the probed domain, with the
  UUID Go Frontend reported, Go's own `thriftrw` encoding and a 597 byte
  metadata payload, and Go's `domain_metadata` row holds the notification
  version 5.
