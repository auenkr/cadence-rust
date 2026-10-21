# Matching production MySQL gate harness (2026-10-20)

Branch: `codex/matching-task-store-abstraction` (base `4bc9873b9`).

This checkpoint is source only. The build lane is reserved for the runtime
worker's two-host rerun, so no Cargo build, Go build, cluster, container or
production gate was executed here. Every check below is a syntax, diff or
offline self-check.

## Why

The earlier production MySQL mixed run passed (`EXIT=0`, parent verified)
but its artifacts were deleted before any evidence was committed, so the
repository held a 12 line note with a reconstructed command line. The gate
also never verified Matching's task garbage collection or its domain
metadata against Go's real schema. This change makes the harness record its
own evidence and adds the missing verification.

## What changed

- `tests/matching-thrift-blob.cjs` (new): decodes Go's ThriftRW task-list
  metadata. ThriftRW is the Thrift *binary* protocol (field type byte, field
  id, value; list and map headers write the element and key/value types
  before the size). The field ids are Go's `sqlblobs.TaskListInfo`: kind 10,
  ack level 12, expiry 14, last update 16, adaptive partition config 18.
  `verifyTaskListBlobDecoder` checks the decoder against the committed
  fixtures in `tests/fixtures/matching-blobs.json` before it is used.
- `tests/matching-mysql-gate.cjs` (new): the production gate. The queries
  are injected, so the decision logic is exercised offline.
- `tests/matching-mysql-gate-check.cjs` (new): the offline self-check. It
  runs one converged case and five failure cases (unpersisted
  acknowledgement, removed control task, mismatched range, missing task-list
  row, acknowledgement ahead of the removed task). It passes.
- `tests/matching-service-compat.cjs`: the probe records the highest task id
  of every queue it uses, keeps one unacknowledged control task, and runs
  the gate after the SDK scenarios. Every `tasks` query is constrained by
  the canonical domain UUID, the task list name and the task type. The MySQL
  observation is bounded by a 30 second timeout and reports the error, the
  terminating signal and the exit status instead of returning an empty
  result.
- `tests/matching_evidence.py` (new) and `tests/run-go-service-baseline.py`:
  the harness writes `command.txt` (the exact invocation and the `CADENCE_*`
  environment without passwords), `run.exit`, `results.json` and the
  database observations into the artifacts directory, and `--evidence-dir`
  copies the selected evidence (probe log, result documents, command,
  source revision, binary and configuration hashes, sanitized
  configurations, build and lint outputs, database observations) into a
  durable directory. Databases, binaries, keys and password configurations
  are never copied. The probe timeout for the production restart mode is
  1200 seconds, because the gate adds a bounded wait for the collector.

## Planned production GC assertions

After the workflow, the restart workflow, the graceful-drain workflow and the
SDK scenarios complete, the probe waits (bounded, 90 seconds) until:

1. every acknowledged task row of the completed workflow's task list is gone
   from Go's `tasks` table (`COUNT(*) == 0` for the canonical domain UUID
   and the task list name, both task types);
2. the control task, which was never acknowledged, is still present;
3. the persisted acknowledgement in Go's `task_lists` blob (decoded with the
   ThriftRW decoder) covers the removed task id of every queue whose
   persisted row was observed before its delivery
   (`ack >= max observed task id`). A Matching queue skips task ids that were
   allocated but never completed, so the gate does not assume an upper bound
   for the acknowledgement. A queue without an observed persisted row has no
   durable garbage collection evidence; its acknowledgement is only bounded
   by the value Go persists for a queue that never acknowledged a task, which
   is `0` (`go/common/persistence/sql/sql_task_store.go`, `LeaseTaskList`
   writes `AckLevel: 0` for a new task list);
4. the persisted range and the live task-list block are read as one
   snapshot and must describe the same range. A lease renewal between the two
   reads is resolved by reading both again inside the bounded retry, not by
   tolerating a difference. The gate records the temporal history of every
   snapshot, so the range movement is visible in the evidence;
5. Go's `domains` table holds exactly one row for the probed domain, with
   the UUID Go Frontend reported, Go's own `thriftrw` encoding and a non-empty
   metadata payload, and Go's `domain_metadata` row holds a positive
   notification version.

The harness additionally asserts that the shared database holds Go's schema
only: every table must exist in `go/schema/mysql/v8/cadence/schema.sql` or
be one of Go's own admin tables (`schema_version`, `schema_update_history`).
A private Rust table fails the gate.

## The task list shard

The gate reads the task-list metadata with the task list shard, not with Go's
domain shard. Go computes the task list shard with
`sqlplugin.GetDBShardIDFromDomainIDAndTasklist`, which hashes the domain
UUID and the task list name modulo the number of database shards, and the
Rust task store computes the same value. With the single database
configuration of this gate every task list is stored in shard 0. The
`domains` table keeps Go's domain shard 54321.

A missing metadata row is reported immediately, with the exact statement,
because that is a defect of the observation and not a slow acknowledgement.
Only a missing acknowledgement waits for the bounded retry.

The full observation document, including the snapshot history, is kept in
`db-observations.json` and copied into the evidence directory.

## Frozen binaries

The harness source is frozen before the gate runs, so the evidence records an
exact harness revision:

- harness HEAD for the recorded gate run: see the evidence directory's
  `source.json`
- Rust: `artifacts/frozen-production/cadence-server-task-store-abstraction`
  SHA256 `35e25467dfb6961d41d30565260c6e4afd9ed80b54857a403445cac57131817d`,
  built from source `df3466479`. Unchanged by this work; no Rust source
  change and no Cargo build is part of this gate.
- original Go: `artifacts/frozen-production/cadence-server-go-original`
  SHA256 `0ed004a0f68aee07f132d6e9e8b7faa8e71c992ebdecb11eb30d7c9a257b9bb5`,
  copied off the tmpfs from `/tmp/cadence-go-baseline-1r0mq_xe/cadence-server`
  so a tmpfs cleanup cannot lose it. It is the original Go build of `go/`
  and is passed with `--binary` only; `--go-mixed-server-bin` is forbidden
  because it enables the test-only discovery overlay.

Neither binary is committed; both stay in the untracked `artifacts/`
directory.

The dirty files in the root checkout (`AGENTS.md`,
`rust/crates/cadence-server/src/matching_registry.rs`,
`rust/crates/cadence-server/src/matching_runtime.rs` and the untracked
`rust/crates/cadence-server/src/matching_shard_processor.rs`) are user
edits and are preserved untouched. This work only changes `tests/` and
`memory/` in the isolated worktree.

## Next command (needs the lane)

```sh
cd /home/ubuntu/.t3/worktrees/cadence-rust/matching-task-store-abstraction

docker run --detach --rm --name cadence-mysql-test \
  --env MYSQL_ROOT_PASSWORD=cadence --env MYSQL_DATABASE=cadence \
  --publish 127.0.0.1:3306:3306 mysql:8.0.36
until docker exec cadence-mysql-test mysql --batch --skip-column-names -uroot -pcadence -e 'SELECT 1' >/dev/null 2>&1; do sleep 2; done
docker exec cadence-mysql-test mysql -uroot -pcadence -e 'CREATE DATABASE IF NOT EXISTS cadence_visibility'

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
NODE_PATH=/home/ubuntu/github/auenkr/cadence-rust/cadence-web/node_modules \
CADENCE_MYSQL_DOCKER_CONTAINER=cadence-mysql-test \
python3 tests/run-go-service-baseline.py \
  --rust-matching --matching-restart \
  --rust-production-binary /home/ubuntu/.t3/worktrees/cadence-rust/matching-task-store-abstraction/artifacts/frozen-production/cadence-server-task-store-abstraction \
  --binary /home/ubuntu/.t3/worktrees/cadence-rust/matching-task-store-abstraction/artifacts/frozen-production/cadence-server-go-original \
  --mysql --history-transport grpc \
  --artifacts-dir "/home/ubuntu/.t3/worktrees/cadence-rust/matching-task-store-abstraction/artifacts/mixed-run-$STAMP" \
  --evidence-dir "/home/ubuntu/.t3/worktrees/cadence-rust/matching-task-store-abstraction/memory/evidence/matching-mysql-production-mixed-$STAMP"
```

The original Go binary is passed with `--binary` only. `--go-mixed-server-bin`
is forbidden in the production lane because it enables the test-only
discovery overlay.

The run is expected to end with `EXIT=0`, `results.json` carrying
`taskGarbageCollection`, `domainMetadata` and `goSchemaOnly`, and the
evidence directory holding the probe log, the result documents, the command,
the hashes, the sanitized configurations and the database observations. The
artifacts directory is removed afterwards; the evidence directory is
committed.

This is still not Matching service completion. The production gate remains
preliminary evidence until the parity audit and the remaining failure and
recovery cases are done.
