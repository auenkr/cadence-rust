# Failed production MySQL gate run (2026-10-20T183524Z)

Exit code `1`. The run itself was healthy: the Go services, the Rust Matching
production binary, the SDK workflows, the SIGKILL restart, the graceful drain
and the SDK recovery all completed. The failure is in the new garbage
collection gate, and it is a harness defect.

## Failure

```
AssertionError [ERR_ASSERTION]: no Matching task ids were observed in Go's tasks table
    at verifyProductionMySqlTaskGc (tests/matching-mysql-gate.cjs:53:3)
    at run (tests/matching-service-compat.cjs:452:41)
```

The gate read the highest task id of a queue *after* the probe had already
polled the task. By then the task was delivered and the collector had
removed its row, so the query matched nothing and no watermark was recorded.

## What the run already proved

- The shared database holds Go's schema only: the 27 tables of
  `go/schema/mysql/v8/cadence/schema.sql` plus Go's own admin tables
  `schema_version` and `schema_update_history`. No private Rust table
  exists (`results.json`, `goSchemaOnly`).
- Matching's task garbage collector removed every acknowledged row of the
  completed workflow's decision and activity queues, while the task of the
  never polled control list survived (`db-observations-manual.json`).
- The persisted acknowledgement of a queue lags the in-memory
  acknowledgement: the drained decision queue of the completed workflow kept
  `ack=0` while its rows were gone, and the same holds for the activity
  queues of the restart and graceful-drain lists. The persisted
  acknowledgement is written by the lease renewal and the acknowledgement
  timer, not by the completion itself, so it is observed with a lag. Go
  persists the acknowledgement inside `taskListDB.CompleteTask`. This is a
  candidate Matching parity gap and is reported separately.

## Harness fix

The watermark is now read while the task is stored and undelivered:

- the completed workflow's decision task is observed right after the
  workflow started and before the first poll,
- its activity task is observed after the decision completed and before the
  activity poll,
- the final decision task is observed after the activity completed and
  before the final poll,
- the SIGKILL restart list records its persisted watermark before the
  signal,
- a dedicated backlog task list without any poller records its watermark and
  is drained by the existing Go SDK, which must show exactly one workflow
  completion.

The gate now takes one expectation per queue, each with the observed
watermark, and it reads the persisted range and the live task-list block as
one stable snapshot.
