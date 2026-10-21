# Matching production MySQL mixed gate (2026-10-20T194644Z)

Fresh production gate run, exit code `0`.

- harness source: `b5e6ba875` (`codex/matching-task-store-abstraction`)
- Rust production binary: SHA256
  `35e25467dfb6961d41d30565260c6e4afd9ed80b54857a403445cac57131817d`
  (built from source `df3466479`, unchanged)
- original Go binary: SHA256
  `0ed004a0f68aee07f132d6e9e8b7faa8e71c992ebdecb11eb30d7c9a257b9bb5`,
  passed with `--binary` only, no test-only discovery overlay
- MySQL 8.0.36 container, Go's real v8 schema, no private Rust table
- topology: original Go Frontend/History/Worker, Rust Matching discovered
  through Go's original Ringpop peer provider, no Go Matching
  (`goMatchingStarted: false`)
- probe result `preliminary-passed`, `acceptanceEligible: false`

## Task garbage collection (strict)

Every acknowledged task row of every drained queue is removed, the persisted
acknowledgement covers the observed durable task id, and the persisted range
matches the live task-list block.

| queue | remaining rows | persisted ack | observed durable id | persisted range | live block |
|---|---|---|---|---|---|
| backlog decision (SDK drained) | 0 | 1 | 1 (strict) | 1 | 1-100000 |
| backlog activity | 0 | 0 | none, no durable evidence | 1 | 1-100000 |
| completed workflow decision | 0 | 2 | 2 (strict) | 2 | 100001-200000 |
| completed workflow activity | 0 | 1 | 1 (strict) | 2 | 100001-200000 |
| SIGKILL restart decision | 0 | 1 | 1 (strict) | 3 | 200001-300000 |
| SIGKILL restart activity | 0 | 0 | none, no durable evidence | 2 | 100001-200000 |

The unacknowledged control task survived the collector
(`pendingTaskRows: 1`). The gate read 45 snapshots; the first snapshot caught
a lease renewal between the persisted row and the live block and the next
snapshot was consistent, which is the stable snapshot retry working as
designed. The acknowledgement of the drained backlog decision queue advanced
from 0 to 1 in the last snapshot, which is the acknowledgement timer of Go's
`taskListDB.UpdateState` behaviour.

## Domain metadata and schema

- Go's `domains` table holds exactly one row for the probed domain
  `service-compat-30f84b05-fbdf-459c-960c-7f30a8bab7d1`, with the UUID Go
  Frontend reported (`01a15ada22977d2d887f19234e4a8cf3`), Go's own `thriftrw`
  encoding and a 597 byte metadata payload
- Go's `domain_metadata` row holds the notification version 5
- the shared database holds 29 tables: the 27 tables of
  `go/schema/mysql/v8/cadence/schema.sql` plus Go's own admin tables
  `schema_version` and `schema_update_history`. No private Rust table and no
  missing Go table.

## SDK, restart and recovery

- the completed workflow shows the full event sequence from the start to the
  completion
- the SIGKILL restart: one durable row with the observed watermark
  `{rows: 1, maxTaskId: 1}` before the signal, one completion event after the
  recovery, first Rust process 2722482, second 2722554
- the graceful drain: one queued row before the signal, one completion event
  after the recovery, draining process 2722554, recovered by 2722749
- the dedicated garbage collection backlog: watermark `{rows: 1, maxTaskId: 1}`
  observed while the task was stored and undelivered, drained by the existing
  Go SDK with exactly one completion event
- query dispatch, isolation dispatch and the native partition configuration
  passed

## Files

`probe.log` (full), `results.json`, `run.exit`, `command.txt`, `server.log`,
`harness-run.log`, `db-observations.json` (the complete gate observation with
its 45 snapshots and the exact statements), `gate-summary.json` (the parsed
summary), `gc-backlog-sdk.log`, `go-sdk.log`, `restart-sdk.log`,
`graceful-drain-sdk.log`, `rust-server-logs/` (the Rust Matching service
logs), `config-redacted/` (the generated configurations without credentials),
`config-hashes.json`, `binaries.json` (the binary hashes), `source.json` (the
source revision).

This is production mixed run evidence for the Matching task garbage
collection, the domain metadata and the shared schema. It is not Matching
service completion: the parity audit, the remaining failure and recovery
cases and the acceptance decision are still open.
