# Matching v7 database lane artifact review guide

Use with the v7 ledger and readiness map. This is a reviewer checklist for the
serial PostgreSQL, MySQL probes, and Cassandra lanes at one immutable
`FINAL_SHA`; it records no run result or runtime claim. Review downloaded
artifacts, not requested flags or job conclusion alone.

## Common identity and topology proof

- Require Actions `headSha`, `out/commit.txt`, and the expected pin to be the
  same full SHA. For the production workflow require `out/binary.sha256`; for
  the Cassandra workflow require `out/binary.sha256` (or `binaries.json` only
  where the artifact is the harness-captured form). Tie the hash to the fresh
  lane build and the tested Rust Matching process.
- Require `out/build.exit` (PostgreSQL/MySQL) or `out/cargo-build.exit`
  (Cassandra) = `0`; `out/gate.exit`, nested `out/gate/lane.exit` when
  present, `out/gate/run/run.exit`, and `results.json` fields `runExitCode`
  and `probeExitCode` must all be zero. Require `probe.log` to contain actual
  JSON result rows with `result: "passed"` for every lane-specific scenario.
- In `out/gate/run/results.json`, require `goMatchingStarted: false`,
  `rustServices: ["matching"]`, and `goServices` containing exactly
  `frontend`, `history`, `worker`. Also verify the topology/service logs and
  configs agree; these JSON labels alone are harness metadata.
- Require the SDK success evidence in `rust-matching/go-sdk.log` plus the
  lane's explicit result rows. The key must be successful SDK scenario output,
  not merely file existence or a zero wrapper exit. For lifecycle proof,
  inspect `restart-sdk.log`, `graceful-drain-sdk.log`,
  `production-failure-recovery-checkpoints.json`, and/or the matching probe
  rows that actually apply to the lane. Require completed restart with durable
  work drained, graceful SIGTERM drain, SIGKILL/failure takeover by the
  survivor, and idle task-list recovery as requested by that lane. A command
  containing `--matching-restart` or `--matching-failure-recovery` is not
  outcome proof.
- Require the lane cleanup record (`postgres-container-cleanup.txt`,
  `mysql-container-cleanup.txt`, or `cassandra-container-cleanup.txt`) to be
  nonempty and show removal of the uniquely named lane container. Check the
  wrapper log/exit too; cleanup-file presence alone is not removal proof.

## PostgreSQL assertions

Review `gate/run/results.json`, `probe.log`, the redacted PostgreSQL config,
and Rust/Go service logs. The `CADENCE_PROBE_PG_GC_DOMAIN` result row must have
`result: "passed"`, and its `taskGarbageCollection` must show acknowledged
decision task rows removed (or only the documented settled throttled residue),
while the unacknowledged pending control remains. Its `domainMetadata` must
show exactly the expected Go domain row/metadata. `CADENCE_PROBE_QUERY_STANDBY`
must report `directChildAnswered: true` and `frontendAnswered: 6`; inspect the
actual answer assertions in the result row/log. `CADENCE_PROBE_LIST_PARTITIONS`
must report three expected ordered partitions per list, matching owners across
both Rust nodes and Go Frontend, and each partition served only by its reported
owner. `CADENCE_PROBE_DURABLE_RESTART` must show nonempty queued backlog before
restart and every queued activity drained after restart. Also require the
auto-config-hint scenario result and ordinary SDK suite success. There is
normally no compact PostgreSQL `db-observations.json`; the direct scenario row
is the expected database proof.

## MySQL probes assertions

In `results.json`, require `goSchemaOnly` to exist and have no unexpected
tables; inspect `presentTables`, `goSchemaTables`, `goSetupTables`, and
`missingGoTables`. `db-observations.json` / promoted result fields
`taskGarbageCollection` and `domainMetadata` must contain the observed Go
`task_lists`/`tasks` metadata and GC outcome, with acknowledged task rows
removed and pending control preserved. The Go schema remains the sole schema
owner. Require the scenario results for backlog forwarding, query standby,
three-partition listing/owner agreement, durable multi-partition restart and
drain, and auto-config hint, in addition to SDK success. Inspect the actual
scenario result data in `probe.log`; successful `results.json` alone does not
name which scenarios ran.

## Cassandra assertions

Require both mixed run evidence and Go-schema interop evidence where emitted.
The lane must demonstrate Go F/H/W and Rust Matching sharing Go-applied
Cassandra schema/keyspaces, not an independent Rust schema. Inspect the
scenario/result rows and `go-seed`, `rust-restart-probe`, and `go-verify`
interop exits/logs if included: Go seed creates the persisted fixture, Rust
reads/writes across restart, and Go verifies the result. The mixed probe must
show SDK success, restart/drain, two-node owner failure and survivor takeover,
and idle recovery. Review concrete persisted task and task-list ownership
records (owner host, range/ack state, and task rows) before and after the
failure/restart; require the semaphore owner/token state to survive and be
released/reacquired as asserted by the lane probe. Do not treat
`acceptanceEligible: true`, a semaphore/task file's existence, or a successful
job as those observations. Check the Cassandra cleanup record and runner logs.

## Confirmed collector/assertion gaps to account for

- `collect-matching-gate-evidence.py` currently accepts any nonempty set of
  probe JSON rows when a gate exits zero. It does not require the lane's named
  scenario rows, successful SDK assertions, restart/recovery outcomes, or
  database fields. Its `sdkEvidenceFiles` and `recoveryEvidence` entries report
  presence, not pass/fail semantics; `databaseCleanup.status: present` likewise
  does not prove the cleanup output indicates successful removal. Review those
  contents manually against the checks above.
- The MySQL helper `check_go_schema_only()` returns `missingGoTables` but only
  raises for unexpected tables. Thus successful collection does not assert
  that every table in Go's schema is present. A reviewer must require
  `missingGoTables: []` as well as no extra tables.
- Current MySQL observations are specifically MySQL queries. PostgreSQL's
  corresponding GC/domain assertions live in its probe row, not a compact DB
  observation. Do not mark that expected difference as missing evidence.
- If Cassandra mixed artifacts do not include the Go-seed/Rust-restart/Go-
  verify interop outputs or an equivalent actual persisted owner/token/task
  assertion, record that evidence as absent for review; do not infer it from
  `results.json` metadata. The standalone interop script's presence is not a
  run result.

These are artifact-review cautions tied to existing required lane assertions;
they add no gates or acceptance requirements.
