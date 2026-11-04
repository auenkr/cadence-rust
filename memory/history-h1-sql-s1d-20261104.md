# History H1 SQL Slice 1d handoff (2026-11-04)

Branch: `history-h1-sql-s1d`, based on `origin/master` at start of work.

## Done

- Added persistence `WorkflowExecutionInfo` <-> SQL `SqlWorkflowExecutionInfo`
  field conversion on top of the existing `sqlblobs` ThriftRW struct adapters.
- Added `snapshot_to_execution_row`, preserving SQL row metadata, snapshot
  start/last-write versions, version histories and checksum. The Go-selected
  `sqlblobs::Encoding` is an explicit input; conversion does not choose a
  serializer configuration or invent a data encoding.
- Added execution-row decode back to typed execution info plus start version,
  version histories and checksum.
- Added a DB-free snapshot/row round-trip test.

## Remaining before Slice 1 is complete

- This is a coherent subset, not full Slice 1 parity. The snapshot contains
  opaque `DataBlob` arrays for activity/timer/child/cancel/signal records, but
  lacks the map keys and typed map values needed to perform Go's keyed
  `populateWorkflowMutableState` reads and create inserts. The SQL info codec
  adapters exist for typed values, but this task did not implement backend map
  row reads/inserts, requested-signal rows, or buffered-event rows.
- The persistence contracts do not currently represent every SQL
  `WorkflowExecutionInfo` field (for example event-store version); converting
  those fields losslessly requires contract work and a Go reference fixture.
- `GoExecutionStore` is not yet implemented by MySQL/PostgreSQL execution row
  stores. ListCurrentExecutions is explicitly unimplemented in the Go source.
  ListConcreteExecutions' continuation token is Go `encoding/gob`; no Rust Gob
  codec/token compatibility implementation or ordering/page-size tests were
  added. Do not substitute a Rust-only token encoding.
- Create continues to persist only its pre-existing execution/current rows;
  mutable-state maps, signal IDs, buffered events and snapshot tasks remain.
  Update/conflict methods and task methods remain for later slices per the
  implementation guide.
- Mixed Go/Rust DB integration and SDK workflow compatibility were not run.

## Validation evidence

- Final code head: `ce17df1ee` (conversion and DB-free round-trip test).
- Command: `gh workflow run rust-ci.yml --ref history-h1-sql-s1d -f package=cadence-persistence`.
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41874523618
- Conclusion: `success` (cadence-persistence clippy and tests on the GitHub
  Actions runner; no local Cargo command was run).
- Earlier code head `54f496a7f` also passed run
  https://github.com/auenkr/cadence-rust/actions/runs/41873711204; superseded
  by the final run above.
