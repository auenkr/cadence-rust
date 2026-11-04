# History H1 SQL Slice 1c progress (2026-11-04)

Branch: `history-h1-sql-s1c`, created from `master` (`18dda13dc`). The older
`history-h1-sql-s1` and `history-h1-sql-s1b` branches contained handoff notes
and codec work only; no execution SQL CRUD implementation was reusable.

## Done

- Added shared MySQL/PostgreSQL execution statement catalog entries for row
  insert/read/delete, current row lock/insert/update/read/delete, and concrete
  execution map cleanup.
- Added native backend row helpers using Matching's existing SQL pools:
  `MySqlExecutionRows` and `PostgresExecutionRows` support execution row read,
  current row read, exists, concrete deletion, conditional current deletion,
  and create. Create locks the shard row and checks `range_id` in the same
  transaction as the current-row transition and execution insert.
- Ported create mode predicates for BrandNew, WorkflowIdReuse,
  ContinueAsNew, and Zombie, including already-started response fields and
  version/state/run checks. Zombie writes its execution row without changing
  the current row.
- Kept the execution blob caller supplied as a Go-compatible `ExecutionRow`
  `DataBlob` boundary. Added DB-free mode and statement tests.

## Remaining in Slice 1

- Typed `WorkflowExecutionInfo` `sqlblobs` encoder/parser and snapshot data
  conversion are not implemented here; the helpers require the caller to
  supply the already Go-encoded execution row.
- Read/reconstruct activity, timer, child, request-cancel, signal, requested
  signal, and buffered-event maps is not implemented. Concrete deletion does
  remove all those SQL rows.
- Current/concrete listing with Go continuation-token behavior is not
  implemented.
- These helpers are not yet wired as the `GoExecutionStore` trait
  implementation; update/conflict-resolution and task APIs remain for later
  slices.
- No MySQL/PostgreSQL integration test was added. CI runs the crate's existing
  tests and compile/clippy checks only.

## CI evidence

- Initial validation on `f8f4963803b54f63053960af8a9f525b7affc7dc`:
  [run 41868126441](https://github.com/auenkr/cadence-rust/actions/runs/41868126441)
  failed clippy on `type_complexity` for the execution row tuple. Fixed with
  named row type aliases and added the transactional create path in
  `bff02f76209a5f4a19a8ede20ae35a427305b77d`.
- Validation for `bff02f76209a5f4a19a8ede20ae35a427305b77d` failed clippy on
  `too_many_arguments` for backend create methods. The create arguments were
  grouped in `CreateExecutionRowRequest`.
- Clippy and tests passed for code head
  `0ddcf3c1b4ac43a0a96d02d0747a00888ae19a7a`:
  [run 41870825957](https://github.com/auenkr/cadence-rust/actions/runs/41870825957),
  conclusion `success`.
