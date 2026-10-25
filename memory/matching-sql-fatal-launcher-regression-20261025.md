# Matching SQL malformed RunID process regression — 2026-10-25

## Scope and source state

- Isolated branch began at `2b66caf3edad528db2327b7c40907b0833e9f826`.
- Fetched and fast-forwarded `origin/t3/matching-sql-fatal-lifecycle` at
  `c884dde214b7b0b92d061de856b60dbe1a6f14e1`, including typed dependency
  `21c16e33`.
- No master merge and no branch cleanup performed.

## Review findings

- The typed conversion is raised only by SQL `for_sql` conversion as
  `MalformedSqlMatchingRunId`; the task writer sends that typed cause to the
  standalone launcher fatal channel. Generic storage failures remain regular
  writer responses. Cassandra uses its own conversion and does not install the
  SQL fatal sender. Synchronous matching forwards raw RunID to its parent and
  does not use the durable writer path.
- The launcher selects the fatal channel alongside listener completion and
  shutdown signals, completes normal runtime/membership cleanup, then returns
  `SqlMatchingWriterFatal`; `main.rs` maps that typed error to exit status 2.
  Cleanup errors do not mask a captured fatal marker. Review found a
  simultaneous graceful-signal/listener-failure vs fatal-channel-ready race;
  the launcher now checks for a queued fatal marker after runtime and listener
  teardown, so the graceful path cannot mask it.
- Existing persistence regressions cover empty RunID (stored absent), canonical,
  uppercase, simple, 38-byte arbitrary-wrapper, and case-insensitive 45-byte
  URN forms; malformed SQL conversion is typed. Existing Matching tests cover
  valid/empty Add, malformed Add reaching persistence rather than request
  validation, and raw sync/forward behavior. No fatal classification by panic
  hook or error text is used.
- The prior writer unit regression only observed the channel. Added
  `tests/sql_writer_fatal_process.rs` to launch `CARGO_BIN_EXE_cadence-matching`
  with standalone SQLite `--data-dir`, issue AddDecisionTask over the actual
  Matching gRPC listener with a malformed nonempty RunID, require an error
  response (no successful Add acknowledgement), and require child exit 2 with
  the typed SQL error marker. The test runs in a child process, so the parent
  test process survives.
- Applied exact formatting corrections prescribed by Actions in launcher and
  matching runtime after phase 2 reported formatting failure.

## CI evidence

- Earlier phase 2 run: <https://github.com/auenkr/cadence-rust/actions/runs/40221096891>
  head `c884dde214b7b0b92d061de856b60dbe1a6f14e1`, conclusion **failure** in
  the changed-runtime formatting step, before Clippy or tests. It is not build
  or test evidence.
- Phase 1 focused runs were reported by the user as passing and are merged to
  master at `592ee24c9`; this records the typed dependency baseline only.
- Current branch's follow-up focused Matching CI:
  <https://github.com/auenkr/cadence-rust/actions/runs/40221421999>, head
  `ae699c9bd1a0fef096f9f86d1e6ffb3ee42aec5d`, status **in progress** at the
  one-time status check before the lifecycle-race follow-up. A source push may
  cancel/supersede it. No conclusion or binary SHA was available then; do not
  treat it as passed. Dispatch and record a focused run for the race fix.

## Remaining limits

- No Cargo, Go, or rustfmt command was run locally. No mixed-cluster gate was
  run. The new subprocess test and source edits remain unverified until the
  focused GitHub Actions run completes.
- This regression exercises standalone embedded SQLite selected by the
  production binary's `--data-dir` option. It does not exercise a separately
  declared `persistence.datastores.*.sql` configuration or external SQL server.
