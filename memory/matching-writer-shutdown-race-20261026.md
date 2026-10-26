# Matching SQL fatal writer shutdown race — 2026-10-26

## Change

Starting point: `origin/t3/sqlite-writer-fatal-exit` at
`e69c7663392db4e2c356d38d2a4a42a4b35125b8`, merged into the isolated checkout
branch `t3/matching-writer-shutdown-race`. That revision had focused Actions run
`40357713048` succeed, including the actual Matching-only seeded-domain SQL
fatal subprocess test.

The remaining shutdown race came from detached per-task-list writer actors.
Stopping registry owners stops a writer's queue, but an actor already awaiting a
blocking SQL append finishes that append and publishes its typed fatal cause
afterward. `MatchingRuntime::shutdown` previously joined registry maintenance,
not these writer actors, so a shutdown signal could win the launcher's `select!`
and its final `try_recv` could run too early.

Each durable registry now shares a `WriterTracker`. The writer actor registers
before it is spawned and releases its guard only when the actor exits. Registry
shutdown stops all owners and joins maintenance, then waits for the active
writer count to reach zero. The actor's existing typed SQL failure send occurs
before actor exit, so the launcher's final channel sample now follows that send
without adding timing delays or changing malformed SQL handling. The tracker is
limited to durable registry writers; library behavior without an installed
fatal sender remains nonfatal.

Added deterministic regression `shutdown_waits_for_inflight_writer_fatal_notification`:
it holds an in-flight writer guard behind a oneshot representing a delayed SQL
write, confirms shutdown remains pending, then releases the write to send the
typed fatal cause and exit. Shutdown completes only afterward and observes the
cause. The existing process test and its exit-code/log assertions remain intact.

## Validation evidence

- Prior base validation: Actions run `40357713048`, success on base SHA
  `e69c7663392db4e2c356d38d2a4a42a4b35125b8`; this is not validation of the
  shutdown race fix.
- Fix validation: [GitHub Actions run 40360696410](https://github.com/auenkr/cadence-rust/actions/runs/40360696410)
  on head `7e435116ebe22273dfe6857d145fba2d64533de2`, conclusion `success`.
  It used `package=cadence-matching` and `clean_target=true`, so clippy and tests
  ran on the fresh runner target without the Rust cache. Formatting checks
  passed. The new `shutdown_waits_for_inflight_writer_fatal_notification`
  regression passed; the test log reports 723 matching library tests passed
  (3 ignored), and the actual
  `standalone_matching_sqlite_malformed_run_id_exits_process_with_two`
  subprocess test passed, retaining the exit-code-2 and malformed SQL marker
  assertions.
- This was a focused crate CI run, not a mixed-cluster gate; no binary SHA256
  applies to this evidence. The workflow records the checked-out head SHA in
  its uploaded CI log artifact.
- No local Cargo, Go, or rustfmt commands were run, per the validation rule.

## Limits

The new regression is a deterministic actor-lifetime/delayed-write test rather
than a second subprocess-level simultaneous SIGTERM and SQL-lock test. The
existing seeded-domain subprocess test continues to verify the real malformed
SQL path and process exit status.
