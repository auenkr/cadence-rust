# Matching reader fatal wiring (2026-10-27) — source complete, UNCOMPILED

No Cargo/CI run yet; parent must dispatch `rust-ci.yml -f package=cadence-matching` (clippy + tests). Nothing staged/committed.

## Go behaviour
`task_reader.go:370-374` `addSingleTaskToBuffer`: `ackManager.ReadItem` error (duplicate / id <= readLevel) -> `logger.Fatal` (host exits, status 1).

## Changes (rust/crates/cadence-matching/src)
- NEW `matching_fatal.rs` (`pub mod` in lib.rs): `MatchingFatal::{SqlRunId(SqlRunIdError), ReaderInvariant(String)}`, `MatchingFatalSender`. `SqlRunId` Display is the unchanged SqlRunIdError text. `test_support::ScriptedReadStore` (cfg(test)): SQLite delegate whose first `get_tasks` returns scripted ids.
- Channel type generalized in matching_registry (field + `with_fatal_writer`), grpc_matching, matching_runtime, matching_writer (`send(MatchingFatal::SqlRunId(cause))`).
- `matching_reader.rs`: `ReaderFatalSink`; sink is a CONSTRUCTOR argument (`TaskReader::with_fatal_invariant_sink`, old `with_dispatch_check_metrics_forwarding` delegates with `None`) so no setter-after-spawn race. The old `set_fatal_invariant_sink` was removed. Pump calls the sink on `QueueError::ReaderState` before `queue.stop()`/break.
- `matching_registry.rs` reader construction builds the sink from `registry.fatal_writer` -> `MatchingFatal::ReaderInvariant`.
- `launcher.rs`: private `FatalExit::{Sql,Reader}`; SQL path still yields `SqlMatchingWriterFatal` with identical text; reader yields new `MatchingReaderFatal`. Same serving-loop / final post-shutdown `try_recv` sampling.
- `main.rs`: `SqlMatchingWriterFatal` -> exit 2 (unchanged); `MatchingReaderFatal` -> exit 1 (zap Fatal).

## Shutdown guarantee
The sink call precedes the pump's `Completed` drop. Registry `owner_work` joins `reader.wait_stopped()` (needs pump completion) during owner stop, before `MatchingRuntime::shutdown` returns and before the launcher's final `try_recv`. Same ordering as writer tracker.

## Tests added
- matching_reader: `matching_reader_reports_fatal_for_duplicate_and_out_of_order_rows` ([1,1],[3,2] injected via ScriptedReadStore), `..._ordinary_rows_do_not_report_fatal`, `..._completion_waits_for_fatal_sink` (multi-thread; blocked sink holds `wait_stopped`).
- matching_runtime: `runtime_reader_invariant_violation_reaches_fatal_channel_before_shutdown_returns`.
- Existing writer/runtime fatal tests updated to match `MatchingFatal::SqlRunId(..)`.

## Risks to check on CI
Compile only unverified: rustfmt/clippy (`too_many_arguments` allowed on new ctor), test timing (ordinary test relies on `wait_read(3)`), runtime test assumes `handle.enqueue` returns Ok and first scripted scan occurs after it.
