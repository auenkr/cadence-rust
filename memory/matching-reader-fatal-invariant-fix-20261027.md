# Matching reader fatal invariant (2026-10-27) — PARTIAL, blocked on wiring

No Cargo/CI run; nothing compiled or tested.

## Invariant
Go `ackManager.ReadItem` (`common/messaging/ackManager.go:68-92`) errors only if itemID <= readLevel or the id is already outstanding (duplicate / out-of-order). `task_reader.go:370-374` then calls `logger.Fatal` (host dies). Rust: `DurableTaskQueue::register_read` -> `QueueError::ReaderState` (`cadence-persistence/src/matching_queue.rs:493-506`), previously only queue.stop().

## Done (matching_reader.rs only)
- `ReaderFatalSink = Arc<dyn Fn(String)+Send+Sync>`, `TaskReader::set_fatal_invariant_sink`.
- Pump admission error branch: on `QueueError::ReaderState(detail)` the sink is called before the existing queue.stop()/break. Normal path unchanged; with no sink set behaviour is identical to before.

## Blocker (files outside my ownership)
The existing fatal channel is typed `UnboundedSender<cadence_persistence::matching_blob::SqlRunIdError>` in `matching_registry.rs` (field 220, `with_fatal_writer` 792), `grpc_matching.rs:449`, `matching_runtime.rs:142`, and the launcher (`launcher.rs:1290,1693-1808`, `SqlMatchingWriterFatal`). Readers are built in `matching_registry.rs:~1456` (and dispatch tests), which is where `set_fatal_invariant_sink` must be called with a closure feeding the channel. Needed:
1. Generalize the channel to an enum (e.g. `MatchingFatal::{SqlRunId(SqlRunIdError), ReaderInvariant(String)}`), updating registry/grpc_matching/runtime/launcher/matching_writer.rs (`fatal.send(cause.clone())` at writer:357) and `SqlMatchingWriterFatal` message.
2. In registry reader construction, `reader.set_fatal_invariant_sink(...)` forwarding to `registry.fatal_writer`.
3. Regression test: needs a faulty store yielding duplicate/non-increasing task ids through `reader_turn` (none existing; the SQLite store enforces the PK), asserting the channel receives ReaderInvariant — and also a runtime-level test like `runtime_shutdown_waits_for_real_delayed_append_and_fatal_notification`. Not written: requires the owned-elsewhere wiring and a store fake whose API I could not validate without compiling.
