# Native Matching TaskWriter (UNVERIFIED source-only, 2026-10-28)

New `rust/crates/cadence-matching/src/matching_writer_native.rs`: `NativeTaskWriter`, a port of `matching_writer.rs`
over `DurableTaskQueueBytes` / `PendingTaskBytes` / `AllocatedTaskBytes` (no String/legacy conversion of bytes).
Preserved: rendezvous (capacity 0) vs bounded mpsc admission, accepted-request survives caller cancellation,
live batch/timeout/capacity sampling, Go-style batch drain, transient-only `append_rewrite` retry,
sticky (kind 1) ConditionFailed -> `StickyTaskConditionFailedError`, notify on success, stop/parking, WriterTracker guard.
Fatal: `NativeFatalSink = Arc<dyn Fn(SqlRunIdBytesError)+Send+Sync>` is invoked in the actor before responses are
sent and before the tracker guard drops.
Tests (sqlite native): invalid-byte roundtrip + source domain column, cancelled accepted append, rendezvous, lease fence
sticky/non-sticky, fatal-before-completion and shutdown wait. Not compiled or run.

Integration (parent): add `mod matching_writer_native;` in lib.rs; wire host fatal enum via the sink in
`new_with_fatal_sink` (pub(crate)); re-export if needed; run clippy/tests on CI. Possible compile risks: `rusqlite` dev-dep
use in tests, `SqlRunIdBytesError` import path, `MatchingTaskStoreError::ConditionFailed` variant shape, `SqliteTaskStore`
implementing `MatchingTaskStoreBytes` via `matching_sqlite` re-export.

Parent integration: registered as public matching_writer_native module and added to runner formatting list. Fixed the cross-domain test to query the source-domain key (the owner-domain key correctly has no task). Made accepted-caller cancellation deterministic by holding a second SQLite connection's IMMEDIATE transaction until the caller future is dropped, preventing an early-completion race. Existing WriterTracker methods and rusqlite dependency are available. Compilation remains pending; no production native registry adoption or host-fatal enum connection is claimed.

First registered CI compiled/clippy passed. Sticky fence fixture failed because owner shutdown can beat the mapped response in append's select (also possible in Go). Parent made the mapping fixture disable the queue's owner-stop waker after writer construction, preserving the queue stopped assertion and explicitly stopping the writer afterward. Production behavior unchanged; rerun pending.
