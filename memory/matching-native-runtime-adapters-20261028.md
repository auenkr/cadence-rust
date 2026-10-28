# Native Matching runtime adapters (2026-10-28)

Implementation is in `rust/crates/cadence-matching/src/matching_runtime_native.rs`.

`native_queue_registry` is a callable production constructor taking an
`Arc<dyn MatchingTaskStoreBytes>`, `MatchingHistoryClient`, actual
`MatchingDomainCache`, `AppState` (including its live `DynamicConfigOverlay`),
cluster name, task ID range, and native fatal sinks. It builds
`NativeQueueRegistry`, installs domain active/name/standby hooks, enables
standby completion with Go's true default, installs read-partition-count and
partition-count-cache providers, and routes writer/reader fatal events through
the launcher channel. Dynamic task-list reads use
`task_list_boolean_bytes` / `task_list_integer_bytes`; no task-list bytes are
converted to String. Domain cache lookup first validates UTF-8 and UUID, then
uses canonical UUID text as required by the cache API.

Bounded source tests cover invalid-byte filter distinctness from U+FFFD and
fatal sink forwarding, including retention of arbitrary writer RunID bytes.

Parent integration seams: the launcher must register this module and call the
constructor; parent-owned membership ownership/routing, parent router/local
poll service, partition refresher, scaler starter, metrics observer, and
transport lifecycle remain to be installed by the launcher. This constructor
currently wires only native read partition count/cache config among the
registry's live providers; add the remaining writer, reader, QPS, forwarding,
limiter and isolation providers as production launcher/config integration
requires. Fatal and domain hooks are wired here directly.

Validation was not run (per assignment: no local Cargo/Go/rustfmt/build/test/
check). Source review only; `git diff --check` is the requested bounded check.
