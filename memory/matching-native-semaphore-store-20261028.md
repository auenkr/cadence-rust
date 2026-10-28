# Native Matching semaphore persistence bytes (2026-10-28)

## Implemented

Added `rust/crates/cadence-persistence/src/semaphores_native.rs` with the coordinated `SemaphorePersistenceBytes` seven-operation interface, byte row/page types, a raw-byte-keyed embedded implementation, and an implementation of the legacy SQL unsupported behavior. The byte adapter stores metadata and token ownership separately by exact `GoString` bytes; it maintains a one-owner/one-token conditional grant invariant, fencing mismatched releases, and emits deterministic token rows followed by owner rows in paged scans.

The embedded adapter is separate from `InMemoryExecutionStore`. It shares no metadata or token rows with the legacy frontend's in-memory persistence. It is not production Cassandra persistence and does not establish shared legacy visibility. Parent integration still needs to register this module and wire the SQL path; the raw Cassandra path is also still pending.

Tests were added for every SQL method using invalid byte inputs, raw-byte identity distinct from U+FFFD, ownership consistency, grant fencing, release fencing, and deterministic scan pagination.

## Evidence and status

Unverified: no Cargo, Go, rustfmt, build, test, check, or CI command was run, per task instructions. Only source inspection and `git diff --check` are permitted here. The parent must register the module, wire SQL/native matching consumers, implement the production raw Cassandra persistence path, and run CI validation on the registered code. No production persistence or mixed-cluster parity claim is made.

## Parent integration checkpoint

Registered `semaphores_native` in the persistence crate and added it to the runner formatter list. Source review found that the one-slot test attempted to grant token 2 and unwrap an AlreadyHeld result, while both native and legacy stores reject out-of-budget token IDs before owner lookup. Corrected the reacquire to token 1 and explicitly asserted token 2 is invalid. Simplified a boolean assertion and named the bucket-holds map type for clippy. No runtime or local compilation evidence exists. The failed semaphore manager worker's draft remains unregistered; this change registers only persistence. Production state sharing and raw Cassandra semaphore persistence remain open.
