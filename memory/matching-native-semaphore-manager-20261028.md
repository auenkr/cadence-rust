# Native Matching semaphore manager — 2026-10-28

Status: implementation and source review recorded; **unverified**. No local Cargo, Go, formatting, or build commands were run. Parent must run the prescribed GitHub Actions validation.

## Implementation

`rust/crates/cadence-matching/src/matching_semaphore_manager_native.rs` implements the native byte-preserving per-bucket manager and registry on `cadence_persistence::semaphores_native::SemaphorePersistenceBytes`. It pages startup scans until the next token is absent or empty, creates free and owner indexes from returned rows, fences grants through persistence conditional outcomes, caps attempts at Go's three, confirms cached owner holds, honors caller deadlines, stops/unregisters idle managers, and uses pointer identity so an old manager cannot unregister a replacement. The manager accepts and retains `GoString` identifiers/owners; it does not convert them through UTF-8 or a legacy string model.

Bounded unit coverage uses `InMemorySemaphorePersistenceBytes` for byte-distinct malformed UTF-8 (`ff`, `fe`) and U+FFFD owners, idempotent reacquire, conditional release fencing, restart/startup index rebuilding, and replacement after stop. These tests have not been executed.

## Source comparison

- Go manager behavior: `go/service/matching/semaphore/semaphore_manager.go`, especially `Start`, `awaitStartup`, `load`, `Stop`, `Acquire`, `grant`, `confirmHold`, and `loadTokenOwnership` (lines 160–481).
- Go owner codec/byte contract: `go/common/semaphore/owner.go` (`Owner.String`, `ParseOwner`). Rust byte parser is `rust/crates/cadence-common/src/semaphore_bytes.rs`.
- Native persistence behavior: `rust/crates/cadence-persistence/src/semaphores_native.rs` (`SemaphorePersistenceBytes`, `InMemorySemaphorePersistenceBytes`); it is byte preserving and uses conditional `GrantOutcome`s.
- Native identifier behavior: `rust/crates/cadence-matching/src/matching_semaphore_native.rs`.
- Existing legacy Rust manager for parity reference: `rust/crates/cadence-matching/src/matching_semaphore.rs`.

## Host acquire wiring API

Use `Registry::get_or_create(IdentifierBytes::new(domain_go_string, semaphore_name_go_string, bucket)?)` (or `get_or_create_with_idle_ttl`) to obtain `Arc<Manager>`, then call `Manager::acquire(&owner_id_go_string, Option<tokio::time::Instant>)`. It returns `Result<AcquireResult, ManagerError>`; `AcquireResult` has `outcome` and `token_id`. Owner validation/decoding remains at the native service request boundary with `cadence_common::semaphore_bytes::parse_owner_bytes`; the manager enforces nonempty owner bytes and preserves them exactly.

## Unresolved / unverified

- No runtime, compilation, clippy, formatter, or test evidence exists for this file yet; CI validation is parent controlled.
- Host service registration and acquire wiring are intentionally outside this change. Parent must register the module and connect the native service path.
- The in-memory manager test's default scan fits one page; persistence's paged scan implementation is covered separately, but a multi-page manager startup fixture remains desirable if the current bounded test harness is extended.
- Mixed Go/Rust failure, restart, and recovery gates remain required for service completion.
