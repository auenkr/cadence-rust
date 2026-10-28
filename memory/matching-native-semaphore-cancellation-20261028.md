# Matching native semaphore cancellation review — 2026-10-28

Owned source changes: `rust/crates/cadence-matching/src/matching_semaphore_manager_native.rs` and the semaphore service review at `rust/crates/cadence-matching/src/matching_semaphore_service_native.rs`.

## Go behavior reviewed

- `go/service/matching/semaphore/semaphore_manager.go:303-390`: grant reserves a free slot before the conditional write. A persistence error has ambiguous commit status, so Go restores the slot; a later conditional write resolves whether it was actually committed. `SemaphoreGrantSlotTaken` means the reservation is stale and remains out of the free set. Applied records the hold. `AlreadyHeld` restores the tentative slot and records the reported owner slot.
- `go/service/matching/semaphore/semaphore_manager.go:197-260`: startup installs the scanned indexes only if the manager was not stopped during the scan.
- `go/service/matching/handler/engine.go:132-140, 474-492`: Go gives manager start a separate background context with a 30 second timeout. A caller's deadline applies to its wait in `awaitStartup`, not to the shared load.

## Changes

Added a reservation drop guard around the awaited conditional write. Cancellation and ambiguous errors restore the reservation if the manager remains Running. Applied disarms the guard and records the hold only while Running. SlotTaken disarms the guard and leaves the stale slot removed. This also prevents a stopped manager from repopulating its stale free/held indexes after the in-flight write returns. The existing startup loader already checks Stopped before installing scan results.

Added a scripted `SemaphorePersistenceBytes` test adapter that parks a grant write and forces one-row scan pagination. The test aborts the acquiring task while the write is parked, checks the free reservation returned, then releases the parked write and verifies the next acquisition and a fresh startup scan. Renamed the previous misleading single-page startup test and removed its inaccurate pagination comment.

The service applies a 30 second timeout independent of the RPC request context only to manager startup/creation. Domain lookup, dynamic configuration, and ownership checks happen before the timeout, matching Go's `getOrCreateSemaphoreManager` ordering; request cancellation is checked after startup and applied to `Manager::acquire`.

## Validation and exact gaps

No Cargo, rustfmt, Go, build, test, check, or CI command was run, per task instruction. Parent-controlled Actions validation remains required. No live Cassandra proof was performed. The scripted adapter exercises cancellation before the underlying write is applied; it does not simulate a write that commits remotely and then loses its response. That ambiguous outcome remains protected by restoring the local reservation and relying on the conditional write's SlotTaken/AlreadyHeld result on retry, as in Go.

Source pointers for follow-up:

- Rust grant and reservation: `rust/crates/cadence-matching/src/matching_semaphore_manager_native.rs`, `Manager::grant`, `Reservation::drop`, `restore_reservation`, `record_hold_if_running`.
- Rust startup stop guard: same file, `Manager::load`, `LoadInFlight`, `Manager::stop`.
- Rust handler startup/cancellation boundary: `rust/crates/cadence-matching/src/matching_semaphore_service_native.rs`, `acquire_inner`.
- Go grant/startup references above.
