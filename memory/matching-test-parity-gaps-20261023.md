# Matching test parity gaps, liveness / unit-test pass (2026-10-23)

Branch: `matching-liveness-tests-20261023` (from origin/master 92668eb2f).
Inventories this pass works from: `matching-go-test-inventory-20261023.md` and
`matching-go-test-inventory-port2-20261023.md`. Matcher and forwarder tests are owned by another worker
and are not covered here.

## Parity gaps found

None. No ported test in this pass failed against the Rust code, so none is `#[ignore]`d.

## Analogue note (not a test failure)

Go `liveness.Liveness` is a daemon with a ticker at TTL/2 and a shutdown callback. Rust
`cadence_common::liveness::Liveness` is only a timestamp, so the two Go event-loop tests were
ported against the registry idle sweep (`matching_registry.rs::sweep_idle`), which is the Rust
production mechanism that unloads idle owners.

- The ported tests call `sweep_idle(now, due_only=false)` at exact TTL marks. They confirm the
  observable behavior: a marked owner stays loaded at TTL, and an unmarked owner is unloaded at TTL.
- Timing is not identical. Go fires shutdown on the first TTL/2 tick after expiry, so unloading can
  land anywhere in [TTL, 1.5 TTL]. Rust unloads on the first scheduled sweep at or after TTL, with
  `due_only` ticks at TTL/2 cadence. The tests do not assert the 1.5 TTL bound.
- Open audit item: confirm the production `maintain_until` cadence matches Go's ticker lag. Not
  checked in this pass.

## Ported in this pass

| Go test (file:line) | Rust test | Result |
|---|---|---|
| liveness/liveness_test.go TestEventLoop_Noop (89) | matching_registry.rs::liveness_event_loop_noop_keeps_marked_owner_loaded | PASS |
| liveness/liveness_test.go TestEventLoop_Shutdown (105) | matching_registry.rs::liveness_event_loop_shutdown_unloads_owner_at_ttl | PASS |
| semaphore/semaphore_manager_test.go TestAcquireRejectsAnEmptyOwnerID (307) | matching_semaphore.rs::matching_semaphore_rejects_empty_owner_without_touching_free_set | PASS |
| semaphore/semaphore_manager_test.go TestUnreserveIsIdempotent (829) | matching_semaphore.rs::matching_semaphore_unreserve_is_idempotent | PASS |

## Not ported (reason)

- handler/context_test.go TestHandleErrNil (38) and TestHandleErrUncategorizedError (129): Rust has no
  generic handleErr. Error mapping is per-RPC (`semaphore_status` in grpc_matching.rs:156). Mapping the
  Go default branch to `ManagerError::Internal` would test a different function, so it is not claimed.
- semaphore/identifier_test.go TestParseRequestOwner (59): no Rust owner-parse function exists in the
  semaphore module. Needs a Rust counterpart before it can be ported.
- semaphore/semaphore_manager_test.go TestStartSkipsRowsItCannotClassify (227): the Rust store
  (InMemoryExecutionStore / SQL) cannot persist malformed semaphore rows, so there is no injection
  point. Needs a row-injecting fake.
- semaphore/semaphore_manager_test.go TestStartLeavesStateUntouchedWhenTheScanFails (254) and the
  other write-conflict, deadline, and failed-grant cases: already recorded as MISSING in the inventory
  (need fault-injecting persistence).

## Evidence

- Liveness group: `cargo test -p cadence-matching --lib liveness_event_loop -- --include-ignored`,
  2 passed, 0 failed, exit 0 (first run exit 101 was a missing `go/idls` submodule in the worktree,
  fixed with `git submodule update --init go/idls`).
- Semaphore group: `cargo test -p cadence-matching --lib matching_semaphore_`, 16 passed, 0 failed, exit 0.
- Full gate: `cargo test -p cadence-matching`, 445 passed, 0 failed, 5 ignored (pre-existing), exit 0
  (`/tmp/final-test.log`, `/tmp/final-test.exit` = 0).
- Clippy gate: `cargo clippy -p cadence-matching --all-targets -- -D warnings`, exit 0 (`/tmp/final-clippy.exit` = 0).
