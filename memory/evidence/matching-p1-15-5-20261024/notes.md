# P1-15 and P1-5 notes (2026-10-24)

Branch: `matching-p1-15-5-20261024`, based on `origin/master` @ `e9b8e9a0e`.
Source of the items: `memory/matching-open-gaps-20261024.md` (on `origin/matching-open-gaps-20261024`).
No cargo was run locally. Verification is CI (`rust-ci.yml`, package `cadence-matching`).

## P1-5: semaphore deadline and unknown-outcome gaps

Go reference: `go/service/matching/semaphore/semaphore_manager.go` (`awaitStartup` :182, `Acquire` :262, `grant` :303, default branch :369)
and `go/service/matching/semaphore/semaphore_manager_test.go` (`TestAcquireHonorsItsDeadlineWhileStarting` :389,
`TestGrantStopsRetryingOnceTheDeadlineHasPassed` :577, `TestGrantRejectsAnUnrecognizedWriteOutcome` :699).

Changes:
- `rust/crates/cadence-matching/src/matching_semaphore.rs`
  - `Manager::acquire(owner_id, deadline: Option<tokio::time::Instant>)`. While startup is in flight the wait is
    `tokio::time::timeout_at(deadline, ...)`, and an expired wait returns the new `ManagerError::Deadline`.
  - `Manager::grant(owner_id, deadline)` checks the deadline before each grant attempt and returns `ManagerError::Deadline`.
    This mirrors Go's `ctx.Err()` check at the top of the loop.
  - `GrantOutcome::Unknown` returns `ManagerError::Internal("unexpected grant outcome ...")` and gives the drawn id back,
    mirroring Go's default branch.
- `rust/crates/cadence-matching/src/grpc_matching.rs`
  - The AcquireSemaphore handler already had the RPC `deadline` in scope (`request_deadline`). It now passes it to
    `acquire`, which before this change was not passed. `semaphore_status` maps `Deadline` to `deadline_exceeded`.
- `rust/crates/cadence-persistence/src/semaphores.rs`
  - `GrantOutcome::Unknown` added (Go `SemaphoreGrantUnknown`). No bundled store constructs it. Only tests do.
- `rust/crates/cadence-matching/src/matching_semaphore_parity_tests.rs`
  - Un-ignored and rewritten to assert real return values:
    - `matching_semaphore_acquire_honors_deadline_while_starting` (was :667). The scan is blocked, and acquire returns
      `Err(Deadline)` at its own deadline. The old outer-watchdog workaround is gone.
    - `matching_semaphore_grant_stops_retrying_at_deadline` (was :691). `ScriptStore.write_delay` makes the first write
      outlast a 10 ms deadline. Go cancels the context inside the write. Rust reproduces that with elapsed time. The
      second attempt returns `Deadline` with exactly one write.
    - `matching_semaphore_grant_rejects_unknown_write_outcome` (was :710). Returns `Internal`, and the free count is unchanged.
  - Existing `acquire(...)`/`grant(...)` calls gained a `None` deadline argument (no behavior change).

Not changed: Go's `enqueue` is a no-op and not part of this item.

Status after this change (not yet verified in CI): the three P1-5 ignored gaps are un-ignored. The "Start does not block on load"
test (Go :420) is still not located. P1-5 is not fully closed until CI is green.

## P1-15: TChannel TLS

Decision: out of scope. Nothing to port. Go has no TLS on the TChannel listener.
Evidence:
- `go/common/rpc/factory.go:63-69`: the TChannel transport is created with only `ServiceName` and `ListenAddr`. No TLS option.
- `go/common/rpc/factory.go:91-92`: TLS is attached only to the gRPC inbound (`grpc.InboundCredentials(credentials.NewTLS(p.InboundTLS))`).
- `go/common/rpc/factory.go:113-116`: TLS is attached only to the HTTP inbound.
- `go/common/config/config.go:140`: `// TLS allows configuring optional TLS/SSL authentication on the server (only on gRPC port)`.
- Rust has no TChannel TLS config (`rust/crates/cadence-matching/src/tchannel.rs` and `matching_history_tchannel.rs` have no TLS match).

Test: none added. The memory's "add one TChannel TLS handshake test if in scope" does not apply, since scope is out.
The existing gRPC TLS and HTTP TLS coverage stays as it is. P1-15 should be closed as an accepted difference
(Go has no feature to match).
