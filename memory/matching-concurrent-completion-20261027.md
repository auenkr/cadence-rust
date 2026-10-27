# Matching concurrent delivery and live lease stealing — 2026-10-27

Scope: `rust/crates/cadence-matching/tests/matching_concurrent_go_parity.rs`.
No production code was changed.

## Added regression scenarios

- `go_concurrent_add_and_poll_delivers_every_activity_once` starts an in-process,
  reachable scripted History gRPC service implementing `RecordActivityTaskStarted`.
  Four concurrent public pollers and four concurrent public adders exercise 100
  activity tasks. The History response echoes each scheduled activity ID, and the
  test asserts the sorted delivered IDs equal the complete unique range. The
  scripted server is aborted after the pollers finish.
- `go_two_live_matching_owners_steal_lease_and_deliver_all_work` constructs two
  distinct public `Matching` handlers with independent registries over one shared
  SQLite task store. Both owners add ten tasks; a later add from the prior owner
  must fail after the replacement has advanced the persisted lease. The new owner
  polls all twenty tasks through the reachable History peer and asserts both sets
  of ten schedule IDs were delivered. The server is aborted after delivery.

These tests exercise public add/poll RPCs and live `Matching` owners. They do not
claim Go's exact `MatchingEngine.Stop` lifecycle: the public handler API has no
per-instance stop operation. The replacement lease and stale-write behavior is
checked through its observable public add failure.

## Remaining variants and exact source cases

- Zero-dispatch then recovery is not added. The Go source case is
  `TestConcurrentAddAndPollActivitiesWithZeroDispatch` (and its isolation twin
  `TestConcurrentAddAndPollActivitiesWithZeroDispatchIsolation`) in
  `go/service/matching/handler/engine_integration_test.go`. No active Rust
  integration test in `rust/crates/cadence-matching/tests` covers zero dispatch
  and subsequent recovery. Although the public poll request carries task-list
  metadata, the current regression has not established whether setting its
  dispatch rate to zero faithfully maps to the Go manager's per-poller RPS
  override; this remains an explicit gap for follow-up.
- Isolation variants are not added. Go cases are
  `TestConcurrentAddAndPollActivitiesIsolation` and
  `TestConcurrentAddAndPollDecisionsIsolation` in the same Go file. Go assigns an
  isolation group on each Add via private `PartitionConfig`, while the public
  Rust Add RPC does not expose that per-task injection. No active Rust concurrent
  add/poll test found in the matching test directory covers those variants.
- Concurrent decision delivery is not added; the new actual delivery scenarios
  exercise activity starts. Existing producer-only decision coverage remains in
  this file.

## Validation

No Cargo, Go, rustfmt, build, check, clippy, test, or format command was run.
Validation must run in CI, owned by the parent task. Until the CI run completes,
these are source changes without test-pass evidence. Record the run URL, head SHA,
and conclusion after CI; mixed-cluster evidence additionally needs the Rust
binary SHA256.
