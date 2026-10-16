# Matching sync and local forwarding continuation (2026-10-16)

Matching is NOT complete. Do not mistake this increment for cross-host parity.

## State

- `a9531588d` wires local sync offers into active-domain adds and dispatcher
  History completion. Original checkpoint: 76 focused tests and server Clippy.
- Uncommitted continuation fixes compilation of local parent resolution by
  explicitly boxing `QueueRegistry::get` (recursive opaque Send proof), removes
  temporary compiler probes, and clones the moved domain string correctly.
- Forwarded adds skip arrival/liveness reporting and duplicate rate limiting,
  never persist, and fail with Go's Unavailable +
  `uber.cadence.shared.v1.RemoteSyncMatchedError` detail.
- Adds walk local ancestor queues before persisting at the entry partition.
  Sync success is independent of missing partition metadata (nested option);
  response metadata belongs to entry owner, not matching ancestor.
- Child polls select local work first, then a forwarding permit. Parent loop
  starts History and completes parent storage; cancellation shares the original
  sender, and parent waiter registration retains the isolation group.
- Parent resolver weakly retains the owner map to avoid an owner/registry cycle.
- Regression tests cover sync success without metadata, child poll -> parent
  backlog, child add -> parent sync, forwarded failure/no persistence, local
  persistence fallback, and parent waiter cancellation.

## Verification

Latest full runner passed workspace tests (105 server unit tests, including
parent waiter cancellation) and workspace all-target strict Clippy. It is now
running Go differential stages in tmux
`cadence-rust:forward-gates`, logs `/tmp/opencode/forward-gates.log` and `.exit`,
artifacts `/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-forward-gates`.
Check final outcome before claiming full validation. Sources changed slightly
while runner was building; rerun focused tests/lint if needed.

Earlier `/tmp/opencode/forwarding-full.exit=1` is NOT validation: it compiled
the changing unfinished draft during the final mixed stage.

## Still required

- Forwarder task permits/rate limiting and live permit/degree reload semantics.
- Backlog forwarding (including root forwarded-backlog OfferOrTimeout).
- Query forwarding and exact forwarded request reconstruction.
- Cross-host transport/routing, ownership/membership and discovery.
- Poll forwarding currently uses the original request for History start rather
  than Go's rebuilt parent request; correct before claiming complete parity.
- Native semaphore state, isolation balancer/adaptive scaling, standalone role
  wiring, failover/restart/concurrent-poller/mixed partitioned SDK coverage.
- Incoming forwarded adds without timeout currently use a 200ms fallback;
  Go trusts the incoming context. Audit cancellation/deadline parity.
