# Matching resume and acceptance checkpoint — 2026-10-29 (updated)

This is a documentation/evidence checkpoint only. Matching remains incomplete;
do not start History migration. No production source was changed here.

## Current source and ownership

- Shared master docs checkpoint begins at `bc4e12fef6b97a28688e7fc2693037407952b479`.
  Its existing frozen Matching candidate remains
  `bf9f3d9387a72c4ed26b4be0a85025e65fd485a`; master docs commits do not move it.
- Raw-byte/wide-deadline review is at
  `origin/t3/matching-rawbyte-deadline-fix`
  (`90aa74112e1c9e4d4c9814f5e1d94a7f6b81176b`), with reviewed ancestry
  `894057f8b` / `fc9552b7a`. It is not integrated or accepted.
- Lifecycle integration is at `a9cb5ed06`; the follow-up
  `origin/t3/matching-lifecycle-fixes`
  (`ccb6cb11fd72229499d3eeadb55a0cc96647475e`) logs and continues after final
  ack and forced-GC errors. Domain-binding fallback, blocked-reader/shutdown
  race and invalid-ticker semantics, plus injected teardown tests, remain open.
- Local wait correction is at `origin/t3/matching-timeout-parity`
  (`18040515dabb2d6fd79ed5b4eba5c933eee25f1f`): backlog now uses
  `LocalTaskWaitTime`. Domain binding and configured-race behavior remain
  unverified.
- An active Terra delegate owns integration of `18040515d` with `90aa741` and
  closure of timeout binding/coverage. A separate Luna scheduler owns
  collection for semaphore `40853270019` and conditional focused raw-byte CI
  dispatch. No newer result is recorded here.
- Frozen baseline `bf9f3d9387a72c4ed26b4be0a85025e65fd485a` remains immutable.
  Native mixed-cluster passes: zero.

## One observed CI snapshot

Run: https://github.com/auenkr/cadence-rust/actions/runs/40853270019

- Ref: `matching-semaphore-reviewed-592a0bb03`
- Exact run head: `592a0bb03660479c890680d316b58067f3527810`
- Observed status: `in_progress`; overall conclusion empty/unavailable. This is
  a live snapshot only, not terminal evidence. No polling or rerun was performed.
- Job `clippy-test` (`123377797436`) was in progress. Fresh target selection
  succeeded. `Clippy` succeeded. `Formatting` succeeded; the run also reported
  successful format check, Matching parity format, Go raw-byte oracle, original
  Go malformed DescribeTaskList oracle, mapped IPv4 CIDR oracle, History TChannel
  wire fixture, production Go owner ordering oracle, Thrift schema check,
  definitions/rollup audit, Go emission formatting check, and metered persistence
  wrapper capture. The `Test` step was still in progress. Later panic-quote
  upload, live Go tally/Rust Matching emissions, and final identity/log uploads
  were pending. No per-test outcome or test exit code was exposed in this live
  snapshot. No binary SHA256 was yet available.
- Artifact metadata read for the same run/head identified `matching-format-patch`
  (ID `12888825410`, 5,661 bytes, API SHA-256
  `6ad51252c9208c9bc4a638d1611910f645a943fc7bd21aa3aac47d52b60ba331`,
  expires 2026-11-01 09:56:21Z). This records artifact provenance only; it was
  not downloaded or applied, so no patch contents or source mutation are claimed.
  The same listing showed `rust-ci-logs` (ID `12887884788`, 151,495 bytes,
  digest `e14d9624765d455d2dbf0b01d542a245b05857fdbc5773c890356668b1e1c365`)
  and completed Go oracle artifacts. The run was still active when observed.
- Relevant semaphore regressions in the source are
  `matching_semaphore_service_native::tests::slow_start_keeps_running_after_caller_cancel_and_reuses_manager`,
  `matching_launch_native::tests::configured_grpc_semaphore_slow_start_deadlines_cleanup_and_retry`,
  `matching_semaphore_manager_native::tests::failed_start_releases_waiter_and_next_request_rebuilds_manager`, and
  `matching_semaphore_service_native::tests::independent_startup_timeout_stops_manager_and_allows_fresh_acquire`.
  They have no result recorded from this live snapshot; await terminal test logs
  before marking them passed or failed.

The earlier focused baseline run [40842468415](https://github.com/auenkr/cadence-rust/actions/runs/40842468415)
passed at `721e196d31b2cd7876fc09086c8ac4b2a8995ba3`; it predates the latest
candidate branches and is advisory for current source. Do not attribute its
pass to those branches. Semaphore run 40853270019 was last observed at exact
head `592a0bb03660479c890680d316b58067f3527810` with Clippy and formatting
successful and tests still running; retain that as the last observation and do
not infer a newer outcome.

## Acceptance and resume requirements

- Complete integration/review of the lifecycle branches and Terra-owned changes;
  select and freeze a new exact candidate after source settles.
- Obtain fresh, exact-head workspace Clippy followed by workspace tests. Preserve
  exact run URL, head SHA, conclusion, named regression outcomes, all available
  exits and binary SHA256. Inspect and retain only relevant small artifacts/logs;
  establish formatting patch contents/provenance before applying any patch.
- Run and document all ten required mixed Go/Rust lanes on the same final source,
  with no Go Matching instance: PostgreSQL, MySQL, Thrift, Configstore, shard
  distributor, two-node MySQL, Cassandra, adaptive two-node, active offer, and
  production TLS. Include SDK workflows and service-to-service failure, restart,
  and recovery behavior. Record actual topology, scenario outcomes, exits and
  Rust binary SHA256. Do not count a passing RPC or adapter test as a lane.
- Resolve the dependency PostgreSQL pool oracle and complete the Matching parity
  audit. There are currently zero native mixed passes; this candidate is not
  service acceptance and does not establish full parity.
- Finish the remaining configuration/source audit and integrate the timeout,
  lifecycle and raw-byte candidates into one immutable, reviewed source before
  final CI. Then obtain exact-head focused CI as warranted, fresh uncached
  workspace Clippy followed by workspace tests, and all ten mixed lanes with
  binary SHA evidence and cleanup records.
- No local Cargo, rustfmt, Go, build, or runtime validation was run. Continue to
  use CI for all such work. Do not claim terminal evidence from the in-progress
  snapshot above.
