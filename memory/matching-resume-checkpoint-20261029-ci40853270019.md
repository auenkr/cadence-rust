# Matching resume and acceptance checkpoint — 2026-10-29

This is a documentation/evidence checkpoint only. Matching remains incomplete;
do not start History migration. No production source was changed here.

## Current source and ownership

- Latest integrated incomplete candidate: `origin/t3/matching-reader-rawbyte-review`
  at `fc9552b7a60fd5a410cd59ee0e5aa68577a7a623`.
- Candidate includes quota changes, native Matching scenarios, semaphore source
  reviewed at `592a0bb03660479c890680d316b58067f3527810`, and reader changes from
  `21a051d36`.
- Additional completed branches are not validated: ack/GC lifecycle
  `d59eaea1be55bb381f1390fb6006728aa0b410b9` and idle lifecycle
  `24af0b8e4e853933f56b8f19e05a90e12f66be2e`. Terra integration worker owns
  merging/reviewing lifecycle branches. Existing Terra worker owns Unleash
  invalid-UTF-8 provider behavior and wide reader deadline work on `fc9552b7a`.
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
- No local Cargo, rustfmt, Go, build, or runtime validation was run. Continue to
  use CI for all such work. Do not claim terminal evidence from the in-progress
  snapshot above.
