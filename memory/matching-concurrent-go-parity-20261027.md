# Matching concurrent Go parity checkpoint — 2026-10-27

Source reviewed: `go/service/matching/handler/engine_integration_test.go`,
`TestConcurrentAddAndPollActivities`, `TestConcurrentAddAndPollActivitiesWithZeroDispatch`,
`TestConcurrentAddAndPollDecisions`, their three isolation variants, and the activity/decision
`TestMultipleEngines*RangeStealing` helpers. Go concurrency uses 20 workers and 100 tasks each.

## Added coverage

`rust/crates/cadence-matching/tests/matching_concurrent_go_parity.rs` adds:

- Two public `MatchingApi` integration tests concurrently issuing 20 × 100 durable AddActivityTask
  or AddDecisionTask RPCs against the real SQLite-backed `QueueRegistry`. They assert all 2,000
  rows exist and workflow identities are unique.
- A SQLite lease-fencing counterpart: the replacement range is greater, the former range cannot
  write, the replacement owner can continue, and the pre-steal task remains present.

## Explicitly not ported

- The Go helper's concurrent poll half is not exercised. Matching poll delivery calls History
  start-task RPCs; the existing scripted integration History service only covers
  `RefreshWorkflowTasks`, so this checkpoint does not pretend task delivery was verified.
- Zero-dispatch throttling is not ported. No assertion in these tests covers a temporary zero
  dispatch limit or subsequent redelivery.
- All three isolation-enabled Go variants are not ported; the public Add RPC does not expose the
  Go helper's per-task `PartitionConfig` injection.
- The range-stealing test drives the public SQLite persistence lease/write boundary, not two live
  `Matching` service instances. Go's old-engine manager shutdown and replacement poll of all 20
  tasks remain unverified here.

## Validation evidence

No local Cargo, Go, rustfmt, build, check, clippy, test, or format command was run, per the task
constraints. This work has no GitHub Actions result yet. The parent owns CI dispatch and should
record the resulting run URL, immutable head SHA, conclusion, and (for mixed-cluster evidence)
Rust binary SHA256 before making a parity claim. Current evidence is therefore implementation
scope only, not validation or service completion.
