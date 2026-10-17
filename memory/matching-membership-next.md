# Matching membership / distributed forwarding continuation (2026-10-17)

Whole-service goal remains active; do not call single-node parity complete.

Implemented after native Thrift transport:
- `cadence-common::membership::Ring`: Go Ringpop's 100 FarmHash32 replicas,
  legacy identity/address key format, distinct-identity format, collision ordering,
  clockwise wrap and unique-host LookupN. Original-Go oracle verifies 4,590
  lookups including shared identities, IPv6, Unicode, partition and semaphore keys.
- `matching_membership::Membership`: atomic replaceable placement snapshots,
  change notifications, exact shared-v1 ownership exception details for task lists
  and semaphore buckets, owner endpoint resolution.
- Queue registry guards admission after the per-key startup gate and checks
  placement again before publishing startup. `unload_non_owned` retains owners
  when membership is unknown; detaches/stops/final-acks obsolete owners.
- Matching membership maintenance future unloads task-list/semaphore managers;
  semaphore Acquire checks ownership. Partition metadata uses ring host addresses.
- Cross-host gRPC poll forwarding through parent resolver, preserving Go's rebuilt
  poll request, domain/poller/isolation identity, forwarded-from, deadline and
  upstream cancellation. Parent response is already History-started.
- Cross-host sync-add and query forwarding branches implemented. Forwarded adds
  preserve source domain, source kind, timeout and partition config. Failed
  forwarding falls back to child persistence. Query forwarding uses incoming
  deadline and completion stays on accepting partition.

Verified before full rerun:
- `/tmp/matching-ring.exit` = 0: Go/Rust placement differential.
- `/tmp/matching-membership-test.exit` = 0: guards and owner move/reload test.
- `/tmp/matching-cross-host.exit` = 0: real gRPC parent listener with separate
  Matching registries/stores, remote backlog poll, remote sync add with no
  duplicate persistence, upstream poll cancellation.
- Earlier strict Clippy passed before remote forwarding additions.

Full suite currently running in tmux `cadence-rust`, window
`matching-full-recheck`, log `/tmp/matching-membership-full.log`, exit sentinel
`/tmp/matching-membership-full.exit`, artifacts
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-membership-full`.
Initial native full run passed workspace tests but failed strict Clippy on a
redundant struct update in Thrift poll conversion; fixed. Do not count that run
as full-suite success. Latest full run includes membership and native-Thrift stages.

Still required:
- Production membership provider/discovery and runtime wiring. New membership
  is injectable via Matching::with_membership; main does NOT configure it yet.
  Do not present injectable multi-host behavior as production cluster completion.
- Persistent/cached outgoing Matching channels; configured message limits.
- Remote query/activity and membership watcher tests (decision path tested).
- Shard-distributor spectator/executor integration, routing exclusion/onboarding,
  assignment guard, distributed partition scaling and config refresh transport.
- Native dedicated Matching HTTP listener using matching.rpc.http.port.
- Production domain metadata/cache feed and Matching metrics/error taxonomy.
- Persistence backend audit, shared multi-host semaphore persistence.

## Dynamic configuration audit correction

Several earlier notes claimed the dynamic-config surface was implemented. The
runtime admin overlay is present, but many Matching properties still use fixed
Go defaults in actors. This is required parity work, not waived cross-cutting work.

Missing Go property keys found by mapping Matching config to constants.go and
searching Rust source (not proof that every other property is fully wired):
matching.persistenceMaxQPS, matching.persistenceGlobalMaxQPS,
matching.enableSyncMatch, matching.rps, matching.workerrps, matching.domainrps,
matching.domainworkerrps, matching.readRangeSize, matching.getTasksBatchSize,
matching.updateAckInterval, matching.idleTasklistCheckInterval,
matching.maxTasklistIdleTime, matching.longPollExpirationInterval,
matching.minTaskThrottlingBurstSize, matching.maxTaskDeleteBatchSize,
matching.outstandingTaskAppendsThreshold, matching.maxTaskBatchSize,
matching.throttledLogRPS, matching.shutdownDrainDuration, system.enableDebugMode,
matching.enableTaskInfoLogByDomainID, matching.activityTaskSyncMatchWaitTime,
matching.appendTaskTimeout, matching.asyncTaskDispatchTimeout,
matching.localPollWaitTime, matching.overrideTaskListRps,
matching.enableClientAutoConfig, matching.excludeShortLivedTaskListsFromShardManager,
matching.recordTaskStartedTimeout.

Next configuration work should preserve live-vs-startup reads from Go actors;
merely replacing defaults at manager creation is not sufficient for live properties.

Latest broad verification: workspace tests passed (150 server tests plus the
workspace/integration suites), and strict workspace/all-target Clippy passed.
The full runner has advanced into Go differential stages; final exit pending.
