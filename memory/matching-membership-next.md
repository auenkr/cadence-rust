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

Audit caution: missing keys must be checked for real Go call sites before adding
behavior. For example, `EnableSyncMatch` and `ActivityTaskSyncMatchWaitTime`
currently appear only in Go config/manager construction, not operational calls.
Making those toggles affect behavior would change the pinned Go logic. Preserve
unused-property behavior; prioritize properties actually read by Go actors.

## Local poll forwarding delay follow-up

Auditing Go matcher.pollNonBlocking exposed a default mismatch: Rust forwarded
polls immediately rather than waiting Go's default 10ms for local tasks. Added
live matching.localPollWaitTime provider and delay before acquiring a forwarding
permit. The wait is raced with local delivery and caller cancellation. Focused
regression (30s configured wait prevents parent registration, next poll after
setting 0s reaches parent) passes: /tmp/matching-localpoll-test.exit = 0.
Current all-server-unit + strict Clippy rerun: /tmp/matching-final-unit.log and
/tmp/matching-final-unit.exit. Full suite continues with the latest build.
Latest local-wait tree: all 151 server unit tests and strict workspace/all-target
Clippy pass (`/tmp/matching-final-unit.exit` = 0).

Remote query follow-up: a real root Matching gRPC listener accepts a forwarded
query from a separately owned child, delivers it to a root worker, and returns
completion to the child. Token names root, not child. Both cross-host tests pass
(/tmp/matching-remote-query.exit = 0). All 152 server tests and strict workspace
Clippy pass in /tmp/matching-live-recheck.log.

Broad run found an expected isolation-test adjustment: Matching's POST-only HTTP
wildcard returns 405 for unknown GET paths. Isolation test now accepts 404/405
and additionally sends a frontend Thrift call to prove rejection (404).
Focused matching/history listener isolation passes (/tmp/matching-listeners.exit=0).
Latest broad rerun uses --skip-rust-checks after the explicit 152-test/Clippy run:
artifacts matching-transport-membership-verified, /tmp/matching-live-recheck.log,
/tmp/matching-live-recheck.exit. Previous full workspace run already passed all
workspace tests; no shared-crate changes since that run.

## Poll deadline / expiration follow-up

Internal gRPC polls now reserve Go's one-second empty-response tailroom too
(previously only native Thrift did). Both decision/activity polls use incoming
request metadata deadline and live matching.longPollExpirationInterval, including
standby sleeps. Regression checks empty short-context polls and runtime config
change on the same manager. All 153 server tests pass in
/tmp/matching-poll-tailroom.log; strict Clippy follows in that process, sentinel
/tmp/matching-poll-tailroom.exit. Latest binary/native Go Thrift recheck queued:
/tmp/matching-deadline-native.log, /tmp/matching-deadline-native.exit, artifact
matching-deadline-native. Broad run uses earlier copied standalone binary;
mixed native queue/History fixture compilation sees latest source. Keep precise
which validation covers the final deadline change.

Additional forwarding audit required: in-process parent forwarding calls
TaskDispatcher.delivery_loop directly, while cross-host forwarding invokes the
full Matching poll handler. Audit parent query pickup, per-hop deadline tailroom,
parent-specific poll expiration, and future override-RPS reads in that local path
before claiming parity. The current cross-host query test covers forwarded
QueryWorkflow to a root poll, not a child poll picking up a root query.
Durable poll deadline follow-up strict Clippy also passes
(/tmp/matching-poll-tailroom.exit=0). Extended the same deadline/expiration budget
to Matching's in-memory fallback, with separate empty decision/activity poll
regression. Latest verification now /tmp/matching-all-poll-budgets.log and
/tmp/matching-all-poll-budgets.exit (all-server-unit plus strict Clippy).
Final deadline tree: all 154 server unit tests and strict workspace/all-target
Clippy pass (/tmp/matching-all-poll-budgets.exit=0). Latest durable Go-generated
native Thrift probe also passes (/tmp/matching-deadline-native.exit=0).

## Final combined verification checkpoint

Frozen-source boundary AND mixed Go/Rust probes pass:
/tmp/matching-mixed-final.exit = 0. Boundary artifacts matching-deadline-go-boundary;
mixed artifacts matching-deadline-mixed. Mixed probe verifies workflow/activity
completion, query/isolation dispatch, native partition config, idle unload/recovery.
Broad runner's first 30 stages all pass in matching-transport-membership-verified;
its failed boundary run is retained, not rewritten. Both final service stages
passed in the independent reruns. Prior full workspace tests plus latest 154
server tests/strict Clippy and native Go transport recheck complete the current
verification categories. Whole-service goal remains incomplete and active.

## Full handler for same-host parent polls (2026-10-17)

The service now forwards same-host parent polls through MatchingApi, not directly
into the parent's dispatcher. A registry factory retains handler state without
retaining its registry; each parent resolution supplies the upgraded live
registry. This avoids an owner/registry cycle and preserves Go's parent query
channel, per-hop deadline tailroom, parent-specific expiration, failover guards,
and future handler-level poll properties. Bare QueueRegistry users retain the
low-level dispatcher fallback; Matching::with_durable_queues always installs the
full handler factory, and with_membership rebuilds it with membership state.

New regressions: child poll receives a root query and completes on root; parent
adds its own tailroom; root-filtered expiration applies to forwarded activity
poll. All 156 server tests pass (/tmp/matching-parent-handler-test.exit=0), and
strict workspace Clippy passes (/tmp/matching-parent-handler-clippy.exit=0).
Final source-name cleanup is covered by another all-server test run in the live
verification chain. Source frozen while this chain runs in cadence-rust tmux:
/tmp/matching-parent-handler-live.log and /tmp/matching-parent-handler-live.exit.
Chain: all server tests, binary build, original Go Thrift, Go boundary delivery,
mixed Go Frontend/History/Worker + Rust Matching. Artifact suffixes:
matching-parent-handler-thrift, matching-parent-handler-boundary,
matching-parent-handler-mixed. Inspect authoritative results before claiming pass.
