# Native Matching partition metadata (2026-10-16)

Continuation after verified dispatch metadata commit 73e1dcc6a. No service role
is complete. Matching remains active before History, Frontend, Worker.

## Implemented storage increment

DurableTaskQueue now exposes partition_config and update_partition_config.
The latter ports tasklist/db.go UpdateTaskListPartitionConfig: serialize with
the database gate shared by lease renewal and ack persistence, preserve the last
persisted ack cursor, fence the SQL write by range ID, and publish the new cached
metadata only after persistence succeeds. ConditionFailed stops/wakes the owner;
other errors are returned without changing cached config (manager retry/stop
policy remains a separate layer, as in Go). Version assignment is deliberately
left to the manager.

Three regressions cover both blob encodings, config retention across rollover,
ack writes and reopen, explicit clearing, stopped/stale owners, and an injected
SQL trigger failure followed by recovery. The existing bidirectional Go SQLite
interop now also writes sparse partition metadata through this native owner API
and asks actual Go persistence to read it after an ack flush.

## Validation

Final workspace passed 406 tests, including all three new regressions. Strict
workspace/all-target Clippy passed. These ran in tmux cadence-rust:
/tmp/opencode/partition-storage-workspace.log,
/tmp/opencode/partition-storage-clippy-final.log,
/tmp/opencode/partition-storage-verify.exit.
Serialized Go storage differential follows it:
/tmp/opencode/partition-native-interop.log and .exit.
Both final markers are 0. Go verified all eight blob fixtures and bidirectional
SQLite persistence including the new native queue configuration. Full 30-stage
service runner was not repeated for this storage-only API increment; previous
73e1dcc6a full pass remains the last mixed-service verification. RPC storage
selection is unchanged by this increment.

## Next coordinated increment

Do not switch only the Update RPC to native storage: Describe, refresh, isolation
selection and partition-count/routing readers currently still use execution
metadata. A split source would silently make updates stale to dispatch readers.

Port manager partition cache and its lock first. Go startup loads root config
from its leased DB metadata; normal nonroot owners load the root row (missing
root is tolerated, other read errors stop startup). Sticky owners have no config.
Root updates validate maps, compare ignoring request version, no-op when initial
config has one read/one write partition, assign previous version+1, persist with
throttle retry, stop owner on terminal/ambiguous failure, and notify the union
of old/new nonroot read/write partitions. Notifications only replace older cache
versions; explicit nil refresh reloads root storage without the version gate.
See go/service/matching/tasklist/task_list_manager.go lines 265-302, 364-540.
Existing executions.rs update/refresh logic has much of the scalar policy, but
must cease being the durable Matching source after the coordinated change.

Then sync matching: taskListManager AddTask -> trySyncMatch -> matcher.Offer.
Rate-limit nonforwarded offers, preserve parent deadline tailroom (1s, max sync
wait), respect local wait config, coordinate poller History-start response before
claiming sync success, persist only unmatched original adds; forwarded adds must
never persist at receiving owner. Cancellation/History errors must retain task
responsibility. Forwarding, routing/scaling, membership, semaphores and production
standalone lifecycle remain open. No complete migration claim is justified.

## Manager/cache continuation

Implemented matching_partition::PartitionCache, root/nonroot startup loading,
versioned root updates with Go persistence retries and fatal stop on failure,
version-gated local owner notifications (including removed partition IDs),
explicit root-row refresh, and native durable Update/Refresh/Describe adapters.
Isolation selection now reads that native cache. Adds/polls return internal
partition configs; Describe returns public API configs. Explicit type validation
prevents invalid enum arithmetic. Execution metadata remains only the development
in-process backend's source, not the durable backend's source.

Go source audit corrected a prior implementation error: dispatch limiting divides
by READ partitions, not write partitions. Read-count config is independent of the
write-count provider still used by ListTaskListPartitions. With
matching.enableGetNumberOfPartitionsFromCache, limiter division uses cached read
map size and AddTask rejects absent write IDs with Go's typed ABORTED
ReadOnlyPartitionError before arrival accounting/persistence. New scoped boolean
config reader uses root-name filters for this flag, like Go.

Three RPC tests cover persistence/recovery, no-op/equality/version assignment,
removed-partition notifications, out-of-order push versus explicit reload,
concurrent updates, fencing, read/write count separation and drain admission.
70 focused Matching tests passed. Full runner currently has 409 workspace tests,
strict lint and all differential/standalone/delivery stages passed; final mixed
stage is pending. Logs: /tmp/opencode/native-partition-full.log and .exit;
artifacts matching-native-partition-full. Final outcome: all 30 stages passed,
marker 0, including mixed Go services/Rust Matching and the existing Go SDK.

Notifications currently address local registry owners. Cross-host notifications,
routing/membership and adaptive scaling still need implementation.

Prepared but UNWIRED scratch source: matching_sync.rs (not in lib.rs) and
cadence-proto/src/task_list_partition.rs (not in lib.rs/build.rs). Neither is
compiled or validated yet. The latter handles default partition map presence,
the same pinned-gogo issue as zero isolation metrics. Add actual-Go wire checks
and mixed-service partition update/Describe coverage before claiming it fixed.
Native manager oracle test draft: /tmp/opencode/native-partition-oracle-test.txt.

## Partition wire-presence follow-up

Manager checkpoint committed/pushed 06c6996fa. Custom public/internal partition
config encoders now emit explicit optional Some values in map entries, including
default empty partitions. Same public Rust paths/fields and upstream protobuf
field numbers retained via extern_path and reexports. Exact-byte/default/
duplicate-key unit test passes for both APIs. Actual pinned Go public/internal
decoders verified omitted empty values become nil, fixed encoding is nonnil.
The Go check runs in existing run-matching-compat.py PartitionWire test.

Native manager now consumes original-Go matching-partitions.json directly,
checking update errors, no-op/write counts, versions, cache and persisted rows.
Passed alongside all existing Go matching differential fixtures.

Mixed Go services/Rust Matching expanded with UpdateTaskListPartitionConfig then
Describe through Go Frontend, checking sparse read ID5 and empty read/write ID0
entries. PASSED including existing Go SDK:
/tmp/opencode/partition-presence-mixed.exit=0,
artifacts matching-native-partition-presence/probe.log reports
nativePartitionConfig passed. Focused wire/oracle run marker:
/tmp/opencode/partition-presence.exit=0.
Strict lint final still running: /tmp/opencode/partition-presence-clippy.log/.exit.
Final strict all-target workspace Clippy passed, marker 0.
Sync module remains unwired/unvalidated; source includes cancellation-safe local
handoff, History response coordination, group routing and deadline-tailroom draft.
