# Replication continuation notes (2026-10-15)

The Rust ReplicateEventsV2 adapter still returns success without applying any
history. Raw-history export and version-history primitives are prerequisites,
not evidence of replication completion. Do not mark this capability complete.

Source anchors:

- go/service/history/ndc/replication_task.go: validateReplicateEventsRequest,
  deserializeBlob, validateEvents and newReplicationTask.
- go/service/history/ndc/history_replicator.go: branch/conflict application.
- go/service/history/ndc/state_builder.go: passive mutable-state reconstruction.
- go/service/frontend/admin/handler.go: raw V2 default ranges, cursor validation.
- go/common/persistence/serializer.go: history batches are workflow.History
  structs inside the versioned ThriftRW codec, not JSON arrays.

The Go replication task validates UUIDs, nonempty decoded primary history,
contiguous event IDs and a constant event version across both runs. It decodes
replication blobs as ThriftRW regardless of their advertised encoding field.
New-run events and continue-as-new require coordinated state reconstruction;
validating and dropping events must never be described as replication.

New Rust history_blob::encode emits the 0x59 preamble, History field 10 typed
as list<struct>, and wire timestamps in nanoseconds. The internal engine uses
milliseconds. The decoder currently converts top-level timestamps back to
milliseconds; lossless nanosecond preservation for arbitrary remote histories
must be resolved before using it for a full passive replica. Unknown event
attributes/enums and some nested timestamp conversions also need auditing.

Raw V2 now normalizes omitted bounds using the cursor's histories before
validation, matching Go. It still slices a per-run event vector; it does not
retrieve arbitrary selected physical branch tokens or preserve original stored
append-batch boundaries. JSON development snapshots have no production branch
store. These remaining limitations block a full raw-history/replication claim.

Required application work: cluster failover-version mapping/gating, durable
history branch storage, passive execution roles, branch LCA/conflict/reset
handling, history-driven reconstruction of activities/timers/children/decisions,
atomic continue-as-new writes, replication queues, acknowledgements and DLQ
retry/merge behavior. Compare against actual Go code and original SDK replay;
never substitute synthetic successful RPC responses for this state machinery.

Cluster version prerequisite (2026-10-15): cadence-common::cluster now ports
next-version arithmetic, version owner resolution (including migrating offsets),
same-cluster checks and enabled/remote cluster sets. 69 cases compare with Go's
actual cluster.Metadata. Global frontend registration and FailoverDomain now
use configured offsets/increment. Current runtime paths pass migration override
false; per-domain dynamic-config lookup remains unported. Config structs now
retain the core clusterGroup fields, but full Go FillDefaults/Validate and
primary-cluster restrictions still need faithful integration. Existing execution
records retain their original current_version; they are not promoted merely by
changing domain metadata. Passive request gating, replication task/source-cluster
validation, queue fencing and state reconstruction remain required next work.
