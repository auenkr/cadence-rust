# Matching membership and Ringpop lock audit

Scope: `matching_ringpop_provider.rs`, `ringpop_provider.rs`, and the
post-initial-refresh path in `matching_membership_lifecycle.rs`, prompted by
run 41108824183 at source `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`.

## Finding

No source-proven nested standard-mutex lock cycle or blocking synchronous
membership callback was found in the audited paths. No code change is justified
by this audit.

## Lock and callback paths

- `MatchingRingpopProvider::start` holds its async `lifecycle` mutex through
  JOIN/bootstrap, SWIM startup, initial provider snapshot publication, and
  transition to `Started`. Its async wait is over network/bootstrap work. The
  post-start refresh worker is only spawned after this method returns. `stop`
  holds the lifecycle mutex while it awaits its owned cleanup task; that task
  joins the monitor and healer and then destroys the Ringpop node. Those tasks
  do not acquire `lifecycle`.
- The membership monitor responds to the node's watch generation by reading a
  state snapshot and converting it to peer members, then `publish_snapshot`
  obtains the snapshot `RwLock` and sends a Tokio broadcast event before
  releasing that guard. `broadcast::Sender::send` does not invoke receiver code
  synchronously. The lifecycle refresh worker receives the event, drains queued
  hints, awaits the provider snapshot, and only then updates the placement ring.
  Its periodic tick and empty-ring refresh signal use the same await-then-update
  path. Placement replacement releases the ring write guard before pruning the
  channel cache or notifying watchers.
- Inbound `/protocol/ping` decodes before taking the state mutex. It holds that
  mutex across readiness/app checks, memberlist change application, watch
  generation update, transition scheduling, dissemination bookkeeping, and
  response encoding. These are synchronous in-memory operations; the production
  incarnation/random closures do not call back into Ringpop. The subsequent
  reverse full-sync scheduling takes the runtime mutex only after the state
  guard has gone out of scope.
- `apply_changes` and local-change recording use `watch::Sender::send_modify`
  and `Notify::notify_one` while holding state. Both publish/wake asynchronously
  and do not run application callbacks inline. The monitor later takes a fresh
  state snapshot after observing the watch update.
- The only nested standard-mutex order found is runtime then state in
  `start_swim`, `stop`, and the immediate-completion branch of `stop_and_wait`.
  No audited code acquires runtime while retaining state: `destroy` drops its
  state guard before calling `stop`, self-eviction drops runtime before taking
  state, and PING drops state before reverse-sync scheduling. The background
  reverse-sync task awaits JOIN without either guard, then takes runtime to
  decrement its job count.
- The periodic healer waits on its timer/test trigger, snapshots member state,
  performs discovery, snapshots again, and runs JOIN-based heal attempts. Heal
  attempts copy request/member data under state, release the guard for network
  JOIN, then take state only for in-memory application; subsequent PING is sent
  after releasing state. There is no lock held across its network awaits.
- Self-eviction takes runtime only to set its one-shot flag, releases it, then
  mutates local membership under state. It gathers targets there and sends
  concurrent PINGs after releasing state. `withdraw` invokes this only during
  shutdown; the supplied before-restart log contains no shutdown trace showing
  that withdrawal began.

## Relation to the failed run

The supplied evidence records successful initial membership refresh and
publication, admitted `ListTaskListPartitions` completing in 776 microseconds,
and later readiness probes timing out. The process snapshot records 10 threads
waiting in `futex_do_wait`, zero CPU ticks over its 500 ms sample, and an HTTP/2
listener receive queue of 2; the direct partition probe records
`settingsReceived: false`. These observations establish a service stall at the
probe time, but they do not identify which futex or lock was involved. The
available pre-restart log contains initial PING traffic and the successful
partition RPC, but no log evidence of a later accepted membership removal,
self-eviction, or shutdown. Therefore this audit does not attribute the gRPC
failure to membership removal, Ringpop lock contention, or any other specific
cause.

No runtime/build/test/format command was run for this audit. Only source and
existing evidence artifacts were read.
