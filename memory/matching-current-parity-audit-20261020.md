# Matching current-source parity audit (2026-10-20)

This read-only audit updates the older production-gap checklist for the current
`t3/matching-runtime-owner` worktree. Source examined includes production
startup in `rust/crates/cadence-server/src/main.rs`, `matching_runtime.rs`,
`grpc_matching.rs`, `matching_registry.rs`, persistence modules, and the pinned
Go Matching service/handler/task-list/semaphore sources. No build or mixed run
was started because the shared MySQL validation lane was occupied. Current
production evidence is recorded separately in
`memory/evidence/matching-dns-build-20261020-2726808/`; it is bounded evidence,
not a Matching acceptance audit.

## Current positive evidence and limits

- Production Rust Matching can be started beside Go Frontend, History, and
  Worker, with Go Matching absent, using the configured shard-distributor and
  the original Go Ringpop discovery. The configured shard-distributor probe
  passed against binary SHA-256
  `679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa`.
  Its result has `acceptanceEligible: false`.
- The production Go YARPC forwarding oracle passed its three focused cases:
  forwarded poll, add/query handler routing, and spectator failure propagation.
  This does not establish cross-host public routing or failure recovery.
- The new two-host harness exercises scoped idle eviction, SDK completion,
  owner crash/recovery, shared task rows/range/ack, and a pinned Go typed-error
  decode. It has not yet been compiled/run after its latest changes. Earlier
  runs exposed a public ABORTED ownership response whose exact RPC is unknown;
  the current source retries empty normal polls and scopes the idle interval,
  but this remains unresolved pending a fresh run.
- Production Matching now constructs its configured default-store task queue,
  domain cache, History client and Ringpop membership path in `main.rs`; do not
  reuse the 10/18 statement that production discovery is entirely absent.
  The configured Mixed Go/Rust gate proves a bounded path, not all backend,
  lifecycle, or failover combinations.

## Current source gaps

1. **Semaphore state is not shared or durable in the production runtime.**
   `Matching::new` builds the semaphore registry from
   `state.executions` (`grpc_matching.rs`), and semaphore metadata, holds,
   scans, grants and releases are methods on `InMemoryExecutionStore`
   (`cadence-persistence/src/semaphores.rs`). In `main.rs`, configured
   Matching task queues open the configured default store, but application
   `state.executions` is still `InMemoryExecutionStore`; `--data-dir` snapshots
   that store to JSON periodically. Two Rust Matching processes therefore do
   not conditionally grant against one shared semaphore row set, and restart
   safety is bounded by snapshot timing. The Go manager's contract is backed by
   `persistence.SemaphoreTokenManager` and conditional token/owner row writes.
   This is the strongest concrete next parity gap: move metadata and bucket
   token/owner operations behind a persistence interface selected by the same
   configured backend, preserving atomic grant outcomes and manager rebuild.
   Validate concurrent acquisitions from two processes and recovery after
   killing/restarting one process before claiming semaphore parity.

2. **Partition cache propagation remains local.** Native partition metadata,
   root updates, persistence fencing, and local owner notification exist, but
   local notification does not reach matching owners on other hosts. Rust
   `QueueRegistry::update_partition_config` updates loaded non-root owners in
   that process and includes old/new read and write partition IDs. Go's
   `taskListManagerImpl.notifyPartitionConfig` sends
   `RefreshTaskListPartitionConfig` to each non-root read partition in the old
   and new configs. Complete the remote calls through the live Matching peer
   path and queue a two-host gate for adding/removing remote partition IDs,
   stale/out-of-order versions, owner movement, and refresh after restart. Do
   not conflate this with existing Ringpop task-list routing or the configured
   shard-distributor gate. Registry/runtime implementation files are protected
   WIP and were not edited in this source-only pass.

3. **Domain failover side effects differ in enumeration, but the core poll
   release path exists.** Go `domainChangeCallback` is triggered by increasing
   failover notification versions on global, non-active-active domains. It
   lists persisted task lists by domain/kind, loads normal decision/activity
   managers and sticky decision managers, then releases blocked polls. Rust's
   matching metadata cache sync establishes a catch-up baseline, detects later
   global notification-version advances, and signals per-domain watches;
   blocked poll handlers release loaded registry owners for that domain. This
   wakes currently loaded blocked polls, but does not enumerate persisted task
   lists or instantiate managers as Go does. Ordinary config updates do not
   trigger Go's callback release behavior; no separate unload/clear-poller
   effect was found in `engine.go:1707-1785`. Queue a mixed failover gate with
   blocked normal decision/activity polls and a sticky decision poll, plus
   active-active and non-global controls. Verify catch-up and unloaded task
   lists separately before changing this path. No source change was made in the
   protected runtime WIP.

4. **Remaining Matching behavior and lifecycle still require the parity audit.**
   The source includes cross-host poll/add/query forwarding, typed ownership
   statuses, local poll cancellation, partition/isolation dispatch, standby
   completion checks, idle cleanup, and configurable shutdown drain. Their
   existence does not establish all Go edge behavior. In particular, complete
   the production failure/restart test with exact RPC and membership snapshots;
   add remote query/activity, domain failover, standby completion, typed handler
   error taxonomy, and production transport/backend coverage as source review
   confirms their respective Go behavior.

5. **Configuration and operational parity is incomplete.** The dynamic-config
   overlay is active, but the old key list must be checked against Go read sites
   before carrying forward any “missing” claim. Audit remaining Go call-site
   settings for construction-time versus live reads, filter scope, defaults,
   and signed/boundary behavior. Metrics/error counters and production listener,
   persistence backend and shutdown lifecycle also remain separate acceptance
   gates; do not expand this into History migration work.

## Recommended sequence

1. Release the shared lane, compile the Go typed-error oracle, then run the
   prepared two-host production recovery harness in tmux `cadence-rust`. Preserve
   the known failed attempt and identify the exact failing RPC, source/owner
   identities, both membership views, and transition timing for any new ABORTED
   response. Do not weaken SDK assertions.
2. Implement the semaphore persistence abstraction against the currently
   selected Matching persistence backend. Add conditional-update races,
   restart rebuild, bucket scan, metadata creation, and two-process acquire
   tests. Keep the in-memory adapter for focused unit tests.
3. After the protected registry/runtime WIP is available for editing, implement
   cross-host partition config notifications and validate owner movement/reload.
   In parallel, prepare the domain failover mixed gate above; source inspection
   does not support the prior claim that ordinary config changes cause Go
   Matching to unload managers.
4. Complete the service parity audit and reproducible mixed Go/Rust failure,
   restart, and recovery evidence before beginning History migration.

No Matching completion claim is made by this audit.
