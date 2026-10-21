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
   local registry notification does not itself notify matching owners on other
   hosts. Go's root update path sends notifications to old/new non-root read and
   write partitions. Complete this through the live Matching peer path and test
   config changes that add/remove remote partition IDs, out-of-order versions,
   owner movement, and refresh after restart. Do not conflate this with the
   existing Ringpop task-list routing or the configured shard-distributor gate.

3. **Go domain-change side effects need a current-path parity check.** Go's
   `domainChangeCallback` clears manager pollers and unloads/refreshes managers
   after domain failover/config changes. Rust has a production metadata cache
   and domain filtering, but a change callback that produces all matching-side
   side effects is not established by the cited mixed gate. Compare
   `engine.go:1707-1785` to the current cache/runtime subscribers; add the
   missing invalidation/notification behavior only after confirming a live Go
   call path and preserve global-domain semantics.

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
3. Implement cross-host partition config notifications and validate owner
   movement/reload. Then continue the current Matching Go-source parity audit
   for domain callbacks and failure/recovery behaviors.
4. Complete the service parity audit and reproducible mixed Go/Rust failure,
   restart, and recovery evidence before beginning History migration.

No Matching completion claim is made by this audit.

## Semaphore backend correction (2026-10-20)

Pinned Go source review corrected the earlier “make production semaphores
shared” recommendation for the current SQL path: Go's
`go/common/persistence/sql/sql_semaphore_metadata_store.go` and
`sql_semaphore_token_store.go` are explicit stubs. Every operation returns a
`types.BadRequestError` with `distributed semaphore is not supported on the SQL
persistence backend`; the comments state that the distributed semaphore
feature is supported only by NoSQL/Cassandra. Therefore a Rust SQLite schema
would change Go behavior and violate the schema compatibility requirement.

The isolated source branch `t3/matching-semaphore-durable` adds a backend
boundary, routes configured SQL metadata create and Matching acquire through
that exact unsupported behavior, and keeps the in-memory adapter for embedded
mode/tests. It does not create SQL tables. Cassandra remains the real persistence
parity gap: port the NoSQL metadata rows and the logged-batch conditional grant
and release semantics on `semaphore_metadata` and `semaphore_tokens`, then wire
them with the Cassandra Matching task/domain store. No Cassandra implementation
or runtime claim is made until a Rust driver and configured NoSQL boundary are
present and validated.
