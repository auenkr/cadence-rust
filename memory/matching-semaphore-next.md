# Matching semaphore manager and registry (2026-10-17)

Prior increments: `a9531588d`, `ca062c861`, `d501f3779`, `79960a154`,
`d5a00e110` (adaptive scaler + isolation balancer). This increment ports the
matching-side semaphore serving path, which the objective explicitly listed as
a completion gate ("do not claim Matching complete while ... semaphores ...
remain incomplete").

## State

- `matching_semaphore.rs` (new) ports Go `service/matching/semaphore`:
  - `Identifier` (`identifier.go`): `new` validation (non-empty domain/name,
    `bucket >= 0`), `to_log_string` (`domain/name/bucket`), `ring_key`
    (`domain_name_bucket`, `common/semaphore.RingKey`).
  - `Registry` (`semaphore_registry.go`): read-then-write `get_or_create`
    (register before `Start` so a concurrent request reuses the manager),
    `unregister` (only when the map still holds the same `Arc`),
    `by_identifier`, `all_managers`, and `sweep_idle` (the liveness callback:
    unregister + stop a bucket idle past `matching.semaphoreIdleTime`).
  - `Manager` (`semaphore_manager.go`): `Created -> Starting -> Running ->
    Stopped`; `start` scans once and refuses a stopped manager (`NotReady`);
    `load` rebuilds the free-set and owner reverse-index from a bucket scan and
    re-marks liveness; `acquire` marks alive, requires `Running`, then `grant`.
  - `grant` mirrors Go: confirm an existing reverse-index entry against the
    token row (`confirm_hold`), else draw up to `MAX_GRANT_ATTEMPTS = 3` free
    ids and settle each with a conditional write, mapping
    `Applied`/`SlotTaken`/`AlreadyHeld(held)` (an `AlreadyHeld` with no token
    is an internal error). Full bucket returns `NoSlot`.
  - Free-set helpers (`add_to_free_set`, `remove_from_free_set`) preserve Go's
    duplicate guard and swap-remove; `reserve` draws a uniform-random id.
- Persistence (`cadence-persistence/src/semaphores.rs`): added the token/owner
  row surface the manager needs on top of the in-memory store —
  `scan_semaphore_bucket` (seeded token rows 1..=budget plus owner rows),
  `get_semaphore_ownership_by_token` (holder or `""`; out-of-range is
  `NotFound`), `grant_semaphore_token` (conditional, returns
  `GrantOutcome::{Applied,SlotTaken,AlreadyHeld}`), `release_semaphore_token`,
  and the `SemaphoreRow`/`GrantOutcome` types (re-exported from the crate root).
- Handler (`grpc_matching.rs`): `AcquireSemaphore` now validates the owner id,
  resolves the domain, checks the `EnableDistributedSemaphore` flag, builds an
  `Identifier`, lazily sweeps idle buckets, `get_or_create`s the manager, and
  calls `acquire`. Errors map like Go: `BadRequest`/`Disabled` ->
  `InvalidArgument`, `NotFound` -> `NotFound`, `NotReady` (Go
  `ErrNotReady`/`ServiceBusyError`) -> `ResourceExhausted` with a
  `ServiceBusyError` detail, `Internal` -> `Internal`.
- `Matching` holds the registry (built from the shared execution store in
  `Matching::new`); `serve_with` spawns a 30s sweeper so a quiet host still
  unloads idle buckets, matching Go's per-manager liveness timer.
- Single process: one host owns every bucket, so the membership ring check is
  a self-check and no routing/forwarding is needed.

## Verified

- 7 `matching_semaphore` unit tests: identifier validation/log/ring keys,
  registry sharing + unregister identity + rebuild, grant-until-full then
  `NoSlot` + re-acquire returns the held slot, load rebuilds from persisted
  holds, `confirm_hold` drops a stale index entry and never re-offers a slot
  another owner holds, a missing semaphore fails the scan without leaving a
  registered manager, and `sweep_idle` unloads/ stops an idle bucket.
- `matching_rpc_semaphore_acquire_grants_and_reports_no_slot` drives the gRPC
  handler end to end: two owners fill a two-slot bucket, a third gets
  `NoSlot`, a holder re-acquires its slot, a bad owner id is
  `InvalidArgument`, a missing semaphore is `NotFound`, and a disabled domain
  is `InvalidArgument`.
- Existing persistence semaphore tests still pass.
- Full `cadence-server --lib`: 131 passed. Strict
  `cargo clippy -p cadence-server --all-targets -- -D warnings`: clean.

## Remaining matching gaps (from the parity audit)

- Standby task completion (`task_completer.go:92`): no `CompleteTaskIfStarted`.
  Only reachable for global domains whose active cluster is elsewhere.
- Strong/sticky query version gates (`engine.go:790-800,1064-1090`): needs the
  `versionChecker`/`supportedClients` infra (frontend-level).
- Handler error taxonomy (`handler/context.go:83-139`): 13 typed errors +
  counters collapsed to generic `Status`.
- Domain-failover poller disconnect (`engine.go:1707-1785`): no
  `domainChangeCallback`.
- Thrift matching transport (gRPC only today).
- Multi-host: cross-host forwarding transport, ringpop membership/ownership,
  shard-distributor autoscaling (out of single-node scope).
