# Matching completion continuation (2026-10-16)

Prior increments: `a9531588d` (sync match), `ca062c861` (local ancestor sync
and poll forwarding). This increment closes the remaining single-node gaps:
forward limiter parity, forwarded-backlog waits, query forwarding, and
standalone durable wiring.

## State

- `matching_forward.rs` (new): Go forwarder token generations
  (`RequestTokens` swaps the whole pool on limit change; in-flight releases
  never inflate the new generation) and `ForwardRate` mirroring
  `quotas.NewDynamicRateLimiter` (5s refresh, ceil burst, zero-rate zero
  burst). Burst uses integer config values so ceil is exact.
- Add walk takes a forward-task permit per ancestor hop (task tokens only for
  the permit; rate checked via `try_forward_task`). Zero tasks/zero rate
  blocks forwarding; live config replaces pools without owner replacement.
- Forwarded root DB-backlog adds (`source == 2`, matching Go
  `TASK_SOURCE_DB_BACKLOG = 2`) wait for pollers through the incoming
  deadline via `offer_forwarded_backlog`, mirroring `OfferOrTimeout`.
- Forwarded polls rebuild the parent request (parent task list + kind +
  identity only), like Go `ForwardPoll`.
- Query forwarding mirrors `OfferQuery`: `QueryChannel::try_offer` succeeds
  only with a decision poller parked in `next` (new waiter counter);
  otherwise the query walks ancestors with a forward-task token (no rate
  limit, like `ForwardQueryTask`). Root with no waiting poller fails fast
  with internal `cannot find parent task list for forwarding` (Go
  `ErrNoParent`); sticky lists never walk. Permits held until resolution.
  The old `delivers_query_to_decision_poller` test now parks the poller
  first; immediate-offer semantics made its old timing flaky.
- Standalone wiring: `grpc_matching::serve_with` serves a prebuilt matcher;
  `main.rs` builds a durable `QueueRegistry` (`matching.db` beside snapshots,
  loopback History client, 10s maintenance pump) when `--data-dir` is set.
  Without `--data-dir` behavior is unchanged (in-memory engine).
- `tests/run-matching-forward-compat.py` (new full-run stage): Go
  `TestRustMatchingForwardRate` boundary check plus Rust `matching_forward`
  unit tests.
- Query cancellation safety: `QueryChannel::Registration` guard withdraws on
  drop (timeout/cancel paths), and `next()` subscribes before taking.

## Verification (this increment)

- 92 focused `matching_` lib tests pass (includes: sync without metadata,
  child poll -> parent backlog, child add -> parent sync, forwarded
  failure/no-persistence, parent waiter cancellation, forward task/rate
  reload, forwarded root backlog late-poller wait, child query -> parent
  poller answer, no-poller query fast-fail, forwarding-disabled reload).
- Strict server all-target Clippy clean.
- Standalone smoke with `--data-dir`: `matching.db` created,
  `service-workflow-compat.cjs` (decision + activity to completion) PASSED,
  restart on the same dir reopens the DB and passes again.
- FULL compatibility run on committed `d501f3779`: **31/31 stages pass**
  (`matching-complete` artifacts, `/tmp/opencode/full-compat.exit=0`),
  including the new `matching-forward-differential` stage, workspace tests,
  strict Clippy, all Go differentials, server build, listener/config checks,
  service workflow probe, public/internal gRPC, Go SDK TChannel, Go-service
  matching delivery, and mixed Go/Rust matching.

## Remaining (explicitly out of single-node scope)

- Cross-host forwarding transport, ringpop membership/ownership, multi-node
  task-list ownership. Single node owns all partitions, as in Go dev mode.
- Adaptive read/write partition autoscaling beyond the configured count
  providers and native partition caches.
- Incoming forwarded adds without timeout use a 200ms fallback; Go trusts
  the incoming context. Audit cancellation/deadline parity.

## Post-completion increments (2026-10-17)

After the 31/31 single-node completion above, the parity audit's remaining
single-process gaps were closed in three increments:

- `d5a00e110` adaptive scaler + isolation balancer
  (`memory/matching-scaler-next.md`).
- `c1667f273` semaphore manager + registry
  (`memory/matching-semaphore-next.md`).
- `3d7e27929` strong/sticky query version gates
  (`memory/matching-query-next.md`).

### Remaining matching gaps (final, with scope)

Single-node scope is complete. The gaps below are either multi-cluster or
cross-cutting and are deliberately out of the single-node bar:

- **Standby task completion** (`task_completer.go`): `CompleteTaskIfStarted`
  and query-only standby polls. Unreachable single-node because every domain's
  active cluster is the local cluster (`is_active_in` is always true); needs a
  second cluster plus a History `DescribeWorkflowExecution` client.
- **Domain-failover poller disconnect** (`engine.go:1707-1785`,
  `domainChangeCallback`): needs the domain-cache change feed, which only fires
  on cross-cluster failover.
- **Thrift matching transport** (`wrappers/thrift`): Go registers a Thrift
  `MatchingService`/`MetaService` wrapper. Internal transport only; the frontend
  reaches matching over gRPC in this port, and the Go SDK never speaks matching
  Thrift, so SDK compatibility is unaffected.
- **Handler error taxonomy metrics** (`handler/context.go`): the typed errors
  themselves are already emitted at each site (StickyWorkerUnavailable,
  RemoteSyncMatched, ServiceBusy, ReadOnlyPartition, EntityNotExists,
  BadRequest, QueryFailed, DomainNotActive via `StartErrorKind::InactiveDomain`).
  Only the per-task-list metrics counters are missing, and the Rust server has
  no metrics subsystem at all (a cross-cutting port, not matching-specific).
- **Multi-host routing/membership/autoscaling** (cross-host forwarding
  transport, ringpop ownership, shard-distributor partition autoscaling):
  deferred; single node owns every partition as in Go dev mode.
- **Dynamic-config surface**: properties are read from the runtime admin overlay
  with Go defaults; there is no file/remote dynamic-config source at startup
  (server-wide gap, not matching-specific).

### Full compatibility re-run (2026-10-17)

All 31 runner stages exit 0 on the committed tree `3d7e27929` (workspace
tests/clippy, all Go differentials, server build, listeners/config checks,
service workflow probe, public/internal gRPC, Go SDK TChannel, Go-service
matching delivery, mixed Go/Rust matching):
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-semaphore-query/results.json`.
