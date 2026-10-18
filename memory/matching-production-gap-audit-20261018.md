# Matching production gap audit (2026-10-18)

Read-only source audit at checkout `691f750e7037eee004cc98417d3957613c8cc9bd`.
The frozen regression evidence at `memory/evidence/matching-frozen-regression-2026-10-18.md`
is for ancestor `3072698e18c8fa3f5e67d566a4ce19ae47f37b9c`; it is useful test
evidence, not a claim about current production wiring or a source-identical run.
Worktree was clean before this report. No implementation, build, or test was run.

## Current production state

* Go Matching is constructed as its own service and receives the shared domain
  cache, persistence, clients, metrics, shard-distributor executor and config:
  `go/service/matching/service.go:47-48,85-120`. `handler.NewEngine` receives
  those dependencies at `go/service/matching/handler/engine.go:157-194`.
* Rust standalone main selects durable queues only when `--data-dir` is present;
  with no data directory it calls `Matching::new`, and non-Matching modes do
  likewise (`rust/crates/cadence-server/src/main.rs:730-786`). Durable storage is
  a single SQLite `matching.db` (`:732-745`), History is hard-coded to loopback
  gRPC with a 1s operation timeout (`:746-758`), range size is fixed at 100000
  (`:759-764`), and maintenance is spawned at a fixed 10s interval without a
  retained cancellation/join handle (`:765-775`). This is useful restartable
  single-process storage, not production persistence-backend parity or a
  standalone Matching service with independent History.
* `Matching::with_durable_queues` is explicitly incremental; its comment says
  domain/admin/query methods still use supplied application state
  (`rust/crates/cadence-server/src/grpc_matching.rs:250-256`). It installs many
  matching config closures from the admin overlay (`:257-520`), but main does
  not provide production peers/discovery or configure a production persistence
  backend. `Config` has no Matching/Ringpop config section
  (`rust/crates/cadence-server/src/config.rs:14-34`).
* Durable registry accepts an `InMemoryDomainStore` for optional metadata and
  defaults it to `None` (`rust/crates/cadence-server/src/matching_registry.rs:93-109,
  391+`); Matching's durable constructor separately consults
  `state.domains`/the admin overlay for domain names and filters
  (`grpc_matching.rs:261-275`). Main loads/saves domain snapshots as in-memory
  stores (`main.rs:545-596`). This is not Go's shared cache with persistence
  refresh/change callbacks. Go's Matching handler registers/unregisters domain
  change callbacks during lifecycle (`go/service/matching/handler/engine.go`
  callback methods around `:1434-1440`; lifecycle around `:200-218`).
* Rust has useful gRPC client caching by endpoint in `matching_membership.rs:13-31,44-88`.
  This does not supply a live peer provider. The Go contract and Rust gaps are
  documented with source detail in `memory/matching-discovery-contract.md`.
  Outbound History is a single configured lazy channel in main, not a
  service-discovered, lifecycle-managed client set.

## Requested live settings: audited state

Go registers all six in `go/service/matching/config/config.go:41,57,86-90,118`
and constructs the property accessors at `:205-223,254`. The task-list manager
captures filtered readers in `go/service/matching/tasklist/task_list_manager.go:1065-1084`.

| Setting | Go behavior/source | Rust source state / missing behavior |
|---|---|---|
| `matching.maxTaskDeleteBatchSize` | Task-list filtered; sampled under GC lock and passed to repeated `CompleteTasksLessThan` until fewer rows deleted (`go/service/matching/tasklist/task_gc.go:65-87`). | No Rust provider or dynamic read found. `DurableTaskQueue` owns a default `TaskGarbageCollector` (`rust/crates/cadence-persistence/src/matching_queue.rs:35-38,150-174`); maintenance uses its fixed collector policy. Preserve signed values and Go's loop semantics; existing note explains zero/negative hazards (`memory/matching-maintenance-live-config.md`). |
| `matching.maxTaskBatchSize` | Task-list filtered; bounds writer batch collection (`go/service/matching/tasklist/task_writer.go:318-328`). | No provider/read found. Rust append path is in `matching_queue.rs`; it has no Go-style bounded writer channel/batch collector. |
| `matching.outstandingTaskAppendsThreshold` | Task-list filtered; sizes append channel at writer creation (`task_writer.go:88-95`). | No provider/read found; Rust queue serializes append via writer mutex (`matching_queue.rs:25-36`), not a bounded append queue with Go backpressure behavior. |
| `matching.appendTaskTimeout` | Task-list filtered per append request, wraps enqueue and persistence wait (`task_writer.go:149-170`). | No provider/read found; append code has no corresponding per-call config timeout (`matching_queue.rs` append methods). |
| `matching.recordTaskStartedTimeout` | Domain filtered, defaulted in config (`config.go:254`); applied per History Record*Started attempt (`go/service/matching/handler/engine.go:1480-1535`). | Implemented in durable path: main currently constructs a plain History client; provider is attached by `Matching::with_durable_queues` (`grpc_matching.rs:467-485`, registry forwarding at `matching_registry.rs:584+`). Verify wiring path is active in current exact constructor and expand validation; no standalone non-durable Go-equivalent RPC client abstraction. |
| `matching.shutdownDrainDuration` | Global duration property in config (`config.go:216`); service shutdown drains before stopping engine (`go/service/matching/service.go` lifecycle and handler shutdown). | No dynamic read found in Rust Matching main/shutdown path; server listener shutdown does not implement Matching's configured traffic drain or task-writer/owner drain parity. |

The named settings are not the full config parity surface. The config file also
defines task reader/writer, forwarding, partition/scaler, isolation, throttling,
and shutdown values; audit each against live call sites before declaring parity.

## Shard-distributor gates

Go `setupExecutor` installs a no-op only when no namespaces are configured;
otherwise it validates at most one namespace, configures TTL, advertised
TChannel/gRPC/host metadata, onboarding percentage, drain observer and a
`ShardProcessorFactory` (`go/service/matching/handler/engine.go:221-316`).
Task-list creation routes eligible managers through executor ownership and
explicitly excludes short-lived UUID names (`engine.go:317-355`; tests
`engine_test.go:530-579`). Rust `matching_registry.rs` has local queue partition
ownership/scaling primitives, but main does not instantiate a shard-distributor
client/executor or provide the Go ownership/onboarding/drain gates. Partition
counts and adaptive scaler are not equivalent to external executor assignment.

## Metrics and outgoing channels

Go injects a metrics client/scope into Matching and its executor
(`service.go:110-120`, `handler/engine.go:157-194,247-260`); task request,
sync/async latency, match-source, writer and lease metrics are emitted at
`engine.go:545-549,586-587,1398-1433,1554-1583` and
`task_writer.go:228-231`. Rust sources have operational logs and in-memory
Matching statistics, but no matching metrics client/scope wiring in standalone
`main.rs`; the inspected Rust writer/History client does not expose parity for
these Go metric families. Rust membership caches remote gRPC channels by
endpoint, but production main does not create/feed a live membership provider.

## Prioritized remaining checklist

1. **NEXT independent slice: production durable queue/config construction and
   domain metadata boundary.** Extract main's durable queue assembly into a
   reusable Matching runtime constructor; provide explicit persistence store,
   History client and config dependencies. Wire missing
   `maxTaskDeleteBatchSize`, `maxTaskBatchSize`,
   `outstandingTaskAppendsThreshold`, `appendTaskTimeout`, and
   `shutdownDrainDuration` semantics at their Go read points; keep
   `recordTaskStartedTimeout` active and prove its constructor wiring. Preserve
   Go filter scope, read frequency, defaults and signed/boundary behavior.
   Replace or bridge snapshot-only domain resolution with a shared cache
   interface that supports refresh and domain change notification. This slice
   does not need Ringpop/discovery implementation.
2. Wire and verify matching-owned production persistence (backend schema,
   leases, ack/read levels, expiry, GC and restart) and independently supplied
   History peer configuration; ensure process shutdown cancels/joins maintenance
   and drains outstanding RPC/writer work.
3. Port shard-distributor executor configuration/ownership, onboarding,
   no-namespace behavior, excluded ephemeral lists and drain signals.
4. Add Matching metric sink and Go metric names/tags at RPC, queue, writer,
   History-client and executor boundaries; connect outgoing channels to the
   runtime peer/client lifecycle.
5. Finish remaining Matching behavioral parity (forwarding/partition ownership,
   semaphore, retries, polling/cancellation, query, rate/isolation and lifecycle)
   and execute the complete mixed-service failure/restart/failover audit.
   Matching remains incomplete; do not start History migration.

## Concrete next task and validation plan

Implement only checklist item 1 as a bounded patch: introduce a runtime config
object for the durable Matching owner and add typed, task-list/domain-filtered
providers for the five missing settings, preserving Go's construction-vs-live
sampling points. Keep SQLite usable as the existing test backend. Add unit
oracles comparing Go source behavior for batch size 0/1/large, append queue
capacity/backpressure, append timeout before and after enqueue, and shutdown
drain timeout; test scoped overlay selection and updates. Add a domain metadata
fake that verifies cache refresh and change notification invalidates/reloads
owner metadata. Do not mix discovery or shard-distributor implementation into
this slice.

Then run focused Rust persistence/server tests and strict all-target Clippy; run
the matching compatibility stages and full `tests/run-*` compatibility runner
against Go Frontend/History/Worker with Go Matching absent, through the existing
Go SDK. Include queued activity/decision completion, cancellation/deadline,
timeouts/retries, and graceful stop/restart with a pending backlog; assert the
same histories/errors and recovered task delivery. Record command lines,
revision, logs/artifact paths and per-stage exit codes in a follow-up memory
evidence file. This is validation to run for that implementation task; this
audit performed none of it.

## Evidence boundary

The frozen report says 34/34 runner checks, 524 workspace tests and strict
Clippy passed, including a mixed Go Frontend/History/Worker + Rust Matching
scenario. It does not verify current checkout `691f750e` specifically, nor
production discovery, configurable production persistence/domain cache,
shard-distributor integration, full Matching metrics, or the crash/restart and
recovery matrix above. Do not use those results to mark Matching complete.
