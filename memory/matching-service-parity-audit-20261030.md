# Matching service source parity audit — 2026-10-30

## Scope and pin

Reviewed the frozen candidate `df461a3e046c6a0da1ac440d7be9acebcc12cd48`
(`origin/t3/matching-failure-coverage-20261030142037`) against the production
Go Matching implementation and the current native Rust path. This is a
source-level lifecycle, dependency, ownership, failure/restart/recovery, and
acceptance-workflow audit. It does not establish that the candidate passed CI or
any mixed-service scenario.

The ignored-test inventory is the 30-case list (13 evidence gaps and 17
prerequisite-gated cases) in
`origin/t3/matching-ci-parity:memory/matching-ci-checkpoint-20261030-41030554541.md`.
There is no separate `matching-ignored-inventory-20261030.md` at that ref.
Those cases remain classified as recorded there; this audit does not recast
source presence as test evidence.

## Go-to-Rust production function map

| Go production path | Rust production path | Source finding | Classification |
| --- | --- | --- | --- |
| `service.go::NewService`, `Start`, `Stop` | `launcher.rs::run`, configured Matching assembly, `MatchingMembershipLifecycle::{start,withdraw,stop_after_drain}`, `MatchingRuntime::{drain,shutdown}` | Go builds the engine, registers gRPC/Thrift handlers, starts the base resource and engine, then on stop evicts self, waits configured drain, stops handler and resource. Rust opens the configured runtime, establishes discovery before `mark_started`, keeps listeners available through withdrawal/drain, then tears membership down. | Implemented in source; exact production lifecycle test evidence missing. |
| `handler/engine.go::NewEngine`, `setupExecutor`, `getOrCreateTaskListManager`, `getOrCreateSemaphoreManager`, `Start`, `Stop` | `matching_launch_native.rs::assemble_configured`; `matching_runtime.rs::MatchingRuntime`; `matching_runtime_native.rs::native_queue_registry`; `matching_registry_native.rs::register/manager/shutdown`; `matching_semaphore_service_native.rs` | Native assembly wires durable task and semaphore byte stores, History client, domain cache, membership, quotas, metrics and fatal sinks. Registry owns queue/semaphore admission and runtime shutdown stops managers and joins writer actors. | Implemented in source; configured-path and backend-specific behavior still need exact-pin runtime evidence. |
| `handler/membership.go::runMembershipChangeLoop`, `shutDownNonOwnedTasklists`, `shutDownNonOwnedSemaphoreManagers` | `matching_membership_lifecycle.rs`; `matching_runtime.rs::native_shard_ownership_check`; `matching_registry_native.rs` ownership policies | Rust refresh lifecycle applies snapshots to membership, ownership checks consult drain state and shard-distributor/local membership, and stale owners are stopped through registry lifecycle. | Implemented in source; live membership changes and ownership transfer evidence missing in required lanes. |
| `handler/engine.go::domainChangeCallback`, `registerDomainFailoverCallback`, `unregisterDomainFailoverCallback`; `task_list_manager.go::ReleaseBlockedPollers` | `matching_runtime.rs` cache callback wiring; `matching_failover_callback_tests.rs`; native registry/cache callback path | The production callback and registry attachment path exist, including late attachment coverage called out in the current native ledger. The checkpoint still lists registration catch-up, callback error and selective release assertions as missing in its ignored inventory. | Partially evidenced; ignored callback cases must retain their test-evidence-missing status until candidate test output proves them. |
| `task_list_manager.go::{Start,Stop,watchForStop,AddTask,DispatchTask,GetTask,RefreshTaskListPartitionConfig,UpdateTaskListPartitionConfig}` | `matching_registry_native.rs`, `matching_writer_native.rs`, `matching_reader_native.rs`, `matching_task_store.rs`, `matching_persistence_native.rs` and backend stores | Durable writers/readers, owner fencing and public partition operations are represented. Older `matching-v6-lifecycle-persistence-audit-20261027.md` found lease/range fencing parity for MySQL, PostgreSQL, Cassandra, SQLite, and source-level drain ordering. | Source implemented; cross-backend restart, data visibility and failure recovery evidence missing at this pin. |
| `handler/engine.go::setupExecutor` and `tasklist/shard_processor.go::{Start,Stop}` | `matching_shard_distributor.rs`, `matching_scaler_runtime_native.rs`, `matching_runtime.rs` configured executor/spectator/cleanup wiring | Configured shard-distributor executor, membership, peer cleanup, scaler and no-op fallback wiring are present. | Source implemented; only the required real shard-distributor mixed lane can establish assignment, TTL expiry, cleanup and takeover. |
| `wrappers/grpc/grpc_handler.go` and generated Matching server; `wrappers/thrift/thrift_handler.go` | `grpc_matching.rs`, `thrift_matching_native_dispatch.rs`, `thrift_matching_native_responses.rs`, `tchannel.rs`, `launcher.rs` | The native gRPC service, Thrift dispatch, and shared Ringpop/TChannel listener are assembled. Existing source tests enumerate service methods and transport surfaces. | Implemented in source; transport interaction, error matrix and SDK compatibility remain evidence-gated. |
| Go task-list manager calls History for `RecordDecisionTaskStarted`, `RecordActivityTaskStarted`, task refresh and query handling (`handler/engine.go`, `task_list_manager.go`) | `matching_history.rs`, `matching_history_native.rs`, `matching_dispatch_native.rs`, launcher History peer resolver | Matching has explicit configured History clients and native dispatch paths; transport selection is configurable for gRPC/TChannel. | Implemented in source; Go History interoperability and workflow outcomes remain evidence-gated. |
| Go `config.NewConfig` and `resource.Params` | `matching_config.rs`, `launcher.rs` config parsing/assembly, service-core dynamic config | Production launcher reads listener, persistence, discovery, domain/config-store, metrics, TLS, throttling and drain inputs. | Partial by inventory: checkpoint's `go_new_config_every_field_has_a_rust_reader` lists 66 Go config keys whose complete key-by-key Rust reader mapping is still missing. |

## Lifecycle, ownership and failure audit

### Implemented in source

- Startup path: `go/service/matching/service.go::{NewService,Start}` builds the Go engine and registers both transports before base resource/engine start. Candidate `launcher.rs::run` assembles configured native stores and dependencies, starts the shared TChannel/Ringpop listener before membership bootstrap, waits for initial membership refresh, then calls `mark_started` before serving RPCs. `matching_membership_lifecycle.rs::start` treats provider start, subscription and first snapshot as barriers.
- Graceful stop path: Go `Service.Stop` evicts, sleeps the live `ShutdownDrainDuration`, closes the stop channel, stops engine and base resource. Rust's launcher withdraws first; `MatchingRuntime::drain` applies the dynamic drain duration, then begins shutdown and stops new admission; shutdown stops executor/spectator, refresh and cleanup tasks, registry/managers, and joins writers. `stop_after_drain` then stops provider refresh and Ringpop. This ordering is consistent with the source audit in `matching-v6-lifecycle-persistence-audit-20261027.md`.
- Ownership/fencing: Go manager lease updates and conditional delete use range ID. Rust task stores use backend conditional writes/deletes (SQL row lock/range conditions; Cassandra LWT) as mapped in the lifecycle persistence audit. Ownership is checked at admission as well as cached-owner operations; the native shard-distributor error path maps not-found to non-ownership and heartbeat failures to Internal.
- Failure propagation: Rust writer/reader fatal sinks route typed persistence/invariant failures to the launcher; launcher retains listeners during drain, joins service tasks and writer actors, then returns a fatal exit. This establishes a source path for process-level failure, not proof of crash/restart recovery.
- Dependency wiring: configured assembly requires task/semaphore persistence stores, History, domain cache, membership, quota, domain parsing, metrics, drain-duration provider and fatal channel. Launcher also installs dynamic config, discovery/Ringpop, optional shard-distributor executor/spectator, transport/TLS configuration, and store-specific startup. These are not silent no-op dependencies in configured production assembly.

### Test evidence missing

- Successful Go Frontend-to-Rust Matching service discovery and RPC; a bound socket is insufficient evidence.
- Actual task/semaphore persistence recovery after graceful restart, SIGKILL, owner takeover and idle recovery on each required backend/topology.
- Configured production startup and stop with Go Frontend, History and Worker active and no Go Matching.
- Candidate-specific Clippy/test evidence and final artifacts. At the latest checkpoint, `rust-ci` run 41030554541 was active and parent-owned; this review did not inspect or alter it.
- The 13 ignored parity cases in the checkpoint remain missing targeted evidence, especially the eight `errIfShardOwnershipLost` branches, manager failure propagation, callback catch-up/selective release, all config keys, typed error mapping and injected Thrift handler failures.
- The 17 dependency-gated ignored cases remain uncredited until their specific opted-in oracle/gate artifacts exist. In particular, PostgreSQL TLS, MySQL socket timeout/cancellation cases and direct generated Go History peer coverage are not implied by generic SDK success.

### Known failing runtime outcome; cause unresolved

The prior PostgreSQL mixed run 40808511931 (old frozen SHA
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`) failed before SDK and recovery
scenarios. It recorded Rust gRPC/TChannel listeners bound, but Go Frontend
`DescribeTaskList` could not route to Matching; retained Go peers added then
removed the only Rust Matching Ringpop member and the ring became empty. The
postmortem `matching-native-postgres-discovery-diagnosis-20261030.md` concludes
that membership reachability eviction is the observed failure, but does not
identify whether Go PINGs failed to reach the Rust listener or Rust mishandled
PING/response. Candidate changes add a bounded inbound PING log and an
independent direct-primary probe; these are diagnostics, not a repair or
acceptance pass.

Treat this as a known failed mixed outcome and an unresolved service-discovery
blocker, not as a proven source-level Ringpop defect. The acceptance lane cannot
proceed to SDK/workflow/recovery assertions while Go's ring excludes Rust
Matching.

### Unknowns not settled by source inspection

- Which exact network event makes Go SWIM mark the Rust Matching node faulty.
- Whether each listener's bind is externally reachable from all Go peer
  containers/addresses in the current CI topology, and whether its PING response
  matches the peer's expectations.
- Whether every configuration key is read in the Rust production path; the
  66-key checkpoint inventory is the authoritative unclosed mapping.
- Whether persistence behavior matches during real process failure/restart,
  particularly each ignored MySQL timeout/cancellation, PostgreSQL TLS and
  Cassandra interoperability scenario.

## Acceptance workflow pin and evidence review

`matching-production-validation.yml` uses `ubuntu-latest`, a per-run
`/tmp/cargo-target-final-${{ github.run_id }}`, builds `cadence-matching`,
records `out/commit.txt`, records `out/binary.sha256`, records `build.exit` and
`gate.exit`, and retains the gate's nested run/probe results. It records the
commit before build and again before the mixed command. For acceptance, compare
Actions `headSha`, artifact `commit.txt`, nested run source identity and the
frozen ref SHA; require all to equal the final full SHA. Inspect the binary SHA
alongside the actual runtime results, topology and cleanup. A green workflow or
requested lane flags alone is not sufficient.

The common acceptance plan remains ten serial mixed lanes: PostgreSQL,
MySQL probes, Thrift/TChannel, configstore, real shard-distributor, two-node
MySQL, Cassandra, adaptive two-node, active-offer, and production TLS. The
active-offer and Cassandra/TLS lanes use their dedicated workflows. Current
checkpoint says dispatch is blocked until run 41030554541 reaches terminal
success with exact SHA, fresh-target evidence, Clippy then workspace tests,
and uploaded exits/artifacts. Parent owns CI; no dispatch or observation was
made here.

## Prioritized next actions

1. After the parent reviews the active workspace CI artifact, require exact
   frozen SHA, fresh target, `clippy.exit=0` followed by `test.exit=0`, and the
   new failure regressions present in the test log. If any pin/artifact
   condition fails, correct that first.
2. Run the first PostgreSQL mixed lane at that same immutable SHA and review
   direct-primary status, Go Frontend routing error, bounded Ringpop PING logs,
   process exit and container cleanup. Do not count a listener bind as
   discovery success. If membership is evicted again, use the PING evidence to
   isolate peer reachability versus request/response handling, repair and
   refreeze before the ten-lane series.
3. Only after PostgreSQL readiness and SDK execution work, run the ten lanes
   serially at the same SHA. Each must have artifact identity, Rust binary
   SHA256, no-Go-Matching topology, SDK and lane-specific success, restart,
   failure/recovery and cleanup evidence. A failed lane requires a coherent
   source repair and a new same-pin validation series.
4. Close the ignored inventory and all-key config mapping against the actual
   candidate test log and retained mixed/oracle evidence. Do not claim service
   completion or start History migration until all evidence and parity items
   close.

No source code or runtime was changed or run during this audit. No local Cargo,
Go, formatter, or gate command was executed.
