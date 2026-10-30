# Matching production parity audit — 2026-10-30

## Scope and result

Audited source at `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`, the exact head
of `origin/t3/matching-grpc-connection-diagnostic-565c8fc`; its parent is
`565c8fc91893fae1a81cd8e47f10f2ee4744c046`. This is a source and evidence
review. No Cargo, Go, formatting, or runtime command was run locally, and no
source behavior was changed.

The prior audit ledger overstated three test gaps: public query dispatch
failure, direct Refresh RPC failure, and actual registered failover callback
coverage all have active tests at this head. The earlier OpenFeature startup
rejection and Matching-only persistence-error-key claims are also stale. See
`matching-current-ignore-reconciliation-20261027.md` for the test-gap
corrections and `matching-final-parity-20261024.md` for the older claims that
must not be carried forward.

This audit did not prove a new source-level Go/Rust behavior mismatch in the
reviewed startup, lifecycle, ownership, dispatch/query, failover, quotas,
configuration, or RPC-error paths. Matching parity is still not accepted:
the current mixed PostgreSQL gate failed before any SDK workflow or restart/
recovery assertions, and its logs show a concrete Go/Rust Ringpop membership
retention failure described below. The evidence is not enough to identify the
precise SWIM message/timer defect, so no speculative runtime change was made.

## Go-to-Rust production behavior map

| Behavior | Go production path | Rust production path and current regression coverage | Audit disposition |
|---|---|---|---|
| Startup and registration | `go/service/matching/service.go` `Service.Start` creates the engine/handlers, registers Thrift and gRPC, then starts the base resource and handler. | `rust/crates/cadence-matching/src/launcher.rs` binds the listeners, assembles configured stores/runtime, starts Ringpop and membership refresh, and opens admission only after startup; `matching_membership_lifecycle.rs` owns provider startup cleanup. `matching_launch_native.rs` has configured startup/admission tests; `matching_rpc_tests.rs::matching_grpc_listener_registers_meta_health_and_gates_startup` covers gRPC registration/startup gating. | Source has an explicit startup sequence and regressions. Mixed production startup/discovery did not remain stable in run 41094318642. |
| Drain and shutdown | `Service.Stop` evicts itself, waits `ShutdownDrainDuration`, closes the service loop, stops handler/engine and base resource. `handler/engine.go` stops executor, task managers, semaphores, and failover callback. | `matching_runtime.rs::begin_shutdown` / `shutdown` and launcher shutdown evict membership, read the live drain duration, stop runtime owners, join writers, and stop listeners. Tests include `shutdown_drain_runs_membership_evictor_before_live_drain_read`, `shutdown_uses_drain_duration_updated_after_runtime_startup`, and `durable_runtime_joins_maintenance_and_flushes_on_shutdown`. | Source-level lifecycle parity has active tests. This gate's probe-failure cleanup required SIGKILL after its two-second deadline; it cannot establish graceful stop or crash/restart recovery. |
| Membership and task-list ownership | `handler/engine.go` configures shard-distributor executor or no-op/hash-ring fallback, resolves/creates task-list managers and checks executor/ring ownership before accepting work. | `matching_runtime.rs` installs shard admission and executor ownership checks; `matching_registry_native.rs` and `matching_membership_lifecycle.rs` manage membership snapshots, local ownership, leases, and provider lifecycle. Existing ownership tests include `installed_shard_ownership_check_matches_notfound_shutdown_and_transient_errors` and routing/membership suites. | Unit/source coverage exists. The terminal Go/Rust run shows the Rust Matching endpoint added to each Go service's Ringpop view, then removed around 23 seconds later; durable membership retention remains an acceptance failure. |
| Task/query dispatch and persistence | `handler/engine.go` `AddDecisionTask`, `AddActivityTask`, `QueryWorkflow`, and task-list manager calls perform sync delivery or persist tasks. Query dispatch registers a waiter, dispatches, returns forwarded responses directly, otherwise waits for worker completion/caller cancellation. | Native behavior is split across `matching_dispatch_native.rs`, `matching_registry_native.rs`, `matching_persistence_limiter.rs`, `matching_query_native.rs`, and RPC implementations. Active failure regressions include `matching_partition_cancellation_tests.rs::query_workflow_dispatch_failure_returns_error_and_cleans_pending_query`, `matching_service_native.rs::query_rpc_propagates_dispatch_forward_failure_and_releases_its_permit`, plus durable persistence/fencing and delivery tests. | Query failure cleanup is not an open gap. Existing unit assertions do not replace mixed SDK, persistence-schema, failover, or process-recovery evidence. |
| Domain failover | `handler/engine.go` `registerDomainFailoverCallback` disconnects eligible polls on global-domain failover and unregisters it at engine stop. | `matching_runtime.rs::start_domain_cache` registers prepare/changed/catch-up callbacks and releases eligible old polls while retaining task-list managers. `matching_failover_callback_tests.rs::registered_failover_callbacks_preserve_catchup_and_release_only_eligible_updates` exercises the registered callbacks; `registered_failover_callback_reaches_native_registry_attached_after_catchup` covers native registry attachment after cache catch-up. | The earlier claim that only the selector helper was covered is stale. Callback registration, catch-up, eligibility, release, and native attachment have active integration assertions. |
| Quotas and dynamic config | `service/matching/config/config.go` binds `MatchingUserRPS`, `MatchingWorkerRPS`, and domain-specific RPS; `handler/handler.go` constructs user/worker limiters and applies them per RPC (some worker response APIs count but accept even when over limit). | `matching_config.rs` reads Go `matching.rps`, `matching.workerrps`, `matching.domainrps`, `matching.domainworkerrps`, semaphore/idle and task-list properties. `launcher.rs` wires runtime config and `matching_runtime.rs::live_shutdown_drain_duration` reads shutdown config at call time. | No reviewed key or limiter mismatch was proven. The exhaustive Go RPC-by-RPC quota/throttle parity matrix remains a bounded audit task; do not infer it from config readers alone. |
| Dynamic-config providers | Go's server plugin registration is in `go/common/dynamicconfig/dynamicconfigfx/fx.go`; providers include file, config store, and OpenFeature plugin paths. | `launcher.rs` handles `DynamicConfigProvider::OpenFeature` at the current provider match (around line 647), registers the Unleash plugin if needed, registers the configured provider, and installs the OpenFeature runtime provider. | The 2026-10-24 assertion that OpenFeature is rejected at startup is obsolete. Do not implement the suggested rejection workaround. |
| RPC and transport errors | Go handler calls `handleErr`; `handler/engine.go` returns query dispatch and refresh manager errors; generated Thrift wrappers map declared errors and defaults. | `grpc_matching.rs`, `matching_service_native.rs`, `thrift_matching.rs`, and native TChannel dispatch own wire mapping. `refresh_rpc_persistence_failure_matches_go_and_observes_expired_caller` checks Internal vs typed ResourceExhausted, message/details, unchanged cache, and manager retention; `matching_service_native.rs::refresh_rpc_returns_storage_failure_without_replacing_native_cache_or_owner` covers native storage failure/cache/owner reuse. | Direct Refresh RPC failure propagation is covered; older note that only cancellation was tested is stale. Full cross-RPC exhaustive typed-error/Thrift default matrices are not claimed by this audit. |

## Terminal mixed-gate review

Run [41094318642](https://github.com/auenkr/cadence-rust/actions/runs/41094318642)
is terminal `failure`, exact head `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`.
The `Matching production validation` job successfully built the fresh binary
(`build.exit=0`), then the mixed gate, lane, and nested run all exited 1. The
binary SHA256 recorded by the workflow is
`618ae960e2f0f8c68d3c6643351936343fad480c4e1d0d8e27afb660746a8605`.
Topology was Go Frontend/History/Worker with Rust Matching and no Go Matching;
PostgreSQL was selected. Go services remained alive after the failed probe.
`results.json` has `acceptanceEligible:false` and no SDK result.

The failure sequence is material:

1. Rust bound gRPC `127.0.0.1:27161` and TChannel `127.0.0.1:22438`, joined
   three Go Ringpop seed addresses, completed startup, and opened admission.
2. Go History, Worker, and Frontend each logged a Ringpop `HostsAdded` event
   for `127.0.0.1:22438`; the Frontend's refreshed hash ring explicitly lists
   the Matching member with `grpc:27161`.
3. At 17:01:37Z, about 23 seconds after addition, all three Go services logged
   `HostsRemoved` for that Rust Matching address. The Frontend then could not
   complete the compatibility readiness call (`Call cancelled`).
4. The final direct probe's TCP connect succeeded in 1 ms, but grpc-js remained
   in connectivity state 1 (`CONNECTING`) and timed out with “Waiting for LB
   pick.” Rust's log shows earlier `ListTaskListPartitions` requests reaching
   `engine-complete`; its final request stops after `engine-begin`. This points
   to a real mixed-runtime readiness/membership failure, but the retained trace
   does not prove whether missed SWIM probes, TChannel reply encoding, channel
   state, or another runtime cause triggered removal. No single root cause is
   asserted.
5. The SDK, queued-activity recovery, restart, and recovery assertions never
   ran. Following probe failure, the gate sent SIGTERM but the Rust process did
   not exit before the two-second deadline and was SIGKILLed. This artifact
   cannot be credited as graceful shutdown evidence.

Evidence captured from the one artifact download is in
`memory/evidence/matching-gate-41094318642/`: exact run exits/source/binary
identity, result and probe JSON, readiness diagnostic, shutdown record, Rust
startup/SWIM excerpt, and Go Ringpop add/remove excerpt. The cleanup artifact
records the PostgreSQL container id/name but not a successful removal result;
there is no cleanup-success assertion to credit. The hosted job has ended.

## Next acceptance work

Do not dispatch another mixed run from this audit. First isolate and fix the
Ringpop membership-retention failure using the saved Go add/remove timeline and
Rust SWIM trace. Any proven source fix needs a fresh uncached, package-unset CI
run with Clippy followed by workspace tests before mixed validation. Then rerun
the serial mixed PostgreSQL lane and inspect every artifact before advancing to
the next lane. Matching remains incomplete; no History migration is authorized
by this audit.

No `gh run` was dispatched for this audit. Gate 41094318642 was inspected once
after becoming terminal. No local Cargo, Go, build, format, test, or runtime
validation was run.
