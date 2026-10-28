# Matching session handoff — 2026-10-28

## Completion state

Matching is NOT complete. Continue Matching only; do not begin History migration. All 13 native RPC methods have source implementations, but the production launcher still uses the legacy queue registry. Native production activation, complete provider wiring, final workspace checks, mixed Go/Rust SDK workflows, failure/restart/recovery, and a full Go parity audit remain required.

## Saved state and validation

User requested saving all progress before starting a new session. This checkpoint includes previously uncommitted integration edits as explicitly UNVERIFIED WIP, including partial worker changes; it is not a claim those worker tasks finished.

- Previous CI https://github.com/auenkr/cadence-rust/actions/runs/40722232123 failed at df9d0b695de29392711c29e7f7f6b9372cd038f5 during clippy, before tests.
- Fixed native isolation selector borrowing `task` into a static closure by using its owned `selector_task`; fixed fixture GoString conversion from &String to &str.
- Fix source head aaa72c32a0493786138552c7cb049d611ae57657.
- Fresh six-crate clean-target gate https://github.com/auenkr/cadence-rust/actions/runs/40723629453 was in_progress at checkpoint. It validates aaa72c32a, NOT this later WIP checkpoint. Inspect its final conclusion and logs next.
- Local git diff --check passed. No local Cargo/rustfmt/Go/build/test commands were run.

## WIP included in this checkpoint

1. matching_service_native.rs / matching_host_native.rs / matching_launch_native.rs: real MatchingMetrics request scopes replacing narrow callbacks; tests and NativeMetricsPollObserver. Read memory/matching-native-request-metrics-20261028.md. Callback signatures changed; verify all callers on CI. Poll observer factory still requires explicit installation.
2. matching_registry_native.rs / matching_registry_routing_native.rs: with_live_routing, weak local service builder, raw parent routing. Read memory/matching-native-registry-routing-20261028.md. New routing module is NOT registered in lib.rs; host still needs a concrete acyclic local service builder and real generated-client partition refresher.
3. matching_shard_distributor.rs / matching_shard_processor.rs / matching_runtime.rs: partial raw heartbeat/executor map and native shard processor support; runtime chooses native factory when native registry attached and exposes executor handle. No completion report or raw-executor memory note received. Review these edits before relying on them. No tests have run for this WIP.

## Next integration work

- Resolve CI errors first; register new modules in lib.rs and CI formatter list as appropriate.
- Shared routing state is unresolved: NativeMatchingControlContext and NativeMatchingEngineContext still capture immutable Option<Arc<MatchingShardRouter>>. Runtime discovers/attaches router later. Use one shared live router state across control/add/query/scaler/registry forwarding so all paths observe the configured router. No parent edits to these context files were made in the resumed session.
- Wire async executor ownership miss lookup, configured shard router, parent routing, partition refresh, real poll/scaler metrics, semaphore, fatal channel, startup/drain/shutdown, and avoid duplicate legacy/native owners.
- Call actual native launcher assembly only when those behaviors are retained; existing helper is not active production adoption.
- Then run fresh dedicated-target workspace clippy followed by workspace tests, followed by mixed Go other-services with Rust Matching (no Go Matching), SDK workflows and failure/restart/recovery. Record run URL, source head, conclusion and binary SHA256. Narrow adapter tests are insufficient.

## Delegation status

User wants 3 parallel Luna workers at normal speed; earlier harder tasks were explicitly allowed Terra. Do not silently substitute another provider/model.

T3 orchestrator_capabilities and t3_thread_read returned `Authentication required` during this resumed session. No new workers were launched. Existing prior async delegates were not polled, cancelled, or replaced, and their current execution state is unknown. Recover terminal completion notifications or authenticated coordination before duplicating their file ownership:

- Raw shard executor/native processor: task suffix d0dc6a80-411e-42f4-bf95-549a45687205; owns matching_shard_distributor.rs, matching_shard_processor.rs, matching_runtime.rs.
- Native metrics: suffix 510a4d70-264c-4792-aa6e-0615b536aa00; owns matching_service_native.rs, matching_host_native.rs, matching_launch_native.rs.
- Registry routing: suffix b781f0ce-95f3-4ddd-84fb-4c5f6e903bd5; owns matching_registry_native.rs, matching_registry_routing_native.rs.

## Constraints

All Cargo including formatting and all Go validation stays on GitHub Actions. Use only standard Ubuntu public-repo Actions, timeout <=90min, small artifacts retained 3 days. Check host load/RAM before workers. Shared checkout: avoid overlapping file ownership. Save findings in memory and push master, as authorized. Do not call Matching complete until full service requirements have direct reproducible evidence.

## Superseding CI for saved WIP

After the checkpoint push, requested cancellation of older in-progress run 40723629453 and dispatched a fresh clean-target six-crate clippy/test/format gate for checkpoint a929a63e9: https://github.com/auenkr/cadence-rust/actions/runs/40724071697 . This is the run to inspect first in the new session. It includes saved metric, registry-routing and raw-executor WIP. Conclusion is pending; no completion claim. The prior fix-only run was superseded before its conclusion was available.
