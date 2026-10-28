# Matching native runtime lifecycle integration (2026-10-28)

## Changes made

- `rust/crates/cadence-matching/src/matching_runtime.rs:124-132` adds runtime-owned native registry callback state and a joined native membership watcher. Both `MatchingRuntime` struct literal constructors initialize these fields at `:159-219`.
- `with_native_queues` at `:231-254` publishes the registry into the callback state and starts membership cleanup if membership was already attached. The existing duplicate durable queue guard remains the rejection point.
- `with_membership` and `start_native_membership_maintenance` at `:548-590` run `NativeQueueRegistry::unload_non_owned(matching_now())` on each membership-ring update; this complements the existing matching handler watcher, which continues legacy queue and semaphore cleanup.
- The cache callback in `start_domain_cache` at `:626-699` still uses the single `matching` callback registration. After its catch-up baseline is established, global non-active-active failovers release polls from both the legacy registry and any subsequently attached native registry. The shared slot means no second callback observer or registration leak is introduced. Native callback execution is spawned through the cache's Tokio runtime handle.
- Shutdown at `:776-875` stops/joins native membership maintenance, then shuts down the selected queue registry and waits for native writer actors before returning. `Drop` signals and aborts the watcher as the abandoned-owner leak guard.

## Native host integration contract

Terra owns `matching_host_native.rs` and `matching_service_native.rs`; they were not edited here. The launcher should construct the native host/service first, pass its `Arc<dyn cadence_proto::native::NativeMatchingApi>` and the real `NativeQueueRegistry` to `MatchingRuntime::with_native_queues`, then attach membership and start RPC listeners. Membership ordering is flexible. The launcher must keep using `MatchingRuntime::drain()` while listeners remain live, stop listeners after drain, and call/await `shutdown()` on success and failure paths. Runtime shutdown owns native queue final acknowledgement, stop, and writer joins; host worker must not duplicate registry shutdown.

The current host worker API needed by this runtime is therefore: a constructor returning the native service trait object plus its actual registry (not a mock/no-op service), and a lifecycle path that invokes the runtime's drain and shutdown methods before dropping that service. Confirm exact constructor names/signatures against Terra's files during integration.

## Validation status

- No local Cargo, rustfmt, Go, build, or test command was run, per task and CI ownership instructions.
- Source-level verification only: the native APIs called here exist at `matching_registry_native.rs:833` (`release_domain_failover_polls`), `:985` (`unload_non_owned`), `:1946` (`maintain_until`); cache callback registration is replacement-by-id at `matching_domain_cache.rs:676-707`; native writer joining exists at `matching_registry_native.rs` and is already used in runtime shutdown.
- Bounded source tests were added at `matching_runtime.rs` in the runtime test module: `durable_queue_ownership_guard_rejects_either_existing_registry` exercises all ownership combinations, and `native_registry_maintenance_stops_and_joins_on_shutdown_signal` uses a real SQLite-backed native registry with no synthetic service fallback. They have not been run; CI compile and execution remain unverified.

## Parent review

Added the initial native membership sweep before waiting for ring changes, matching the legacy watcher. Changed native membership join errors to accumulate until after registry shutdown/writer joins instead of returning early and skipping queue cleanup. Strengthened the maintenance regression to start a real raw-byte-named queue owner, signal maintenance shutdown, assert the queue stopped and bound the writer-tracker join. The old empty-registry-only characterization above no longer describes the final test. No local validation ran; these changes postdate CI 40719459374 and require another gate after remaining worker integration.
