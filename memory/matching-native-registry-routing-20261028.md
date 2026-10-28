# Native Matching registry routing attachment — 2026-10-28

Implemented source, not locally built or tested (per host policy):

- `rust/crates/cadence-matching/src/matching_registry_native.rs`: `with_live_routing` attaches `Membership::native_membership`, optional `MatchingShardRouter`, a required local service builder, and a required partition refresher. Owner resolution is byte-preserving via `remote_channel_bytes`; a shard-router error stops forwarding as in legacy `resolve_parent_dispatcher`, and a local owner invokes the required local builder. There is no implicit local dispatcher fallback in this attachment.
- `NativeLocalPollHost` keeps a dummy detached registry plus `Weak<Mutex<State>>`; every local parent call upgrades the live registry before invoking the builder. The callback type is `Fn(NativeQueueRegistry) -> Arc<dyn NativeMatchingApi> + Send + Sync`, called per forwarded request. It must construct a service from that registry and must not capture a service or registry that creates an owner-resolver cycle.
- The registry's existing `notify_read_partitions` builds raw `/__cadence_sys/{root}/{partition}` byte names, fans out concurrently, keeps the caller deadline, logs refresh failures and does not fail the triggering operation. The attachment requires the host's generated-client `NativePartitionRefresher`; it must log and return `Ok(())` for owner lookup failures and propagate RPC errors. Native registry itself logs callback errors, matching legacy caller behavior.
- `rust/crates/cadence-matching/src/matching_registry_routing_native.rs` exposes the routing callback types and documents the byte-preserving host contract. Parent must register this module in `lib.rs` if desired; the implementation method is available directly on `NativeQueueRegistry`.

Legacy pointers for parity review: `matching_registry.rs::resolve_parent_dispatcher` (around line 2610), `matching_registry.rs::notify_read_partitions` (around line 2211), and `grpc_matching.rs::bind_local_poll_service` (around line 1199). Native pointers: `matching_registry_native.rs::with_live_routing` (around line 671), `resolve_parent_dispatcher` (around line 2190), and `notify_read_partitions` (around line 1880).

Remaining integration and verification gaps:

1. Host/launcher must call `with_live_routing` with a per-call builder that constructs the native Matching service from the supplied registry without capturing the full service in the builder closure.
2. Host/launcher must provide a generated-client refresher that resolves each raw task-list owner, converts `NativePartitionRefresh` to the generated request without lossy task-list conversion, preserves the deadline, logs owner lookup failure as success, and propagates only RPC errors.
3. Add bounded tests for raw forwarded parent names, raw partition refresh name/config/deadline, and registry teardown while a weak local host remains. No test/build/check/format command was run here.
4. `matching_registry_routing_native.rs` is not registered in `lib.rs` by this task; its contract currently has no independent public module path until parent assembly registers it.

## Parent checkpoint follow-up

Registered matching_registry_routing_native in lib.rs and in the CI rustfmt file list. Source review found with_live_routing used an undefined `state` field initializer; replaced it with Arc::downgrade(&self.state), preserving weak ownership. git diff --check passed; compilation and tests remain pending. CI 40724071697 predates this correction; inspect its logs and run a new gate after integrating any additional fixes. Concrete host builder/refresher and production attachment are still required.
