# Native Matching scaler and isolation runtime adapter (2026-10-28)

This work adds a production native isolation provider attachment in
`matching_runtime_native::native_queue_registry`. It samples
`system.enableTasklistIsolation` and `system.allIsolationGroups`, scopes the
switch through the domain name, retains GoString group bytes, and enables the
groups only for non-sticky task lists. The selector retains the requested
`isolation-group` bytes. The unit test explicitly distinguishes invalid `ff`
from the UTF-8 replacement character.

The native registry exposes `with_scaler_starter`, not a scaler config builder.
This change does **not** attach a scaler starter. The prior `matching_scaler_native`
implementation remains an algorithm and owner-scoped update loop, but production
startup is still absent. There is no completed dynamic scaler config provider,
native Matching DescribeTaskList remote callback, YARPC header/deadline adapter,
or production scaler metrics sink here. Do not claim scaler parity based on this
adapter. The request for sampled-config coverage is also still open.

Launcher integration target: the launcher should pass the `NativeQueueRegistry`
returned by `native_queue_registry(...)` through an additive scaler attachment
helper once that helper accepts the live Matching membership/shard router,
dynamic config state, and a non-noop metrics sink. It must install the result
with `NativeQueueRegistry::with_scaler_starter(...)`. The remote DescribeTaskList
must route the partition name via the matching membership owner resolver, call
the native `MatchingApiClient`, and preserve the request YARPC headers and
deadline. The current `native_queue_registry` signature has no membership
argument, so a correct remote callback cannot be constructed there alone.

No Cargo, rustfmt, Go, build, test, CI, commit, or push command was run. Existing
CI run 40719459374 was pending on an earlier source revision before lifecycle
follow-up d6b008e24; it does not validate this change. Remaining review should
check compile/type compatibility, native isolation selector behavior (drained
groups, poller window and partition assignment), sampled dynamic scaler config,
scaler callback routing/headers/deadline, metrics, and mixed-cluster recovery.
