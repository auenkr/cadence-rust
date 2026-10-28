# Native Matching engine context adapter (2026-10-28)

Status: implementation added, **unverified**. No local Cargo, Go, rustfmt, build,
test, check, or CI command was run, per task constraints.

Added `rust/crates/cadence-matching/src/matching_engine_context_native.rs` with
`NativeMatchingEngineContext` implementing `DecisionAddContext`,
`NativePollContext`, and `NativeQueryContext`. Its `with_control` constructor
requires the actual `AppState`, shared `Arc<MatchingDomainCache>`, concrete
control context, membership, optional shard router, and explicit poll registrar.
The observer provider is optionally injected. Poll failover subscriptions are
created before the future is returned, using `InMemoryDomainStore::subscribe_failover`.
Parent channels route with raw task-list bytes through the shard router when
present and otherwise through membership; routing errors propagate. Remote
parents use `RemoteNativeMatching`. Config uses byte-valued task-list filters,
domain names from the real cache, `forwarderMaxChildrenPerNode` default 20,
`longPollExpirationInterval` default 60 seconds, `overrideTaskListRps`, and
`enableClientAutoConfig`.

A focused raw-config test was added for invalid UTF-8 task-list keys. `git diff
--check` passed. That is the only verification performed.

Remaining work before this adapter is complete:

1. Register the module and assemble the context in the service owner, supplying
   the production poll cancellation-map guard (the callback is mandatory).
2. Verify dynamic-config property names, units, domain-key behavior, and the
   query default deadline against the matching Go implementation and the exact
   `grpc_matching` path; this implementation has not been compiled or audited.
3. Add direct tests for synchronous failover subscription readiness and raw
   membership/shard-router parent routing, including propagated routing errors.
4. Run the matching crate CI workflow, fix compiler/clippy/test failures, then
   record the workflow URL, head SHA, and conclusion here.

## Parent registration and review

Registered the context module and runner formatter entry. Checked trait signatures and byte routing methods against current source, and checked poll config keys/defaults against `grpc_matching.rs` (60-second default expiration, rate override 0, auto-config false). Removed a redundant duration conversion. Changed invalid-UTF-8 failover subscription from immediately-ready to pending: invalid domain validation must return the identifier error, not fabricate a failover event. Moved test source after implementation for clippy. These are source checks only. Configurable poll expiration overrides, query deadline policy, production admission/metrics, real poll registrar construction and launcher attachment still need host integration review.
