# Native Matching control context — 2026-10-28

## Scope implemented

Added `NativeMatchingControlContext` in
`rust/crates/cadence-matching/src/matching_control_context_native.rs`. It is a
constructible production adapter for the existing
`matching_engine_control_native::NativeControlContext` trait. Its constructor
requires the actual `AppState`, `Arc<MatchingDomainCache>`, native queue
registry, live `Membership`, and optional `Arc<MatchingShardRouter>`.

The adapter resolves domain IDs and names through the cache, uses Membership's
raw task-list ownership check, and reads all control policy from the shared
dynamic-config overlay. Task-list filters use byte APIs, including scaler
enablement, poller window and write partition count. `ListTaskListPartitions`
owner lookup uses the shard router's byte-preserving resolver when available;
otherwise it uses Membership's raw-byte owner lookup. Returned host names use
the matching `Host.address` / `ResolvedHost.address` value, matching the
Go-style host address rather than the gRPC endpoint.

Focused regression source was added for raw owner routing keys, invalid-byte
dynamic-config filter distinction, and missing cache domain lookups.

## Limits and validation

This contribution does not register the module or attach the context to the
production launcher; those are owned by the parent task. It does not implement
poll/add/query contexts. No Cargo, Rust, Go, formatting, build, or test command
was run, per task constraints. The regression tests are source changes only;
they are not reported as executed. Only `git diff --check` is permitted here.

Parent correction: imported the trait through `matching_engine_native`, registered the module and runner formatting, and changed loaded-queue checks to the registry ownership policy with cached=true. Always checking the ring again was incorrect for shard-distributor ownership and Go's cached excluded-list fast path. Removed the unused retained AppState field. Execution validation remains pending.
