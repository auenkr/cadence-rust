# Matching shard-distributor candidate source review (2026-10-20)

Review target: branch `t3/matching-shard-distributor-candidate`, commits `8569a9f88` and `9a791267a`, based on accepted root `c9c1f4f5b`. This is source-only review. No build or tests were run because the shared Cargo cache lane was not released. Runtime/config integration remains owned elsewhere and was intentionally excluded.

## Source findings

- `TonicSpectator::get_shard_owner` caps its 10-second per-RPC ceiling by the caller's remaining deadline. Go Matching's resolver passes a 1-second context (`go/common/membership/sharddistributorresolver.go:136-140`), so this layer preserves the effective 1-second resolver bound.
- Go resolver metadata mapping is still an integration requirement: `hostIP` is mandatory; `tchannel` and `grpc` parse as unsigned 16-bit decimal values; absent/invalid ports are omitted, and the host address uses TChannel port or zero. Executor ID is the host identity.
- `ShardDistributorMatchingConfig::report_ttl_nanos()` provides Go's one-minute default, but `ShardDistributorExecutor` does not consume report TTL; its `ttl_shard` is for idle shard status. The Matching processor factory must use the report TTL when integrated.
- Executor stop waits for processor stop tasks before its DRAINING heartbeat and joins background lifecycle tasks. Assignment application is serialized against heartbeat ticks; processor callbacks remain caller-owned by design.
- Candidate spectator clears the full owner snapshot when onboarding disables and on stream reconnect/termination before retry; initial state is awaited before lookup and cache misses use unary RPC. This is broadly consistent with the resolver's onboarding-gated, namespace-specific spectator contract. Retry/watch timing parity is not established by source comparison alone.
- Config validation enforces at most one namespace, non-empty namespace, positive heartbeat interval, and client hostPort when namespaces are set. Runtime integration must invoke validation and supply the advertised `hostIP`, gRPC and TChannel ports.

## Prepared validation

`tests/run-shard-distributor-review-gates.sh` is a serialized focused runner. It sets `CARGO_BUILD_JOBS=1`, defaults `CARGO_TARGET_DIR` to `rust/target`, then runs the two Rust test filters, strict cadence-server Clippy, and pinned Go resolver tests. Do not invoke until the cache lane is released. It is a gate plan, not evidence of a passing result.

After those checks, mixed Go/Rust validation still needs to run with Go Frontend/History/Worker and no Go Matching, exercising assignment, resolver metadata, owner movement, failure, restart and recovery. This note does not claim service parity or completion.

## Reviewed source hashes

- `rust/crates/cadence-server/src/matching_shard_distributor.rs`: `b45e8e4e37d5f0b5536325266b190de706f39c253323d9a9cc0aea03c7b68250`
- `rust/crates/cadence-server/src/config.rs`: `54a81669c727ec0d036e388b2fc31cb151dde8a979b4592de8f7f7f07eb3465f`
- `tests/run-shard-distributor-review-gates.sh`: `2a14a2fc1c385d5908b4eb6fbb91f648b121f3f58aa3db97025fee4dbe87af59`
