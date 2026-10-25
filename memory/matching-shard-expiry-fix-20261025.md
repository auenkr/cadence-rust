# Matching shard-distributor executor expiry investigation (2026-10-25)

## Evidence

- Actions runs [40198127804](https://github.com/auenkr/cadence-rust/actions/runs/40198127804) and [40205093375](https://github.com/auenkr/cadence-rust/actions/runs/40205093375) fail in the `shard-distributor` lane at `sdAssertExecutorExpired` in `tests/matching-service-compat.cjs` (90 second deadline). The assertion polls the real shard-manager control API `stats` and `assignments`; a killed executor still present in either means it has not expired / released ownership.
- The failed job log shows the Go baseline probe raises `RuntimeError` and uploads `matching-production-shard-distributor`; this checkout could not retrieve the artifact through `gh run download`, so per-probe JSON/log details were not available locally.
- Rust Matching connects to an external real Go shard-manager in this lane. The Rust-side `grpc_shard.rs` is the separate in-process ShardDistributor implementation and does not drive the real manager leader loop.
- Rust's Matching shard-distributor configuration carries `peer_ttl` separately from each namespace's `ttl_report` (`rust/crates/cadence-service-core/src/config.rs`). The executor expiry observed by this test belongs to Go shard-manager's leader / peer TTL behavior, not a Rust matching expiration loop.

## Conclusion

No code change is justified from the available evidence. In particular, changing the Rust in-process adapter cannot make a killed executor disappear from the real Go shard-manager's stats/assignment state. The actual lane config, Go control implementation, and uploaded probe log must be consulted to identify whether the real leader is stalled or the test's effective peer TTL exceeds its deadline. Do not weaken the assertion or alter Go behavior. Parent should retrieve artifact `matching-production-shard-distributor` (run 40205093375 artifact URL is in the Actions job log) and inspect `out/gate/run/probe.log`, config and etcd leader state before assigning a fix.

## Validation

No source changes; no Cargo/Go/format commands run. No CI dispatched.
