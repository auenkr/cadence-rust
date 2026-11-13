# history-ttl (2026-11-13)

## Root cause
`matching add task: missing TTL` is the yarpc validator error (`ValidateRequestContext`, yarpc v1.55.0 api/transport/request.go) from Go Matching when the inbound request has no deadline. Rust History's `add_activity_task`/`add_decision_task` (rust/crates/cadence-history/src/wiring/assembly/queue_deps.rs) built tonic requests with no timeout, so no `grpc-timeout` header was sent. Go always calls matching with a context deadline (taskRPCCallTimeout = 2s). The tokio timeout around the call was only local. The string is wrapped by `status_err`.

## Fix
`matching_metadata()` now calls `request.set_timeout(TASK_RPC_CALL_TIMEOUT)` (2s). Unit test `matching_metadata_sets_grpc_timeout_deadline`.

## Evidence
- rust-ci (cadence-history) 43442967555: success (first run 43442184287 failed clippy: items after test module, fixed).
- Two-cluster active gate (two_cluster, rust_history, side=active) 43444341291: success, `GATE SUMMARY failed: []`, 0 occurrences of `missing TTL`.
- 12 history-mixed-gate combos at head 0c74f1ab16c3b1b3895e4a2b6d46a91c68b00e0f (after merging origin/master): 43445749560 43445766103 43445778797 43445795558 43445810322 43445823885 43445837343 43445851414 43445864495 43445878903 43445893649 43445908681: all success, each log `GATE SUMMARY failed: []`, 0 `missing TTL`.
