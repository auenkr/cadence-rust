# fix-lb-typed-flags (2026-11-19)

Finding P1: `rust/crates/cadence-history/src/wiring/assembly/matching_lb_config.rs` read LB dynamic config via
`DynamicClient::get_value` (OpenFeature object eval; Unleash string payload "4" -> as_i64 fails -> 1).
Fix: new `LbFlags` uses `get_typed` with Go types: numTasklistRead/WritePartitions Int,
enableGetNumberOfPartitionsFromCache Bool, enablePartitionIsolationGroupAssignment Bool,
tasklistLoadBalancerStrategy String. Test `lb_config_uses_typed_getters` with a string-payload client.
Other get_value additions since d1dba9370 under rust/: only this file (+ an in-memory client test) — none else.

Head 796365c433dd3c60971a8ba9cdc4bb79eba9c28f
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/44427922163 success
- history-mixed-gate mysql two_history=true (grpc, workflow has no transport input): https://github.com/auenkr/cadence-rust/actions/runs/44427956823 success, `GATE SUMMARY failed: []`
- binary: 1bff6bf73a21fec6717e0e3e1478b5f6587deb32542a386c573b5b09f16d4d62  /tmp/cadence-history-gate-44427956823-1/debug/cadence-server
