# Shard acquire pump parity (History)

Go: `shard/controller.go` shardManagementPump + acquireShards (~380-480).
Rust: `ShardRegistry::acquire_shards` / `shard_management_pump` (wiring/assembly/controller.rs), spawned in launcher.rs.
- Ticker every `history.acquireShardInterval` (default 1m); membership change runs on_membership_change (close unowned) then acquire.
- acquire: `history.acquireShardConcurrency` workers (min 1), lookup failure logged, owned shards created eagerly via engine_for_shard, shutdown flag stops passes; metrics AcquireShards*, NumShardsGauge.
- Lazy path (entry_for_shard) unchanged. Shutdown sets flag and aborts pump.
- No new unit test (pump needs full HistoryEnv); covered by gates.

Gated SHA 731adce535a4b8324d4acc1bddd7b1cd0978e9e1 (code commit; evidence file added after).
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/44824892251 success
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44825791232 success
- history-mixed-gate mysql: https://github.com/auenkr/cadence-rust/actions/runs/44825796291 success, failedSteps []
- history-restartgate mysql: https://github.com/auenkr/cadence-rust/actions/runs/44825800216 success, failedSteps []
