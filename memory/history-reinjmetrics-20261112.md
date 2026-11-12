# history-reinjmetrics (2026-11-12)

## Ported
- ShardTaskReinjector (shard_factory.rs): Go `ReinjectHistoryTasks` write path via `GoExecutionStore::create_history_tasks`
  (transfer (1,1) + timer (2,2) categories, id allocation under `lock_task_writes`, update_max_read_level,
  ShardOwnershipLost -> close_shard). Executions handle wired in queue_provider.rs and engine_factory.rs.
  SQL stays "not implemented" (store returns the error, as Go). Deviation: no EnableTimerQueueV2/active-active timer-cluster lookup.
- Shard controller metrics/logs (wiring/assembly/controller.rs, scope HistoryShardController): GetEngineForShard latency/error,
  NumShardsGauge, ShardItemCreated/Removed/Closed, ShardItemAcquisitionLatency, RemoveEngineForShardLatency,
  MembershipChanged, AcquireShards counter+latency, create/remove/membership logs.
- Handler RPC metrics (handler/metrics_handler.rs, wired in service.rs inside the rate-limit wrapper): per-RPC
  CadenceRequests, CadenceLatency, CadenceLatencyHistogram and Go `updateErrorMetric` counters + Internal/Uncategorized error logs.

## Skipped / not done
- Handler-specific counters EmptyCompletionDecisionsCounter, CadenceErrNonDeterministicCounter, ReplicationMessageTooLargePerShard.
- Context-tag (GetContextTags) on handler scopes; yarpc-code mapping (no yarpc in Rust).
- Queue processors/task executors already carry metrics (task/, queue/, queuev2/ ~25 files); no new gap list was built.
- ratelimit_aggregator metrics remain P2.

## CI evidence
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43177086953 (success, handler metrics head)
- Gate lanes: see below.
