# History handler gap port (branch history-gap-handler2)

Done: `handler/handler_impl.rs` (`HandlerImpl`) ports handler.go lifecycle (start/stop/prepare_to_stop 5s delay/health)
and the engine-routed RPCs with Go validation order (shutdown check, domain, host rate limit, task-token
deserialize+validate, shard-registry engine lookup). `wiring/mod.rs` now compiles persistence/resources/assembly.

Explicit dependency gaps (return an internal error naming the gap, not silent stubs):
- StartWorkflowExecution / SignalWithStartWorkflowExecution (+ retry on AlreadyStarted/CurrentWorkflowConditionFailed): `HistoryEngine` has no start/signal-with-start.
- GetReplicationMessages (token fan-out, sort, size cap), NotifyFailoverMarkers, GetFailoverInfo (coordinator not exposed to handler).
- RemoveTask, ReapplyEvents (DataBlob event decoding), RatelimitUpdate (aggregator), cross-cluster RPCs (deprecated queue).
- DescribeHistoryHost returns shard ids only (no domain cache stats / controller status).
- Not yet done: `lookup`, `service.go` port, real `main.rs` startup, grpc/thrift wrapper audit.
- Enum fields from persistence ints (parent_close_policy, task_list_kind, cron_overlap_policy, last_failure_category) are mapped to None in engine_state.rs (conversion not wired).

## CI evidence
- rust-ci package=cadence-history, head b3d6de0fe: SUCCESS https://github.com/auenkr/cadence-rust/actions/runs/42220933092
