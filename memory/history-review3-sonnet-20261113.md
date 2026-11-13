# History review 3 (Sonnet 5.5, independent) — 2026-11-13 merges

Scope: Rust diffs of history-failcoord, history-ttl, history-standbyrepl, history-review2fix, history-soakfix,
history-s3gate, history-swap, history-esvis (`git diff <merge>^1 <merge>^2 -- rust/`), compared with Go. Static review only
(no cargo/go/docker run). Only items checked against Go source are listed.

Checked and found equivalent (no finding): failover marker `creation_time` persist/read (shard_events.rs, replication.rs);
`is_current_workflow_guaranteed` uses `state_in_db` (Go mutable_state_builder.go:794); Passive close skips version-history
advance (Go `updateWithLastWriteEvent`, mutable_state_builder.go:1855); `SetHistoryTree` after started event (Go
state_builder.go:168) and `SetHistoryBuilder` (state_builder.go:566); 0x59 preamble on codec blobs (Go
common/codec/version0_thriftrw.go:50) while serializer blobs stay unversioned (Go serialization/thrift_encoder.go:103, Rust still
correct to use plain `encode` for tasks/activity infos); timer `schedule_decision` guard (Go mutable_state_util.go:110);
Sarama FNV-1a partitioner, key=workflowID (Go producer_impl.go:104); ES message field set (Go es_visibility_store.go:818-940).

## P1

### P1-1 Rust-written history events carry millisecond timestamps in a nanosecond wire field; merges paper over it with heuristics
- Rust: `rust/crates/cadence-history/src/wiring/assembly/engine_events.rs:101` (`timestamp: Some(now_ms)`), `:329`
  (`now / 1_000_000`); written raw by `rust/crates/cadence-thrift/src/convert.rs:1512` (file comment at `:40`: IDL timestamps are
  Unix nanos). Heuristic patches: `cadence-common/src/cloud_archiver/write.rs` (~235, `ms.abs() < 1e14` ⇒ ×1e6),
  `cadence-history/src/wiring/assembly/queue_deps.rs` `archive_nanos`, `wiring/persistence/es_visibility.rs` `nanos()`.
- Go: `go/service/history/task/transfer_task_executor_base.go:252,428-430` pass `startEvent.GetTimestamp()` etc. straight through as
  UnixNano; HistoryEvent.Timestamp is nanos everywhere.
- Scenario: events written by Rust History (activity started etc.) are read by a Go service/CLI/archival reader and show 1970
  dates (ms interpreted as ns). In the visibility paths the units are guessed per value:
  `scheduled_execution_timestamp = start_event.timestamp(ms) + backoff*1e9` (transfer_active_task_executor.rs:363-364) is
  ~1.8e12+1e10 < 1e14, so `nanos()` multiplies by 1e6 and delay-start/cron workflows get a wrong ScheduledExecutionTime
  in ES (and the S3 index timestamps). A genuine ns value below 1e14 (before 1970-01-02) is also mis-scaled.
- Fix: make Rust events nanos at creation (push_event takes ns), or convert once at the persistence boundary; then delete the three
  heuristics. Needs a check of which Rust producers feed `StartEvent.timestamp` (queue_deps_transfer_active.rs:389,
  queue_deps_standby.rs:279).
  (Verification note: I confirmed the Rust write side; I did not find any ms→ns conversion between push_event and
  convert.rs:1512.)

## P2

### P2-1 Transfer-active `schedule_decision` lacks Go's `HasPendingDecision` guard (same bug soakfix fixed for the timer path)
- Rust: `rust/crates/cadence-history/src/wiring/assembly/queue_deps_transfer_active.rs:369-371` (calls
  `add_decision_task_scheduled_event` unguarded); caller `task/transfer_active_task_executor.rs:777`.
- Go: `execution.ScheduleDecision` (`go/service/history/execution/mutable_state_util.go:110-116`), used at
  `go/service/history/task/transfer_active_task_executor.go:1527`.
- Scenario: a transfer task whose `update_workflow_execution(create_decision_task=true)` runs while a decision is already pending
  gets an error from the double-schedule (the soakfix test `double_schedule_errors...` documents it) and the task retries
  forever instead of being a no-op.
- Fix: add `if self.state.has_pending_decision() { return Ok(()) }` (or route both executors through one shared helper);
  re-check `ndc_workflow.rs:249-251` `ms.schedule_decision` for the same guard.

## P3

### P3-1 Kafka producer lacks Go/sarama retry and fails lazily
- Rust: `wiring/persistence/es_visibility.rs` `KafkaProducer::send/connect` (single `produce`, connection on first publish; TLS
  rejected in `visibility_wiring.rs`). Go: sync sarama producer built at startup (`common/messaging/kafka`), default
  `Producer.Retry.Max=3`.
- Scenario: a transient broker leader change fails the visibility task once more often than Go; a mis-set topic is discovered at
  the first workflow instead of at start-up. Fix: a small bounded retry on publish and an eager `connect()` in
  `build_visibility_writer`; document TLS gap.

### P3-2 `matching_metadata` deadline covers only the RPC, not Go's whole task-op context
- Rust: `queue_deps.rs:160-170` (2s on add_activity/add_decision). Go: `transfer_task_executor_base.go:53,104,155`
  (`taskRPCCallTimeout` wraps the call incl. client-side routing). Rust also wraps in `tokio::time::timeout` at
  `transfer_task_executor_base.rs:109,112`, so behavior matches closely; only note: other History outbound gRPC callers outside
  this diff (`parent_close_policy_client.rs:88` has it; the remote-History forwarding in `queue_deps_transfer_active.rs`
  I did not verify) should be audited for the missing-TTL yarpc rejection.
