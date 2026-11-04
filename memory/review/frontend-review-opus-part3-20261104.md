# Frontend review (Opus, part 3): Thrift/gRPC field mapping — 2026-11-04
Base: master 3ace896eb. Read-only review, no builds. The production Frontend gRPC listener is
`rust/crates/cadence-matching/src/grpc.rs` (spawned by launcher.rs:2256), which uses
`cadence_proto::convert` (cv). Thrift is `rust/crates/cadence-thrift/src/convert.rs`.

## Findings

### O10 — HIGH — gRPC StartWorkflowExecution `first_run_at` converted to ms, Go uses ns
- Rust: rust/crates/cadence-proto/src/convert/mod.rs:382-384 (`t.seconds * 1000 + nanos/1e6`)
- Go: go/common/types/mapper/proto/api.go:3727 (`FirstRunAtTimestamp: timeToUnixNano(t.FirstRunAt)`)
- Consumer: rust/crates/cadence-frontend/src/workflow_backend.rs:611-614 and
  cadence-api/src/history_request.rs:36-43 compare it against now in **nanoseconds**.
- Failure: SDK starts a workflow with FirstRunAt = now+1h over gRPC; Rust sends ~1.7e12 (ms)
  which is < now_nanos, so no first-run delay is applied and the cron/delayed workflow starts
  immediately; History also receives a ms value where Go stores ns.
  Same bug for SignalWithStart (grpc.rs:1156/1202 also use `cv::start`, then copy the field at 1225).
- Fix: use `nanos(v.first_run_at)` (seconds*1e9+nanos), as history_in.rs:388 and client.rs:48 already do.

### O11 — HIGH — DescribeWorkflowExecution `workflow_execution_info` timestamps use ms converter
- Rust: rust/crates/cadence-proto/src/convert/mod.rs:443 (`workflow_execution_info.map(info)`;
  `info` = `info_with_time(.., timestamp)` which treats input as **ms**, mod.rs:388-390)
- Go: go/common/types/mapper/proto/api.go:5443-5470 (`unixNanoToTime` for start/close/execution/
  scheduled_execution time). Production values from History are unix ns (the list-open/closed
  paths already use `info_nanos`, grpc.rs ~1960).
- Failure: Go/Java SDK `DescribeWorkflowExecution` over gRPC returns StartTime about 1e6x in the
  future (year ~55000+) or overflowing; CLI `workflow describe`, UI and SDK code relying on
  start/close time break.
- Fix: use `info_nanos` in `describe` (and audit every other `cv::info` call, see O13).

### O12 — HIGH — PendingActivityInfo drops 7 fields (both Thrift and gRPC)
- Rust type: rust/crates/cadence-api/src/tasks.rs:2054 (no last_heartbeat_timestamp,
  expiration_timestamp, last_worker_identity, last_failure_details, last_failure_options,
  started_worker_identity). Thrift: rust/crates/cadence-thrift/src/convert.rs:3884-3918 reads/writes
  only ids 10,20,30,40,70,80,90,110,150 and hard-codes `last_started_timestamp: None` (id 60 is never read).
  Proto: convert/mod.rs:447-461 (`last_failure` = reason only, `..Default::default()`).
- Go: go/idls/thrift/shared.thrift:1761-1778 (ids 50,60,100,120,130,135,140);
  go/common/types/mapper/proto/api.go:2033-2053 (`LastHeartbeatTime`, `LastStartedTime`,
  `ExpirationTime`, `FailureWithOptions(reason, details, options)`, `LastWorkerIdentity`,
  `StartedWorkerIdentity`).
- Failure: `cadence workflow describe` / SDK DescribeWorkflowExecution through the Rust Frontend
  never shows heartbeat time, started time, expiration, worker identity or failure details of a
  pending/retrying activity, even though Go History returns them. Breaks retry-debugging tooling
  and any client logic that reads these fields.
- Fix: add the fields to the type, map Thrift ids 50/60/100/120/130/135/140, and map proto with
  `failure(reason, details, options)` + nanosecond timestamps.

### O13 — HIGH — gRPC ListWorkflowExecutions / ScanWorkflowExecutions / CountWorkflowExecutions bypass the Frontend handler
- Rust: rust/crates/cadence-matching/src/grpc.rs:1897-1908, 2078-2101. Unlike list-open/list-closed
  (grpc.rs ~1914, which check `workflow_api.serves(..)`), these always use `self.visible()` →
  `InMemoryExecutionStore` (grpc.rs:385, 570) and convert with ms `cv::info`.
- Go: frontend handler ListWorkflowExecutions/Scan/Count go to visibility manager (ES/Pinot/DB);
  mapper api.go:5443 uses ns.
- Failure: SDK/CLI `workflow list -q ...` or `count` over gRPC against a real cluster returns empty
  or local-only results (or NotFound for the domain), with wrong timestamps; Thrift path differs from gRPC path.
- Fix: route these through `call_with_headers` / hook_operation like list-open, add the request
  converters (query, page size, next_page_token passthrough), and use `info_nanos`.

### O14 — MEDIUM — WorkflowExecutionInfo missing parentDomainId, autoResetPoints, activeClusterSelectionPolicy
- Rust: rust/crates/cadence-api/src/service_types.rs:400+ (no such fields); Thrift FromThrift for
  WorkflowExecutionInfo in cadence-thrift/src/convert.rs never reads ids 70, 110, 170;
  proto `info_with_time` (convert/mod.rs:395-428) never sets `auto_reset_points`,
  `active_cluster_selection_policy`, or parent `domain_id`.
- Go: go/idls/thrift/shared.thrift:429,436,443; mapper api.go:5459-5466
  (`FromParentExecutionInfoFields(t.ParentDomainID, ...)`, `AutoResetPoints`, `ActiveClusterSelectionPolicy`).
- Failure: CLI `cadence workflow reset --reset_type BadBinary/LastContinuedAsNew` and
  `reset-batch` read `AutoResetPoints` from DescribeWorkflowExecution; through the Rust Frontend they
  get none and fail with "no reset point". Parent domain id is lost for cross-domain children.
- Fix: add fields to the type, map Thrift 70/110/170 and proto equivalents.

### O15 — LOW — FailoverDomain `failover_timeout` silently truncated/wrapped
- Rust: rust/crates/cadence-proto/src/convert/mod.rs:817 (`d.seconds as i32`)
- Go: proto mapper uses `durationToSeconds` (int32 via checked conversion helper, same as `seconds()` here).
- Failure: an out-of-range duration wraps to a negative/garbage timeout instead of the
  conversion error used elsewhere (`seconds()`, mod.rs:34-44). Fix: use `seconds(v.failover_timeout)?`.

## Out of scope but noticed
- rust/crates/cadence-matching/src/grpc_internal.rs:1165-1169 converts child-completion
  `event_time` to **ms** while Go HistoryEvent.Timestamp is ns (History internal API; same class as the
  earlier event_time bug). Flag for the History migration.

## Checked and found consistent with Go
enum_in (0 → nil, value-1 offset, unknown → InvalidArgument); Payload/data empty↔nil; RetryPolicy
(seconds↔Duration); FailureOptions/next_retry_interval; StartWorkflowExecutionRequest (all fields
except first_run_at, O10); memo/header/search_attributes map conversion; ActiveClusters in/out;
decision/activity task responses (scheduled/started/attempt times in ns, timeouts in seconds);
PendingDecisionInfo; PendingChildExecutionInfo; WorkflowExecutionConfiguration; list-open/closed
(info_nanos, StartTimeFilter); RespondQueryTaskCompleted in/out; DescribeTaskList pollers (ns) and
TaskListStatus incl. isolation metrics; poll requests; ListTaskListPartitions; GetTaskListsByDomain;
FailoverEvent (ns); schedule spec/action/policies/state/info (ns timestamps, seconds durations);
history_in start-child attributes (first_run_at ns despite the misleading `millis` name — rename suggested);
client.rs start/signal-with-start request out (ns).
Not exhaustively re-verified at field-ID level in this pass: Admin service Thrift structs and
remaining cadence-thrift/src/convert.rs structs beyond those named above.
