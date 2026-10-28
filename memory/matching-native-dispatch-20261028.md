# Native Matching dispatcher (matching_dispatch_native.rs) - 2026-10-28

Status: UNVERIFIED. Source written without any local Cargo/rustfmt/Go run; parent CI must compile, format and run it. Not registered in `lib.rs`, not staged or committed.

File: `rust/crates/cadence-matching/src/matching_dispatch_native.rs` (additive; legacy `matching_dispatch.rs` untouched).

## Ported from `matching_dispatch.rs`
- Detached delivery worker + claim flag + oneshot cancellation (`poll_with_lifecycle`, `delivery_worker`, `delivery_loop`), blocked-poller release, window vs caller-deadline wait.
- Claim via `NativeTaskReader::poll_group(bytes)`, `NativeSyncMatcher::poll(&GoString)` (only when a reader exists, as legacy), or `DurableTaskQueueBytes::read_batch_with_clock` fallback.
- Ack (`complete` + GC), failed-delivery rewrite (`NativeTaskWriter::append_rewrite(PendingTaskBytes)`, else `rewrite_failed`), queue stop on rewrite failure, sync task `finish`.
- History: `start_decision_native_with_deadline`, `start_activity_native_with_deadline`, `refresh_workflow_tasks_native_until`, `mutable_state_native_with_deadline`; `start_error_kind` Discard / InactiveDomain / Retry handling identical to legacy.
- Forwarding: `NativeForwardState` (permits, add tokens, add rate, local wait), `forward_poll`, `forward_backlog_task`, `accept_forwarded_backlog` (uses `NativeSyncMatcher::offer`), `forward_query_poll`. Parent is `NativeParent::{Dispatcher, Service(Arc<dyn NativeMatchingApi>)}`.
- `queue_status` maps throttled backend errors (bytes and legacy variants) to ServiceBusyError, others Internal.
- Domain, task list, poller id, isolation group, workflow id and run id stay `GoString`/bytes; no UTF-8 fallback for key/data.

## Concrete remaining seams (not silently dropped)
1. `NativeMatchingApi` trait is defined locally (poll decision/activity, add decision/activity, with a deadline). No implementation exists yet: local in-process native service adapter and a remote byte-preserving client (legacy `Parent::Remote` with YARPC gRPC headers `rpc-service/rpc-caller/rpc-encoding`, fault injection `inject_client_call`) must be written by the parent. Legacy `Parent::Service(Box<grpc_matching::Matching>)` is not used.
2. Poller registry: local `NativePollerManager` (GoString-keyed id -> cancel callback) replaces `cadence_common::poller::Manager`. Missing vs legacy: `Info` rate-per-second, `count_at(timestamp)` time-windowed counts, `set_cancel` semantic of `CancelOutstandingPoll` is only `cancel()` here, and the forwarded-poll `rate_per_second = TASK_DISPATCH_RPS` rewrite. Swap to the raw poller helper when it lands.
3. Metrics: `matching_metrics::{MatchingMetrics, PollMetricContext}` are `String` based and not used; replaced by `NativePollObserver` hooks (poller_count, poll_success, poll_match_latency, poll_match with partition-config tags missing, async handoff latency, already_started, timeout, forward latencies, invalid isolation group). Parent must adapt to real metrics.
4. Task info log (`matching_task_info_log::emit`) takes `&str` domain: events are skipped for a non-UTF-8 domain. "Duplicated decision/activity task" text is generic ("Duplicated task") and `wf-task-list-name` is empty because the raw lifecycle does not yet carry task type / task list name.
5. `ForwardError` for `forward_backlog_task` uses the UTF-8 `TaskListName::parse`; a non-UTF-8 source task-list name returns `NoParent` (Go byte parsing not ported). `accept_forwarded_backlog` rejects non-UTF-8 parent names with InvalidArgument.
6. Native query channel (`matching_query_native::QueryChannel`) is not owned by the dispatcher: like legacy, the caller serves it during `forward_query_poll`.
7. Native generated message field names/types (`n::matching::{PollFor*Request, AddDecisionTaskRequest, AddActivityTaskRequest}`, `n::api::{TaskList, WorkflowExecution}`, `partition_config` as `HashMap<GoString, GoString>`, `schedule_to_start_timeout` as `prost_types::Duration`, `source: i32`) were assumed to mirror the legacy model; CI compile may require adjusting them.
8. Legacy fault-injection hooks and the Go-oracle remote forward test (`CADENCE_MATCHING_GO_PEER`) are not ported.
9. Not ported (reader/sync-matcher integration tests): legacy tests that need a configured `NativeTaskReader` + `NativeSyncMatcher` (sync match, isolation groups, backlog forwarding, forwarded parent loop). Current tests cover the queue-fallback claim path only.

## Tests in the file (all `#[tokio::test]`/`#[test]`, unverified)
- throttle -> ServiceBusy mapping; parent request rebuild keeps raw bytes
- non-UTF-8 workflow id delivered and acknowledged
- retryable start error rewrites then redelivers raw bytes
- poll dropped mid-start rewrites task for next poller (detached ownership)
- poll lifecycle raw poller id register/release
- forwarded backlog rejects non-UUID source domain
- release_blocked_pollers returns empty poll

## Parent TODO
Register module in `lib.rs`, run rustfmt via CI workflow, run `cargo clippy` + `cargo test -p cadence-matching` on CI, fix compile fallout, then adopt in production wiring.

## Parent review and corrections

Registered the module additively and added it to CI formatting. Removed the reduced local poller map in favor of `Mutex<cadence_common::poller_bytes::Manager>` and raw `Info`. Begin/end now preserve history TTL, rate, group counts and cancellation, and parent forwarding resets the rate to TASK_DISPATCH_RPS. Updated the lifecycle test to check retained history and original rate after end.

Corrected both task-list routing sites to use existing TaskListNameBytes; invalid bytes in a root or partition name are no longer rejected merely for UTF-8. Found and corrected a data corruption bug in backlog forwarding: the worker's run_id_text helper JSON-quotes arbitrary bytes for logging and had been reused for the forwarded execution RunID. Forwarding now uses TaskDataBytes::run_id_bytes directly. The helper remains only on the log path. ConditionFailed messages now use raw_status, and throttled errors use raw_detailed_status, requiring RawStatusService at production registration. All corrections remain source-only pending CI. Metrics/log details, remote client/fault hooks, production adoption and reader/sync integration tests remain open.
