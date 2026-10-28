# Matching native standby completer (UNVERIFIED: nothing compiled or run)

`rust/crates/cadence-matching/src/matching_standby_native.rs` (new, not in lib.rs yet).
Port of `matching_standby.rs` onto native protobuf `n::history::DescribeWorkflowExecutionResponse`,
`AllocatedTaskBytes`, `TaskListKeyBytes` and `MatchingHistoryClient::describe_execution_native`.

## Seams for the parent
- Add `mod matching_standby_native;` in lib.rs.
- `checker_native(domains: DomainResolverNative, history, key: TaskListKeyBytes, enabled: Option<EnabledNative>, metrics, ctx) -> DispatchCheckNative`
  (`Fn(AllocatedTaskBytes) -> Future<Result<bool,Status>>`). The raw reader must accept this type
  (existing `matching_reader::DispatchCheck` takes UTF-8 `AllocatedTask`).
- `DomainResolverNative = Fn(&GoString domain_id) -> Result<StandbyDomainNative{active_in_cluster, active_active, name: GoString}, Status>`:
  the parent wires this to the UTF-8 domain cache (`is_active_in` + `describe`) and maps lookup errors to Status
  (legacy used `Status::not_found(message)`); the callback is the only place raw name bytes enter.
- `EnabledNative = Fn(&GoString domain_id, &GoString tasklist, i32 task_type) -> bool`.
- `PollMetricContext.domain` is still `String` (metrics tag, owned elsewhere).
- Retry/metric helpers are duplicated privately from `matching_standby.rs` (not edited); parent may
  make those `pub(crate)` and dedupe.

## Behaviour preserved
Go retry policy 1s/10s/300s; domain/History errors retried then returned unchanged with one
completion-failure metric; not-started exhaustion returns "task not started" without failure metric;
young missing execution (<=24h) retried and counted as failure on exhaustion, older completes with
STANDBY_TASKS_COMPLETED; dropped future counts one failure (cancellation).

## Tests (unrun)
Raw ids reach History unchanged, task_started rows, active/active-active/disabled skip, not-started
exhaustion (3 calls with max_attempts=2), domain+history error exhaustion (4 calls), cancellation.
Risks: native field names (`PendingActivityInfo.schedule_id/state`, `close_status`) copied from
tchannel_native codec; `Option::is_none_or` and `Arc::into_inner` need recent Rust (already used / 1.70+).

## Parent integration and test review

Registered the native module and its formatter path. The initial one-attempt fixture used maximum_attempts=0, which actually means unlimited retries; fixed it to force expiration using explicit elapsed time. Added a real typed EntityNotExistsError envelope test through raw_detailed_status and the complete checker runner for both young and old tasks; it asserts History attempt count, completed/not-started/failure metrics and terminal result. These changes remain unverified pending CI. Production domain resolver/native registry adoption still required.
