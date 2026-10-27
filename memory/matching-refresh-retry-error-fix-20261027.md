# Matching refresh persistence errors and cancellation — 2026-10-27

Confirmed source mismatch: Go task_list_manager.go RefreshTaskListPartitionConfig
uses throttleRetry.Do(ctx, GetTaskListInfo), returns its error unchanged, then
handler.go calls handleErr. Non-typed persistence failures become Internal;
throttled errors pass through as typed ServiceBusy. Rust PartitionCache::refresh
instead returned Unavailable for all non-NotFound errors and ignored caller
cancellation during retry waits.

PartitionCache now uses the same Go-compatible error classification as update,
retains NotFound, and exposes refresh_until for cancellation-aware persistence
retry. Non-root initial config loading uses the same error classification.
Registry refresh has a drop guard and deadline predicate shared with the blocking
closure, preserving Go's first-attempt semantics while cancelling retry waiting.
The gRPC handler forwards the caller deadline. Pushed configurations still only
update the cache and do not invoke persistence.

The RPC regression in matching_partition_cancellation_tests.rs injects permanent,
timeout and throttled read errors after loading a real manager. With an expired
caller, it asserts exactly one read, correct status/details, original storage
message, unchanged cache and a manager that remains running. Refresh errors do
not have update's stop/failure-counter behavior. All source and tests require CI;
no local Cargo, Go or formatting ran.
