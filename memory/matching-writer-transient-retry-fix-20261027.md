# Matching writer rewrite transient retry fix (audit B1) - 2026-10-27

## Go source traced
- `go/service/matching/tasklist/task_writer.go:95-97`: `throttleRetry` = `persistenceOperationRetryPolicy` + `WithRetryableError(persistence.IsTransientError)`.
- `go/common/persistence/data_manager_interfaces.go:3187`: transient = InternalServiceError, ServiceBusyError, TimeoutError, DBUnavailableError.
- `go/common/util.go:50-52,151`: initial 50ms, max 10s, expiration 30s. Rust `append_rewrite` already uses identical constants (+1s/10s throttle policy); unchanged.

## Rust change (`rust/crates/cadence-matching/src/matching_writer.rs`)
- `append_inner` CreateTasks error classification: `retryable` was only `StorageError::Sql(_)`; now `QueueError::Storage(e) if e.is_transient()`.
  `is_transient()` = `Sql(_)` or `Backend{Timeout|Throttled|Internal|DbUnavailable}`. Permanent/BadRequest backend errors, MalformedSqlMatchingRunId
  (fatal path) and others stay non-retryable. Sticky (kind 1) `ConditionFailed` remains retryable as before.
- Only `append_rewrite` uses the outer retry; plain `append` stays single-attempt (unchanged).

## Tests added (same file)
- `matching_writer_rewrite_retries_transient_backend_errors_like_go`: fake store fails CreateTasks once per kind (Timeout, Throttled, Internal, DbUnavailable); expects 2 calls and Ok.
- `matching_writer_rewrite_does_not_retry_nontransient_backend_errors`: `Permanent` -> 1 call, Err.

## Validation (not run locally per policy)
Parent dispatch: `gh workflow run rust-ci.yml --ref <branch> -f package=cadence-matching` (clippy + tests). Record run URL/SHA/conclusion here once run.
