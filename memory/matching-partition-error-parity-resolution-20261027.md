# Matching partition-config update error parity (2026-10-27)

Source reading only. No Cargo/Go/CI run; parent owns CI. Resolves open item 3 of `matching-persistence-deviation-resolution-20261027.md`.

## Go trace
1. `go/service/matching/handler/handler.go:470-497` `UpdateTaskListPartitionConfig`: `return response, hCtx.handleErr(err)`.
2. `engine.go:1239-1279` returns BadRequestError (adaptive scaler on, non-normal kind, nil config, non-root) or the raw `tlMgr.UpdateTaskListPartitionConfig` error.
3. `task_list_manager.go:454-490`: `validatePartitionConfig` (BadRequestError); `throttleRetry.Do` retries only `persistence.IsTransientError` (InternalServiceError, ServiceBusyError, TimeoutError, DBUnavailableError; `data_manager_interfaces.go:3187`); on any final error: counter `TaskListPartitionUpdateFailedCounter`, `c.Stop()`, raw error returned.
4. `tasklist/db.go:139-157` returns the store error unchanged. The metered wrapper (`metered/base.go`) only counts, never converts.
5. `handler/context.go:83-145 handleErr`: passes through only listed typed errors (InternalService, BadRequest, EntityNotExists, ServiceBusy, ...). Default arm: `&types.InternalServiceError{Message: err.Error()}`. `FromError` then gives `CodeInternal` with no typed detail.

## Category table (Go -> Rust after fix)
| Persistence error | Go wire | Rust before | Rust now |
|---|---|---|---|
| ConditionFailedError (lost lease) | Internal | Unavailable | Internal |
| TimeoutError | Internal | Unavailable | Internal |
| DBUnavailableError | Internal | Unavailable | Internal |
| InternalServiceError / untyped | Internal | Unavailable | Internal |
| ServiceBusyError (SQL throttling) | ResourceExhausted + ServiceBusyError detail | Unavailable | same as Go |
| ctx deadline in `throttleRetry` | Internal ("context deadline exceeded") | n/a | not covered here, see below |

## Change
`matching_partition.rs`: new `update_failure_status` replaces `Status::unavailable`; the same mapping as `matching_dispatch::queue_status` (Throttled -> ServiceBusy, else `Status::internal`). `queue.stop()` before the error is unchanged (equal to Go `c.Stop()`).

Tests (`matching_partition.rs`): existing "failed to update" case now asserts `Code::Internal`, no details, cache unchanged, queue stopped, using a real fenced store (newer lease owner). New `update_failure_status_matches_go_handle_err_categories` covers every category above. Not yet run on CI.

## Not covered (not in owned files)
- Deadline: Rust `PartitionCache::update` runs in `spawn_blocking` with no ctx, so a caller deadline during the persistence retry is not turned into Internal "context deadline exceeded". `registry.update_partition_config_with_deadline` (matching_registry.rs ~1985) owns that; needs a follow-up there.
- `BackendErrorKind::BadRequest` (Go BadRequestError passes through) is Internal here; unreachable for UpdateTaskList.
- Metric `TaskListPartitionUpdateFailedCounter` in Rust not checked.
- Exact public-response test through gRPC/Thrift not added (needs files outside ownership).
