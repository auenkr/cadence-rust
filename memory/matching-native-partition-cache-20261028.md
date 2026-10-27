# Matching native partition cache (2026-10-28)

Status: written, NOT compiled or run (no local Cargo by policy). Parent must register and run CI.

## Files
- `rust/crates/cadence-matching/src/matching_partition_native.rs` (new): `NativePartitionCache`, `NativeStoredPartitionConfig` (`HashMap<i32, Vec<GoString>>` + version), `PartitionError{kind,message: GoString}`, `validate`, `from_storage`/`to_storage` (pub(crate)).
- `rust/crates/cadence-persistence/src/matching_retry.rs`: additive `persistence_bytes` / `persistence_bytes_until` (same algorithm as `persistence_until`; legacy API untouched).

## Behavior ported from matching_partition.rs
load (sticky kind!=0 -> empty; `TaskListNameBytes::parse`; root from queue metadata, non-root reads root via retried `get_task_list`, NotFound -> None); push only replaces strictly older version; refresh replaces (even newer / clears) and NotFound is an error leaving cache unchanged; update: validate first (same wording/order, no failure callback), no-op rules, version = cached+1 wrapping, retried fenced `update_partition_config` with caller cancellation, on persistence failure callback once + `queue.stop()` + cache unchanged. Legacy count-field default-map synthesis via `partition_map`.

## Error mapping
Throttled -> ServiceBusy (typed detail in `to_status`), BadRequest -> InvalidArgument, all else (ConditionFailed, Timeout, DBUnavailable, Internal) -> Internal. Retry keeps Legacy/Backend transient classification; other native errors (ConditionFailed{GoString}, MalformedSqlRunId) are stashed and restored unchanged, so raw message bytes survive.

## Limitations
- `PartitionError::to_status()` is diagnostic only: tonic messages are `String`, invalid UTF-8 is lossy. Native transports use `message` bytes + `kind`.
- Native `Display` of `ConditionFailed` is `{:?}` of the message; for non-ConditionFailed errors `message` is the Display text. Go's exact wording for those store errors is unchanged from legacy behavior.
- `Backend` native errors come back from retry as `Legacy(Backend)` (same kind/message).

## Integration (parent)
Add `pub mod matching_partition_native;` to `cadence-matching/src/lib.rs`, add file to the formatter list (file is not rustfmt-formatted: Faulty impl lines are long), run clippy + `cargo test -p cadence-matching -p cadence-persistence` on CI. Likely first-compile risks: unused imports, `is_none_or` (already used by legacy), `partition_map` type inference. Tests: invalid-byte roundtrip, legacy UTF-8 visibility, count synthesis, validation order/no callback, update rules, fenced stop + single callback, push rules, refresh replace/NotFound/error kinds, cancellation prevErr + raw-bytes retention, invalid-name raw error.

Parent registered the module and CI formatter entry. Renamed the lossy tonic conversion to `to_diagnostic_status` so it cannot be mistaken for the future native wire conversion. Raw errors must go through a byte-preserving transport encoder. Existing tests were updated to the diagnostic method name. This does not wire production handlers.
