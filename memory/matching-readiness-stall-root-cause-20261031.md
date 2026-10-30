# Matching readiness stall root cause (reentrant overlay lock)

Source: GDB stacks in run 41114430171 (tag validation/matching-stall-stack-7b46ef884). Tokio workers blocked in std Mutex::lock on `DynamicConfigOverlay`; no other thread held it.

Cause: `NativeMatchingControlContext::{task_list_boolean, poller_window_nanos, write_partition_count}` locked the overlay, then called `cached_domain_name` -> `RateLimitedDomainSource::get_domain` -> `read_rate_snapshot`, which locks the same overlay. std Mutex is not reentrant, so the thread deadlocked itself (after a cache miss/rate-limited lookup), then others piled up. Explains HTTP/2 SETTINGS never arriving and the shutdown SIGKILL.

Fix: branch `t3/matching-overlay-reentrancy-fix` @ b4f16b334 resolves the domain name before locking. Validation: rust-ci package=cadence-matching run 41118272139, then fresh workspace CI and postgres gate. Not yet accepted. Other call sites holding the overlay lock across domain lookups should be audited.

## Progress 2026-10-31 (later)

1. Reentrancy fix (b4f16b334) removed the stall: run 41119172073 reached a Frontend response and shut down cleanly.
2. New failure there: Go Frontend DescribeTaskList -> protobufjs "Cannot read properties of null (reading 'newTasksPerSecond')". Diagnostic run 41121315799 captured raw direct bytes: map entry `32 08 0a 06 zone-a` with no value field. Cause: native codec (`cadence-proto/build/native_wire.rs`) used prost `hash_map::encode`, which omits default message values; Go reads nil and re-encodes nil. Fix: `native::wire::message_map` always emits the value (commit 396ca859 on `t3/matching-overlay-reentrancy-fix`, tag `validation/matching-map-value-396ca859`); rust-ci cadence-proto run 41123206198 success.
3. PostgreSQL gate run 41123877578 on that tag: readiness and DescribeTaskList now pass; fails at tests/matching-service-compat.cjs:1161 "persisted decision task must be drained, not re-enqueued by a second schedule" (2 !== 1), then SIGKILL on shutdown. Next: investigate that assertion (evidence /mnt/storage/service/ci-evidence/g-41123877578).

Note: a "Nightly continue (2am)" scheduled task in another thread dispatches validation runs concurrently and can cancel mine via workflow concurrency.
