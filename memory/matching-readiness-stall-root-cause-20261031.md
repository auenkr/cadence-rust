# Matching readiness stall root cause (reentrant overlay lock)

Source: GDB stacks in run 41114430171 (tag validation/matching-stall-stack-7b46ef884). Tokio workers blocked in std Mutex::lock on `DynamicConfigOverlay`; no other thread held it.

Cause: `NativeMatchingControlContext::{task_list_boolean, poller_window_nanos, write_partition_count}` locked the overlay, then called `cached_domain_name` -> `RateLimitedDomainSource::get_domain` -> `read_rate_snapshot`, which locks the same overlay. std Mutex is not reentrant, so the thread deadlocked itself (after a cache miss/rate-limited lookup), then others piled up. Explains HTTP/2 SETTINGS never arriving and the shutdown SIGKILL.

Fix: branch `t3/matching-overlay-reentrancy-fix` @ b4f16b334 resolves the domain name before locking. Validation: rust-ci package=cadence-matching run 41118272139, then fresh workspace CI and postgres gate. Not yet accepted. Other call sites holding the overlay lock across domain lookups should be audited.
