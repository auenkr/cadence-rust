# Native sync matcher (2026-10-28)

New file `rust/crates/cadence-matching/src/matching_sync_native.rs`: `NativeSyncMatcher` / `NativeSyncTask`,
a byte-preserving port of `matching_sync::SyncMatcher`. **Compilation and tests are unverified until parent CI.**

- Task = `cadence_persistence::matching_native_task_store::AllocatedTaskBytes`; source domain and isolation groups = `GoString`.
  Group sets, compatibility (`task group empty || equal`) and unknown-group fallback (to `""`) compare raw bytes.
- `offer` zeroes `task_id` and rewrites partition config with a byte-keyed copy of
  `task_isolation::dispatch_partition_config` (private to the module; `String` version untouched).
- Behavior identical to the legacy matcher: FIFO, Guard-based withdrawal on cancel, offer succeeds only on `finish(Ok)`,
  zero local wait only matches a parked poller, timeout boundary resolved under the state lock, stop signal, forwarded bit.
- `sync_match_budget` / `request_deadline` are re-exported from `matching_sync`, not duplicated.

Integration: add `pub mod matching_sync_native;` to `lib.rs` (parent owns). Native AddTask handler should call
`offer(allocated_bytes, &GoString group, &GoString source_domain, forwarded, local_wait, deadline)` using
`sync_match_budget(...)`/`request_deadline(...)`; the poll side calls `poll(&GoString group)` and must call
`finish(Ok(()))` only after History start succeeds. Never convert via `TryFrom<AllocatedTaskBytes>` on this path.
Tests cover ff/fe/U+FFFD group isolation, raw bytes delivery, unknown/default groups, FIFO, cancellation, late outcome.
