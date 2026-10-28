# Native Matching registry (`matching_registry_native.rs`) — 2026-10-28

Status: **UNVERIFIED**. Written without local Cargo; the parent must add
`pub mod matching_registry_native;` to `lib.rs` and run CI (clippy + tests).

## What it is
Byte-exact port of the lifecycle in `matching_registry.rs` (get at ~1037–1739,
scaler 1744–2041, partition update/notify/refresh 2042–2334,
unload/flush/maintain/sweep 2336–2602, `resolve_parent_dispatcher` 2606) onto
the native stack: `MatchingTaskStoreBytes`, `DurableTaskQueueBytes`,
`NativeTaskWriter/Reader/SyncMatcher/TaskDispatcher`, `NativePartitionCache`,
`poller_bytes`, `EmaQpsBytesTracker`. Identity is `(Uuid, GoString, i32)`, so raw
`\xff`, `\xfe` and U+FFFD names are distinct owners and leases. Lossy text
appears only in `diag()` (logs, metric tags).

Preserved: per-key Weak startup gates and cancellation-safe `spawn_blocking`
startup; ownership checks before and after lease I/O; stopped-registry flag;
`detach` by `Arc::ptr_eq`; `owner_work` joining readers after blocking cleanup;
ack/idle timers, GC, final ack; writer/reader fatal sinks; live config providers
captured per owner; partition update/refresh cancellation on caller drop and
deadline; background root-config notification (5s); weak registry handle for
forward and scaler (no owner/registry cycle).

## Typed native seams (additive API)
`NativeMembership`, `NativeQueueOwnershipPolicy`, `NativeDomainHooks`,
`NativeParentRouter`, `NativeLocalPollFactory`, `NativePartitionRefresher`,
`NativeScalerStarter`/`NativeScalerHost`, `NativeFatalSinks`,
`NativePollObserverProvider`. The parent wires these in production.

## Known gaps / compile risks (check first in CI)
- The adaptive scaler loop uses String-keyed `matching_scaler` types, so the loop is injected via `NativeScalerStarter`; `ScalerDaemon` and `ScalerConfig` are assumed to exist as imported.
- Legacy `Membership`, shard router, `MatchingFatal` and the domain store are String-based; they are replaced by the seams above, not ported.
- Constructor and method names of native modules were taken from reading and may differ slightly: `NativeTaskReader::with_fatal_invariant_sink`, `NativeTaskWriter::new_with_fatal_sink`, `NativeForwardState::new/with_observer/add_tokens/forward_backlog_task`, `NativePartitionCache::load/update_until/refresh_until/push/get`, `to_native_status`, `DurableTaskQueueBytes` setters, `QueryChannel::with_index`, `NativeSyncMatcher::new/offer/stop_signal`, `checker_native`, `matching_panic::capture`. Expect fix-up compile errors.
- Test fixture assumes `SqliteTaskStore` implements `MatchingTaskStoreBytes` (native sqlite adapter name unconfirmed in `matching_sqlite_native.rs`).
- The legacy test suite was not copied; only 3 native tests: raw ff/fe/U+FFFD identity, replacement-owner unload guard, unknown task type.
