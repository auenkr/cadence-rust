# History failover coordinator parity audit (2026-11-13)

Go: `go/service/history/failover/{coordinator,marker_notifier}.go`, handler `NotifyFailoverMarkers`/`GetFailoverInfo`,
`shard/context.go` `AddingPendingFailoverMarker`/`ValidateAndUpdateFailoverMarkers`, `domain.CleanPendingActiveState`.

| Go function | Rust | Status |
|---|---|---|
| constants, `errRecordNotFound` | `failover/coordinator.rs` consts, `FailoverError::EntityNotExists` | MATCH |
| `NewCoordinator` / retry policy (50ms x2, 2 attempts) | `CoordinatorImpl::new`, `PersistenceDomainManager::clean_blocking` | MATCH |
| `Start` / `Stop` (CAS status, shutdown chan) | `start` / `stop` | MATCH |
| `NotifyFailoverMarkers` / `ReceiveFailoverMarkers` (blocking send) | try_send + spawned send | MATCH |
| `GetFailoverInfo` | `get_failover_info` | MATCH |
| `receiveFailoverMarkersLoop` (30m cleanup ticker) | `receive_failover_markers_loop` | MATCH |
| `notifyFailoverMarkerLoop` (jittered timer, keep requests on error) | `notify_failover_marker_loop` | MATCH |
| `handleFailoverMarkers` (version drop/invalidate, min creation time, gauge reset, `!updated` early return) | `handle_failover_markers` | MATCH |
| `cleanupInvalidMarkers`, `notifyRemoteCoordinator`, `aggregateNotificationRequests` | same names | MATCH |
| `markerNotifier.notifyPendingFailoverMarker` | `marker_notifier.rs` | MATCH |
| `CleanPendingActiveState` | `failover_domain_manager.rs` | MATCH |
| Handler `NotifyFailoverMarkers`, `GetFailoverInfo` | `handler_impl.rs` | MATCH |
| `ValidateAndUpdateFailoverMarkers` / `AddingPendingFailoverMarker` drop rules and skip reasons | `shard_context.rs`, `wiring/assembly/failover.rs` | MATCH |
| Marker `CreationTime` kept on pending markers | Rust `FailoverMarker` had no creation time. Markers read back were re-stamped with "now", so `GracefulFailoverLatency` and first-creation-time tracking were wrong. The replication task's creation time was also dropped. | FIXED: `FailoverMarker.creation_time` is stored, persisted in shard info (3rd tuple field) and set from the replication marker. Test `failover_marker_creation_time_is_preserved` |
| `history.Client.NotifyFailoverMarkers` routing by domain-ID peer | `LocalFailoverHistoryClient` hands tokens to this process's coordinator only | OPEN: needs the history RPC client and membership resolver; unchanged in single-host and 2-host gates |

## CI evidence (head d3711b0a98ebfad4bd6f0df12c2b63474c013dc3)
- rust-ci cadence-history on the fix commit aa3b4a5: https://github.com/auenkr/cadence-rust/actions/runs/43442844264 success.
- 12 history-mixed-gate combos (mysql/postgres/cassandra x grpc/tchannel x two_history true/false), each log `GATE SUMMARY failed: []`:
  43444246550 43444260556 43444275521 43444289330 43444302274 43444317827 43444330417 43444343919 43444359883 43444372735 43444386538 43444399934
