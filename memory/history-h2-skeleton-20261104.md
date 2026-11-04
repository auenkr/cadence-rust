# History H2 skeleton progress (2026-11-04)

Branch: `history-h2-skeleton`, based on `master` at `797ff0f15`.

## Ported in this phase

- Replaced `cadence-history` `not_migrated` process entry with the shared Rust launcher restricted to `--services history`; shared launcher configures the History internal gRPC endpoint from `history.rpc.grpcPort` (default 7834) and its configured bind/TLS settings. `cadence-server --services history` already selects this listener through `run_history`.
- Added History `Config` and default values for shard count, range size and initial shard controller/cache/rate limit runtime settings. The `numHistoryShards` setting maps to the common persistence config.
- Added `ShardStore` contract (`get_shard`, `create_shard`, compare-and-swap `update_shard`), in-memory store, owner selection through shared Ringpop-compatible hash ring, ownership-loss close behavior and `ShardOwnershipLost` carrying owner host.
- Workflow ID mapping calls `cadence_common::membership::fingerprint32`, same signed-short FarmHash behavior as Go. Go source vectors: empty string / 1000 -> 242; `workflowId` / 1000 -> 580.
- Added exact-name manifest for all IDL RPC methods. The gRPC HistoryAPI is currently served by shared launcher History adapter; its declared IDL method list has 45 methods.

## Thrift `HistoryService` RPC names (go/idls/thrift/history.thrift)

| Wire method | Skeleton behavior |
|---|---|
| `StartWorkflowExecution` | Method identifier declared; handler wiring pending |
| `GetMutableState` | Method identifier declared; handler wiring pending |
| `PollMutableState` | Method identifier declared; handler wiring pending |
| `ResetStickyTaskList` | Method identifier declared; handler wiring pending |
| `RecordDecisionTaskStarted` | Method identifier declared; handler wiring pending |
| `RecordActivityTaskStarted` | Method identifier declared; handler wiring pending |
| `RespondDecisionTaskCompleted` | Method identifier declared; handler wiring pending |
| `RespondDecisionTaskFailed` | Method identifier declared; handler wiring pending |
| `RecordActivityTaskHeartbeat` | Method identifier declared; handler wiring pending |
| `RespondActivityTaskCompleted` | Method identifier declared; handler wiring pending |
| `RespondActivityTaskFailed` | Method identifier declared; handler wiring pending |
| `RespondActivityTaskCanceled` | Method identifier declared; handler wiring pending |
| `SignalWorkflowExecution` | Method identifier declared; handler wiring pending |
| `SignalWithStartWorkflowExecution` | Method identifier declared; handler wiring pending |
| `RemoveSignalMutableState` | Method identifier declared; handler wiring pending |
| `TerminateWorkflowExecution` | Method identifier declared; handler wiring pending |
| `ResetWorkflowExecution` | Method identifier declared; handler wiring pending |
| `RequestCancelWorkflowExecution` | Method identifier declared; handler wiring pending |
| `ScheduleDecisionTask` | Method identifier declared; handler wiring pending |
| `RecordChildExecutionCompleted` | Method identifier declared; handler wiring pending |
| `DescribeWorkflowExecution` | Method identifier declared; handler wiring pending |
| `ReplicateEventsV2` | Method identifier declared; handler wiring pending |
| `SyncShardStatus` | Method identifier declared; handler wiring pending |
| `SyncActivity` | Method identifier declared; handler wiring pending |
| `DescribeMutableState` | Method identifier declared; handler wiring pending |
| `DescribeHistoryHost` | Method identifier declared; handler wiring pending |
| `CloseShard` | Method identifier declared; handler wiring pending |
| `RemoveTask` | Method identifier declared; handler wiring pending |
| `ResetQueue` | Method identifier declared; handler wiring pending |
| `DescribeQueue` | Method identifier declared; handler wiring pending |
| `GetReplicationMessages` | Method identifier declared; handler wiring pending |
| `GetDLQReplicationMessages` | Method identifier declared; handler wiring pending |
| `QueryWorkflow` | Method identifier declared; handler wiring pending |
| `ReapplyEvents` | Method identifier declared; handler wiring pending |
| `RefreshWorkflowTasks` | Method identifier declared; handler wiring pending |
| `ReadDLQMessages` | Method identifier declared; handler wiring pending |
| `PurgeDLQMessages` | Method identifier declared; handler wiring pending |
| `MergeDLQMessages` | Method identifier declared; handler wiring pending |
| `NotifyFailoverMarkers` | Method identifier declared; handler wiring pending |
| `GetCrossClusterTasks` | Method identifier declared; handler wiring pending |
| `RespondCrossClusterTasksCompleted` | Method identifier declared; handler wiring pending |
| `GetFailoverInfo` | Method identifier declared; handler wiring pending |
| `RatelimitUpdate` | Method identifier declared; handler wiring pending |

## gRPC `uber.cadence.history.v1.HistoryAPI` RPC names (go/proto/internal/uber/cadence/history/v1/service.proto)

| Wire method | Skeleton behavior |
|---|---|
| `StartWorkflowExecution` | Generated service method; handler served by shared launcher |
| `SignalWorkflowExecution` | Generated service method; handler served by shared launcher |
| `SignalWithStartWorkflowExecution` | Generated service method; handler served by shared launcher |
| `ResetWorkflowExecution` | Generated service method; handler served by shared launcher |
| `TerminateWorkflowExecution` | Generated service method; handler served by shared launcher |
| `DescribeWorkflowExecution` | Generated service method; handler served by shared launcher |
| `QueryWorkflow` | Generated service method; handler served by shared launcher |
| `ResetStickyTaskList` | Generated service method; handler served by shared launcher |
| `GetMutableState` | Generated service method; handler served by shared launcher |
| `PollMutableState` | Generated service method; handler served by shared launcher |
| `RecordDecisionTaskStarted` | Generated service method; handler served by shared launcher |
| `RespondDecisionTaskCompleted` | Generated service method; handler served by shared launcher |
| `RespondDecisionTaskFailed` | Generated service method; handler served by shared launcher |
| `RecordActivityTaskStarted` | Generated service method; handler served by shared launcher |
| `RespondActivityTaskCompleted` | Generated service method; handler served by shared launcher |
| `RespondActivityTaskFailed` | Generated service method; handler served by shared launcher |
| `RespondActivityTaskCanceled` | Generated service method; handler served by shared launcher |
| `RecordActivityTaskHeartbeat` | Generated service method; handler served by shared launcher |
| `RequestCancelWorkflowExecution` | Generated service method; handler served by shared launcher |
| `RemoveSignalMutableState` | Generated service method; handler served by shared launcher |
| `ScheduleDecisionTask` | Generated service method; handler served by shared launcher |
| `RecordChildExecutionCompleted` | Generated service method; handler served by shared launcher |
| `ReplicateEventsV2` | Generated service method; handler served by shared launcher |
| `SyncShardStatus` | Generated service method; handler served by shared launcher |
| `SyncActivity` | Generated service method; handler served by shared launcher |
| `DescribeMutableState` | Generated service method; handler served by shared launcher |
| `DescribeHistoryHost` | Generated service method; handler served by shared launcher |
| `CloseShard` | Generated service method; handler served by shared launcher |
| `RemoveTask` | Generated service method; handler served by shared launcher |
| `ResetQueue` | Generated service method; handler served by shared launcher |
| `DescribeQueue` | Generated service method; handler served by shared launcher |
| `GetReplicationMessages` | Generated service method; handler served by shared launcher |
| `GetDLQReplicationMessages` | Generated service method; handler served by shared launcher |
| `ReapplyEvents` | Generated service method; handler served by shared launcher |
| `RefreshWorkflowTasks` | Generated service method; handler served by shared launcher |
| `CountDLQMessages` | Generated service method; handler served by shared launcher |
| `ReadDLQMessages` | Generated service method; handler served by shared launcher |
| `PurgeDLQMessages` | Generated service method; handler served by shared launcher |
| `MergeDLQMessages` | Generated service method; handler served by shared launcher |
| `NotifyFailoverMarkers` | Generated service method; handler served by shared launcher |
| `GetCrossClusterTasks` | Generated service method; handler served by shared launcher |
| `RespondCrossClusterTasksCompleted` | Generated service method; handler served by shared launcher |
| `GetFailoverInfo` | Generated service method; handler served by shared launcher |
| `RatelimitUpdate` | Generated service method; handler served by shared launcher |

## Validation and gaps

- Corrected `ShardOwnershipLost` redirect data to carry the selected member's named TChannel address, matching Go `CreateShardOwnershipLostError`; membership identity remains the ownership comparison key. Added a focused regression test.
- The generic TChannel dispatcher now recognizes the `HistoryService` IDL and emits an explicit `HistoryService::<method> is not implemented yet` transport error instead of “unknown procedure”. This does not yet make the History service listen on its own TChannel endpoint.
- Validation status: GitHub Actions `rust-ci.yml` must be rerun for the final pushed head; run URL, final head SHA, and conclusion will be recorded after CI. No local Cargo command was run.
- Important H2 gap: the shared launcher starts History gRPC, but does not yet bind a History TChannel/Thrift listener, and History Thrift requests are not dispatched to a service-specific handler. The static method lists plus generic explicit error are not full handler registration.
- Dynamic config coverage is intentionally incomplete: the current `Config` carries the shard/controller and selected cache/rate-limit values only; the full ~200-field Go `service/history/config.Config` and every dynamic key/default remain for follow-up.
- The common launcher History gRPC adapter is shared service runtime behavior, not a full Go-equivalent History engine. Unimplemented history logic, operational shard polling/recovery, persistence-backed shard state, metrics parity and mixed Go-service/SDK failure-restart-recovery validation remain. The H1 SQL implementation is on a separate branch and is not wired into this skeleton.
