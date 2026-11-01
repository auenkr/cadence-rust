# Frontend workflow service wiring (branch fe-workflow-service, 2026-11-01)

Base: origin/frontend-domain-admin. Goal: the Rust Frontend serves the workflow API
through `handler_workflow::Handler` instead of the legacy in-process engine.

## Design
- `cadence-service-core/src/workflow_api.rs`: `WorkflowApi` trait (JSON `Value` in/out,
  `CadenceError`), `CallerContext`, Thrift codec table (`codec(method)`), `error_to_thrift`,
  `dispatch_thrift`. `AppState.workflow_api: Option<Arc<dyn WorkflowApi>>`.
- `yarpc::dispatch` (Thrift/TChannel) tries the hook first; unserved methods fall through to the
  legacy engine (domain, admin, schedule, ...).
- `cadence-matching/src/launcher.rs`: `run_frontend_with_hook(bin, only, factory)`; the factory is
  called right before the gRPC/TChannel serve tasks with the final `AppState` + `Config` and the
  result is stored in the cloned transport states.
- `cadence-frontend/src/workflow_backend.rs`: `ProductionBackend` (WorkflowBackend) sends History
  Thrift over TChannel (`call_thrift`, service `cadence-history`, `HistoryService::<m>`), envelope
  field IDs from go/idls/thrift/history.thrift (10 domainUUID, 20 inner request). Addresses from
  `services.history.rpc.{host,port}` (default 7934) / matching 7935; env override
  `CADENCE_HISTORY_TCHANNEL_ADDR`, `CADENCE_MATCHING_TCHANNEL_ADDR`.
  `HandlerApi` adapts `Handler::dispatch_with_metadata`.

## Done (chunk 1, commit "install Handler-backed WorkflowService hook")
Start (with CreateHistoryStartWorkflowRequest backoff/expiration), SignalWithStart, Signal,
Terminate, RequestCancel, Reset, Describe, Query, ResetStickyTaskList, RefreshWorkflowTasks,
RecordActivityTaskHeartbeat, RespondActivityTask{Completed,Failed,Canceled}, RespondDecisionTaskFailed
(Thrift transport only).

## Remaining gaps
- gRPC transport (`cadence-matching/src/grpc.rs` Frontend adapters) still uses the legacy engine;
  needs per-RPC proto->api->hook->api->proto routing.
- Poll decision/activity (Matching Thrift + history read for decision history), RespondDecisionTaskCompleted
  (+ new decision task), RespondQueryTaskCompleted (Matching).
- GetWorkflowExecutionHistory (history_node reader), visibility list/scan/count/archived, task-list ops,
  ByID Thrift types, GetSearchAttributes/ClusterInfo are handled inside Handler.
- Wrapper chain (ratelimited, metered, versioncheck, accesscontrolled, clusterredirection) not yet composed
  in `build_workflow_api`; only domain lookup is set.
- Ringpop-based History shard routing (static address only).
- Mixed-gate evidence.

## CI evidence
| run | head | package | conclusion |
|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/41314182425 | 21647b8441ba3be0fabe246fac25a8afca09fbe3 | cadence-frontend | in_progress (update) |
