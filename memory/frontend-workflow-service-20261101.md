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

## Done (chunk 2)
`cadence-persistence::history_reader` (Go ReadHistoryBranch port) + MySQL/Postgres `HistoryNodeStore` impls; `AppState.history_node_store`. Cassandra reader still a gap.

## Done (chunks 3-5)
- Chunk 3: Matching polls, RespondDecisionTaskCompleted/QueryTaskCompleted, task-list ops, GetWorkflowExecutionHistory via history reader.
- Chunk 4: SQL visibility reader (`cadence-persistence::visibility_reader`; MySQL/Postgres `VisibilityStore`), ListOpen/ListClosed in `workflow_visibility.rs`.
- Chunk 5: gRPC routing: `Frontend::call_with_headers` (cadence-matching/src/grpc.rs) sends hooked ops (signal, query, terminate, restart, reset, refresh, diagnose, cancel, describe, task-list ops, polls, responds, heartbeat, search attributes) through `state.workflow_api`; `CallerScope` tower layer carries headers + `grpc-timeout`; shared `status_from_error_type`. Go `GoVersionChecker` (frontend.enableClientVersionCheck read at startup) installed in `build_workflow_api`.
- CI fixes found on the way: audit-matching-metrics regex vs rustfmt-wrapped arms; test module placed last in matching_mysql/postgres; SQL `DomainMetadataPersistence::get_domain/list_domains` overrides (cache-restart describe).

## Remaining gaps (accurate as of this commit)
- gRPC ops still on the legacy engine: start, signal_with_start(+async), start_async, get_workflow_execution_history, all visibility list/scan/count (legacy `st::StartTimeFilter` is ns).
- Visibility ListWorkflowExecutions/Scan/Count (advanced visibility) return "not supported"; ListArchived reader not wired; separate `visibilityStore` datastore config not opened (default store pool/shard 0 used).
- Wrapper chain only has domain lookup + version check; ratelimited, metered, accesscontrolled, clusterredirection are not composed (Handler `with_*` hooks exist).
- ByID Thrift types (Respond/Record*ByID) not in the Thrift codec table.
- Cassandra history/visibility readers; Ringpop-based History shard routing (static address only); Restart/Diagnose via History; raw history responses.
- Mixed-gate evidence (`frontend-mixed-gate.yml`, record Rust binary SHA256) NOT run yet: the gap list is not empty.

## CI evidence
| run | head | package | conclusion |
|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/41314182425 | 21647b8441ba3be0fabe246fac25a8afca09fbe3 | cadence-frontend | failure (op! macro arm order; fixed in 28ea37e96) |
| https://github.com/auenkr/cadence-rust/actions/runs/41314489059 | 28ea37e96b22d690a6851b9ebe89e34406258317 | cadence-frontend | failure (result_large_err; fixed in 8ea626d09) |
| https://github.com/auenkr/cadence-rust/actions/runs/41314823718 | 8ea626d097a8dffb213b8d456baf5a7c21d1ed45 | cadence-frontend | failure (rustfmt only) |
| https://github.com/auenkr/cadence-rust/actions/runs/41319101002 | 64427e1aa | cadence-frontend format_all | success (patch produced) |
| https://github.com/auenkr/cadence-rust/actions/runs/41319515360 | 4108543cebdadd66a972ee6380c58c9e374610fb | cadence-frontend format_all (chunk 3: polls/respond/task-list/history) | in_progress (update) |
| https://github.com/auenkr/cadence-rust/actions/runs/41319817327 | 7227bb94b | cadence-frontend format_all | success |
| https://github.com/auenkr/cadence-rust/actions/runs/41320201631 | 29759b838 | cadence-frontend format_all (visibility) | success |
| https://github.com/auenkr/cadence-rust/actions/runs/41322544883 | after c2e0867b7 + domain get fix | cadence-persistence | success |
| https://github.com/auenkr/cadence-rust/actions/runs/41322909641 | same | cadence-matching | success |
| https://github.com/auenkr/cadence-rust/actions/runs/41323599602 | gRPC routing | cadence-matching format_all | success |
| https://github.com/auenkr/cadence-rust/actions/runs/41324189872 | version checker | cadence-frontend format_all | in_progress |
