# Frontend API coverage (2026-11-03/04)

Branch: `t3/frontend-api-coverage`. Head SHA: `b7404ea995d5ad86f8074bc4c78066c14afa539a`.

## CI evidence (all success, repo auenkr/cadence-rust)
| Run | Gate | Rust binary SHA256 |
|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/41701747106 | rust-ci (clippy + workspace tests) | n/a |
| https://github.com/auenkr/cadence-rust/actions/runs/41703220863 | frontend-mixed-gate mysql, kafka=true, two_history=true | 001fdf319078c953142918438df03a07df14d01982b5d044efb450daf8f0cfe9 |
| https://github.com/auenkr/cadence-rust/actions/runs/41705505640 | frontend-mixed-gate postgres, two_cluster=true | 52abf65b9e808954fd7f1d73e64371f7d97618b8d71c597a2acc0c72f171fb08 |
| https://github.com/auenkr/cadence-rust/actions/runs/41707320501 | frontend-mixed-gate cassandra | e4b182b332598de2cf8cafdb7b6cd404973e8de32409f300c2d2a45684e85f77 |

## APIs now covered (TChannel and gRPC where possible; steps in tests/frontend-gate/api.go, main.go)
GetClusterInfo, GetSearchAttributes, domain register/list/deprecate/delete/failover/list-failover-history,
SignalWithStartWorkflowExecution, manual decision/activity flow (RespondDecisionTask*, RespondActivityTask
Completed/Failed/Canceled, heartbeat, ByID variants, identity-too-long checks), ResetWorkflowExecution,
RestartWorkflowExecution, DiagnoseWorkflowExecution, schedule update/pause/unpause/list (gRPC CLI),
admin CLI: cluster describe, workflow describe, refresh-tasks, history_host describe,
isolation-groups get-global/get-domain, tasklist list.

## Rust bugs fixed (to match Go)
- DeleteDomain requires Deprecated status (BadRequest otherwise) - yarpc.rs, handlers.rs.
- ByID activity requests build the task token with ScheduleID = EmptyEventID (-23).
- PollerInfo.LastAccessTime used ms instead of unix nanos (caused CLI panic, Go history/worker panics, restart failures).
- Admin RefreshWorkflowTasks now delegates to History (was in-memory only); Thrift arm added.
- Respond*/heartbeat reject identity over limit ("Identity length exceeds limit."); SignalWithStart does not validate identity (as Go).
- Warn-limit metrics counter wired via WarnSink in request_validator.

## Deferred / weak
- sendRawWorkflowHistory NOT implemented (Rust raw_history always empty; Go default false). Needs raw blob read path, request-header access, thriftrw transient decision serialization.
- DeleteDomain "Domain still have workflow execution history." check not ported.
- Several steps only assert "routed" (nil or typed non-Internal error) because Go behaviour is state dependent: DeleteDomain on deprecated domain, FailoverDomain, cancel responses, restart, diagnose.
- Admin APIs not covered: DLQ, queue, shard and replication admin, operational dynamic config, DeleteWorkflow, MaintainCorruptWorkflow, CloseShard. Thrift ResendReplicationTasks arm unrouted. CountDLQMessages and UpdateTaskListPartitionConfig are gRPC-only.
- No new unit test for admin refresh History delegation.
- Audit claim that the validator is never bound is stale; it is bound in production.
