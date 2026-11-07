# History gap: TransferActiveDependencies adapter (branch `history-gap-queuetransferactive`, 2026-11-07)

File: `rust/crates/cadence-history/src/wiring/assembly/queue_deps_transfer_active.rs` (registered in assembly/mod.rs).
Not wired into launcher.rs (item 5 of history-gap-queuedeps-20261107.md).

## Real
- `TransferActiveDepsImpl::new(env, cache)`; call `bind(&registry)` once the ShardRegistry exists.
- `get_or_create_workflow_execution_with_timeout` -> `EngineExecutionCache::get_or_create` (+ timeout -> ContextDeadlineExceeded),
  wrapped in `EngineActiveTransferContext` (idempotent release, Go `loadMutableState` stale-state logic, update as active).
- `EngineActiveMutableState` (ActiveMutableState + DecisionState) over `EngineMutableState`: execution info, activity/decision/child
  infos (from `Inner.pc_*`), request-cancel and signal infos (decoded from the loaded snapshot blobs), versions, completion/start
  events, `schedule_decision`.
- History client calls via the local `ShardRegistry::engine_for_workflow` (same as `RegistryHistoryClient`): start, signal,
  request cancel, terminate, remove signal, schedule decision. `get_current_execution` via `GoExecutionStore`.
- config accessors (parent close policy worker/threshold/batch size).

## Not done (TODO(link), explicit errors unless noted)
- `record_child_execution_completed`: task level `CloseEvent` is lossy; engine needs the full completion HistoryEvent.
- Mutations missing from `EngineMutableState`: child started / start-child failed / external cancel+signal (requested, failed) events;
  `child_execution_initiated_event` (decode of the initiated_event blob).
- `send_parent_close_policy_request`, `reset_workflow`, `current_version_history`; `find_auto_reset_point` returns None (auto reset disabled).
- `allow_internal` always true (WFCache), `advanced_visibility_writing_enabled` false (config prop missing).
- `get_child_execution_domain_id` returns the parent domain (child domain id not carried in task-level ChildExecutionInfo).
- Engine request types lack delay-start/jitter/cron-overlap for child start; `active_cluster_selection_policy` cluster attribute dropped.
- Multi-host: shards on other hosts surface ShardOwnershipLost.
- Caveat: request-cancel/signal infos are read from the load-time snapshot (engine state does not track them).

## Evidence
See bottom (filled after CI).
- rust-ci.yml package=cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/42341090019 head 3b09db344a1dd4a2cbbf55fee6d8c2423469e7e3 conclusion: success (clippy + tests; code head, memory-only commit follows).
