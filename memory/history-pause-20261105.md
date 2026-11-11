# History migration paused — 2026-11-05

User requested saving progress and pausing immediately. No further migration, workers or validation should run until the user resumes.

## Saved state
- Master code head: `f760852a3b15c2b14ca664abd7cb73b3f39cbe2c` (assembly D1–D8 merged).
- Verified CI conclusion: success, https://github.com/auenkr/cadence-rust/actions/runs/41954599236 on that exact head. Passing compilation/tests does not establish service parity.
- Active Claude engine retry was cancelled and replaced by GPT-6.1 Sol, low effort, default service tier. Sol was then cancelled at the immediate pause request. Its checkout `/home/auenkr/.t3/worktrees/cadence-rust/t3-cc864b15`, branch `t3/history-engine-slice`, is clean: no unfinished edits or new commits to recover/merge.
- Latest four Claude gap workers failed with rate limits before implementation. Do not describe their work as completed.
- Existing worker branches and evidence are preserved; no unverified branch was merged for this pause.

## Status and remaining work
History is INCOMPLETE. Assembly and substantial policy/module ports exist; concrete runtime behavior and full Go compatibility are not established.
- Engine: start workflow, signal-with-start, lifecycle/failover callback and concrete TODO(link) adapters remain open (memory/history-gap-engine-20261105.md).
- Execution/mutable state: core builder/transaction/replay coverage, typed records and concrete context adapters remain incomplete. Earlier mutstate2 and execctx2 work was merged, but those workers explicitly reported partial coverage.
- Timer queue/executors: history-gap-timerq2 remains unverified/red in the prior checkpoint; quotas unimplemented stub and dependency shims require resolution.
- Decision: typed errors, semaphore API types, continue-as-new inheritance and validation/order differences remain (memory/history-gap-decision-20261105.md).
- Handler/startup/transports: complete function-level audit and concrete wiring still required.
- Required acceptance: Rust History + Rust Matching + Rust Frontend with Go Worker, existing SDK workflows, MySQL/PostgreSQL/Cassandra, failure/restart/recovery; fresh dedicated CI target, clippy then workspace tests, binary SHA256 and reproducible evidence. Not yet achieved.
- Matching/Frontend: prior acceptance evidence exists, but the deferred load-soak latency/signal visibility failure remains recorded in memory/evidence/fe-soak-ci-20261104.md. Do not infer flawless parity from crate CI.

## Resume
Read this checkpoint, history-port-audit-20261105.md, individual gap notes and actual branch ancestry before scheduling narrow slices. All Cargo/Go validation stays on GitHub Actions. No native goal object exists in this harness; pause is recorded here and enforced by stopping work.

## Update 2026-11-06
- Merged history-gap-execctx3 (b1ce3c03a, CI 42218036353 green): history_builder Add* audit, state_builder apply_events per Go. Remaining: state_rebuilder, workflow_repairer, integrity, cache, context, events cache/notifier (blocked on Go-shaped MutableState); Semaphore/RequestCancelActivityTaskFailed API types missing.
- Merged history-gap-misc (e820fc717, CI 42218039455 green): ported DynamicRateLimiter + quotas.Collection (removed the only unimplemented!). Notes in memory/history-gap-misc-20261106.md.
- Master now 7ede814ce; merge CI pending. Other 2026-11-06 workers (timerq3, engine3, handler2, decision3) not merged.
- history-gap-handler2 (0d3d30e41) NOT merged: enables wiring persistence/resources/assembly modules and adds handler/handler_impl.rs; CI 42218488492 unverified at pause. Gaps in memory/history-gap-handler-20261105.md (Start/SignalWithStart need engine methods; replication/failover/RemoveTask/ReapplyEvents/RatelimitUpdate/cross-cluster RPCs return gap errors; lookup, service.go, real main.rs, wrapper audit not started).
- Merged history-gap-timerq3 (df62bd8ac; code head CI 42218452225 green, last commit memory-only): queue/factory.rs + common.rs; master timer processors kept. Timer task executors still partial (memory/history-gap-timerq3-20261106.md).
- Merged history-gap-decision3 (bd802889c; code head ba65e607c CI green: history 42220071139, api 42221059615): typed errors, CAN ASCP/domain lookup, AcquireSemaphore decision. Gaps in memory/history-gap-decision3-20261106.md.

## 2026-11-06 20:05 IST pause (session 2)
- Merged to master: startup (6050c9e), mutstate3 (bf91786, typed TypedMutableState, Add*/Replicate* methods NOT ported), engine4 (e41c88a, fixes engine_cache/engine_state/engine_factory compile errors; CI green on a1bf430).
- Master CI after engine4: run 42234625308 (see result below).
- Unmerged: history-gap-apitypes (head e5880ca, CI pending/unverified; worker hit API rate limit),
  history-gap-transferexec (head 318362c, CI rerun 42234619633; audit incomplete).
- Worker mutstate4 (Add*/Replicate* methods from mutable_state_builder_methods_*.go) failed on rate limit before any commit; branch deleted. Relaunch next session.
- Next: port Add*/Replicate* methods, real queue factory (visibility writer + archival client), TLS, dynamic config backends, grpc/thrift wrapper audit; then mixed-cluster gate.
- 20:20 update: master CI after engine4 GREEN (run 42234625308). Merged apitypes (0d6f6a4, CI green per crate on code heads) and mutstate4 (CI 42235261718 GREEN after merging master): typed Add*/Replicate* for timer, timedout, completed, cancellation, common. Still unported: activity, child_workflow, signal, started, decision, semaphore methods; task generation/events cache/insertWorkflowRequest in ported methods.
- history-gap-transferexec NOT merged: CI rerun 42234619633 FAILED (needs master merge + fix); audit incomplete.
- engine4 remaining gaps: visibility writes/orphan branch cleanup no-op, request dedup rows, replicate_failover_markers no-op, no DLQ failover processor, NewEngine/Start/Stop/createMutableState + engine/interface.go not ported, TODO(link) facades in deps.rs/runtime.rs/types.rs.
- apitypes gaps: ParentWorkflowDomainID/jitter_start_seconds lack Thrift fields; state_builder reads parent_workflow_domain not _id; no round-trip tests for new converters.

## 2026-11-07 pause (11:25 IST, at user request)
Merged today (all CI green on code heads; master CI dispatched after each, latest run 42344423196 on transfermut merge):
transferexec, queuefactory, ms-childsignal, transferexec2, engine5, ms-activity, ms-decision, taskinfra, ms-taskgen, engine6,
queuedeps, ms-started2, ms-taskwire, apitypes2, queuestandby, queuetimeractive, engine7, startup2, queueexecrows,
queuetransferactive, handlerfailover, ndcadapters, timermut, ms-taskwire2, archwriters, transfermut. No open worker branches.
Per-branch details: memory/history-*-20261107.md.

KEY DESIGN DECISION FOR NEXT SESSION: three mutable-state types exist — engine `EngineMutableState` (used by queue adapters,
timer/transfer mutators), `TypedMutableState` (execution/, ported Add*/Replicate* + task gen), replay
`mutable_state::MutableStateBuilder` (StateBuilder/NDC). Go has one. Unify (likely make engine use TypedMutableState)
before finishing NDC adapters (TODO(ndc-adapters) stubs) and wiring queues live.

Remaining gaps:
- Queues not live: `MissingExecutorsProvider` in queue_factory; no real QueueExecProvider in launcher.rs.
- Timer adapter: retry_workflow / update_workflow_execution_with_new_as_active TODO(link) (needs continue-as-new run creation).
- Transfer adapter: send_parent_close_policy_request, reset_workflow error; allow_internal always true.
- NDC: execution cache/mutable state/state builder/rebuilder/txn manager/new_mutable_state/context hooks stubbed; standby delay hardcoded 5m.
- Task gen: AddContinueAsNewEvent, reset tasks + buffered flush in close, task_id/visibility 0 until shard assigns, activity timer status not stored (dup tasks).
- Archival: HistoryBatchSource over history store, s3/gcs writers, archival signal client, dynamic-config rates.
- Replication clients to remote clusters dial without TLS. History DLQ manager. OpenFeature DC.
- Then: mixed-cluster gate + parity audit.

## 2026-11-11 pause (13:30 IST, at user request)
Workers: hard = Sonnet 5.5 low, simple = gpt-6-luna medium, 3 parallel. All workers finished; no open worker branches.
Merged today (each CI green on its code head; per-branch notes memory/history-*-20261111.md):
parallel-task-processor, owned-mutstate (plan 1), mscodec (2a), typed-replay (3a), queueexec-assembly (live queue executors),
msoverlay (2b), ndccache (4b), typed-replay2 (3b partial), mstxn (4a), gate-fix1. Latest master ccc351922, CI run 42983617639 (pending at pause).

Mixed gate (Rust History+Frontend+Matching, Go Worker, MySQL/gRPC): still FAILS, run 42979556819 (27/144 steps).
start-echo and start-child now pass. FIRST BLOCKER: RespondDecisionTaskCompleted not ported —
engine_decision.rs handle_decision_task_completed TODO. Needs Go decision/handler.go HandleDecisionTaskCompleted,
task_handler.go command processor, checker.go, plus engine mutable-state methods for activity/timer/child/completion events (~3k Go lines).
Split into several worker tasks next session.

Other open gaps: 3b leftovers (marker/search-attr/semaphore storage in typed state, CAN new-run state, NDC builder TODO);
typed replay emits empty workflow-request payloads (4a now rejects them); task-ID allocation coverage at shard boundary;
plan 5a/5b/6/7/8 not started; hierarchical WRR scheduler not ported; reinjector unbound; timer standby load_workflow,
timer retry/continue-as-new TODO; NDC workflow persistence + terminate/failover/schedule_decision explicit errors.

## 2026-11-11 resume progress (21:45 IST, master f3e7f821b)
- Merged: history-ndcbuilder, history-ndctxn, history-gate-fix2, history-gate-fix3, history-gate-lanes, history-ndcgaps, history-decisiongaps (see each memory/history-*-20261111.md).
- Mixed gate green: mysql/grpc/false+true, mysql/tchannel/false, postgres/grpc/false+true (memory/history-gate-lanes-20261111.md).
- CI fix e63ced4ba: rust-ci/gate concurrency groups now unique per package+SHA for dispatches (parallel runs no longer cancel each other).
- In progress (1 worker): history-cassandra (Cassandra shard + execution stores done; v2 writers, visibility writer, env.rs wiring, gates pending).
- Open: local activity dispatch; advanced-vis search-attr task after binary checksum; replication notify_tasks; resetter/NDC unit tests; inter-host gRPC TLS; parent-close-policy worker; parity audit; final clean CI.
