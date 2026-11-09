# Mutable-state consolidation / NDC unblock plan — 2026-11-09

Audit base: `c42480cdc` (checkout HEAD when inspection began). Research only: no Rust edits, no Cargo/Go commands, no validation run. Read AGENTS.md, the latest 2026-11-07 pause section, history-gap-mutstate{,2,3,4} notes and ndcadapters notes, then the implementations below. Earlier inventories are historical: method-name matches and earlier green crate CI are not parity evidence.

## Decision

Do NOT replace EngineMutableState wholesale with TypedMutableState now. Consolidate the **representation and persistence boundary first**, use an exclusively owned NDC working state under the existing engine cache lock, and migrate replay onto typed Replicate* operations family by family. Retain engine timer/transfer/event/task methods until differential and mixed gates prove their replacements. One authoritative mutable instance per locked workflow operation; never maintain three live copies with write-through synchronization.

The long-term target is typed state plus thin engine/NDC facades, not permanent JSON round trips. A shared lossless envelope/codec is needed now; a single universal trait and complete algorithm unification are not prerequisites for NDC. A bridge that only serializes ExecutionInfo or calls engine `copy_to_persistence` is insufficient.

## Actual surfaces and incompatibilities

Paths below are relative to `rust/crates/cadence-history/src/` unless otherwise qualified.

- `engine/deps_ms.rs::MutableState`: Send+Sync; mutation through `&self`, async event reads, owned projections, query registry, optional Arc downcast. `ExecutionContext` is async and has a separate downcast hook. Queue adapters require concrete `Arc<EngineMutableState>`, not just this trait.
- `wiring/assembly/engine_state.rs::EngineMutableState`: `Mutex<Inner>` plus original `pc::WorkflowSnapshot`, domain, queries, clock and StateParams. Inner contains engine and persistence execution views, mirrored decisions, full persistence activities plus separate heartbeat map, timer status, child views, CAS (`db_next_event_id`), buffers, deltas and tasks. Original snapshot is not current state. `mirror_decision` and `refresh_activity` maintain projections.
- `engine_events*.rs`, `engine_decision.rs`, `engine_timer.rs`, `engine_tasks.rs`: active event generation, retries, buffered events, concrete transfer/timer tasks, timer status persistence. `engine_events_transfer.rs` uses snapshot cancel/signal membership plus deleted-ID overlays. `queue_deps_transfer_active.rs` reads cancel/signal records from the original snapshot; a changed new record is invisible there until this is migrated.
- `engine_close.rs::close_transaction(now, active)` returns ClosedTxn; active flushes buffers and generates timers; false **rejects any new history**, so false is tasks-only, not NDC passive replication. `finish_transaction` clears deltas only after successful persistence. Engine close presently emits only a subset of pc mutation fields; do not treat it as a complete canonical serializer.
- `engine_cache.rs::persist`: assigns event task IDs, appends history, allocates task IDs/visibility, submits mutation + optional new snapshot; calls finish only on success. It chooses UpdateCurrent versus IgnoreCurrent and always WorkflowRequestMode::New. This is not a generic NDC passive/conflict transaction API. `update_with_new_as_active` accepts a snapshot, but does not itself append/allocate the new run's history/tasks.
- `execution/mutable_state_builder.rs::TypedMutableState`: public owned maps, typed Add*/Replicate* in mutable_state_methods_*.rs, full pc WorkflowExecutionInfo, generated task vectors/environment. `load` rebuilds indexes but does not install every transaction/version field; callers must initialize current version/condition intentionally. Typed snapshot lacks persisted buffers, heartbeat side map, semaphore records, checksum and workflow_timer_tasks. Typed ActivityInfo (`mutable_state_activity.rs`) lacks pc timer_task_status, embedded event blobs/encodings, task_list_kind and failure category. Shared representation must retain these, not default them away.
- Typed `close_transaction_as_mutation/snapshot` drains events/tasks and calls cleanup immediately; it also generates timers without a policy argument. `close_transaction_handle_activity_user_timer_tasks` calls task generator with time 0. These methods cannot be dropped directly into a retrying persistence context. `insert_workflow_requests` and `workflow_requests` are distinct; started replication queues the former with default data while close exports the latter and clears both.
- `mutable_state.rs::MutableStateBuilder`: JSON Record execution/pending maps, separate next-event counters, decision copy, semaphore_state, checksum; `execution/state_builder.rs::StateBuilder` owns this concrete type. `apply_events` directly edits JSON, updates version history, calls replicate_event, dispatches all families including semaphore, and sets batch boundary fields. It accepts only one batch; no domain/request/execution/new-run inputs like NdcStateBuilder. Replacing the type alias will not compile or preserve behavior.
- `ndc/mod.rs::NdcMutableState`: `&mut self` operations, **borrowed** version histories (including mutable reference), no downcast or replay capability hook. A Mutex-backed EngineMutableState cannot safely implement these getters. `NdcWorkflow` also returns borrowed state and owns replacement/release/context operations. Do not leak references, retain a mutex guard across awaits, or introduce unsafe lifetime casts.
- `wiring/assembly/ndc_adapters.rs`: Todo cache/workflow creation, builder, rebuilder, transaction manager and new_mutable_state. Existing request conversion/deserializer/cluster logic are usable. `NdcReplicator` runs sync algorithms in spawn_blocking; `wiring/persistence/engine_ndc.rs::block_on` is the existing async-store bridge. Keep runtime handle access explicit and test this execution route.
- Existing usable infrastructure: `execution/context.rs::{TransactionState, WorkflowContextStore, WorkflowExecutionContext}`, `execution/state_rebuilder.rs::{StateRebuilder, RebuildContext}`, `ndc/transaction_manager.rs::TransactionManagerImpl`; `engine_ndc.rs::{SqlHistoryPager, SqlWorkflowContextStore, SqlTransactionManagerEnv, WorkflowContextHooks, TransactionManagerCollaborators}`. These are collaborators/contracts, not completed concrete state implementations. Do not create another independent persistence/cache stack.

## Representation contract and adapters

Proposed new `execution/mutable_state_bridge.rs`: an owned `StateEnvelope` containing typed working state plus persistence-only residuals, original keyed blobs/encoding where necessary, version metadata, heartbeat/timer status and transaction preparation metadata. Implement explicit field mappings against `cadence-persistence/src/history_contract.rs`, not serde conversion of unmatched structs. Preserve untouched keyed blobs byte-for-byte; overlay known modified fields on decoded records. Unknown fields of modified blobs require an explicit merge policy/test, not blind re-encoding. The envelope is a transitional representation, not a fourth mutation engine.

Proposed `wiring/assembly/ndc_state.rs`: owned envelope implementing NdcMutableState and TransactionState; lends real owned VersionHistories. Add a narrow **object-safe replay operation** to NdcMutableState (domain/request/execution/batch + optional new-run handling via builder) or another explicit capability contract; existing mock implementors must be updated. Avoid mandatory Any/downcast as the application algorithm. Engine stays Mutex-backed; NDC acquires its same cache lock, exports current clean state once, owns mutations, persists, then invalidates engine context before unlocking. Never rehydrate from stale snapshot after an engine dirty transaction. Reject dirty checkout explicitly until a tested export of all live deltas exists.

Both paths use pc snapshot/mutation wire representations and existing shard/context machinery. New owned state is not published to an Arc used by queues; on success invalidate/reload, preserving query registry behavior by retaining the context/query owner. On error clear cached mutable state and discard the working copy; release guard exactly once, including panic. No separate SqlExecutionCache alongside EngineExecutionCache for the same workflows.

## Dependency-ordered worker briefs

Each brief is a narrow Luna-sized change; split further rather than assign the whole table to one worker. No live queue activation until all required policy/transaction pieces pass. Exact proposed new names below are design suggestions, not existing symbols.

### 1. Remove the NDC state ownership blocker (first structural unblock)

Scope: new assembly/ndc_state.rs; ndc/mod.rs capability seam; assembly/ndc_adapters.rs::HistoryReplicatorEnv::new_mutable_state. Implement owned typed working state with real version-history references, version/condition/state getters and construction with domain/clock/task-gen parameters. Add the replay capability signature and explicit errors for unsupported operations; do not replace all Todo components or claim replication works. No engine/queue method rewrites. Acceptance: new_mutable_state returns a concrete state; version-history edits are observable through getters; domain/run initialization and CAS defaults match Go; no unsafe references; existing NDC mocks compile. This unlocks subsequent builder/cache work immediately, but is explicitly not an operational NDC gate.

### 2a. Lossless persistence load/export codec

Depends 1. Scope: proposed execution/mutable_state_bridge.rs; engine_state.rs::from_snapshot factored codec only; pc history_contract/sqlblobs readers. Implement snapshot-to-envelope and unchanged-envelope-to-snapshot, retaining all keyed blobs, metadata, heartbeat, buffer encoding, semaphore/checksum/workflow timer task records. No event behavior changes. Acceptance: populated non-default Go-shaped fixtures round-trip all fields and opaque payloads; malformed encodings error, never `.ok()`-drop; activity schedule ID/timer ID come from keys; current/start/last-write versions survive independently. Include multiple buffered blobs and non-current version branches.

### 2b. Changed-record mutation overlay

Depends 2a. Scope: bridge converters for typed mutation/snapshot, activity/timer/child/cancel/signal deltas. Map timer task_id storage convention explicitly to task_status; retain pc-only activity fields on updates. Preserve keyed deletion/rewrite semantics, heartbeat and workflow-request rows. Acceptance: change one field in each family, persist/reload, prove all other fields retained; delete final map entry versus absent rewrite is covered against actual persistence writers. Do not infer rewrite behavior from field names.

### 3a. Typed replay dispatch: start, decision, activity

Depends 1; independent of codec until fixture integration. Scope: execution/state_builder.rs new typed replay entry, execution/mutable_state_methods_{started,decision,activity}.rs. Preserve legacy entry for existing callers. Port ApplyEvents dispatch using typed Replicate*, incoming timestamps/versions, first batch ID, supplied domain/request/workflow/run identity; no Add* calls or new event IDs. Account for Replicate* task-generation side effects under passive policy. Acceptance: replay multi-batch start/decision/activity/retry histories; exact event IDs/task IDs, decision attempts and pending indexes; no duplicate append or active-only task generation. Compare Go expected state, not just legacy output.

### 3b. Typed replay dispatch: remaining families and NDC builder

Depends 3a. Scope: state_builder.rs timer/child/cancel/signal/marker/close/semaphore arms; assembly/ndc_adapters.rs::TodoNdcStateBuilder replacement. Preserve unknown-event errors, parent-domain-ID precedence, clear sticky semantics, next_event_id and last_first_event_id. Typed state currently has no semaphore storage: bridge/persist semaphore state before accepting these events. Implement continued-as-new replay close transition and optional new-run **independent state**, validating run linkage and branch version. Acceptance: event-family fixture matrix, missing attributes/unknown types, terminal-event restrictions, continued batch + new-run batch; old/new states do not alias. No need to implement active AddContinueAsNewEvent in this task.

### 4a. Transaction prepare/commit separation

Depends 2b. Scope: typed builder close/cleanup and new ndc_state TransactionState implementation; execution/generated_task_encoding.rs. Introduce policy-aware preparation on a working copy or immutable prepared transaction; cleanup only after commit (or discard entire exclusively owned working state on failed context). Passive permits incoming history but must not flush active buffered events or run active-only generators. Keep engine_close unchanged initially. Carry persisted CAS separately from next-event ID. Acceptance: injected history append/SQL failure, retry/possibly-successful write and encoding failure leave recoverable events/tasks/CAS; passive/active policy fixtures; allocate task IDs once and update both wrapper and encoded task blob. Reconcile insert_workflow_requests with exported workflow_requests and real encoded request type/version; no empty dedup data accepted silently.

### 4b. Shared cache lock and NDC workflow shell

Depends 1,2a. Scope: engine_cache.rs::{acquire,load_state,clear}, proposed ndc_workflow.rs, TodoNdcExecutionCache replacement. Own ReleaseFunc and concrete context, checkout clean envelope while holding same workflow lock, borrowed NDC state comes from owned box. Implement clear/replace/set_history_size/load plus workflow vector/suppress/revive using execution/workflow.rs policy, not invented behavior. Persistence methods can remain explicit errors until 5. Acceptance: concurrent engine/NDC acquisition serializes; error and panic release once; clear/reload sees persisted state; dirty checkout fails without discarding engine changes; query waiters do not hang on invalidation. Keep current-run resolution and lock order aligned with engine cache; don't recursively acquire held lock.

### 5a. NDC context persistence and new-run transactions

Depends 3b,4a,4b. Scope: concrete WorkflowContextHooks for SqlWorkflowContextStore in engine_ndc.rs/assembly; NdcWorkflow persist/create/update/passive methods. Delegate ordering to execution/context.rs; support explicit UpdateWorkflowMode/WorkflowRequestMode, append incoming history preserving source event task IDs, allocate local generated queue task IDs through shard, notify only in correct order. Handle both old mutation and new snapshot/history/tasks, ownership loss, CAS and possibly-successful retries. Acceptance: incoming single-run history persists/reloads; duplicate replication is idempotent; missing prefix yields RetryTaskV2; continued-as-new writes both histories and atomic mutable-state/current-run update; injected append/CAS/shard-loss failures recover. Engine persist(false) must NOT be called as a shortcut.

### 5b. SyncActivity bridge

Depends 2b,4a,4b,5a passive update. Scope: ndc_state NdcMutableState::{replicate_activity_info,create_next_activity_timer,generate_activity_retry_tasks}; activity_replicator.rs unchanged policy. Carry version/attempt/heartbeat, reset timer masks only when requested and persist masks with timer tasks. Acceptance: stale/equal/new versions, attempt ordering, heartbeat ordering, missing activity, retry timer and restart dedup. Audit engine request conversion for fields absent from current local SyncActivityRequest; transport parity is not implied by this adapter test.

### 6a. Rebuild/conflict state adapter

Depends 3b,4a,5a. Scope: TodoNdcStateRebuilder -> StateRebuilder + SqlHistoryPager + concrete RebuildContext. Implement branch paging/fork callback, final event/version validation, history size, passive snapshot, refresh tasks and timestamp order. Acceptance: multiple pages, exact end-of-batch and middle-of-batch rejection, branch switch, no copied old-run CAS, refresh tasks once. RebuildContext::MutableState is generic: use owned envelope, do not convert back through JSON.

### 6b. Transaction collaborators and reapplication

Depends 5a,6a. Scope: TodoNdcTransactionManager -> TransactionManagerImpl/SqlTransactionManagerEnv + TransactionManagerCollaborators; EventsReapplier concrete impl; WorkflowContextHooks reapply/merge/notify. Use actual ASCP/domain active-cluster selection, reset collaborator and durable event-reapplied resource IDs. Acceptance: new/existing/zombie/suppressed/current workflows, conflict resolution with third current run, backfill, reapply dedup across restart, release on every error. Engine is_resource_duplicated currently always false: do not reuse it. Split reset collaborator into a follow-up narrow worker if full reset implementation is absent; keep failing path explicit until done.

### 7a. Active continue-as-new transition

Depends 2b,4a; can follow replay work independently of conflict gate. Scope: execution/mutable_state_methods_completed.rs missing AddContinueAsNewEvent; reuse mutable_state_methods_started.rs::add_workflow_execution_started_event_for_continue_as_new, history_builder.rs::add_continued_as_new, decision/task_handler.rs::retry_cron_continue_as_new_attributes and checker inheritance rules. Return an explicit old-run transition + new owned state/history, not just snapshot. Handle parent domain ID/name, ASCP blob, retry attempt/expiration versus cron reset, first-run chain, memo/search/header/partition/reset points, backoff and first decision tasks. Acceptance: decider/retry/cron table, cancellation and invalid-attribute order versus Go, no early close/persist on failure. The existing new-start helper is substantial but does not constitute complete CAN orchestration.

### 7b. Engine/timer new-run wiring

Depends 7a,5a shared transaction preparation. Scope: queue_deps_timer_active.rs::{retry_workflow,update_workflow_execution_with_new_as_active}, engine_cache.rs::update_with_new_as_active/persist, decision concrete continue-as-new operation. Store pending new-run transaction in operation/context owner (not global cache), append/assign both runs' tasks/history and commit atomic old/new state through context machinery. Preserve engine_timer workflow_retry_backoff/workflow_cron_backoff and existing timer/transfer mutators. Acceptance: timeout retry, cron after timeout/completion, decision CAN; restart between append and SQL write; competing current-run CAS; no duplicate backoff/close tasks; existing SDK follows new run and parent relationships survive.

### 8. Incremental engine facade consolidation, then live gates

Only after above bridge proves lossless. Move one family at a time (timer status/sequence, transfer child/cancel/signal, activity, decision/start/close) from Inner mutation algorithms to typed state, retaining public engine/deps_ms and queue adapter contracts. Replace snapshot cancel/signal readers with authoritative pending maps. A family worker owns its converter and differential fixture updates; central engine_state/close/cache changes stay serialized. Delete legacy replay facade only after all StateBuilder/repairer/integrity/cache/context callers migrate; never delete unported semaphore or dedup behavior. Activate QueueExecProvider separately after real dependency audit.

## Conflicts and sequencing

One owner at a time for engine_state.rs, typed builder fields/close, ndc/mod.rs traits and assembly/ndc_adapters.rs constructor wiring. Family replay workers may own separate methods files but coordinate dispatcher edits through one integrator. 2a/2b must settle representation before cache/export wiring; 4a must settle policy/cleanup before any persistent NDC update; 7b cannot safely use the current optional-snapshot-only helper. Existing engine tests remain regression requirements, not implementations to replace by mocks. No broad formatting/refactors in these slices.

## Data-loss / semantic hazards to block in review

1. Loading original engine snapshot after heartbeat/timer/transfer mutation loses new data; original cancel/signal reads can resurrect a completed record.
2. Losing activity timer masks, embedded events/encodings, failure category or task-list kind when converting to smaller ActivityInfo; heartbeat details/time are a separate SQL map.
3. Confusing timer `task_id` (engine convention for status) with generated queue task ID; integer narrowing and seconds/nanoseconds conversions must be explicit.
4. Dropping semaphore state, checksum, workflow timer keys, all version-history branches, buffered-event blob boundaries or unrecognized blob fields.
5. Mismatched next-event counters and CAS; replay records last_first_event_id at batch boundaries, not per event. Empty constants -23/-24 differ from default 0.
6. Clearing deltas before durable success; draining tasks before encode succeeds; assigning fresh source history task IDs on replication; losing generated tasks or wrapper/blob ID disagreement.
7. Active flush/generation during passive replay; duplicate timer creation because typed activity model cannot persist masks; time 0 used as real visibility timestamp.
8. New-run snapshot without its history/tasks or without an atomic current-run update; resetting retry expiration or first-run chain; omitting ASCP/reset points/parent domain.
9. Losing request/event-reapplied dedup rows or replacing encoded row data by defaults; engine dedup=false is not a safe adapter.
10. New independent cache locks permit divergent writers; query registry lifetime is process-local and must not be serialized/replaced casually. Deadlock if sync block_on is invoked directly on an async runtime worker or locks span awaits.

## Acceptance and evidence policy

All implementation workers: CI only for Cargo (including formatting), clippy then relevant tests via rust-ci.yml on their branch; no local Go validation. Start with cadence-history package; final fresh uncached dedicated target on standard ubuntu-latest, clippy then workspace tests. Record exact head SHA, run URL, conclusion and mixed binary SHA256 in memory. No new validation evidence was generated by this audit.

Fixture tests must compare full persistence snapshots/mutations, tasks and histories against Go behavior; legacy/typed agreement alone can share the same bug. Required mixed gates after adapters: Rust History with the accepted other services, original Go Worker and existing SDK; initial replication, sync activity, duplicate/out-of-order/gap repair, branch conflict/failover/backfill/reapplication, CAN/retry/cron, crash/restart/CAS/ownership recovery; MySQL/PostgreSQL/Cassandra. Use original Go counterparts where isolating Rust History parity requires them. CPU-heavy gates/builds on GitHub Actions; only approved low-CPU already-built soaks local in cadence-rust tmux with work-safe/nice/ionice and resource guards. No service-completion claim until reproducible full parity audit.

## Open questions for implementation audit (not reasons to block task 1)

- Exact SQL/Cassandra semantics for empty rewrite lists, sentinel writes, checksum invalidation and workflow_timer_tasks need field-level verification against writers before 2b completion.
- Should persistence-only activity fields move into the typed ActivityInfo itself or remain residuals? Start residuals to avoid widespread API churn; move fields when that family's operations become authoritative.
- Durable event-reapplied resource encoding and request type/version encoding need a Go fixture; current started insert_workflow_requests path is insufficient.
- Is the reset collaborator complete enough for conflict/backfill reapplication? Keep this branch explicit and audit reset modules before promising operational conflict resolution.
- Current NDC SyncActivity local request is a projection; enumerate omitted wire fields before declaring transport parity.
- Query ownership after state invalidation needs a concrete cache test; preserving only serialized state is insufficient.
