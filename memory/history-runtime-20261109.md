# History runtime continuation — 2026-11-09

Base: master c42480cdc. History is incomplete. Rust Matching and Frontend are required in acceptance lanes; Go Worker remains.

## Work and findings
- Hard worker audit recommends a lossless owned-state bridge before replacing EngineMutableState; see history-mutstate-unification-plan-20261109.md. Preserve heartbeat/timer masks, opaque blobs, buffers, CAS, and transaction cleanup semantics.
- First Luna activity-status worker stopped with uncommitted partial work; replacement worker is completing it in the same checkout. Nothing from that slice is merged yet.
- First live-queue worker implemented only a fail-fast startup guard on t3/wire-history-queues, not executor assembly. Not merged. Split the actual construction into narrow config/domain adapters then processor/executor assembly.
- Added history-mixed-gate.yml and --rust-history-binary mode to the existing SDK harness: Rust History/Frontend/Matching and Go Worker only; supports MySQL/PostgreSQL/Cassandra lanes, two History hosts with SIGKILL/restart, gRPC/TChannel outbound selection, fresh CI target, binary hashes, process commands and source SHA evidence. This is validation infrastructure, not a passing acceptance claim.
- Existing Frontend gate mode still uses Go History/Worker. The new mode reuses the existing SDK suite, which must be expanded for History replication, conflict/failover, CAN/retry/cron, and persistence failure cases before full parity can be claimed.

## Validation
- Python AST parse and shell syntax checks passed locally; no Cargo/rustfmt/Go builds or tests run locally.
- Initial MySQL/gRPC single-host diagnostic gate dispatched: https://github.com/auenkr/cadence-rust/actions/runs/42567851631 on 6ba567bfd51302966c9c257c9764cdb4c5bf9578, observed in progress; no conclusion yet. Evidence identity: memory/evidence/history-runtime-20261109/mysql-grpc-initial.json.
- Final requirement remains fresh dedicated CI target, clippy followed by workspace tests, and full runtime/parity evidence. No History service completion is claimed.

## Activity timer status review
- Worker implementation 8d030b8d4 on t3/history-activity-timer-status adds timer mask tracking and duplicate/retry regression coverage; not merged pending CI.
- Review found an incompatible persistence extension: Go common/persistence/data_manager_interfaces.go explicitly marks LastHeartbeatTimeoutVisibilityInSeconds as NOT written to the database. Removed sqlblobs field 76 and restored zero on decode in 41d36eb8b3a695a9ecb23d0ffe73a0d06dbe4661. Tests assert persisted timer mask and transient heartbeat hint separately.
- Corrected-head CI: https://github.com/auenkr/cadence-rust/actions/runs/42568093309, observed in progress. No local Cargo/Go validation.
- New worker dispatch temporarily deferred by required load guard: host load 7.06 (>6), MemAvailable 3.7GiB at 11:05 IST. Existing CI work continues on runners. No resource limits changed.

## Next bounded implementation assignments
1. Queue policy adapter: new assembly/queue_policy.rs only plus mod export. Build queue::config::QueueConfig and shard_deps::ProcessorConfig from config::Config dynamic properties; implement queue::task_allocator DomainCache/ActiveClusterManager over HistoryDomainCache and existing active cluster manager. No live queue startup changes in this task. Needed by RealQueueShardContext constructor. Tests must establish dynamic reload and domain/error/cluster projection semantics.
2. Owned NDC state: follow task 1 of history-mutstate-unification-plan-20261109.md; real owned version histories, state/version/condition accessors and replay seam, no unsafe references. Keep unsupported operations explicit until subsequent codec/replay/cache slices are complete; no independent persistence/cache stack.
3. QueueExecCommon and executor assembly after queue policy: instantiate actual task processor/redispatcher, real per-shard dependencies, active and each enabled remote standby executors, bind shared registry. Wire into launcher only after constructed; keep lifecycle owned and stopped on shard release. Unsupported standby mutable-state behavior requires its own follow-up, not success no-ops.

## Queue configuration adapter
- Added assembly/queue_policy.rs with QueueConfig and ProcessorConfig builders over History Config callbacks. Callbacks retain live dynamic values/domain filters; threshold maps are converted from BTreeMap to HashMap on each read. No guessed defaults or captured startup values.
- Added regression coverage for dynamic batch/retry/threshold changes after queue adapter construction. CI-only validation pending.
- Allocator domain/active-cluster adapters and live executor provider are still open. This helper alone does not process tasks.

## CI results and runtime diagnosis (13:18 IST)
- Queue config adapter 882d6511b: success, https://github.com/auenkr/cadence-rust/actions/runs/42568401701 (cadence-history clippy/tests); merged/pushed master.
- Initial mixed gate 6ba567bfd: FAILURE, https://github.com/auenkr/cadence-rust/actions/runs/42567851631. Rust History/Frontend/Matching and Go Worker booted. SDK StartWorkflowExecution fails because handler_impl.rs still returns the historical engine-start gap even though engine/interface.rs now delegates to real implementation. Queue factory logs absent transfer/timer processors. Preserved binary hashes, source SHA, steps, SDK and History logs under evidence/history-runtime-20261109/initial-mysql-grpc/.
- Timer status CI 42568093309 failed on two fields incorrectly added to engine ActivityInfo projection. Engine already holds its timer data separately; removed those projection fields in efa4f3924. New CI: https://github.com/auenkr/cadence-rust/actions/runs/42581586431, pending. No timer branch merged yet.
- Host load recovered below 6; narrow Luna work can resume.

## Compaction checkpoint (13:20 IST)
- Resumed on clean master 55bfaccd5; progress dashboard now reflects actual failing mixed gate and incomplete runtime wiring.
- Timer correction head efa4f3924a25583c8b709fb558359fca4f046897 still in progress at https://github.com/auenkr/cadence-rust/actions/runs/42581586431.
- Resource check: load 0.11, MemAvailable 3.9GiB. Next independent Luna slices: Start/SignalWithStart handlers, queue allocator domain adapters, owned NDC state structural seam. No local Cargo or Go validation.

## Timer persistence validation (13:23 IST)
- Reviewed complete timer branch diff against c42480cdc. Heartbeat timer visibility remains runtime-only per Go; SQL blob roundtrip asserts no invented field 76. Typed snapshot retention alone is not a full persistence bridge.
- History CI 42581586431 on efa4f3924a25583c8b709fb558359fca4f046897: clippy succeeded, tests observed running.
- Separate cadence-persistence CI dispatched on the identical commit via t3/history-timer-persistence-validation (separate ref avoids cancelling History CI): https://github.com/auenkr/cadence-rust/actions/runs/42581990928. Conclusion pending; do not merge until relevant checks pass.
- Three normal-mode Luna implementation slices dispatched: handlers (medium), queue domain adapters (medium), owned NDC structural seam (high). Each owns a separate checkout and must commit/push and dispatch CI.
## Queue allocator domain adapters — 2026-11-09
- Added `wiring/assembly/queue_allocator_adapters.rs` and module export. It projects the real Matching metadata through `HistoryDomainCache::inner()` into the queue allocator fields, delegates active-active workflow selection to the existing `DomainActiveClusterManager`, and provides `new_history_task_allocator` as a narrow constructor helper. It does not change launcher/provider startup.
- Projection preserves global/local state, failover-end presence (pending-active), active-active presence, active cluster, deprecated/deleted status, info presence, and replication cluster membership. Missing failover/active-active fields stay absent. Unknown domain maps to `QueueError::EntityNotExists` (allocator skips it); other cache errors map to transient retry errors. ACM entity-not-found/internal errors are mapped to queue equivalents.
- Focused tests cover populated metadata projection, absent failover/active-active values, and positive/negative replication cluster filtering. CI is the only Rust validation path; no Cargo, rustfmt, or Go command was run locally. Tests for live cache unknown-domain/error mapping and the CI result remain outstanding at this checkpoint.
- Limitations: cache metadata's decoded status uses the API enum; unknown raw status currently has no separate allocator projection because allocator only carries a deprecated/deleted boolean. Active-active lookup still delegates to the existing History manager implementation, whose workflow routing semantics are not broadened here. No History completion claim.
- Requested CI dispatch after push: `rust-ci.yml`, package `cadence-history`; run https://github.com/auenkr/cadence-rust/actions/runs/42582457492, head `8c9c4b3c28903ef652090828238adf17262bdb2e`. Last observed state: `in_progress` (no conclusion available yet).
## Start and SignalWithStart handler routing
- Implemented `StartWorkflowExecution` handler dispatch to the real engine delegate. It preserves Go ordering: nil request, domain validation, rate limit, shard selection by nested workflow ID, engine call. Go does not add a handler shutdown check for start.
- Implemented `SignalWithStartWorkflowExecution` dispatch with Go ordering including shutdown, then domain/rate checks, shard selection, and a single retry only for persistence already-started/current-workflow-condition conflicts. Added focused coverage that accepts exactly those conflict classes and rejects duplicate/unrelated errors.
- The initial mixed SDK gate failed before exercising either engine operation because these handler methods returned explicit gaps. After this change the same SDK start path should reach the real engine; CI result will determine whether further engine/runtime blockers remain. No local Cargo or Go commands were run.
- Remaining limitations are explicit: engine start/signal-with-start semantics and their persistence/visibility/queue dependencies are implemented but not yet proven end-to-end on this branch. Existing mixed gate coverage is narrower than History parity; queues/NDC, replication/failover, other persistence backends, failure/restart/recovery and SDK workflow coverage remain incomplete. This bounded handler fix does not complete History.
- Handler-routing branch `t3/history-start-handler-routing`, code head `afd85ad232c91f8e15fb26007ee11028540ac706`; cadence-history CI dispatched on standard GitHub runner: https://github.com/auenkr/cadence-rust/actions/runs/42582354379. At dispatch observation (2026-11-09 07:55 UTC) status was `in_progress`, conclusion empty. CI compilation/test results are pending; no validation is claimed yet.
- After recording that dispatch, pushed evidence commit `b56aeebeb658150b18f6cd5ea2db2cd242b7bba4` and dispatched cadence-history CI again against the complete branch head: https://github.com/auenkr/cadence-rust/actions/runs/42582416352. At 07:55:56 UTC it was queued; a later observation showed `in_progress`, conclusion pending. The earlier run was obsolete and had completed before cancellation could be applied.
