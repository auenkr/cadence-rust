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
