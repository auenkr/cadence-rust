# Matching lane progress (Claude session, master head 6af476941 era)
Master 6af476941 included: go-error-parity, runid, respond-query, domain-uuid, ignored-tests/poller, absent-query, nil-tasklist test, tchannel-tls (Go has none), deflake poll-expiration test, adaptive-scaler `changed` fix, durableRestart order-independent probe, mysql GC gate residue tolerance.
Lane results on 6af476941: mysql-probes 40195314790, thrift 40195809764, cassandra 40196298460, active-offer 40196794076, http-tls 40197288308, configstore 40197629016, postgres rerun 40204240682, mysql two-node rerun 40204660498 = SUCCESS.
**shard-distributor FAILS twice** (40198127804, 40205093375) with "after-sigkill: real leader loop did not expire executor(s) ... within 90s" (tests/matching-service-compat.cjs sdAssertExecutorExpired:189). It passed earlier on older head (40192102913, dce2ab05e). Cause unknown: regression from a later commit (bisect dce2ab05e..6af476941) or flake. OPEN.
Clean-target workspace clippy+tests: 40193747635 success (a791ed980); not rerun on 6af476941.
Binary SHA256s: only recorded from failed lanes in /mnt/storage/service/ci-evidence (not in repo). Still owed for every final-head lane (artifact binary.sha256).
Other open: Cassandra DescribeTaskList rate cause unconfirmed (no Go control run); adaptive scaler has no gate lane; isolation balancer/persistence path not audited.
Matching NOT complete.

## Bounded isolation assignment/persistence audit (2026-10-27)

At source `9e719ab6e6c96d8c64d4880868a18ea602bad2ef`, completed the bounded
Go/Rust production-path audit for isolation balancing, task attributes,
forwarding, and scoped configuration. No concrete observable mismatch found.
Production source map, persisted active/standby/forwarded/retry/reload trace,
existing test pointers, and remaining runtime-evidence checklist are in
`memory/matching-isolation-parity-audit-20261027.md`. This closes the audit
item only; parent still owns final-pin mixed evidence and Matching acceptance.

## Pause checkpoint
- User paused work. Shard-distributor investigation worker (branch t3/mw-sd-expiry) was stopped before any commit/push; no WIP exists. Re-launch from the brief in this note: bisect dce2ab05e..6af476941 for the after-sigkill executor-expiry failure (runs 40198127804, 40205093375; logs in /mnt/storage/service/ci-evidence/l4, not in repo).
- Clean-target workspace CI 40428681163 on master head 1b9df63e4: in_progress  (at pause time).
- No other workers or gate lanes running. Still open: shard-distributor failure, final-head binary SHA256s for all lanes, Cassandra rate cause, adaptive scaler lane.

## Resumed with three Luna workers, October 27

User explicitly requested three normal-speed Luna workers and continued Matching completion.
All three were launched with `gpt-6-luna`, medium reasoning, default service tier,
in isolated delegated worktrees: panic wire quoting, Cassandra UUID cache/metric/quota
context, and adaptive SDK retry failure from run40375459588. No local Cargo/Go
validation is allowed; parent owns serial mixed dispatch. No mixed gate started here.

The earlier shard-distributor failure entry above is historical: later recovery
run40218022772 passed on a5a8e2a14638efcc2d7f6cc5bd6ec027271978dd, as recorded
in matching-progress-20261026.md. Still repeat that lane on the final combined pin.
Do not spend another worker bisecting the historical failure absent a current repro.

Verified run https://github.com/auenkr/cadence-rust/actions/runs/40428681163 completed
success on1b9df63e42d760fac1bab3eb516ca06e1e23d1be. Inspected downloaded logs:
Clippy and workspace tests exit0; fresh-target selection ran, rust-cache was skipped.
Required runtime/OpenFeature and TLS/query formatting exits are0; advisory whole
workspace formatting is1. Compact proof is retained under
memory/evidence/matching-clean-workspace-40428681163/. This predates pending fixes
and does not establish final Matching acceptance.

Final acceptance remains the ledger's fresh workspace CI plus ten serial mixed
lanes on one immutable source revision. Gate uploads now exclude generated
cadence-server, shard-distributor-real and configstore-seed executables, retaining
runtime results, logs, source identity and binary hashes to limit artifact storage.
