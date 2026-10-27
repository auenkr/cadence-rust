# Matching lane progress (Claude session, master head 6af476941 era)
Master 6af476941 included: go-error-parity, runid, respond-query, domain-uuid, ignored-tests/poller, absent-query, nil-tasklist test, tchannel-tls (Go has none), deflake poll-expiration test, adaptive-scaler `changed` fix, durableRestart order-independent probe, mysql GC gate residue tolerance.
Lane results on 6af476941: mysql-probes 40195314790, thrift 40195809764, cassandra 40196298460, active-offer 40196794076, http-tls 40197288308, configstore 40197629016, postgres rerun 40204240682, mysql two-node rerun 40204660498 = SUCCESS.
**shard-distributor FAILS twice** (40198127804, 40205093375) with "after-sigkill: real leader loop did not expire executor(s) ... within 90s" (tests/matching-service-compat.cjs sdAssertExecutorExpired:189). It passed earlier on older head (40192102913, dce2ab05e). Cause unknown: regression from a later commit (bisect dce2ab05e..6af476941) or flake. OPEN.
Clean-target workspace clippy+tests: 40193747635 success (a791ed980); not rerun on 6af476941.
Binary SHA256s: only recorded from failed lanes in /mnt/storage/service/ci-evidence (not in repo). Still owed for every final-head lane (artifact binary.sha256).
Other open: Cassandra DescribeTaskList rate cause unconfirmed (no Go control run); adaptive scaler has no gate lane; isolation balancer/persistence path not audited.
Matching NOT complete.

## Pause checkpoint
- User paused work. Shard-distributor investigation worker (branch t3/mw-sd-expiry) was stopped before any commit/push; no WIP exists. Re-launch from the brief in this note: bisect dce2ab05e..6af476941 for the after-sigkill executor-expiry failure (runs 40198127804, 40205093375; logs in /mnt/storage/service/ci-evidence/l4, not in repo).
- Clean-target workspace CI 40428681163 on master head 1b9df63e4: in_progress  (at pause time).
- No other workers or gate lanes running. Still open: shard-distributor failure, final-head binary SHA256s for all lanes, Cassandra rate cause, adaptive scaler lane.
