# Progress checkpoint 2026-10-21 (paused to respect usage limit)

Codex limit reached; all workers now Claude Haiku 5.5 / Sonnet 5.5. All workers stopped at pause.

## Branches on origin (all pushed)
- t3/matching-integration-20261021 @ see `git rev-parse origin/...` : Haiku integration of telemetry+MySQL, PG, Cassandra, SQL multi-DB. NOT verified (no clippy/gate result accepted).
- codex/matching-postgres-parity @ 10d040909 : PG gate WIP; full mixed PG gate NOT passed.
- t3/matching-cassandra-protocol @ 82dc7e32a : non-v4 CQL protocol rejected with clear error; v1-v5 support needs driver work (known limitation).
- matching-dynconfig-ratelimit-20261021 @ b696117c0 : WIP port of 15 missing dynamic-config keys, host rate limiter, tasklist type mapping. Untested unless its commit says so.
- matching-metrics-gaps-20261021 @ dbf380602 : WIP for 14 missing Matching metrics. Untested unless its commit says so.
- audit/matching-go-parity-20261021 @ 956fb5d21 : audited OLD master; many gaps stale.
- audit/matching-matcher-20261021 @ 104260be6 : 8 Go-vs-Rust matcher/tasklist differences (backlog forward to parent, cross-host partition config notify, adaptive scaler remote metrics, CancelOutstandingPoll semantics, query-without-poller fast fail, forwarder degree clamp, timers, shutdown codes).
- audit/matching-metrics-20261021 @ 6b3a837f5 : 14 of 93 Go metrics missing; tag parity unchecked.

## Next
1. Review/merge integration branch; run workspace clippy + MySQL/PG mixed gates with real exit codes.
2. Merge dynconfig + metrics WIP branches, test.
3. Fix matcher audit items 1-5 (SDK-visible first); verify item 5 against Go.
4. Redo parity audit on the merged branch. Cassandra non-v4, per-DB mixed gates, recovery evidence.
5. Root checkout has user's uncommitted WIP (lib.rs, matching_registry.rs, matching_runtime.rs, matching_shard_processor.rs, AGENTS.md): untouched.
Matching is NOT complete; later services untouched.

## Update 2026-10-22 (saved WIP, unverified unless stated)
- t3/matching-integration-20261021 @ f17c25d95: dynconfig + metrics-gaps merged; evidence in memory/evidence/matching-integration-20261022/ (confirm final clippy/test ran on this head).
- matching-go-parity-fixes-20261022 @ a478cd639: items 4,5,6,8 fixed; based on pre-merge c32a31e91, needs rebase onto integration.
- matching-backlog-forward-20261022 @ 6676a9684: WIP (ForwardError only; backlog leg unfinished).
- matching-partition-config-20261022 @ 2165ed645: WIP items 2,3,7.
- matching-grpc-tls-20261022 @ c328e357f: WIP, never compiled.
- Audits: audit/matching-parity-v2-20261022 @ 54f3688f1 (P0: mixed-cluster proof, gRPC TLS, sync-match config), audit/matching-verify-20261022 @ 0e4e65e2d, audit/matching-forward-plan-20261022 @ 9f306dca5.
- Host has 2 CPUs: run one cargo job at a time (load hit 22 with parallel builds).
