# history-lastgaps (2026-11-12)

Branch `history-lastgaps` (from origin/master 503889cb1). CI-only validation.

## Ported
1. Handler counters (handler/handler_impl.rs):
   - `EmptyCompletionDecisionsCounter` (scope HistoryRespondDecisionTaskCompleted) when decisions are empty.
   - `CadenceErrNonDeterministicCounter` (scope HistoryRespondDecisionTaskFailed, tag `domain`, `_unknown_` if lookup fails) on cause UnhandledDecision, plus the info log.
   - `ReplicationMessageTooLargePerShard` (scope HistoryGetReplicationMessages, tag `shard_id`).
   - Context tags: Go `GetContextTags` is never set outside tests, so nothing to port (no-op).
2. Queue/task metrics gap list: Rust uses Go identifier strings, comparison of go/service/history/queue + task against Rust found everything present except `DataInconsistentCounter`, emitted from Go `mutableStateBuilder.logDataInconsistency` (execution package, scope WorkflowContextScope, ~8 call sites). Out of queue/task scope; still NOT ported.
3. Ratelimit aggregator metrics (ratelimit_aggregator.rs, scope GlobalRatelimiterAggregator): Initialized/Reinitialized/Updated/Decayed, LimitsQueried/HostLimitsQueried/RemovedLimits/RemovedHostLimits.
4. Tests (wiring/assembly/real_engine_tests.rs): per-domain consistent-query-not-enabled; start-child Abandon (0) / Terminate (2) asserting decoded child info (`MemExecutions::child_infos` decodes the blobs). `TestConfig.no_consistent_query_by_domain` added.
   - NOT done: active-active query variant (TestQueryWorkflow_DecisionTaskDispatch_Complete_ActiveActive). Harness is prepared (`Harness.domain_active_clusters`, TestDomains returns active_clusters) but no test written; `EngineStartHost::active_cluster_info_by_cluster_attribute` ignores the attribute and uses the first entry, which would need a real port first.

## Evidence
- rust-ci (package=cadence-history) success: https://github.com/auenkr/cadence-rust/actions/runs/43187050844 head 99276d26e98ea60253f5435945b610ed689b469f
- Gate runs: see bottom (filled after dispatch).

Final head a4df1c976d69526619bc818b4132329f568dfa83 (after merging origin/master):
- mysql/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43188363694 success, GATE SUMMARY failed: []
- cassandra/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43188368774 success, GATE SUMMARY failed: []
- postgres/tchannel/false: https://github.com/auenkr/cadence-rust/actions/runs/43188373717 success, GATE SUMMARY failed: []
- rust-ci full workspace: https://github.com/auenkr/cadence-rust/actions/runs/43188379294 success
