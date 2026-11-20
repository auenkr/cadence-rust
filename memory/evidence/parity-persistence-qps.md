# parity-persistence-qps
Gap: frontend/history `persistenceMaxQPS` and `persistenceGlobalMaxQPS` were ignored.
Go: `persistence/client/factory.go buildRatelimiters` (limiter only if maxQPS()>0 at construction),
rate = `permember.PerMember(service, global, host, resolver)`, `quotas.NewDynamicRateLimiter`,
wrappers `ratelimited.*` return `ServiceBusyError{"Persistence Max QPS Reached."}`.
Rust: `cadence-persistence/src/rate_limited.rs` (reuses cadence_common DynamicRateLimiter).
- History: wraps GoExecutionStore, GoHistoryV2Store, HistoryNodeStore, ShardStore in `wiring/assembly/env.rs`
  (defaults 9000/0; member count = History ring size).
- Frontend: wraps history_node_store in matching `launcher.rs` (defaults 2000/0; members("cadence-frontend")).
- New `ServiceBusy` variant on HistoryReadError / HistoryPersistenceError, mapped at consumers.
Known gaps: caller-type bypass (`system.rateLimiterBypassCallerTypes`) not applicable (no ctx);
Frontend domain/visibility stores and History visibility/domain stores not yet wrapped.
## CI evidence
(pending)
