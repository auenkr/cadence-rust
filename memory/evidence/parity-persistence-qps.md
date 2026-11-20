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
Gated SHA 2c0bf0402dfa9a99ee824b0bb5388a2c5376f478 (later commits only edit this file).
- rust-ci cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/44817805644 success
- rust-ci cadence-history: .../runs/44818526660 success; cadence-frontend: .../runs/44818532378 success; cadence-matching: .../runs/44818536903 success
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44819718981 success
- history-mixed-gate (mysql): https://github.com/auenkr/cadence-rust/actions/runs/44819729443 success, failedSteps [] (428 steps)
- frontend-mixed-gate (mysql): first run 44819724322 failed only sdk-async-frontend-restart-no-lost-messages (1 async/Kafka message lost across Frontend restart; path does not touch wrapped stores, treated as flake); rerun https://github.com/auenkr/cadence-rust/actions/runs/44822497406 success, failedSteps [] (325 steps), same SHA.
- Unit tests: rate_limited.rs (per_member, disabled when host QPS<=0, ServiceBusy text after burst).
