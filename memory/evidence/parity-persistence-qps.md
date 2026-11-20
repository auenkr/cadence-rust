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

## Follow-up: parity-persistence-qps-2 (remaining stores)
Wrapped with the same per-service limiter (Go factory.go: domain, visibility, queue/domain-replication use `ds.ratelimit`):
- Frontend (`cadence-matching/src/launcher.rs`): `RateLimitedDomainMetadata` (domain + domain replication queue/DLQ + audit log) and `RateLimitedVisibilityStore` (new `VisibilityError::ServiceBusy`, mapped to ServiceBusyError in `workflow_visibility.rs`).
- History (`wiring/assembly/env.rs`): `RateLimitedDomainMetadata` and `RateLimitedVisibilityWriter` (`wiring/persistence/visibility.rs`).
- Frontend has no Rust task/queue store; the History domain-cache source (`DomainMetadataSource`) is a different trait and is not wrapped.
- Caller-type bypass (`system.rateLimiterBypassCallerTypes`): still a gap, store calls carry no caller type.
- Deviation: `RateLimitedVisibilityStore::list_native` charges after the inner call (cannot tell beforehand whether the store pages natively), so a rejected native list still reaches the store.
- Two clippy fixes on the way (missing Arc path, items_after_test_module); failed runs 44830479646, 44831529875.
### CI (gated SHA b03f6f11c71e8ca3a5eed8e77fcce7e1165e01cd; later commits only edit this file)
- rust-ci workspace: https://github.com/auenkr/cadence-rust/actions/runs/44832958464 success
- rust-ci cadence-history: .../runs/44832136473 success; cadence-persistence (earlier commit): .../runs/44829742608 success; frontend/matching (earlier commit): .../runs/44830474934, .../runs/44830484000 success (covered again by the workspace run)
- history-mixed-gate mysql: https://github.com/auenkr/cadence-rust/actions/runs/44832962256 success, failedSteps []
- frontend-mixed-gate mysql: https://github.com/auenkr/cadence-rust/actions/runs/44832967902 success, failedSteps []
