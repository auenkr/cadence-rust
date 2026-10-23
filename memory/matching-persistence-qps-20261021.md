# Matching persistence QPS enforcement (2026-10-21, source only)

Implemented shared host/cluster persistence QPS enforcement for Matching:
- `matching_persistence_limiter.rs`: `MatchingPersistenceLimiter` reads `matching.persistenceMaxQPS` and `matching.persistenceGlobalMaxQPS`, refreshes every 5s, divides global by matching members, clamps to host, defaults host=3000/global=0. Zero initial rate leaves store unwrapped.
- `matching_persistence.rs`: wraps task store, domain metadata source, and semaphore persistence with shared limiter; maps rejection to Throttled backend/ServiceBusy.
- `main.rs`: constructs one limiter, wraps configured default store task/domain source and configured semaphore store; refreshes member count.
- `matching_domain_cache.rs`: supports limiter in default-store constructor.
- `handlers.rs`/`grpc_matching.rs`: map Throttled semaphore errors to ServiceBusy.

No build/test commands run (source-only lane).
