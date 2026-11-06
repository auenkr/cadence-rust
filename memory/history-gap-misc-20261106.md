# History gap audit (misc areas) 2026-11-06, branch history-gap-misc

Scope: ndc, replication, failover, shard, taskdlq, queuev2, task, workflowcache, query, config, resource, constants.
Grep for todo!/unimplemented!/TODO/stub:
- `task/deps_exec.rs::new_simple_dynamic_collection` was `unimplemented!` (panic when priority assigner / task rate limiter built). FIXED: ported quotas.DynamicRateLimiter (ttl 5s, minBurst 1, rps 0 => burst 0) + quotas.Collection (lazy per-key).
- shard_context replication queue state "not supported": matches Go (error), no gap.
- replication/deps.rs GetWorkflowExecutionRawHistoryV2 "not supported by this admin client": adapter-level wiring, owned by wiring work.
- Remaining `TODO(link)` markers in task/queue/queuev2 deps are shim traits (adapter wiring), not missing logic.
