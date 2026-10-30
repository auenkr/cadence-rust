# ListTaskListPartitions cold domain lookup deadline parity (2026-10-30)

## Finding

The claim that Rust `ListTaskListPartitions` is missing inbound deadline
forwarding into a cold domain-cache lookup is incorrect. No production change
or cancellation regression should be added for this claim.

Go's Matching handler (`go/service/matching/handler/handler.go`,
`ListTaskListPartitions`) creates a handler context from the inbound context,
then calls the engine. The engine implementation
(`go/service/matching/handler/engine.go`, `ListTaskListPartitions` ->
`getAllPartitions`) does not use that handler context for domain resolution; it
calls `domainCache.GetDomainID(request.GetDomain())`. The Go cache API has no
context parameter. On a cold name lookup, `DefaultDomainCache.getDomain` first
calls `checkDomainExists`, which creates a fresh `context.Background()` timeout
of `domainCachePersistenceTimeout` (3 seconds). After acquiring `refreshLock`
and rechecking the cache, it calls `refreshDomainsLocked(context.Background())`.
That refresh applies the same 3-second cache-owned timeout to metadata and
domain-page persistence operations. Thus the inbound RPC deadline does not
cancel either the cold existence check or refresh; the cache's own timeout
applies.

Rust's Matching gRPC implementation follows the same boundary:
`grpc_matching.rs::list_task_list_partitions` resolves through
`domain_id_by_name`, which calls the cache's context-free `get_domain` in a
blocking task. `matching_domain_cache.rs::get_domain` uses the same fixed
3-second persistence timeout for the existence read, then performs the
coalesced refresh under `refresh_lock`; refresh metadata and page calls share a
3-second refresh deadline. The inbound `Request` is not forwarded into this
cache lookup, matching Go behavior.

The cold lookup's lock wait can exceed the inbound deadline in both versions.
The cache miss refresh remains coalesced and retains its completed-success
recheck boundary. Forwarding the request deadline would change Go behavior and
could cancel a shared refresh for other callers.

## Validation and follow-up

- Confirmed by source tracing only; no Go or Cargo commands, formatters,
  runtime probes, CI dispatches, or PR actions were run, per task constraints.
- No code change means no new deadline/cancellation regression is appropriate.
- No additional validation is needed for this no-change correction. Any later
  cache timeout change should first add a deterministic Go oracle showing a
  changed contract, then test cache coalescing and cancellation only before a
  refresh completes successfully.
- This note is separate from the Matching readiness-timeout diagnosis; it
  provides no evidence about that failure's cause.
