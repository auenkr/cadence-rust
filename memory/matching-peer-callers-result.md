# Matching peer-resolver callers audit (2026-10-25)

No code changed, so no CI run.

## Findings
- `FromSemaphoreBucket` does NOT exist anywhere in `go/` (grep, non-test and test). Semaphore routing goes through `semaphore.RingKey` (`go/common/semaphore/routing.go:43`) plus `membershipResolver.Lookup`.
- `GetAllPeers` exists only in the History client peer resolver (`go/client/history/peer_resolver.go:44,116`, caller `go/client/history/client.go:922` CountDLQMessages). It is a History-client concern, not used by Matching. Defer to History.
- Matching process uses the membership Resolver directly: Lookup, WhoAmI, Subscribe, EvictSelf.

## Go vs Rust table
| Use | Go | Rust |
|---|---|---|
| Subscribe for ring changes | go/service/matching/handler/membership.go:61 | cadence-matching/src/matching_membership.rs:118 (`subscribe`) |
| Non-owned tasklist unload (Lookup+WhoAmI) | handler/membership.go:131,137 | grpc_matching.rs:1147-1155 loop with `owns` (matching_membership.rs:173) |
| Non-owned semaphore unload | handler/membership.go:179,187 | grpc_matching.rs:1155 `owns(&manager.identifier().ring_key())` |
| Semaphore ring key `domain_sem_bucket` | go/common/semaphore/routing.go:43, semaphore/identifier.go:72 | matching_semaphore.rs:87 (tests :720,:849) identical format |
| Tasklist ownership check | handler/engine.go:1648 | matching_membership.rs:237 `check_task_list` |
| Semaphore ownership check | handler/engine.go:1670-1682 | matching_membership.rs:265 `check_semaphore` (same error text/type) |
| Partition host info | handler/engine.go:1314 (`Lookup`+GetAddress) | matching_membership.rs:121 `owner`; `owner_by_address` :200 |
| Empty-ring lookup error | hashring Lookup | matching_membership.rs:121-130 (signals refresh) |
| EvictSelf on Stop | go/service/matching/service.go:145 | matching_runtime.rs:88,632 `with_membership_evictor` / drain_service |
| MemberCount/Members (rpc factory, permember quotas) | go/common/rpc/factory.go:240, quotas/permember/permember.go:37 | `hosts()` matching_membership.rs; not Matching-process callers beyond rpc factory |

## Residual
Bucket hashing/peer ordering of the ring is shared `cadence_common::membership::Ring` (Go: farm.Fingerprint32, 100 replica points, `hashring.go:53`); not re-verified here beyond existing tests. No gap found.
