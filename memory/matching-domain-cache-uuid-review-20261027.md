# Matching domain-cache UUID oracle review (2026-10-27)

## Scope and evidence

- Reviewed oracle head `16f437bce` and its test source at `b93b162061cb32b62caecc22673241579edbec30`.
- Actions run: https://github.com/auenkr/cadence-rust/actions/runs/40470232857, head `b93b162061cb32b62caecc22673241579edbec30`, success. The workflow report says the overlay oracle and `cadence-matching` CI completed successfully.
- Reviewed Go cache implementation in `go/common/cache/domainCache.go`, Rust cache in `rust/crates/cadence-matching/src/matching_domain_cache.rs`, matching handler resolver in `rust/crates/cadence-matching/src/grpc_matching.rs`, and the handler/domain call path in `rust/crates/cadence-matching/src/grpc_internal.rs`.
- Compared immutable candidate `ff3b638b4` with master `5bbd06d32`: `git diff --name-only` contains no paths under `rust/`, `go/`, `idls/`, or `config/`. The delta is documentation/evidence and helper/test infrastructure, not production logic in those areas.

## Oracle assessment

`tests/go-domain-cache-uuid-oracle_test.go` is overlaid into package `cache` and invokes `GetDomainName` for uppercase, compact, and hyphen-padded spellings. It mocks `GetDomain` to report that each raw spelling exists, then mocks `GetMetadata` and `ListDomains` to refresh a record whose persisted ID is canonical. The first call therefore exercises raw-key miss, persistence existence check, refresh, and the post-refresh raw-key lookup. It expects `InternalServiceError` (“exists but cannot be loaded”). The second call reinstalls the `GetDomain` expectation, takes the raw-key miss path again with refresh throttled by `lastRefreshTime`, and expects the same error. A canonical-ID lookup then succeeds from the refreshed cache.

This is valid evidence for raw cache key behavior under the mocked persistence responses. It is not a backend UUID spelling oracle: no real Cassandra/SQL persistence is involved, and mocked `GetDomain` success does not establish that any backend accepts or canonicalizes the alternate spelling. The test comment correctly limits its purpose; claims should retain that limit.

Go source confirms `getDomainByID` probes the literal `id` before and after `refreshDomainsLocked`, while refresh indexes records using `domain.info.ID`. Rust `get_domain_by_id` probes the literal `id`, requests that ID from its source, refreshes, and probes the same literal again. Thus both cache implementations behave consistently for the oracle’s mocked existence and canonical refresh data.

## Normalization, handler, metrics, and quota

The unmerged patch `e2f2bd5cd` normalizes recognized cache lookup spellings. Current master does not contain that patch and uses raw keys. Normalizing a recognized spelling would allow a canonical cache hit where Go retains a raw-key miss, so that patch must not be adopted as a cache parity fix. Its malformed-input fallback does not resolve this recognized-input difference.

The production matching handler resolver calls `domain_name(id)` for quota/error handling, and `metric_domain_name(id)` is explicitly cache-only through `cached_domain_name`. The mocked Go oracle does not prove a Matching RPC telemetry/quota outcome or real backend parsing. In particular, do not cite it as a complete handler metric/quota comparison; it tests `GetDomainName`. Canonical cold-cache telemetry behavior requires its own source trace if a discrepancy is identified.

The initial Cassandra normalization work remains unmerged on `t3/cassandra-uuid-cache-parity`; it is outside this reviewed branch and this note makes no acceptance claim for it.

## Review disposition

No production source changes requested. Keep the oracle conclusion scoped to matching Go cache behavior with controlled persistence responses. This review does not establish Matching service acceptance or real persistence UUID parser/canonicalization parity.
