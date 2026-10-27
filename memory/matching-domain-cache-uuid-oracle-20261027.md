# Matching domain-cache UUID spelling oracle (2026-10-27)

Status: implementation checkpoint pushed; optional original-Go Actions oracle is pending.

The Actions workflow accepts `workflow_dispatch` input `go_domain_cache_uuid_oracle=true`. It overlays `tests/go-domain-cache-uuid-oracle_test.go` at a virtual test path in `go/common/cache`, then runs only `TestDomainCacheSuite/TestDomainCacheUUIDSpellingOracle`. Production Go files are unchanged. Runner logs and the overlay/command/source SHA are uploaded as a three-day artifact.

The oracle tests uppercase, compact, and leading/trailing-hyphen spellings against one canonical record. For each spelling it establishes the persistence `GetDomain` existence lookup, triggers a metadata/list refresh, checks the raw spelling again while refresh is throttled, then confirms the canonical spelling resolves. It does not infer cache behavior from `gocql.ParseUUID`.

Source inspection before Actions execution:

- Go `go/common/cache/domainCache.go`: `getDomainByID` checks and caches the exact input ID before and after refresh; `refreshDomainsLocked` inserts records under `record.Info.ID`. A found persistence record that remains absent from the exact cache key yields `InternalServiceError`.
- Rust `rust/crates/cadence-matching/src/matching_domain_cache.rs`: `get_domain_by_id` checks exact `id`, asks source for that exact `id`, refreshes, and checks exact `id` again. No proven divergence; no production change made.
- Therefore the expected oracle outcome for alternate spellings is an existence check followed by “exists but cannot be loaded”; the canonical UUID resolves. Actions result is needed to establish the original Go result.

Actions run URL: pending.
Head SHA: pending.
Conclusion: pending; do not treat this checkpoint as validated until the optional oracle step passes or produces a concrete sourced defect.
