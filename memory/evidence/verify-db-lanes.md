# verify-db-lanes (2026-11-19)

Source: origin/master 594ae6b11 (after merge perf-pg-sql-thread, which changed `outside_runtime` and is also used by cassandra.rs). Covers the persistence lanes that were not run for that merge (History PG was already covered).

## Part A: mixed gates (all conclusion=success)
| Gate | DB | Run | GATE SUMMARY |
|---|---|---|---|
| History | cassandra | https://github.com/auenkr/cadence-rust/actions/runs/44476810409 | failed: [] |
| History | mysql | https://github.com/auenkr/cadence-rust/actions/runs/44476814437 | failed: [] |
| Frontend | cassandra | https://github.com/auenkr/cadence-rust/actions/runs/44476818881 | failed: [] |
| Frontend | postgres | https://github.com/auenkr/cadence-rust/actions/runs/44476822781 | failed: [] |
| Frontend | mysql | https://github.com/auenkr/cadence-rust/actions/runs/44476826968 | failed: [] |
| Matching | cassandra | https://github.com/auenkr/cadence-rust/actions/runs/44476831133 | success |
| Matching | mysql | https://github.com/auenkr/cadence-rust/actions/runs/44476835252 | success |
The Matching gates only support MySQL and Cassandra, so there is no Matching PostgreSQL lane. No regression was found.

## Part B: History domain-conversion memo pruning, no change
Go `refreshDomainsLocked` (go/common/cache/domainCache.go:481-486) seeds the new cache with `c.GetAllDomain()` before it applies the ListDomains page, so Go never removes deleted domains from its cache. The Rust memo in cadence-history wiring/resources/domain_cache.rs only holds ids that the underlying MatchingDomainCache still has (which matches Go), so it is bounded the same way. Pruning would change behaviour compared with Go, so the nit is closed with no code change, and no CI run was needed.
