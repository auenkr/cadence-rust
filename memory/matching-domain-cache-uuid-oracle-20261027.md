# Matching domain-cache UUID spelling oracle (2026-10-27)

Status: original-Go UUID spelling oracle and the complete `cadence-matching` CI workflow passed on Actions.

The Actions workflow accepts `workflow_dispatch` input `go_domain_cache_uuid_oracle=true`. It overlays `tests/go-domain-cache-uuid-oracle_test.go` at a virtual test path in `go/common/cache`, then runs only `TestDomainCacheSuite/TestDomainCacheUUIDSpellingOracle`. Production Go files are unchanged. Runner logs and the overlay/command/source SHA are uploaded as a three-day artifact.

The oracle tests uppercase, compact, and leading/trailing-hyphen spellings against one canonical record. For each spelling it establishes the persistence `GetDomain` existence lookup, triggers a metadata/list refresh, checks the raw spelling again while refresh is throttled, then confirms the canonical spelling resolves. It does not infer cache behavior from `gocql.ParseUUID`.

Source inspection before Actions execution:

- Go `go/common/cache/domainCache.go`: `getDomainByID` checks and caches the exact input ID before and after refresh; `refreshDomainsLocked` inserts records under `record.Info.ID`. A found persistence record that remains absent from the exact cache key yields `InternalServiceError`.
- Rust `rust/crates/cadence-matching/src/matching_domain_cache.rs`: `get_domain_by_id` checks exact `id`, asks source for that exact `id`, refreshes, and checks exact `id` again. No proven divergence; no production change made.
- Therefore the expected oracle outcome for alternate spellings is an existence check followed by “exists but cannot be loaded”; the canonical UUID resolves. Actions result is needed to establish the original Go result.

First Actions run: https://github.com/auenkr/cadence-rust/actions/runs/40469791881
First run head SHA: `ada08789d553cf71238f1ebe2fa11a024c71e5ff`.
First conclusion: failure before test execution. Step `Run original-Go domain-cache UUID spelling oracle` failed because the overlay file imported unused `testing`; this was an oracle harness defect, not a Go cache result. The push-triggered duplicate run `40469781867` was canceled by configured concurrency.

Rerun URL: https://github.com/auenkr/cadence-rust/actions/runs/40470232857
Rerun head SHA: `b93b162061cb32b62caecc22673241579edbec30`.
Oracle step conclusion: success, exit `0`. The log shows each alternate spelling has a successful existence check followed by the expected `*types.InternalServiceError` on both cold/refresh and warm access; canonical ID resolves to `uuid-spelling-oracle-domain`. Artifact `domain-cache-uuid-oracle` contains the run log, command, overlay, source SHA, and exit code. Overall workflow conclusion: success; Matching clippy, formatting, and tests also passed.

Conclusion: Go and current Rust cache lookup shape agree on exact raw-key lookups. This oracle supplies no evidence for changing Rust production behavior. The alternate-spelling canonicalization behavior of any particular persistence backend remains outside this cache-only test.

## Cold canonical UUID read-through addendum (2026-10-27)

Added the `cold_canonical_UUID_after_initial_scan` subtest to the existing overlay oracle. It runs an empty initial scan, advances the mock clock past the refresh throttle, then simulates a domain that exists in persistence and is present in the following canonical-record refresh. The cold canonical lookup returns `uuid-spelling-oracle-domain`; the warm lookup returns it from cache without a second persistence call. Existing alternate spelling assertions are unchanged and pass.

Focused Actions run: https://github.com/auenkr/cadence-rust/actions/runs/40474548772, head `e5ac70a8f19c32c281d9f3bed182c7a21504291a`, conclusion success. The original-Go overlay passed, as did the requested `cadence-matching` clippy, formatting, and tests. Compact artifact copy: `memory/evidence/matching-domain-cache-cold-canonical-20261027/`. This is caller read-through evidence only; it does not prove Matching metric attribution or backend Cassandra UUID parsing.
