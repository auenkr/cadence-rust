# Review: Matching Cassandra UUID cache normalization (2026-10-27)

Reviewed implementation commit `e2f2bd5cd1af34a83db938496a95783b6f4403a9` from `t3/cassandra-uuid-cache-parity` (commit `cb853811f` is its validation note). No implementation edits made.

## Findings

No concrete parity defect found in the reviewed normalization patch.

- **Pinned parser semantics match.** `go/go.mod` and `go/go.sum` pin `github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1`. Its cached source at `/home/auenkr/go/pkg/mod/github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1/uuid.go:62-85` iterates runes, counts hex digits, accepts exactly 32, accepts both hex cases, and skips `-` iff the digit count is even. There is no initial/final length gate. Thus leading, trailing, and repeated hyphens are accepted at even decoded-digit offsets; a hyphen after an odd number of decoded digits is rejected. The Rust byte loop at `rust/crates/cadence-matching/src/matching_domain_cache.rs:258-280` has the same valid-input language and canonical lowercase UUID result. Non-ASCII runes are rejected by Rust as individual non-hex bytes, equivalent to Go's rune rejection.
- **Warm cache and newly loaded cache use the same canonical key.** On refresh, Cassandra records are normalized before insertion (`matching_domain_cache.rs:961-976`). `get_domain_by_id` normalizes valid Cassandra input before checking the cache (`:748-760`); the no-I/O telemetry resolver does likewise (`:780-783`). Example: with a cache record `00000000-0000-0000-0000-00000000000A`, both `0000000000000000000000000000000a` and the canonical spelling resolve to the same entry immediately after refresh and after a miss-triggered refresh.
- **Malformed-ID error precedence is preserved for cache API calls.** Empty ID is rejected before lookup (`:748-750`), as in Go `GetDomainByID` (`go/common/cache/domainCache.go:377-384`). For malformed nonempty Cassandra IDs, `normalize_domain_id` returns `None`; the patch falls back to the original ID (`:752-757`) and calls the existing Cassandra source validator (`:761-763`) before refreshing the cache. It does not synthesize a cache-layer error. Valid alternate spellings are sent to the source in canonical form, but persistence parses them to the same UUID. Cache-only telemetry returns no label for malformed input, with no persistence/error side effect.
- **Source scoping is correct.** Default `DomainMetadataSource::normalize_domain_id` is identity (`:81-87`); only `CassandraDomainMetadataStore` overrides it (`:211-214`). MySQL, Postgres, and SQLite/SQL retain their strict parsing and error paths.
- **Caller boundaries align.** RPC metric labeling and persistence limiter domain labeling use `cached_domain_name` (`grpc_matching.rs` resolver near lines 460-485; `matching_persistence_limiter.rs:210-223`). RPC quota lookup goes through `Matching::domain_name` / History strict domain lookup (`grpc_matching.rs:1746-1756`), so cache normalization does not change quota validation or handler precedence.

## Reproduction matrix (source-level; no local executable)

For a 32-hex payload `0123456789abcdef0123456789abcdef`: canonical and uppercase UUID spellings accept; the unhyphenated spelling accepts; leading/trailing hyphens accept; repeated hyphens at even digit counts (including `--payload--`) accept; a hyphen after an odd digit count rejects; 31/33 hex digits, non-hex characters, and whitespace reject. These results follow directly from the pinned Go loop and Rust loop; no Go or Rust program was executed locally.

## Validation evidence and limits

The author-provided CI record in the adjacent validation note reports `rust-ci.yml` run `40463522678`, exact head `e2f2bd5cd1af34a83db938496a95783b6f4403a9`, clean target, and success (clippy/fmt/tests; Matching suite 726 passed, 0 failed, 3 ignored). I did not independently retrieve that run. Unit tests establish Rust helper/cache expectations but do not alone prove Go parity across all parser inputs, caller error precedence, or service behavior. This review source-compared the exact pinned parser instead. No Go oracle was warranted. No mixed Go/Rust gate was dispatched, per request; parent-owned mixed backend/recovery gates remain necessary for service acceptance.

No speculative CQL migration or dependency change is recommended.
