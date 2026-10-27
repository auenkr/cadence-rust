# Matching cold domain publication race (2026-10-27)

## Change

The Matching domain-name resolver now publishes a cache discovery through
`InMemoryDomainStore::insert_matching_metadata_if_missing`. The UUID check and
insertion share the store write lock, so a lookup racing after a refresh callback
cannot replace newer configuration or failover state. Existing
`sync_matching_metadata` behavior remains the full refresh path and is unchanged.

The persistence regression in `domains.rs` covers both cases: a stale cache
snapshot after a newer configuration/failover refresh leaves the refreshed
record intact, and metadata for an absent UUID is inserted for a cold domain.
The gRPC resolver is wired to the missing-only helper. Go cache callback
semantics were not changed.

## Validation

- No local Cargo, Go, build, test, formatting, or runtime commands were run.
- `git diff --check` passed before commit.
- Fresh workspace `rust-ci.yml` dispatch with `clean_target=true`:
  [run 40479696516](https://github.com/auenkr/cadence-rust/actions/runs/40479696516)
- CI head: `bacaad40bd4e4558e6be8a683a1f219d031ef95e`.
- Last observed CI state: `in_progress`; conclusion not yet available.
