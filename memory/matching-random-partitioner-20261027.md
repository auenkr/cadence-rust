# Matching RandomPartitioner dependency support (2026-10-27)

## Changes

- Vendored Scylla `Token` now retains Murmur3's existing `i64` behavior and supports RandomPartitioner magnitudes through the full signed 128-bit MD5 result. The Random representation is not truncated into `i64`; ordering preserves the entire range, including Cassandra's `-1` ring sentinel.
- RandomPartitioner hashes MD5 over the serialized partition key and applies gocql's signed-big-endian/absolute-value rule. `PartitionerName::from_str` recognizes `RandomPartitioner`, so prepared table metadata selects this hasher through the existing token routing path.
- Metadata fetch reads `system.local.partitioner` before parsing local and peer token strings, then applies the same configured partitioner to all node rows. Invalid Random tokens are rejected and the node is skipped instead of receiving a random dummy token.

## Domain and references

Pinned upstream reference: `go/go.mod` names `github.com/gocql/gocql v0.0.0-20211015133455-b225f9b53fa1`; its `token.go` `randomPartitioner.Hash`, `randomToken.Less`, and `newTokenRing`, plus `token_test.go` `TestRandomPartitionerMatchesReference`, establish MD5 signed big-endian absolute-value hashing and numeric ring ordering. Compatibility vectors are empty key -> `58332598431525814501020785164969033090`; `test` -> `12707736894140473154801792860916528374`.

The supported native domain is non-negative signed 128-bit MD5 values (including the boundary `2^127` produced by absolute value of the signed minimum) plus Cassandra's `-1` ring sentinel. Values beyond `2^127` and other negative values are invalid and rejected. Go `big.Int` provides arbitrary precision but does not expand Cassandra's native token domain.

## Validation status

- Added source tests for pinned gocql vectors, RandomPartitioner name selection, full-range ordering, the `-1` sentinel, and boundary rejection.
- `git diff --check` passed.
- No Cargo, Go, or rustfmt command was run because the project requires those on Actions. No Actions run has been dispatched from this child checkout; parent owns CI dispatch and test follow-up.
- No mixed-cluster compatibility evidence was produced here. This work establishes the vendored driver dependency path only; it is not a claim that Matching parity or service acceptance is complete.
