# RandomPartitioner routing review (2026-10-27)

Checkout started at `5540c78d0` (`Support Cassandra RandomPartitioner in Matching routing`), already identical to fetched `origin/t3/matching-cql-integration`; no merge commit was needed.

## Source comparison

- Pinned gocql is `github.com/gocql/gocql v0.0.0-20211015133455-b225f9b53fa1` (`go/go.mod`, `go/go.sum`). Its `token.go` `randomPartitioner.Hash` makes an unsigned big-endian `big.Int`, and when the high bit is set subtracts `2^128` then calls `Abs`. Thus the output domain is `[0, 2^127]`; the exact minimum signed digest maps to `2^127`. Sign-bit masking is incorrect. Source retrieved from the exact pinned commit `b225f9b53fa1`.
- Cassandra RandomPartitioner ring token minimum is `-1`; it is the ring sentinel, not a possible digest result. The Rust random token order correctly sorts `-1` before nonnegative magnitudes, and the shared ring search uses first token `>=` query with wraparound (Rust `routing/locator/token_ring.rs:20-58`, matching gocql `GetHostForToken`). Exact-boundary selection is therefore inclusive as in gocql.
- Rust `random_token_from_signed_digest` implements the same signed absolute value, including the `i128::MIN` case. Tests now make that explicit, cover the different result from sign masking, retain pinned gocql vectors, and assert empty input does not become the sentinel.
- Random metadata parsing allows `-1..=2^127`, rejects outside that domain. The MD5 output cannot produce `-1`; peer metadata may use it as minimum.
- Murmur3 code path is unchanged. `PartitionerName::from_str` recognizes Murmur3, CDC and Random suffixes; unknown names return `None`. `query_peers` currently converts unrecognized/null `system.local.partitioner` to default Murmur3 and peer token parsing follows that fallback. This is consistent with existing default behavior, but it is unsafe for unsupported cluster partitioners such as Cassandra `ByteOrderedPartitioner` (gocql recognizes `OrderedPartitioner`, hashes the key bytes as ordered token, and errors on unknown partitioner in `newTokenRing`). No Ordered token implementation or fallback change was made because it exceeds this narrow token fix.

## Runtime parity limit found

This commit does **not** establish ordinary Cassandra RandomPartitioner prepared-statement routing. `query_table_partitioners` reads `system_schema.scylla_tables` (`cluster/metadata/fetching.rs:1981-2021`); on Cassandra that Scylla-specific table is missing and the routine returns an empty map. Table construction then stores `partitioner: None` (`fetching.rs:1692-1716`), and `Session::extract_partitioner_name` returns that option for prepared statements (`client/session.rs:2024-2045`), followed by `.unwrap_or_default()` to Murmur3 (`session.rs:2019-2022`). This means supported Random hashing is applied when partitioner is explicitly supplied or table metadata supplies it, but ordinary Cassandra tables fall back to Murmur3. Correcting this requires carrying `system.local.partitioner` into table/prepared metadata, which is broader metadata/session work explicitly excluded from this review.

## Validation boundary

No Cargo, Go, rustfmt, mixed-cluster gate, or Actions workflow was run/started. Parent workflows own CI. New unit tests require Actions validation before treating this source change as green. Runtime RandomPartitioner behavior on an actual Cassandra cluster remains unverified and incomplete for ordinary tables for the reason above.
