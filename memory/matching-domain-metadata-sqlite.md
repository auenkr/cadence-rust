# Matching Go SQLite domain metadata reader

## Current source boundary

`rust/crates/cadence-persistence/src/matching_domain_metadata.rs` adds a
read-only reader for Go SQLite `domains` and `domain_metadata`. It decodes the
Go `sqlblobs.DomainInfo` ThriftRW payload, retains nested blobs as raw bytes and
encoding strings, and leaves `InMemoryDomainStore` unchanged. The reader keeps
`failover_notification_version` independent from failover/config versions.

The DomainInfo decoder follows generated Go behavior: known fields require the
generated wire type, wrong top-level types are skipped, repeated correct fields
use the last value, wrong-element `Clusters`/`Data` collections are skipped and
become nil (including overwriting an earlier collection), and trailing bytes
after the struct are accepted. Collection handling is local to DomainInfo and
does not change the shared Thrift decoder.

## Go SQLite oracle

`tests/matching-sqlite-go_test.go::TestRustMatchingDomainMetadataInterop` uses
Cadence's actual Go `sqlDomainStore` via `sql.NewFactory(...).NewDomainStore()`.
The test runner initializes the checked-in Go schema, creates standard and
active-active domains, updates the standard domain, and verifies Go reads. It
injects raw ThriftRW fields to exercise generated Go decoder behavior: field 28
wrong i32, valid i64, then wrong string; a non-empty wrong-element-type
Clusters list; and a non-empty wrong-key-type Data map. Go returns notification
version 101, nil Data, and its default cluster, confirming those raw cases.

The Go seed, Rust read-only interop, and Go verify phases passed on 2026-10-19
against the same temporary database created from
`go/schema/sqlite/cadence/schema.sql`. Exact focused runner:

```sh
python3 tests/run-matching-domain-metadata-compat.py
```

The runner executes Go seed, the ignored Rust read-only database interop test,
then Go verification against the same file. The final run exited 0; log:
`/tmp/cadence-domain-metadata-interop-release.log`.

The focused persistence suite also passed:

```sh
CARGO_BUILD_JOBS=1 cargo test --manifest-path rust/Cargo.toml -p cadence-persistence matching_domain_metadata
```

Result: 8 passed, 0 failed, 1 ignored (the database test is run separately by
the runner above). Final log: `/tmp/cadence-domain-metadata-cargo-release.log`.

An earlier focused compile failed with Rust E0597 around
`rusqlite::Statement::query_map`; the source now collects mapped rows with an
explicit loop, and the later focused run passed. A first unit run also
exposed a mistaken test expectation for page two's UUID; that expectation was
corrected and the rerun passed. Strict all-target persistence Clippy passed:

```sh
CARGO_BUILD_JOBS=1 cargo clippy --manifest-path rust/Cargo.toml -p cadence-persistence --all-targets -- -D warnings
```

Log: `/tmp/cadence-domain-metadata-clippy-final.log`. This reader is not
evidence that Matching service parity is complete.

## Known compatibility boundary

`list_domains` rejects page size zero with `InvalidPageSize`. Go does not
validate zero and can panic when an empty result reaches its last-row token
logic; retaining a Rust panic would not be safe. This is an explicit boundary
difference, not parity evidence. Go invalid UUID input panics through
`serialization.MustParseUUID`; Rust returns `InvalidUuid`. The oracle covers
canonical, simple, braced, and URN forms accepted by `google/uuid` and tests the
invalid case only as a behavior difference. These request-boundary differences
must remain documented in the parity audit.
