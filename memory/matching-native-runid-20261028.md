# Matching native raw RunID backend resolution — 2026-10-28

Implemented additive backend resolution methods on `TaskDataBytes` in
`rust/crates/cadence-persistence/src/matching_blob.rs`. The raw model accepts a
`GoString` in `with_request_run_id`, preserving arbitrary Go string bytes and
clearing stale persisted UUID bytes. `run_id_bytes` returns the request bytes
unchanged, or the canonical textual UUID bytes when only persisted UUID bytes
exist.

SQL resolution follows the pinned `github.com/google/uuid` v1.6 `Parse` input
length/prefix rules: 32 and 36 byte text, 45 byte case-insensitive `urn:uuid:`
form, and 38 bytes where the parser ignores bytes 0 and 37 and parses bytes
1..37. The wrapper bytes are deliberately not decoded as UTF-8, so arbitrary
`ff`/`fe` wrappers work. Empty raw SQL RunID resolves to absent (`None`), while
malformed byte sequences return `SqlRunIdBytesError` carrying the exact
original `GoString` and a diagnostic. The existing `SqlRunIdError` and
legacy string APIs remain unchanged.

Cassandra raw resolution consumes bytes directly and follows gocql's UUID
hex parsing behavior, including hyphen skipping only at even nibble offsets,
exactly 32 hex digits, and rejection of empty input. Raw task encoding still
rejects an unresolved RunID, so it cannot silently omit or alter the request
identifier.

Added source tests for the accepted SQL text forms, invalid UTF-8 wrapper
bytes, distinct malformed `ff`/`fe` inputs, original-byte error retention,
empty SQL versus Cassandra behavior, Cassandra hyphen placement, and refusal
to encode unresolved IDs. Prior raw blob optional/duplicate/map behavior
tests remain in the same module.

Validation was not run by design: no Cargo, Go, or rustfmt commands were run
locally. `git diff --check` completed successfully. Parent owns CI validation
and commit handling; this note makes no claim that the Rust tests compile or
pass.
