# Ringpop member primitive oracle fixture

`tests/generate-ringpop-member-primitives.py` copies the pinned
`github.com/uber/ringpop-go@v0.10.0` module into a temporary directory and
uses a Go overlay to run `tests/ringpop-member-primitives-oracle_test.go` inside
package `swim`. The generator invokes the original unexported
`LabelMap.checksum`, `Member.checksumString`, `shouldProcessGossip`,
`memberlist.genChecksumString`, and `memberlist.ComputeChecksum`; no algorithm
is reimplemented in the fixture generator. It writes
`rust/crates/cadence-common/tests/fixtures/ringpop_member_primitives.json`.

Coverage includes all six status values (including unknown), nil old member,
incarnation ordering, equal state, precedence over a demonstrably higher label
checksum, label checksums on both signed halves, Unicode and empty labels,
nil/empty maps, sorted memberlist input, tombstone exclusion, and an empty list.
Regenerate with `python3 tests/generate-ringpop-member-primitives.py` from the
repository root. The source module path is pinned to
`/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0`.

Evidence: generator completed successfully on 2026-10-18:
`ok github.com/uber/ringpop-go/swim` and wrote the checked-in JSON fixture.

Bounded consumer task update (2026-10-18): fixture now contains a complete
6-by-6 status matrix at older, equal, and newer incarnations; nil-old inputs
for all statuses; equal-state checksums greater, lower, and equal in both
directions; signed checksum examples; and labelled Unicode plus equivalent
maps assembled in different insertion orders. `toOracleMember` emits nil
labels as JSON `null`, preserving the distinction from an allocated empty
map. `rust/crates/cadence-common/tests/ringpop_member_oracle.rs` consumes the
fixture and compares checksum, member string, gossip decision, and aggregate
checksum results. The Go generator passed. The Rust consumer currently fails
on exactly `lower_precedence_higher_checksum_must_reject process` (Rust true,
oracle false), demonstrating that lower gossip precedence currently falls
through to label checksum comparison. Other fixture comparisons ran without
mismatches. This is expected pending the separately assigned implementation
repair; do not claim parity until that repair is integrated and this test
passes.
