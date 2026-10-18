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
