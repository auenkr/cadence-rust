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

Initial bounded consumer run (2026-10-18, before the repair): the fixture
contained a complete 6-by-6 status matrix at older, equal, and newer incarnations; nil-old inputs
for all statuses; equal-state checksums greater, lower, and equal in both
directions; signed checksum examples; and labelled Unicode plus equivalent
maps assembled in different insertion orders. `toOracleMember` emits nil
labels as JSON `null`, preserving the distinction from an allocated empty
map. `rust/crates/cadence-common/tests/ringpop_member_oracle.rs` consumes the
fixture and compares checksum, member string, gossip decision, and aggregate
checksum results. The Go generator passed. The Rust consumer failed on exactly
`lower_precedence_higher_checksum_must_reject process` (Rust true, oracle
false), demonstrating that lower gossip precedence fell through to label
checksum comparison. The repair and final validation below supersede that
pre-repair result.

## Final repaired validation (2026-10-18)

Applied the member core, precedence repair, original Go oracle, and expanded
consumer in order. Regenerated the fixtures from pinned ringpop-go v0.10.0;
`git diff --exit-code` confirmed the committed JSON was byte-for-byte stable.
The repaired Rust consumer passed (1 test, 0 failures); focused core tests
passed (4 unit tests and the oracle integration test). Strict
`cargo clippy -p cadence-common --all-targets -- -D warnings` passed. The
existing `python3 tests/run-ringpop-wire-compat.py` runner passed its Go oracle
and all 11 Rust wire tests. Commands used `CARGO_BUILD_JOBS=1` and the warmed
target at `/home/ubuntu/.t3/worktrees/cadence-rust/t3-e197a1fc/rust/target`.

This validates deterministic member checksum and merge primitives only; it
does not establish production Ringpop/SWIM/provider behavior or complete
migration parity.
