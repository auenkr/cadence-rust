# Ringpop member primitives implementation checkpoint

Date: 2026-10-18. This slice adds deterministic, pure Rust functions in
`cadence-common::ringpop_member`: label FarmHash32 XOR, member checksum string,
aggregate membership checksum, state precedence, and gossip acceptance. It
does not add provider wiring, memberlist state, join, ping, gossip, or lifecycle.

Implementation follows the pinned source
`/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0/swim/member.go`
(`checksumString`, `LabelMap.checksum`, `shouldProcessGossip`,
`statePrecedence`) and `swim/memberlist.go` (`genChecksumString`). Label
lengths use UTF-8 byte counts and signed big-endian i32 encoding, each pair's
FarmHash32 is XORed, and conversion to i32 preserves two's complement. Member
strings append `#labels<signed>` only for nonzero label checksums. Aggregate
checksum sorts strings, excludes tombstones and hashes each entry followed by
`;`.

Focused unit vectors cover all known states plus unknown status precedence,
incarnation ordering, unknown tombstone rejection, UTF-8/empty labels,
label-order independence, empty-vs-nil labels, signed negative checksum
existence, member representation, and aggregate order/tombstone/empty cases.
At checkpoint time `cargo test -p cadence-common ringpop_member::tests` and
strict all-target Clippy were still running in tmux window
`cadence-rust:ringpop-member-checks`; report final results before relying on
this implementation. A complete Go-generated oracle fixture has not yet been
committed; do not claim differential parity until one is generated from the
pinned Go routines and compared against Rust.

## Precedence repair verification

A follow-up repair fixes `should_process_gossip`: lower state precedence now
returns `false` immediately, and signed label checksums are compared only when
precedence is equal, matching the pinned Go `swim/member.go` implementation.
Regression coverage constructs an alive gossip member whose signed labels
checksum exceeds that of an existing suspect member and verifies it is rejected.

Verification in the repair worktree on 2026-10-18:

- `CARGO_BUILD_JOBS=1 cargo test -p cadence-common ringpop_member -- --nocapture`:
  exit 0, 4 passed.
- `CARGO_BUILD_JOBS=1 cargo clippy -p cadence-common --all-targets -- -D warnings`:
  exit 0.
- `python3 tests/run-ringpop-wire-compat.py`: exit 0; Go wire fixture check and
  11 Rust wire tests passed.

A Go-generated differential oracle for the member checksums and merge behavior
is still pending; the wire compatibility result does not establish that parity.
