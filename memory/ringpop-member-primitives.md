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
