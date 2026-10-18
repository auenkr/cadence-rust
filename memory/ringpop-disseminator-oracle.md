# Ringpop dissemination oracle fixture

`python3 tests/generate-ringpop-member-primitives.py` runs temporary tests inside
pinned `github.com/uber/ringpop-go@v0.10.0/swim`, using a Go overlay; the
algorithms remain the original `disseminator` methods. The generator refreshes
both `ringpop_member_primitives.json` and
`ringpop_disseminator.json` under the cadence-common fixture directory.

The dissemination vector covers `RecordChange` replacement/reset, the
`IssueAsSender` callback after same-address replacement, a failed send with no
callback, repeated successful sends through removal, receiver filtering by
source and source incarnation, checksum-mismatch full sync including local
source identity, outgoing tombstone/labels/timestamp representation, and
`AdjustMaxPropagations` at pingable counts 0, 1, 8, 9, 10, 98, 99, 100, and
999. Map-derived change arrays are sorted by address solely for stable JSON;
change contents, counters, and remaining queue are preserved.

Evidence (2026-10-18): generator exited 0 with
`ok github.com/uber/ringpop-go/swim`; checked in fixture was produced by that
run. No Rust build or edit is part of this oracle task.
