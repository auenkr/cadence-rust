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

## Rust consumer and expanded edge coverage (2026-10-18)

The bounded dissemination implementation from candidate `6c71b6774` and repair
`187f4aeff` is applied in the isolated consumer branch. The Rust integration
consumer reconstructs the Go test inputs and compares every emitted change,
full-sync flag, queue snapshot, and per-address propagation counter to
`ringpop_disseminator.json`. `Disseminator::propagation_counts` is a read-only
observable added for that comparison. Wire fields retain Go timestamps and
tombstone encoding through `Change` serialization.

The Go oracle now normalizes every emitted change slice by address, including
`failedSent` and repeated sender snapshots that originate from a Go map. Added
original-Go vectors cover factor-driven `maxP` values 0 and 1, checksum equality
with no deltas, and all deltas filtered by sender (which triggers full sync on
checksum mismatch). Existing count vectors build a memberlist with a local
member plus the requested number of alive peers, exercising the original
`NumPingableMembers` behavior that excludes local.

The generator was run twice consecutively; output bytes matched with `cmp`.
Focused consumer result: `cargo test --locked -p cadence-common --test
ringpop_disseminator_oracle -- --nocapture` exited 0 (1 passed). The run used
`CARGO_BUILD_JOBS=1` in tmux session `cadence-rust`, window
`dissemination-consumer`, with the idle warm target under `t3-b501a8ae` to avoid
the concurrent HTTP listener build under `t3-02f97237`. A workspace-wide
`cargo fmt --all -- --check` is not clean on the pre-existing workspace; only
the touched Rust source and consumer were formatted directly with rustfmt.
