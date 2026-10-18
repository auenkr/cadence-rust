# Ringpop memberlist Update oracle

`tests/generate-ringpop-memberlist-update.py` copies the pinned
`github.com/uber/ringpop-go@v0.10.0` module into a temporary package and runs
`tests/ringpop-memberlist-update-oracle_test.go` inside its `swim` package using
a Go overlay. The fixture calls the original unexported
`memberlist.Update` directly; it does not port or emulate the algorithm. A
mock clock is injected into the temporary `Node` for deterministic local
incarnation refutation. Only member snapshot order is sorted for comparison;
input and applied change sequences retain their original order.

Fixture: `rust/crates/cadence-common/tests/fixtures/ringpop_memberlist_update.json`.
It records each case's input changes, returned applied changes, sorted final
members, membership checksum, and local member. Coverage includes remote
insert then update, rejection of an unknown tombstone, all six status values
(including unknown) crossed against all six gossip statuses at older, equal,
and newer incarnations, stale/equal/newer local gossip, and an 80-change batch.
Timestamps, sources, source incarnations, labels, tombstone flags, returned
changes, and checksums remain in the JSON output.

## Reproduction evidence (2026-10-18)

From the repository root, run:

```sh
python3 tests/generate-ringpop-memberlist-update.py
```

Two consecutive generator invocations completed with exit code 0. Both Go
package runs reported `ok github.com/uber/ringpop-go/swim` (0.175s and
0.171s). `cmp` confirmed the resulting fixtures were byte-identical; SHA-256:
`b1eb10818b7b74ed946d5480ad5924e2b3c6d8ec899bfc5ed787c1f3be8f7ac1`.

This is an oracle fixture only. It does not implement or establish production
SWIM/Matching parity; those remain incomplete.

## Explicit initial state and Rust consumer update (2026-10-18)

Each fixture row now includes `initialLocal`, the ordered `initialMembers`
(snapshot before calling Go `Update`), and `nowUnixSeconds` plus
`nowNanoseconds`. This avoids reconstructing setup from case names or final
results. Snapshot capture occurs before `Update`, which mutates member objects
in place. Oracle labels and fields retain JSON `null` versus empty-map state;
accepted changes remain in original order. Only final members are sorted.

The Rust integration consumer is
`rust/crates/cadence-common/tests/ringpop_memberlist_update_oracle.rs`. It
rebuilds each explicit snapshot, supplies the fixture clock and a deterministic
insertion index, and compares all accepted change fields/order, sorted final
members, local state, and checksum. A hidden snapshot constructor was added to
the candidate core to permit starting from remote members. `Memberlist::new`
retains its prior initial-checksum behavior; oracle snapshots reproduce the Go
fixture's manually initialized zero checksum.

The pinned generator was run twice; both invocations passed and produced
byte-identical fixture JSON (SHA-256
`c77eb329808ed5fa308c2bf96249014a81ee8444a2137bd021ddb732fa041450`). The
fixture contains 112 rows, including 108 status/incarnation combinations and
the explicit local, remote, tombstone, and 80-change scenarios. Generator
output is preserved in `memory/evidence/ringpop-memberlist-update/`.

The Rust consumer was run from the `cadence-rust` tmux session with
`CARGO_BUILD_JOBS=1`. It exited 101 with exactly three reported mismatches,
all for `batch_80_no_truncation`: expected 80 accepted changes, got 64; final
members differ; checksum differs. Other cases, including local repeated
refutation and all accepted-change comparisons, matched this draft. This is
expected against the candidate's known 64-item limit and is evidence of a
mismatch, not a passing parity claim. See `consumer-final.log` and
`consumer-final.exit`. This bounded oracle does not claim production SWIM or
Matching parity.
