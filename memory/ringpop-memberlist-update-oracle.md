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
