# Ringpop memberlist update core checkpoint

Date: 2026-10-18. Added `cadence_common::ringpop_memberlist::Memberlist`, a
transport-independent implementation modeled on pinned ringpop-go v0.10.0
`swim/memberlist.go` `Update`. Calls process every supplied change in slice
order. Incarnation time and unknown-member insertion position are callbacks
supplied by the caller; the insertion position is clamped to Go's
`rand.Intn(member_count)` range. The core owns accepted-state decisions, local
refutation state replacement, member insertion/replacement, and aggregate
checksum update. It does not own stopped node gating, locks, events,
dissemination, lifecycle, timers, or transport.

Focused Rust coverage verifies remote insertion and update, rejection of an
unknown tombstone, accepted-change contents for local refutation, injected
local incarnation, updated checksum, complete processing of a 70-change batch, insertion range clamping, and local
source/status/labels/timestamp plus single clock sampling on refutation. Existing pinned-Go
member primitive and dissemination oracle tests pass. Strict all-target Clippy
passes. See `repair-checks.log` for the exact repair evidence.

Parity remains pending. The requested actual-Go differential fixture for
`memberlist.Update`, including local refutation and the accepted-change list,
has not been implemented. Existing Go fixtures exercise only the member
primitives and dissemination. The focused Rust test is not differential
proof. Also missing are the requested broad state/incarnation comparison
matrix, labels/checksum differential for update results, and generator support
for this new core. Do not treat this checkpoint as completion of Matching,
production SWIM, or the requested slice parity audit.

## Combined final validation

The repaired implementation and Go-backed Rust consumer passed the combined
final gate. See `final/` for command logs and exact exit markers, and
`../../ringpop-memberlist-update-oracle.md` for the ordered source commits,
consumer starting state, and scope. All seven execution steps (two Go fixture
generation runs, focused unit tests, memberlist oracle, member oracle,
dissemination oracle, wire runner, and strict locked all-target common Clippy)
exited 0; the two fixture hashes match. This result covers the bounded
`memberlist.Update` slice only, not production SWIM or Matching completion.
