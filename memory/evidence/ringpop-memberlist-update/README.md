# Ringpop memberlist update core checkpoint

Date: 2026-10-18. Added `cadence_common::ringpop_memberlist::Memberlist`, a bounded,
transport-independent implementation modeled on pinned ringpop-go v0.10.0
`swim/memberlist.go` `Update`. Calls process at most 64 changes, in slice order;
callers must submit any remaining changes in a later call. Incarnation time and
unknown-member insertion position are callbacks supplied by the caller. The
core owns accepted-state decisions, local refutation state replacement, member
insertion/replacement, and aggregate checksum update. It does not own stopped
node gating, locks, events, dissemination, lifecycle, timers, or transport.

Focused Rust coverage verifies remote insertion and update, rejection of an
unknown tombstone, accepted-change contents for local refutation, injected
local incarnation, and updated checksum. Existing pinned-Go member primitive
and dissemination fixture generator and Rust consumers pass. Strict all-target
Clippy passes. Evidence logs are `checks.log` and `regressions.log`.

Important limitation: the requested actual-Go differential fixture for
`memberlist.Update`, including local refutation and the accepted-change list,
has not been implemented. Existing Go fixtures exercise only the member
primitives and dissemination. The focused Rust test is not differential
proof. Also missing are the requested broad state/incarnation comparison
matrix, labels/checksum differential for update results, and generator support
for this new core. Do not treat this checkpoint as completion of Matching,
production SWIM, or the requested slice parity audit.
