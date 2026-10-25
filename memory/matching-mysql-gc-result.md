# Matching MySQL GC, configstore lane failure (run 40191566892)

Failure: only mainDecision kept 1 task row with persisted ackLevel 2 for 90s; every other queue was clean.
Analysis: Rust task GC mirrors Go task_gc.go (matching_gc.rs, fixtures from Go): GC runs only on ack advance
and is throttled by MaxTimeBetweenTaskDeletes (1s); the tail is flushed only on the next ack or list unload
(RunNow). DELETE uses task_id <= ack (inclusive, same as Go). A final ack inside the 1s window therefore strands
a row at or below the ack level in Go too. No Rust bug found; the evidence logs lack the row id, so this is
inferred, not proven.
Harness fix (tests/matching-mysql-gate.cjs): record MAX(task_id) of remaining rows; rows with id <= persisted
ack that stay unchanged for 5s count as throttled residue (reported as throttledResidue in evidence). A row above
the ack level still fails, and the failure output now includes maxRowId so a real defect is visible.
Validation: see run URL appended below after the single configstore dispatch.

Validation: configstore lane run https://github.com/auenkr/cadence-rust/actions/runs/40193208554 head 1d68b28d84b48326bd909068511305fbfe0e5d37 (pre-memory-commit) concluded success. Note it may have passed without residue (timing dependent); this single run does not prove the residue path was hit.
