# Matching readiness diagnostic snapshot

The readiness failure path now writes a separate
`rust-process-evidence-<label>-direct-partition.json` companion when the first
process evidence snapshot for a readiness label is captured successfully. It
stores the snapshot label and the exact `lastDirectDiagnostic` object as it
stood at capture time; that object retains its RPC `startedAt` and `completedAt`
timestamps. Later readiness probes do not rewrite this companion because the
process evidence capture is guarded by `processEvidenceCaptured`.

`tests/matching_evidence.py` already copies `rust-process-evidence-*.json` from
the probe directory, so the companion is included with no copier change.

Validation limit: source inspection and `git diff --check` only, as requested.
No Cargo, Go, Node syntax, build, formatting, tests, runtime, or CI were run.
