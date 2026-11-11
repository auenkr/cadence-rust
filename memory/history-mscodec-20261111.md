# History mutable state persistence codec — 2026-11-11

## Done

- Added `execution/mutable_state_bridge.rs` with snapshot-to-envelope and unchanged envelope-to-snapshot codec functions.
- The envelope retains the complete original `WorkflowSnapshot`, so keyed opaque cancel/signal/semaphore rows, heartbeats, checksums, tasks, workflow requests, timer records, buffered blob boundaries and other auxiliary fields export byte-for-byte unchanged.
- Decoded activity and timer records remain keyed by the persistence map keys. Buffered event and keyed activity/timer/child/version-history decode failures return errors.
- Kept current version (from the selected version-history branch), persistence start version and last-write version independently.
- Routed `EngineMutableState::from_snapshot` through the codec without changing event generation behavior.
- Added tests for non-default version branches, independent versions, multiple buffered blobs, opaque auxiliary payloads and malformed activity/timer/buffered-event encodings.

## Gaps / scope boundary

- This is the load/unchanged-export boundary only. Changed-record overlay semantics, unknown-field merge behavior, mutation export and actual persistence-writer semantics remain item 2b and are not claimed here.
- Workflow execution info retains the existing engine loader's explicit projection; the original source record is retained verbatim in the envelope snapshot.

## Validation evidence

- Local validation: `git diff --check` only. No local Cargo, rustfmt or Go command was run.
- CI dispatched with `gh workflow run rust-ci.yml --ref history-mscodec -f package=cadence-history`.
- Run URL: pending.
- Code head SHA: `349614331602e7eef7f1a403f510aa33bb0fc3cc`.
- Conclusion: pending CI.
