# Typed History replay follow-up — 2026-11-11

Branch: `history-typed-replay3`

## Done

- Typed replay treats `MarkerRecorded` as Go does: no mutable-state update and no error.
- Typed execution info already had a `search_attributes` map. `ReplicateUpsertWorkflowSearchAttributesEvent` now merges the event's indexed fields and generates the normal visibility transfer task for non-passive callers. Passive history replay suppresses that active task generation.
- Added continued-as-new replay entry accepting the new-run batch. It closes the old run, checks the new start's `continuedExecutionRunId`, checks that its version matches the continued-as-new branch version, creates a fresh independent typed state, and replays the new-run history there.
- Replay-created workflow request rows are removed from typed replay because Go only persists those rows with strong idempotency enabled and this replay API has no domain policy input. This prevents empty synthetic request payloads from reaching transaction preparation.
- Semaphore events still return the explicit unsupported typed-replay error because typed persistence state has no semaphore storage. Do not accept these events until semaphore state is represented and persisted.
- Added focused tests for marker/upsert, continued-as-new linkage/version/non-aliasing, request-row absence, and the semaphore error.

## Validation

- Local validation: `git diff --check` only. No Cargo, rustfmt, or Go command was run locally.
- CI run URL: pending
- Code head SHA: pending
- Conclusion: pending

## CI evidence

- Successful run: https://github.com/auenkr/cadence-rust/actions/runs/42988820951
- Validated code head SHA: `aee60fec25cc565b22c437623724ac70e0fb3724`
- Conclusion: success; cadence-history Clippy and tests passed.
- Earlier run `42985399424` found a moved-event compile error in the new test; fixed by using `std::slice::from_ref`.
- Run `42985992031` found a Clippy `cloned_ref_to_slice_refs` lint; fixed with that same borrowed slice.
- Run `42986696513` passed Clippy but found the test's completed-state expectation was wrong; corrected it to Go's completed value (`2`).
- Run `42987720341` found that run linkage validation needed the expected new `WorkflowExecution` as an explicit input. Added that input and verified it matches the continued-as-new attribute.
- All validation was on GitHub Actions. Local `git diff --check` passed; no Cargo, rustfmt, or Go command was run locally.
