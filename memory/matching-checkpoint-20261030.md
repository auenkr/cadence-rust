# Matching checkpoint 2026-10-30

Matching is still INCOMPLETE. Prior orchestration thread stopped (Codex usage limit, then 429s).

## CI state (auenkr/cadence-rust)
- Run 40912650991 (combined candidate, head f4ffd30798955dabe34bbd0efe5736bacad17f61): completed, conclusion=failure.
- Run 40912428949 (semaphore validation, head 8fa96afb8cb5463747000bdf96b0f98fcd3a29e8): completed, conclusion=failure.
- Lifecycle CI earlier failed formatting check (see 6aa7f508f), clippy/tests did not run.
- Failure causes of the two runs were not inspected in this checkpoint; read `gh run view <id> --log-failed` first.

## Next steps
1. Inspect the two failed runs; fix formatting/test failures via CI-only cargo.
2. Land remaining worker fixes (see matching-orchestration-checkpoint-20261029-2203.md).
3. Run fresh-target clippy+tests and mixed gates (.github/workflows/matching-*.yml); record run URL, SHA, binary sha256.
4. Complete parity audit before moving to History.

## Resume observations (2026-10-30)

Latest user preference: TWO workers; Codex Luna only, normal/default speed,
high thinking for hard tasks and low/medium for simple tasks. This supersedes
the prior three-Terra roster. Do not cancel existing workers just to reduce the
count; let them finish and replace only below two.

T3 orchestration tools are unavailable in the resumed tool catalog:
`mcp__t3_code__orchestrator_capabilities` was attempted and returned
`is not a function`; tool discovery exposed no delegate/capability tools.
No Luna replacements launched. Prior worker terminal state is not established.

Failed logs inspected:
- https://github.com/auenkr/cadence-rust/actions/runs/40912650991 at
  `f4ffd30798955dabe34bbd0efe5736bacad17f61`: Clippy fails `let_and_return`
  in `matching_reader.rs:792` and `matching_reader_native.rs:669` (return
  `dispatch_timeout(...)` directly). Exit101; tests not established.
- https://github.com/auenkr/cadence-rust/actions/runs/40912428949 at
  `8fa96afb8cb5463747000bdf96b0f98fcd3a29e8`: configured semaphore launch
  regression still times out at `matching_launch_native.rs:497`;
  976 passed, 1 failed, 2 ignored, exit101. This branch does not contain
  the integration candidate's later lookup/refresh fixture correction.

No source fixes or local Cargo/Go/runtime commands executed on resume.

## Exact-head CI checkpoint (2026-10-30)

Inspected GitHub Actions run 41055162424 once:
https://github.com/auenkr/cadence-rust/actions/runs/41055162424

- Workflow: `rust-ci`, event `workflow_dispatch`.
- Head: `5ee69b5572688c2dba2245c7b411c7182c5b9b16` (the requested exact
  code SHA).
- At inspection, the run was still `in_progress`; Clippy and Formatting had
  completed successfully, while the workspace Test step was in progress.
- No terminal conclusion, test exit code, totals, or artifact evidence is
  established yet. The requested PKG-empty / fresh-target / clippy.exit=0 then
  test.exit=0 evidence must be checked if this run reaches success.
- No immutable gate ref or PostgreSQL mixed-validation dispatch was created,
  because terminal success has not been established. Do not poll this run per
  the user instruction. Matching remains incomplete; the supplemental
  `f009e8d` queued activity branch remains unintegrated.

## Recovered later remote candidates

Remote refs and CI inspected on resume; these are unaccepted candidates:
- `t3/invalid-ticker-recovery`, head
  `4ec0bb103f98b3fc5065e861223536ac0be0e229`; source architecture commit
  `2e2b400f955433402c8f7e0f0413904ba29489f1` and atomic-import correction.
  https://github.com/auenkr/cadence-rust/actions/runs/40912942632 failed.
- `t3/matching-semaphore-typed-errors`, head
  `f0e876398862bba9d8cc6d663a46d424474ab7ce`; changes persistence error
  types, semaphore manager/service and gRPC transport. CI
  https://github.com/auenkr/cadence-rust/actions/runs/40912963402 succeeded
  at that exact head. Validation scope still needs inspection before credit.
- Semaphore cancellation branch's later `dabc3d4a0` is evidence-only.

Fetched candidates without changing master source. Do not redo these fixes
before reviewing them. T3 capability tool remains unavailable on recheck;
no replacement worker was started, no prior task terminal state inferred.
