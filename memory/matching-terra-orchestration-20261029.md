# Matching continuation: three Terra workers — 2026-10-29

Matching remains incomplete. Do not begin History migration. This note records
orchestration and terminal CI evidence, not source acceptance.

## Confirmed terminal semaphore CI result

Read with `gh run view 40853270019 --json url,headSha,status,conclusion,jobs`
and `gh run view 40853270019 --log-failed` on this continuation.

- URL: https://github.com/auenkr/cadence-rust/actions/runs/40853270019
- Exact head: `592a0bb03660479c890680d316b58067f3527810`.
- Status: completed; conclusion: failure.
- Clippy and formatting succeeded. Test step failed with exit 101.
- Matching tests: 975 passed, 2 failed, 2 ignored.
- `matching_launch_native::tests::configured_grpc_semaphore_slow_start_deadlines_cleanup_and_retry`
  panicked at `matching_launch_native.rs:494` by unwrapping `Err(Elapsed(()))`.
- `matching_semaphore_service_native::tests::slow_start_keeps_running_after_caller_cancel_and_reuses_manager`
  panicked at `matching_semaphore_service_native.rs:550` with the same error.
- No binary SHA or mixed-cluster acceptance is established by this observation.

This supersedes the earlier in-progress observation for this exact run only.

## Requested worker roster

User explicitly selected **three workers** and restricted models to Codex
GPT-6.1 Terra for harder work and Luna for simple work. All three current tasks
are hard parity/integration work, so all use provider `codex`, model
`gpt-6.1-terra`, high reasoning, default service tier, isolated tool-created
worktrees. They may not spawn children. All Cargo/Go/runtime validation stays
on public-repository standard GitHub Actions runners; no local validation.

Parent thread: `3ed37b44-d79a-4171-9dfc-1fdd778fea93`.

1. Semaphore startup CI failure repair:
   `node:delegated-task:command%3Amcp%3Af18c299c-79dd-4b96-a532-5d7a80b68d03%3Adelegate-task%3A07d1a05e-22ed-42e2-a473-01c3883599a3`.
   Owns recovery of exact failed source, root-cause repair and focused CI.
2. Timeout/raw-byte integration:
   `node:delegated-task:command%3Amcp%3Af18c299c-79dd-4b96-a532-5d7a80b68d03%3Adelegate-task%3A1cd6194d-7f7f-48ff-b5b2-caae502f8f5e`.
   Owns integration of `18040515d` and `90aa741`, domain-bound timeout
   configuration and cancellation/deadline regressions.
3. Lifecycle parity closure:
   `node:delegated-task:command%3Amcp%3Af18c299c-79dd-4b96-a532-5d7a80b68d03%3Adelegate-task%3Ad24fcf01-451e-44e1-8cfd-4b3d65dc614b`.
   Owns lifecycle integration/fixes, domain lookup/rename binding, ticker
   semantics, blocked-reader shutdown races and injected teardown failures.

Each launch returned running with completion wakeup instructions. Do not poll
these tasks or infer completion from this note. Integrate only their actual
returned commits and evidence. Previous checkpoint task cancellation attempts
returned `task_not_found` because those tasks belong to another parent thread;
their state is unknown, and they are not counted as controlled by this roster.
Workers must recover existing branch results before duplicating changes and
must not edit other worktrees.

## Acceptance still required

### Returned candidates and replacement roster

The initial three workers completed. Their results are **unaccepted source**:

- Semaphore: `origin/t3/semaphore-startup-cancel`, head
  `16dd8158ea094886ef5c83bb7e46f5296e5a2f0f`, source
  `a86d7a915a1f64d4fac9c5b260b016b95c9c8d6a`. Both failed tests had invalid
  encoded workflow-ID lengths and were corrected. Focused CI
  https://github.com/auenkr/cadence-rust/actions/runs/40910747745 was observed
  in progress at that source; no terminal result is recorded here.
- Lifecycle: `origin/t3/matching-manager-lifecycle`, head `fa4f1fd0a`, source
  `96a8dee48df24ffc1c634c73ce82a92a385d9135`. Adds GC error logging and an
  injected two-owner teardown regression. Includes overlapping shared ancestry.
  CI https://github.com/auenkr/cadence-rust/actions/runs/40910919611 was
  pending in the worker's observation. Major lifecycle defects remain open.
- Timeout/raw-byte: `origin/t3/matching-timeout-domain-binding`, head
  `1a26589ebf633951877ee306eb19abccd5563ef5`, source
  `b61ba5d954fb8bc46e816298dbde4101dd3a5ead`. Preserves requested source
  ancestry, binds task/poll wait and writer providers to resolved domain name
  with live values, and tracks claimed backlog handoff to avoid duplicate retry.
  CI https://github.com/auenkr/cadence-rust/actions/runs/40910970647 was
  pending in the worker's observation. Timeout race review remains required.

A subsequent independent semaphore review completed at
`origin/t3/review-semaphore-startup`, head `59acdb3c3` (review note only).
**Hold the semaphore candidate**: unconditional caller cancellation can discard
a successful confirm/grant that Go returns successfully, and ordinary startup
scan errors lack Go's bucket-error wrapper. See the branch's
`memory/matching-semaphore-independent-review-20261029.md` for source pointers
and required controlled race regressions. Fixture lengths were confirmed valid.

Current three Terra implementation assignments, each launched with callback
wakeup instructions (do not poll; these observations are not current liveness):

- Lifecycle closure:
  `node:delegated-task:command%3Amcp%3Af18c299c-79dd-4b96-a532-5d7a80b68d03%3Adelegate-task%3Ade6edeba-21bb-4fea-bbb3-afcfd20e711a`.
  Owns lookup-failure-before-lease and rename-stable binding, ticker side
  effects/recovery, blocked-reader shutdown and periodic condition-failure
  eviction. Must return independently applicable followup fixes.
- Candidate integration and timeout audit:
  `node:delegated-task:command%3Amcp%3Af18c299c-79dd-4b96-a532-5d7a80b68d03%3Adelegate-task%3Af4d1f4bc-73c9-4118-b997-7a4042fc59a9`.
  Owns combining the exact three initial branches and checking/fixing timeout
  semantics. Was steered with the semaphore hold and separate fix ownership;
  must not claim final acceptance before followups are integrated.
- Semaphore review corrections:
  `node:delegated-task:command%3Amcp%3Af18c299c-79dd-4b96-a532-5d7a80b68d03%3Adelegate-task%3A76daaef4-f3f1-4b4c-9872-f1316c079cc9`.
  Owns production cancellation-success race semantics, controlled confirm/grant
  regressions, ordinary startup error wrapping and the extra metadata lookup
  audit. Must return independently applicable followup fixes.

Combine and review the resulting source, freeze one immutable candidate, then
obtain fresh dedicated-target workspace Clippy followed by workspace tests,
the PostgreSQL pool oracle, all ten required serial mixed Go/Rust SDK failure,
restart and recovery lanes, and the remaining configuration/source parity
audit. Record run URL, exact head, conclusion, binary SHA256, actual topology,
scenario results and cleanup. Native mixed passes remain zero in the current
acceptance record. A focused CI pass cannot establish service completion.
