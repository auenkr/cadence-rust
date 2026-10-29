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

Combine and review the resulting source, freeze one immutable candidate, then
obtain fresh dedicated-target workspace Clippy followed by workspace tests,
the PostgreSQL pool oracle, all ten required serial mixed Go/Rust SDK failure,
restart and recovery lanes, and the remaining configuration/source parity
audit. Record run URL, exact head, conclusion, binary SHA256, actual topology,
scenario results and cleanup. Native mixed passes remain zero in the current
acceptance record. A focused CI pass cannot establish service completion.
