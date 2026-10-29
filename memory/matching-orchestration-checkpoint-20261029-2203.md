# Matching orchestration checkpoint (2026-10-29 22:03 IST)

Matching remains incomplete. No candidate source accepted onto master. No
workspace or native mixed acceptance claimed. All Cargo/Go/runtime validation
remains CI-only. User requests exactly three workers, Terra for hard work.

Returned integration: `origin/t3/matching-semaphore-startup-repair`, evidence
`6316a39ebac6eb9b644675035024eba7dd65b1a2`, source
`f4ffd30798955dabe34bbd0efe5736bacad17f61`. Retains lifecycle idle binding,
timeout providers and forwarded deadline fixes, applies failed-log formatting,
corrects configured semaphore fixture lookup/refresh-list inconsistency.
CI https://github.com/auenkr/cadence-rust/actions/runs/40912650991 pending
at latest observation. Needs independent review and other workers' followups.

Returned semaphore adapters: `origin/t3/matching-semaphore-cancel-parity`, source
`8fa96afb8cb5463747000bdf96b0f98fcd3a29e8`; correction commits
`4268911f45dbb9ad38e2fed9e127273c80cb367c` and `8fa96afb...`.
Context propagated into Cassandra I/O; success/cancellation and typed errors
still require independent review and closure. CI
https://github.com/auenkr/cadence-rust/actions/runs/40912428949 in progress.

Returned lifecycle configuration: `origin/t3/matching-lifecycle-startup`, source
`3d36aef92aab362ba52772868ba3307f11283359`, evidence `b4329277e`.
Adds partition/forwarding/QPS/burst binding; invalid ticker architecture was
NOT implemented. CI
https://github.com/auenkr/cadence-rust/actions/runs/40912368833 in progress.

Terminal evidence newly verified: run40911583315 at `5abd83ef...` failed:
974 passed, 1 failed, 2 ignored; configured semaphore launch test timed out
at matching_launch_native.rs:497, test exit101. Runs40911834316
(`465697d7...`) and40911874411 (`e981d53e...`) failed runtime formatting,
exit1. Run40912311594 is cancelled and superseded by40912428949.

Current three delegate task suffixes (prefix
`node:delegated-task:command%3Amcp%3Af18c299c-79dd-4b96-a532-5d7a80b68d03%3Adelegate-task%3A`):

- `cb84767a-2ba3-4c2a-9ed1-9a13c2576d65`: implement retained partially
  started manager after invalid ticker, exact lease/reuse/shutdown parity.
- `c95fde33-7e6e-41e7-87cf-22682c039cdf`: independent semaphore cancellation
  review and typed internal/untyped/timeout/ServiceBusy detail closure.
- Integration review/remote-wire forwarding replacement launched this turn;
  task identifier is in orchestration tool history. Owns actual remote-wire
  deadline observation and cancellation/completion boundary regressions.

All use T3 delegate_task worktree isolation, codex gpt-6.1-terra high/default.
Follow callback next: do not poll running delegates. New reviews are new tasks.
After corrections: reconcile exact commits, freeze reviewed candidate, fresh
uncached dedicated-target workspace Clippy then tests on CI, PostgreSQL pool
oracle, all ten serial mixed SDK failure/restart/recovery lanes with no Go
Matching and exact Rust binary SHA256, full parity audit. Do not advance History.
