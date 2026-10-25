# Matching active validation checkpoint

Matching remains incomplete. Implementation master before this documentation
checkpoint: `a5a8e2a14638efcc2d7f6cc5bd6ec027271978dd`.

## Active Actions

- Fresh uncached whole-workspace clippy then tests:
  https://github.com/auenkr/cadence-rust/actions/runs/40217841072
  tests `6af476941031193317fdd6c44070e9785e6384b9`, `clean_target=true`.
  Last authoritative observation: in progress, Test step. Master descendants
  through a5a8e2a14 change only the mixed test fixture and documentation, not Rust
  implementation. Do not claim this run tests the newer fixture.
- Shard-distributor mixed rerun:
  https://github.com/auenkr/cadence-rust/actions/runs/40218022772
  tests `a5a8e2a14638efcc2d7f6cc5bd6ec027271978dd`.
  Last authoritative observation: in progress, Rust toolchain setup.
  Only one mixed gate is dispatched. Its selection snapshot must confirm which
  executor was killed; retain TTL expiry and assignment-cleanup outcomes.

## Worker assignments and preference

User now requires Codex gpt-6-luna for new workers, after the existing OpenCode
evidence worker finishes. Do not start replacement OpenCode/Claude or older Luna
models. gpt-5.6-luna previously failed with model_not_found; no work was produced.

- Existing OpenCode evidence task: `matching-luna-evidence-repair-r2`.
  It owns compact SHA256/exit/scenario evidence from eight green mixed runs on
  6af476941, correcting the false Go-only-baseline characterization. Let it finish.
- Codex gpt-6-luna task: `matching-luna-observable-parity-audit-r2`.
  It owns current-source non-durable Describe and membership/semaphore shutdown
  parity, with actual behavior fixes/tests if justified. No mixed gate dispatch.

Completed source-audit note is integrated at
memory/matching-longpoll-config-source-audit-20261025.md. Both decision/activity
long-poll expiry tests are active; earlier ignored-test claims were stale.
The shard selection patch and qualified diagnosis are integrated in
memory/shard-distributor-mixed-gate-20261025.md. Root cause remains a hypothesis
until the new gate's identity snapshot and expiry result are inspected.

## Resume actions

Inspect completed Actions artifacts and worker results, integrate reviewed fixes,
then pin the final candidate and run missing fresh workspace/serial mixed lanes
for that candidate. Keep source SHA and binary SHA256 explicit. Do not treat old
green runs, incomplete artifact reviews, or architecture-only ignored tests as a
completion proof. No future-service work. All Cargo/Go/format remains Actions-only.
