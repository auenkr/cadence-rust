# Matching Luna integration handoff — 2026-10-30

Matching remains incomplete. All Cargo/Go/formatting/runtime validation CI-only.
Two workers, Codex gpt-6-luna high thinking, default/normal speed.

## Returned candidates (not accepted onto master)

- Semaphore review source `26f6a88535463740d722cdeac922571a7424a943`,
  branch `t3/matching-semaphore-parity-review`, docs
  `396957ee71665d60a9efcbff03d809dd79cc425d`. No correctness defect found by
  worker in bounded cancellation/Cassandra context/typed transport review.
  Focused Matching CI https://github.com/auenkr/cadence-rust/actions/runs/41021199544
  confirmed in progress at exact source SHA; last observation Test running.
  Worker report incorrectly calls run40912963402 workspace tests: authoritative
  artifact review in matching-semaphore-ci-scope-40912963402.md proves only
  persistence tests. Correct that wording when importing candidate docs.
- Lifecycle source `ffa57359f801aeb2f5f60d6383a3675443f7cfe9`, branch
  `t3/matching-lifecycle-recovery`, docs head `c431e39eb`. Removes reader
  let_and_return errors, ports partial startup after invalid liveness ticker
  and lease reuse/shutdown regression. Excludes domain binding3d36aef92 and
  configured semaphore fixture repairs c37d8a8, so it is not the complete
  predecessor integration. Focused run
  https://github.com/auenkr/cadence-rust/actions/runs/41021537962 pending at
  worker return; Rust validation had not run at its last observation.

## Current ownership

T3 task prefix:
`node:delegated-task:command%3Amcp%3A392f7b55-aa6b-49c6-b90d-d3fa0e4a1c5e%3Adelegate-task%3A`.

- `e23b4a3b-b2f1-4d52-a47c-7595e512a3d1`: combine both returned sources,
  audit omissions from f4ffd30798955dabe34bbd0efe5736bacad17f61, restore required
  construction-time domain binding and launch fixture repairs, check remote
  forwarded deadlines, correct CI scope claims, dispatch clean_target=true
  full workspace CI. Do not merge master or create PR.
- `ed5ed6a0-8f5f-48cf-bc78-4e4d3b97c258`: investigate native PostgreSQL
  discovery and shutdown failure40808511931 using retained artifacts/harness.
  Own harness/config repairs only; report overlapping service diagnosis to
  avoid lifecycle edits. Do not dispatch mixed gate on incoherent old source.

Both dispatch tools returned running; completion callbacks wake parent.
Do not poll delegates or infer later status. Admission before latest worker:
load5.95, MemAvailable3.8GiB, within limits. No local heavy validation.

After coherent integration: review actual exact-head fresh workspace artifacts,
freeze candidate, run all ten serial native mixed lanes with Go Frontend,
History and Worker and no Go Matching, retain SDK/restart/recovery and Rust
binary SHA256 evidence, close parity audit. Do not advance History migration.
