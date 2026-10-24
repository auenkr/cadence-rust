# Cassandra mixed Matching acceptance on GitHub Actions

- Workflow: `matching-cassandra-mixed-gate.yml`
- Run: https://github.com/auenkr/cadence-rust/actions/runs/40079714508
- Run ID: `40079714508`; job `cassandra-acceptance` passed.
- Head SHA: `2432047f861f4ab68735e3b0d91c0a82303fb7d0`.
- Conclusion: `success` (2026-10-24).
- Runner: `ubuntu-latest`; job timeout: 90 minutes.
- Cargo target: `/tmp/cadence-cargo-target-cassandra-40079714508` (fresh and unique to this run; no Rust cache action).
- Rust binary SHA-256: `6276cb36698e1b1e7c178cf2e5d058a421dbd26f86c83da66a8fffae84f9fa95`.
- Reproduction: `gh workflow run matching-cassandra-mixed-gate.yml --ref t3/cassandra-mixed-gate -f extra_args='--matching-restart --matching-idle-check-seconds 3'`.
- Effective gate: `tests/gates/run-cassandra-lane.sh out/gate <fresh-target>/debug/cadence-matching --matching-restart --matching-idle-check-seconds 3`.

The acceptance result reports `acceptanceEligible: true`, `goMatchingStarted: false`, Rust service `matching`, and Go services `frontend`, `history`, `worker`. The Cassandra probe passed (`probeExitCode: 0`) and the wrapper passed (`runExitCode: 0`). The checkpoint evidence covers two-node ring agreement, idle eviction/recovery, SIGKILL owner recovery, task persistence and range/ack transitions, semaphore persistence and owner crash/recovery, release/fresh acquire, and domain pagination. The wrapper saves and removes its uniquely labeled Cassandra 4.1.7 container.

The first gate attempt stopped on the harness's `:gap` semaphore owner suffix, which Go rejects because owner IDs require a numeric hold ID; the probe now uses `:1`. The next attempt reached the SDK paced-activity phase and failed when a returned activity input was null. The subsequent run kept all payload assertions, recorded Matching task-list owners on both Rust nodes around the paced poll, and passed; its probe log records both owners agreeing. Neither failure was hidden by relaxing assertions.
