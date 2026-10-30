# Matching PostgreSQL probe artifact diagnosis (2026-10-30)

## Candidate and CI identity

- Supplemental candidate: `t3/matching-queued-activity-candidate-5ee69b5`
- Candidate source SHA: `565c8fc91893fae1a81cd8e47f10f2ee4744c046`
- PostgreSQL mixed gate: [run 41056791529](https://github.com/auenkr/cadence-rust/actions/runs/41056791529), conclusion: failure.
- Its artifact is `matching-production-postgres`, artifact id `12977801450`, 70,585 bytes, not expired at inspection. Downloaded evidence includes `matching-production-postgres/gate/run/probe.log`.
- Fresh workspace CI: [run 41058036608](https://github.com/auenkr/cadence-rust/actions/runs/41058036608), exact head SHA `565c8fc91893fae1a81cd8e47f10f2ee4744c046`; it was still `in_progress` at the one requested inspection. Do not dispatch a competing mixed gate until this run is terminal and successful.
- Failed gate Rust binary SHA256: `d9a76461f356a783d89dd4309e585d773f78f07410c17cb2c385899302f32b50`.

## Artifact and failure diagnosis

The suspected missing log was not omitted by workflow collection. The artifact upload in `.github/workflows/matching-production-validation.yml` includes `out/` with only generated executable exclusions and three-day retention. The artifact API reports one uploaded artifact. The downloaded `gate/run/probe.log` is present. The local `gh run download` attempt reported `file exists` while extracting `build.exit`; the destination already contained a partially or previously extracted artifact. That local extraction collision explains the apparent missing-file report, not an Actions upload filter.

`probe.log` reports that Rust Matching did not become ready before restart. The Go Frontend readiness call timed out after 19.999 seconds and direct primary gRPC status timed out after 10 seconds with `Waiting for LB pick`. The Rust server log shows both listeners opened, successful Ringpop join acknowledgements from Go Frontend, History, and Worker, membership publication, admission opening, and `ListTaskListPartitions` handler progress through owner lookups. The probe then records graceful shutdown timing out and SIGKILL. `results.json` records `probeExitCode: 1`, `acceptanceEligible: false`; the SDK workflow and restart/failure-recovery assertions did not run. The Go service log also contains `Not enough hosts to serve the request` errors during task polling, but this evidence does not isolate a safe source fix.

## Action and next gate

No source change or acceptance weakening is justified by the evidence. No diagnostic retention patch is needed: `probe.log`, `results.json`, and `rust-server-before-restart.log` were retained in the artifact. Preserve the supplemental candidate source as-is.

Next: after workspace run 41058036608 reaches a terminal successful conclusion, dispatch the PostgreSQL-only `Matching production validation` workflow on the immutable candidate SHA `565c8fc91893fae1a81cd8e47f10f2ee4744c046`. Record its URL, head SHA, conclusion, artifact contents, and binary SHA256 here. If workspace CI fails, diagnose that first and do not start the PostgreSQL gate.
