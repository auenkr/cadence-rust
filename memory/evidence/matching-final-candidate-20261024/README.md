# Matching final candidate validation (in progress)

Pinned branch: matching-final-candidate-20261024
Source: 27d3ff5f60f0604553824420da0cf61026eda468

This is a validation tracker, not service sign-off. All implementation slices
and focused tests are integrated. No History migration begins before this
candidate's acceptance and parity audit complete.

| Required lane | Run | State when recorded |
| --- | --- | --- |
| Fresh uncached workspace clippy then tests, ownership/metric oracles | https://github.com/auenkr/cadence-rust/actions/runs/40108643448 | success |
| PostgreSQL SDK, two-node recovery and extended probes | https://github.com/auenkr/cadence-rust/actions/runs/40108645569 | success |
| Thrift/TChannel | https://github.com/auenkr/cadence-rust/actions/runs/40153127238 | success |
| Cassandra | https://github.com/auenkr/cadence-rust/actions/runs/40153493941 | success |
| MySQL extended probes | https://github.com/auenkr/cadence-rust/actions/runs/40155956238 | success with corrected harness; failed40153901033 preserved |
| Configstore | https://github.com/auenkr/cadence-rust/actions/runs/40155523508 | success |
| Real shard distributor | https://github.com/auenkr/cadence-rust/actions/runs/40156296140 | running |
| Active offer with Go History, zero inserts | pending | not started |

Run mixed lanes serially. Preserve source, binary SHA256, command, exit files
and compact probe output for each. Earlier successes remain scenario evidence
only and cannot stand in for this integrated candidate. Workspace formatting
has known unrelated debt; advisory status must remain explicit, with no
source mutation before checks.

Already integrated behavior evidence includes the root-start remote refresh
production test, typed live OpenFeature reads, Unleash polling/evaluation and
lifecycle, actual Go/Rust metric sink comparison, configstore long-poll expiry,
SDK workflow/restart/drain/crash scenarios, active-offer zero-insert audit, and
file watcher reload/error-retention/shutdown tests. The requirement mapping is
memory/matching-completion-audit-20261024.md; update its stale status lines
with actual final results, not blanket claims inferred from green workspace CI.

## MySQL readiness correction (2026-10-25)

Failed MySQL run40153901033 exposed a probe readiness race: Frontend could
route to the secondary before the restarted primary bound gRPC. Integrated
harness correction is master commit `e06fd8ca6`, pinned for rerun on
`matching-final-validation-20261025`. Both that commit and original candidate
27d3ff5f have exactly the same `rust/` tree hash
`823ff09bee39660110e07c915e449ff84a33bbf0`; no production, Go or workflow files
changed. The validation branch adds the exact-primary readiness assertion and
memory updates. Existing candidate lane evidence remains bound to its original
source; new runs must record their own source and binary hash. Configstore
40155523508 is currently running on the original candidate. After it ends,
rerun MySQL using the corrected validation branch before the remaining lanes.
See `memory/mysql-final-40153901033-restart-diagnosis.md`.

## Typed refresh test supplement

Master merge `a568bcae1` adds only the two real-wire typed History refresh
error tests and their evidence note. No production source changed. Fresh
Matching package run40155206531 passed strict clippy and tests on worker code
`4cabb9cecee5a177a2a9462a9e8838ed9fb13324`. Workspace formatting remains
advisory debt as described in the note. This supplement does not alter the
production candidate or the corrected mixed-gate validation branch.

## Integrated master fresh-target supplement

Run https://github.com/auenkr/cadence-rust/actions/runs/40155846010 validates
master source `1fc3a899c00c6eac5b2079b933092ca0df270313` with
`clean_target=true`, workspace strict clippy followed by workspace tests.
Success; compact evidence is in `integrated-workspace/`. It includes the merged typed-refresh tests and the
readiness harness fix; no production Rust source differs from27d3ff5f.
Automatic cached run40155703537 was superseded/cancelled in favor of this
explicit fresh-target gate.

Corrected MySQL rerun: https://github.com/auenkr/cadence-rust/actions/runs/40155956238 on validation branch `matching-final-validation-20261025` (source e06fd8ca6); success.
