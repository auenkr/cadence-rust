# Matching native gate coverage check — 2026-10-29

Read-only source/CI coverage audit. **No lane is passed by this note.** Current
ledger says Matching remains incomplete: workspace CI run 40807471166 passed at
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`, PostgreSQL run 40808511931 failed
before SDK execution, and the other nine mixed lanes are undispatched
(`matching-native-acceptance-ledger-20261029.md:3-9,13-29,37-61`).

## Lane wiring and required dispatches

Workflow definitions exist for all ten required lanes. The production workflow
exposes the six required modes (`postgres`, `thrift`, `shard-distributor`,
`configstore`, `mysql-probes`, `adaptive`) and has distinct diagnostic modes
that must not substitute for adaptive acceptance
(`.github/workflows/matching-production-validation.yml:3-16,80-106`). The
remaining lanes have dedicated MySQL, Cassandra, active-offer, and TLS workflow
definitions. Their relevant inputs and scenario flags are in the current
dispatch map (`matching-acceptance-evidence-readiness-20261027.md:25-40`).

Run the lanes serially at the frozen `FINAL_REF`, using these exact inputs from
the dispatch map:

```sh
gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=postgres
gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=mysql-probes
gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=thrift
gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=configstore
gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=shard-distributor
gh workflow run matching-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f extra_args='--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3 --matching-exporter prometheus --matching-cold-domain-metric-probe'
gh workflow run matching-cassandra-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f extra_args='--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3'
gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=adaptive
gh workflow run matching-active-offer-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"
gh workflow run matching-production-tls-mixed.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"
```

The supplemental dependency oracles are separate and non-substituting:

```sh
gh workflow run matching-dependency-oracles.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"
```

Also run same-pin fresh workspace CI with Clippy before workspace tests, as the
readiness guide requires (`matching-acceptance-evidence-readiness-20261027.md:38-40,72-85`).

## What source wiring establishes

- The mixed harness accepts only supported Rust services, defines
  `--rust-services matching` as the production lane, and derives every other
  service as Go (`tests/run-go-service-baseline.py:37-43,368-375`). The database
  wrappers invoke that mode (`tests/gates/run-mysql-lane.sh:42-45`,
  `tests/gates/run-postgres-lane.sh:37`,
  `tests/gates/run-cassandra-lane.sh:45-46`). This establishes intended Go
  Frontend/History/Worker plus Rust Matching topology. Every artifact still
  needs runtime process/config evidence proving that topology and no Go Matching.
- Each mixed workflow builds the Matching binary on its unique/fresh target and
  records its SHA256; see production validation lines 46-56, two-node MySQL
  lines 33-41, Cassandra lines 34-46, active-offer lines 29-38, and TLS lines
  27-40. Uploads retain artifacts for three days. This does not establish that
  a scenario ran, that the tested process used that binary, or that the run
  used the frozen final SHA. Verify Actions `headSha`, `commit.txt`, scenario
  exits/results, and process/binary association per the readiness guide
  lines 17, 44-46.
- The workflow args request SDK and recovery coverage: common production lanes
  include restart, two-node recovery, and short idle checks; individual lanes
  add backend/transport/scenario probes. The harness describes the behavioral
  assertions and sets implied recovery flags for Cassandra and shard-distributor
  (`tests/run-go-service-baseline.py:61-77,423-442`). Flags prove intent only;
  inspect results and SDK/recovery artifacts. In particular, PostgreSQL's
  current run failed before SDK, and its cleanup artifact did not prove the
  container was removed (`matching-native-acceptance-ledger-20261029.md:50`).
- Supplemental PostgreSQL TLS, actual Go History peer, and native PostgreSQL
  persistence checks are invoked by the dependency-oracles script, not by the
  ten mixed lanes (`tests/gates/run-matching-dependency-oracles.sh:1-23,149-152,235-249`).
  They cannot replace shared-cluster SDK/recovery validation.

## Gaps and next actions

1. **All ten runtime lanes remain uncredited.** Nine have no dispatch; PostgreSQL
   failed during Frontend discovery and did not reach SDK or recovery. Fix or
   diagnose the native discovery/readiness/shutdown problem through the owning
   integration work, then re-freeze a coherent source SHA if source changes.
   Collect all ten serially and review actual results, cleanup, topology, hash,
   and SDK/restart/crash/idle outcomes. Do not infer success from a green build,
   requested flags, or older artifacts.
2. **No cross-lane same-pin evidence yet.** Current mixed candidate is frozen at
   `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`; preserve its pin unless fixes
   force a replacement. For every run compare Actions `headSha` and
   `out/commit.txt` with the candidate, retain binary SHA256, then record
   scenario-specific evidence and cleanup. Workspace CI already passes at the
   candidate; it does not offset mixed-lane failures or omissions.
3. **Independent semaphore startup timeout is still uncovered.** The Go source
   trace records `handler/engine.go` starting semaphore loading with an
   independent 30-second background context; this is distinct from the caller's
   RPC context. The new component coverage note explicitly says the independent
   startup timeout is not tested (`matching-semaphore-startup-tests-20261029.md:3-7`).
   Existing cases cover initiating-caller cancellation while accepted startup
   continues, a concurrent caller deadline while waiting, and a failed scan
   releasing waiters. These do not prove the 30-second loader timeout, timed-out
   load cleanup/unregistration, or successful later reacquisition. Add a
   controllable startup-timeout test with an injected short duration/test clock;
   assert waiter result, manager removal and subsequent fresh load. Keep it
   separate from caller cancellation/deadline assertions. The reported current
   coverage is source-level and unverified until CI executes it.
4. **Supplemental oracles remain pending.** Dispatch the focused dependency
   oracle only after/alongside serial lane work without concurrent resource-heavy
   gates; record its URL, head SHA, conclusion and artifact. It is supplemental,
   not an eleventh mixed lane.
5. **Parity audit remains open.** The ledger calls for reconciling native runtime
   source behavior and ignored-test exclusions; RPC inventory and component
   tests are not full parity proof (`matching-native-acceptance-ledger-20261029.md:72-94,104-110`).
   Close that audit and retain reproducible evidence before declaring Matching
   complete or moving to History.

No CI was dispatched or polled; no local Cargo, Go, test, build, format, or
runtime validation was run. No source/workflow changes were made.
