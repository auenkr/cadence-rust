# MySQL extended mixed run 40173683827

- URL: https://github.com/auenkr/cadence-rust/actions/runs/40173683827
- Workflow: `matching-production-validation.yml`, input `lane=mysql-probes`
- Branch/head: `t3/matching-validation-history-ring-20261025` / `29bdd2b04b573309112286c57f1c347f0ba0b60c`
- GitHub conclusion: `success`; build, gate, lane, and run exits: `0`
- Rust Matching binary SHA256: `e3792553dfc83b9779858d9a94390a928374e2ed8d59a3a7124d6b8df43283cf`
- Artifact 12592035248 ZIP SHA256: `0259bb1931de5ae01f0290605fe79ee96244c12f2133e04a0951f320baf22a3b`; ZIP retained outside the repository at `/mnt/storage/service/ci-evidence/run-40173683827/artifact-12592035248.zip`.

Topology from `results.json`: original Go Frontend, History and Worker with Rust Matching; `goMatchingStarted=false`; production Go Ringpop discovery is active. This is the original-Go-services mixed topology, not a standalone fixture. The workflow's main probes used gRPC Matching and History.

The probe's literal result is `preliminary-passed` and `acceptanceEligible:false`. It reports SDK idle recovery, graceful drain/recovery, process restart and failure recovery; durable restart drained queued tasks from root and child partitions; child-to-root and parent-to-child backlog forwarding; standby query forwarding; partition listing across two Rust Matching nodes; and AutoConfigHint. Full compact scenario fields are in `summary.json`.

Limit: this remains scenario evidence, not Matching acceptance. MySQL GC observations report a pending row and explicitly say durable GC evidence was not observed for two activity queues; no claim of complete GC parity is made. Preserve `preliminary-passed` / `acceptanceEligible:false` literally.

The full artifact ZIP and extracted raw logs/binary are outside git under `/mnt/storage/service/ci-evidence/run-40173683827/`. Only compact text metadata and scenario summary are committed here.

## MySQL GC queue observations

`mysql-gc-queues.json` is a selective extraction of the current run's raw
`taskGarbageCollection` queue objects from both artifact paths
`gate/run/results.json` and `gate/run/rust-matching/db-observations.json`.
Each source path's SHA256 is recorded there; both extracts agree. The queue
names, observed maximum task IDs, ack levels, remaining task rows and original
notes are preserved. No older dated GC evidence was used.

For `backlogActivity` and `restartActivity`, `durableGcEvidence:false` means no
persisted task row for that queue was observed before its delivery. This is a
limitation of the observation, not evidence of a GC bug. The run's
`acceptanceEligible:false` is expected for this non-Cassandra lane by the gate's
classification; retain that literal and the `preliminary-passed` result.
