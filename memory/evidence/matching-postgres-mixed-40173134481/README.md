# PostgreSQL production mixed lane — run 40173134481

Run: https://github.com/auenkr/cadence-rust/actions/runs/40173134481

Candidate branch: `t3/matching-validation-history-ring-20261025`  
Exact source: `29bdd2b04b573309112286c57f1c347f0ba0b60c`  
Workflow/input: `matching-production-validation.yml`, `lane=postgres`  
Conclusion: success.

Fresh Matching build, gate, lane and service-probe exits were all `0`. The
fresh `cadence-matching` binary SHA256 is
`8adfef671d7ce020e14079e829b04419892af15ffe79d5baa0328c7dd50a052a`.
Exact commands, source identity, exits, results, recovery checkpoints and
probe logs are retained here. The original workflow artifact ZIP and SHA256
are at `/mnt/storage/service/ci-evidence/run-40173134481/`; the extracted
repository evidence does not include binaries.

## Topology and scenarios

The artifact reports topology **Go Frontend/History/Worker with Rust Matching
via production Ringpop**, `goMatchingStarted=false`, and Go services
`frontend`, `history`, `worker`. No Go Matching process was started. The Rust
Matching service discovered through the original Go Ringpop provider; History
routing used gRPC.

Go SDK restart and recovery checks passed: restart resumed and completed the
pre-restart workflow/activity; crash/failure recovery and idle unload/recovery
completed; graceful drain recovered its queued task. Checkpoints and SDK logs
are preserved. PostgreSQL probes passed for task GC/domain metadata, standby
query forwarding, partition listing, durable restart/drain and AutoConfigHint.

The emitted classifications are preserved literally: `preliminary-passed`
and `acceptanceEligible:false`. This is one production mixed lane, not full
Matching acceptance. The result explicitly says general
`productionDatabaseEvidence` was not collected; PostgreSQL scenario evidence
is separately present. Active-offer, backlog/configstore and shard-distributor
were not requested in this lane. Cassandra acceptance remains a separate gate.

The lane artifact includes `postgres-container-cleanup.txt`; its recorded
`docker rm --force` output names the disposable container. The original ZIP
also retains the container log. No local Cargo/Go/rustfmt command ran.
