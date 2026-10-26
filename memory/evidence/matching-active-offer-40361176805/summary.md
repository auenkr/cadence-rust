# Active-offer mixed run 40361176805 (preliminary evidence)

Run: https://github.com/auenkr/cadence-rust/actions/runs/40361176805 (success, 2026-10-26 16:39–16:44 UTC); head/embedded source `12a9a12cfb1cc72a04a2fd3443a8ce964b53cc8c` (embedded `commit.txt` matches). Artifact downloaded to `/mnt/storage/service/ci-evidence/40361176805`; the bulky archive remains outside the repository.

## Hashes and exits

- Fresh Rust Matching build SHA-256 recorded by CI: `6552269110cac7eae2f86aafc3d6a231dbfa4bc682e8405f7b68b3dd5213d770` (`binary.sha256`, path `/tmp/cadence-cargo-target-active-offer-40361176805/debug/cadence-matching`). Artifact does not contain that matching executable, so this is the CI-generated hash record and cannot be independently re-hashed from the downloaded artifact. The bundled `gate/run/cadence-server` is the Go baseline server, SHA-256 `f474fd39009582528a0e384cbc0261d6ca4b37e1e21b017b8c990e9259f53d00`, not the Rust binary.
- `build.exit=0`, `gate.exit=0`, `gate/lane.exit=0`, `gate/run/run.exit=0`; `results.json.probeExitCode=0`, `runExitCode=0`, `failure=null`.

## Scenario evidence and scope

- `activeOffer.json`: passed; history scheduled task and task token delivered to the active poller; 0 persisted task rows and 0 insert attempts. `probe.log` records the same workflow/run as passed.
- Mixed topology: Rust Matching with Go Frontend/History/Worker; `results.json.goMatchingStarted=false`; SDK log reports Go SDK echo, activity retry, child workflow, workflow retry/task retry, timeout, signal and cancellation scenarios passed through this topology. Go services remained alive after probe.
- SIGTERM graceful drain: `matching-shutdown.json` records SIGTERM and exit code 0. `production-failure-recovery-checkpoints.json` records queued work completed by the surviving Rust node and SDK completion (one completion event).
- SIGKILL recovery: checkpoints record the killed Rust host, survivor, ownership range 1→2, ack 0→1, and SDK workflow completion (one completion event, no remaining queued rows). The durable restart queue also had one row before kill and recorded one workflow completion afterward.
- Two-node ownership sampling showed both clients agreeing on the owner. Failure-recovery checkpoints show changing owner endpoints across samples and recovery after the selected owner crash; this is evidence of takeover/recovery, not a separate full cluster failover acceptance run.
- MySQL cleanup record names container `cadence-rust-gate-mysql-lane-20261026T164238-8292`; the lane cleanup step completed. No Go Matching instance was started.

This is explicitly preliminary, not final Matching acceptance: `results.json.acceptanceEligible=false`, `parityGate` says production mixed SDK/restart evidence only and Cassandra acceptance requires `--cassandra`; the gate log calls the probe preliminary. It is also only the named active-offer + MySQL lane, not the final combined candidate gate.

## Mismatch / required follow-up

The source predates fatal registry clone fix `e69c7663392db4e2c356d38d2a4a42a4b35125b8` (which installs the fatal writer sender on the RPC handler registry clone). Despite green exits and scenario assertions, do not use this run as evidence for the final candidate. Rerun the combined candidate on the fixed source and bind the result to its exact source SHA and fresh binary SHA. This evidence records preliminary scenario behavior only. TLS gate 40364035990 was dispatched on master `01148d6cf9c1e18a26e1e3385337d5f0c53bd534`; one status read at recording time: in progress. No polling performed.
