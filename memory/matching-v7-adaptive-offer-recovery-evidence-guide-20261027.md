# Matching v7 adaptive, offer, and recovery evidence guide

Read-only mapping of current probes and collectors for the adaptive, active-offer,
and two-node MySQL lanes. This is not runtime evidence: no run or pass is claimed.
Use with `matching-v7-final-acceptance-ledger-20261027.md` and
`matching-acceptance-evidence-readiness-20261027.md`; exact-pin, topology, exit,
binary SHA256, SDK, and cleanup checks there remain mandatory.

## Adaptive MySQL lane

Dispatch the `adaptive` lane from `matching-production-validation.yml`, which
supplies `--probe-adaptive-scaler` and common `--matching-restart`,
`--matching-failure-recovery`, and idle recovery arguments. The
`adaptive-single-diagnostic` lane intentionally removes two-node failure
recovery and cannot substitute.

- `gate/run/results.json` and `gate/run/probe.log`: inspect final scenario row
  (`probeScenarios.adaptiveScaler`) and its `result`, plus top-level `result`,
  `runExitCode`, `probeExitCode`, `goServices`, `goMatchingStarted`, and
  `rustServices`. Requested flags alone do not show execution.
- `gate/run/rust-matching/adaptiveScaler.json` (scenario writer uses the exact
  filename `adaptiveScaler.json`) records the scenario result. The assertion
  checks the persisted adaptive `partitionConfig` write-partition count grows
  and version advances; it compares each Rust node's Go-shaped static
  `ListTaskListPartitions` result with Go Frontend; then verifies 24 load
  workflow histories each have exactly one completion event and the Go SDK
  subprocess succeeds on the scaled queue. `adaptive-scaler-sdk.log` contains
  SDK output. If retry-only was separately requested, inspect
  `adaptive-scaler-retry-only-sdk.log`; it is supplemental and does not replace
  the full SDK scenario.
- Rust logs are under `gate/run/rust-matching/rust-server-*.log`. If adaptive
  trace capture was requested, trace records may be in those logs and can be
  extracted with `extract-matching-adaptive-trace.py`; trace capture is an
  optional diagnostic input, not required output from the standard dispatch.
- Common recovery evidence: `production-failure-recovery-checkpoints.json`,
  `production-crash-recovery-sdk.log`, `production-idle-recovery-sdk.log`,
  `restart-sdk.log`, `graceful-drain-sdk.log`, `matching-shutdown.json`,
  `db-observations.json`, and Rust server logs when emitted. Review actual
  checkpoints and SDK output, not file presence.

## Active offer MySQL lane

`matching-active-offer-gate.yml` invokes the mixed MySQL gate with
`--probe-active-offer`, restart, two-node failure recovery, and idle recovery.

- `gate/run/results.json` / `probe.log`: require the actual
  `probeScenarios.activeOffer` result and successful top-level runtime outcomes
  and topology fields (Go Frontend/History/Worker, Rust Matching, no Go
  Matching).
- `gate/run/rust-matching/activeOffer.json` records `taskList`,
  `pollerIdentity`, `workflowId`, `runId`, `historyScheduledTaskDelivered`,
  `taskTokenDelivered`, `persistedTaskRows`, and `persistedTaskInsertAttempts`.
  The probe starts a real Rust decision poller, then schedules through Go
  Frontend/History; it requires the matching workflow/run and nonempty token,
  durable task row count zero, and MySQL trigger-audited insert attempts zero.
  Trigger/table are dropped in `finally`; successful completion alone does not
  expose a separate drop-result artifact.
- SDK and recovery output: inspect `go-sdk.log` and the restart, drain, crash,
  and idle SDK/checkpoint files listed above, along with the MySQL cleanup
  record. The active-offer JSON does not itself prove these separate scenarios.

## Two-node MySQL recovery and side probes

The standard `matching-mixed-gate.yml` runs the two-node MySQL lane with restart,
failure recovery, and a three-second idle check. The active-offer and standard
adaptive lane also request those common checks. Inspect:

- `gate/run/results.json`, `probe.log`, `gate/run/run.exit`, plus outer
  `build.exit`, `gate.exit`, and `mysql-container-cleanup.txt`.
- `gate/run/rust-matching/production-failure-recovery-checkpoints.json` for
  recorded stages; `production-crash-recovery-sdk.log` for the SDK; and
  `rust-server-before-restart.log`, `rust-server-secondary.log`, and any
  restarted server logs for process context. The compatibility probe asserts
  both nodes converge on owner views, kills an owner with SIGKILL, keeps the
  survivor alive, verifies takeover/recovery and resumed workflow completion,
  then checks the restarted member rejoins discovery. Read the checkpoint
  payload and logs for which node owned each exercised partition.
- Graceful drain: `graceful-drain-sdk.log` plus the `gracefulDrainRecovery`
  object printed in `probe.log` / result output, including workflow/run/task
  list, owner, queued row counts and range before signal, draining PID,
  recovering Rust PID, completion events, and Go service PIDs. The assertion
  requires queued work to recover and complete.
- Restart and idle recovery: `restart-sdk.log`, `production-idle-recovery-sdk.log`,
  `matching-shutdown.json`, and `db-observations.json`. Verify actual pre-stop
  persisted rows/watermarks, post-restart drain/completion, idle unload and
  recovery events. `results.json` has summary labels such as
  `processRestart` and `idleUnloadAndRecovery`; inspect their actual values.
- Late-domain and quota side probe is **not** enabled by the standard adaptive,
  active-offer, or two-node MySQL dispatches. When explicitly run with
  `--matching-cold-domain-metric-probe --matching-exporter prometheus`, inspect
  `matching-cold-domain-metric-probe.json`: `nameLookup` (late name lookup and
  decision/activity names), `partitionLookup` (late name lookup and partition
  keys), `grpcCode` (RESOURCE_EXHAUSTED, numeric 8), and both request outcomes.
  `matching-exporter-semantics.json` records `coldDomainQuotaProbe` and positive
  canonical `cadence_requests_per_tl` samples. This probe also requires late
  domain names to resolve, a one-token domain burst rejection, and the positive
  request metric. Its `coldClaimLimit` explicitly allows a background refresh
  to observe either domain before its first RPC.
- Dual exporter metrics are likewise **not** standard adaptive/offer/two-node
  lane assertions. If the exporter-enabled two-node MySQL harness is run,
  `results.json` records `matchingExporter`, `matchingExporterAddress`,
  `matchingExporterEndpoints`, and `matchingExporterSemantics`; inspect the
  latter's `scrapedEndpoints`, positive metric sample counts, labels, and
  requests. For Prometheus inspect `prometheus-scrape.txt` or
  `prometheus-scrape-samples.jsonl.gz`; for StatsD inspect
  `statsd-packets.json`; for M3 inspect `m3-packets.json` and decoder output.
  A second endpoint is evidence only when the two-node config and both endpoint
  scrape/capture observations are present. No such requirement is implied for
  the three default lanes absent exporter arguments.

The collector `tests/collect-matching-gate-evidence.py` retains scenario rows,
exit records, `results.json`, named SDK logs, recovery JSON, cleanup output, and
hashes in `collection.json`; that report explicitly sets `serviceAcceptance` to
false and requires manual review. It inventories evidence, it does not turn
missing files or requested flags into assertion passes. No assertion removal or
weakening against the requirements of these three named lanes was identified
in the inspected current probes. The late-domain/quota and dual-exporter probes
are opt-in adjuncts rather than required assertions of those lane dispatches.
