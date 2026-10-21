# Matching MySQL production mixed run (2026-10-20)

## Earlier passing run (artifacts lost)

The first production MySQL mixed run finished with `EXIT=0` and
`results.json result=preliminary-passed, acceptanceEligible=false`. The
parent read the passing probe and results directly.

The run covered: SDK workflow/query/isolation/native partition, SIGKILL
restart with one durable row and one completion, graceful drain recovery with
one queued row, and a process restart with the same Go pid.

The artifacts directory of that run was deleted before the evidence was
committed, so this repository holds no verbatim copy of its probe log, its
results document or its command line. The command line in the earlier note
was reconstructed from memory and never matched a recorded invocation. It is
not evidence and must not be cited as a reproducible command.

The surviving state of that attempt:

- frozen Rust binary:
  `artifacts/frozen-production/cadence-server-task-store-abstraction`
  SHA256 `35e25467dfb6961d41d30565260c6e4afd9ed80b54857a403445cac57131817d`
  (source `df3466479`)
- original Go binary, recovered from the failed follow-up attempt and kept on
  disk: `artifacts/frozen-production/cadence-server-go-original`
  SHA256 `0ed004a0f68aee07f132d6e9e8b7faa8e71c992ebdecb11eb30d7c9a257b9bb5`
- the failed follow-up attempt is still in `/tmp/cadence-go-baseline-1r0mq_xe`;
  it failed before the probe started because `NODE_PATH` was not exported
  (`Cannot find module '@grpc/grpc-js'`), so its probe log and results
  describe a harness invocation problem, not a service failure.

GC verification was not covered by that run and stayed unverified.

## Passing production gate (2026-10-20T194644Z)

A fresh production run with the frozen Rust binary
(`35e25467dfb6961d41d30565260c6e4afd9ed80b54857a403445cac57131817d`, source
`df3466479`) and the original Go binary
(`0ed004a0f68aee07f132d6e9e8b7faa8e71c992ebdecb11eb30d7c9a257b9bb5`, passed
with `--binary` only) finished with exit code `0` and probe result
`preliminary-passed`. The evidence is in
`memory/evidence/matching-mysql-production-mixed-20261020T194644Z/` and the
summary is in `EVIDENCE.md` of that directory.

It covers the strict task garbage collection of every drained queue against
the observed durable task ids, the survival of the unacknowledged control
task, the persisted range against the live task-list block, Go's domain
metadata row and notification version, the shared database holding Go's
schema only, the SDK workflows, the SIGKILL restart, the graceful drain
recovery and the SDK drained garbage collection backlog.

This is production mixed run evidence. It is not Matching service
completion.

## Repaired, reproducible evidence

The harness now records its own evidence instead of relying on a manual
copy. `tests/run-go-service-baseline.py` writes `command.txt` (the exact
invocation and the `CADENCE_*` environment without passwords), `run.exit`,
`results.json`, `db-observations.json` and the probe log into the artifacts
directory, and `--evidence-dir` copies the selected evidence into a durable
directory before the artifacts directory is removed. See
`memory/matching-mysql-gate-harness-20261020.md` for the new gate and the
next command.

This is not Matching service completion. The production gate remains
preliminary evidence: the full parity audit, the remaining failure and
recovery cases and the acceptance decision are still open.
