# Matching PostgreSQL membership and gRPC failure diagnosis — 2026-10-30

## Evidence reviewed

The failed run is [41094318642](https://github.com/auenkr/cadence-rust/actions/runs/41094318642), built at exact source head
`6c73d9aa447171d4eeda37d71dd0a955d0996a9b` (parent
`565c8fc91893fae1a81cd8e47f10f2ee4744c046`). The matching artifact was
downloaded once into the empty directory
`/tmp/cadence-41094318642-review-20261030`.

The run topology was Go Frontend/History/Worker and Rust Matching, with no Go
Matching. PostgreSQL container startup and fresh Rust Matching build succeeded.
The binary SHA256 was
`618ae960e2f0f8c68d3c6643351936343fad480c4e1d0d8e27afb660746a8605`.
Recorded exits are build `0`, nested run `1`, PostgreSQL lane `1`, and gate
`1`. `acceptanceEligible` is false. The SDK, restart, and recovery assertions
did not run.

Relevant files in the downloaded artifact:

- `matching-production-postgres/gate/run/rust-matching/list-task-list-partitions-readiness-before-restart.json`
- `matching-production-postgres/gate/run/rust-matching/rust-server-before-restart.log`
- `matching-production-postgres/gate/run/rust-matching/matching-shutdown.json`
- `matching-production-postgres/gate/run/server.log`
- `matching-production-postgres/gate/run/probe.log`
- `matching-production-postgres/gate/run/results.json`
- `matching-production-postgres/gate/postgres-container-cleanup.txt`

## Timestamp-aligned conclusions

Go History, Worker, and Frontend each logged `HostsAdded: 127.0.0.1:22438`
between 17:01:14.452Z and 17:01:14.534Z. They independently logged
`HostsRemoved: 127.0.0.1:22438` between 17:01:37.550Z and 17:01:37.811Z.
This is not gate teardown: the final direct Rust gRPC probe did not begin until
17:04:30.103Z, failed at 17:04:40.109Z, and the probe wrote SIGTERM at
17:04:40.110Z. The Rust child was SIGKILLed at 17:05:00.115Z after the
20-second shutdown bound. The cleanup artifact contains only container name
`cadence-rust-gate-postgres-lane-20261030T170051-8786`; it has no cleanup
completion time or success result. That missing cleanup record does not affect
the earlier 17:01:37Z removal, which predates both probe failure and teardown
by several minutes.

The direct gRPC diagnostic targeted `127.0.0.1:27161`, recorded insecure TLS
credentials, no proxy variables present, and no `NO_PROXY` setting. A separate
TCP connect succeeded in 1 ms. grpc-js reported connectivity state `1`
(`CONNECTING`) and deadline code `4`, `Waiting for LB pick`; no response was
returned. Thus TCP reachability is established, while gRPC channel readiness
and completion of this exact direct request are not. A successful TCP connect
does not itself prove that an application accepted a connection or completed
the HTTP/2 handshake.

The Rust trace contains two `ListTaskListPartitions` calls reaching
`engine-complete`, followed by a third that reached `engine-begin` and then
stopped logging. Its lines have no timestamps or request identity. They cannot
be aligned to the direct probe at 17:04:30Z, so they do not prove that the
grpc-js call entered the Matching handler or that it stalled there. The eight
bounded inbound SWIM PING trace lines show Ringpop application handling, but
do not show TChannel response frame writes or Rust's outbound probe outcomes.
No Go or Rust source evidence proves the exact membership-removal trigger; no
production behavior change is justified by this run.

## Bounded diagnostic added

This branch adds opt-in `CADENCE_MATCHING_TRANSPORT_DIAGNOSTICS=1` tracing for
at most 64 `ListTaskListPartitions` RPCs. Each handler entry and completion
records a Unix-millisecond timestamp, peer address, domain, task-list name,
elapsed time, and outcome. The production validation workflow enables this
only for its mixed run. The failed-probe harness now also makes one bounded,
two-second HTTP/2 SETTINGS handshake probe alongside the existing TCP and
grpc-js observations. Its output distinguishes TCP connect from an HTTP/2
server response; correlated server entry/completion events distinguish those
transport stages from handler execution. No readiness assertion, workflow
assertion, or Matching behavior changes.

## Next evidence-producing step

After a fresh uncached package-unset Rust CI run passes (Clippy followed by
workspace tests), dispatch only `lane=postgres` at the immutable commit SHA
for this diagnostic branch. Inspect its artifact for the new
`transportDiagnostic.http2Probe`, timestamped handler events, Go add/remove
timeline, exact SHA256 and all nested exits. If HTTP/2 settings arrive but no
handler entry correlates to the direct RPC window, trace Tonic request
dispatch next. If handler entry has no completion, investigate the exact
request's Matching work; if it completes, inspect gRPC response delivery. Do
not rerun or credit SDK/recovery scenarios until readiness succeeds. Keep
PostgreSQL cleanup separate from the earlier membership timeline and require
an explicit cleanup result in the next run evidence.

No SDK, restart, or recovery conclusion is claimed from run 41094318642.
