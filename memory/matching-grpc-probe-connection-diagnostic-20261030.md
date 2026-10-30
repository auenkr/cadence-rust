# Matching direct gRPC readiness connection diagnosis (2026-10-30)

## Source and failed mixed-gate evidence

The queued-activity candidate is `565c8fc91893fae1a81cd8e47f10f2ee4744c046`
on `t3/matching-queued-activity-candidate-5ee69b5`. The failed PostgreSQL
mixed gate inspected here is **base evidence**, not candidate evidence:
[`run 41056791529`](https://github.com/auenkr/cadence-rust/actions/runs/41056791529)
was built from `5ee69b5572688c2dba2245c7b411c7182c5b9b16`. Its Rust binary SHA256
is `d9a76461f356a783d89dd4309e585d773f78f07410c17cb2c385899302f32b50`.

Artifact `matching-production-postgres` (id `12977801450`) was downloaded into
the previously empty directory
`/tmp/matching-probe-12977801450-inspect-20261030`. It contains
`gate/run/probe.log`, `ports.json`, `rust-matching-command.json`,
`list-task-list-partitions-readiness-before-restart.json`, and the Rust and Go
service logs. Nested exits are build `0`, probe/run `1`, lane `1`, and gate `1`.
The topology records Rust Matching only and Go Frontend/History/Worker, with no
Go Matching. Readiness failed before SDK scenarios; `acceptanceEligible` is
false. Go services remained alive. Rust SIGTERM shutdown exceeded its 20-second
bound and was killed with SIGKILL.

## Connection-path findings

- The exact direct probe target is `127.0.0.1:13898`. The Rust launch command
  passes `--matching-grpc-port 13898`, and Rust logs the bound Matching gRPC
  listener at `127.0.0.1:13898`. No configured-versus-bound port mismatch is
  present.
- The probe constructs `new MatchingAPI(address, matchingCredentials)` with a
  bare `127.0.0.1:<port>` target. For this PostgreSQL lane no CA is configured,
  so `matchingCredentials` is insecure. The call adds no channel options or
  explicit resolver. The process environment is inherited from the workflow;
  the captured artifact does not record proxy variables.
- Rust prebinds the listener before Ringpop advertises it, then starts the gRPC
  `serve_with_listener_tls_or_no_identity` future in the Tokio `JoinSet` used
  by the service supervisor. That task is spawned alongside the other service
  listeners; source review found no path that awaits it before polling the
  supervisor.
- The probe artifact records Go Frontend `DescribeTaskList` timing out after
  19.999 seconds at `127.0.0.1:22427`. Its final direct probe to
  `127.0.0.1:13898` ended with gRPC code 4 after 10 seconds and detail
  `Waiting for LB pick`. That is a grpc-js channel/subchannel readiness timeout,
  not a response from the Matching handler.
- The Rust log contains a `ListTaskListPartitions` trace that reaches
  `engine-complete`. It is not timestamped or correlated to the final direct
  probe attempt, so it proves a handler path completed at some point but does
  not identify why grpc-js's recorded final call had no LB pick. The Go
  Frontend timeout is a separate call path. No admission, persistence, or
  handler stall is established by these records.

## Bounded diagnostic change

The candidate already saves the direct RPC's address, request, timestamps,
status and response, and traces bounded handler stages. To resolve the remaining
transport ambiguity without changing readiness criteria or acceptance
assertions, `tests/matching-service-compat.cjs` now records, with each direct
probe, the grpc-js channel connectivity state and an independent TCP connect
result capped at one second. It records the exact target/resolver mode, which
proxy environment variable names are present (never values), whether a
`NO_PROXY` setting exists, and whether credentials use a custom CA or insecure
mode. This should distinguish a listener that is not accepting TCP from a
grpc-js resolver/channel/proxy mismatch at the next mixed run.

This is a harness-only diagnostic. It does not weaken or skip readiness and
does not change Matching runtime behavior. The candidate's queued-activity
owner-crash recovery assertions are preserved.

## CI state and next action

The exact-candidate workspace run
[`41058036608`](https://github.com/auenkr/cadence-rust/actions/runs/41058036608)
was still `in_progress` at the one post-review inspection on 2026-10-30; Clippy
and formatting had succeeded and workspace tests were running. Its head is
`565c8fc91893fae1a81cd8e47f10f2ee4744c046`. No result or exit code is inferred
from that partial state. Once the exact candidate workspace run is successful,
dispatch the existing PostgreSQL-only Matching production lane at immutable
candidate ref `565c8fc91893fae1a81cd8e47f10f2ee4744c046`. Record its head,
binary SHA256, nested exits, and the new `transportDiagnostic` object from the
artifact before drawing a connection conclusion.
