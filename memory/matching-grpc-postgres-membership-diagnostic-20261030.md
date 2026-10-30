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

## Diagnostic implementation CI

The first fresh run, [41097675024](https://github.com/auenkr/cadence-rust/actions/runs/41097675024), used clean target `true` and package unset at commit
`e684d428fdbdae625bfa898948885a8410961d65`. Runner formatting passed, then
Clippy rejected the newly added counter's deprecated `fetch_update` call and
the environment-variable early-return lint. Tests did not run. Both exact
findings were corrected, and the runner-produced rustfmt patch for
`grpc_matching.rs` was applied and committed.

The corrected source at `4a5da9fb0b9709e40d1117244c1faf82bc60a27f` passed a
fresh uncached package-unset workspace run:
[41098477832](https://github.com/auenkr/cadence-rust/actions/runs/41098477832)
completed successfully, with Clippy followed by workspace tests. No cache step
ran; Cargo used the dedicated `$RUNNER_TEMP/cargo-target-final` target.

The immutable tag
`validation/matching-grpc-transport-evidence-4a5da9fb` points to that exact
SHA. PostgreSQL-only run
[41100032910](https://github.com/auenkr/cadence-rust/actions/runs/41100032910)
was dispatched at this tag after confirming no other Matching production
gate was active. Its full artifact was downloaded to the unique directory
`/tmp/cadence-artifacts-41100032910` after the run completed.

## PostgreSQL transport diagnostic follow-up

Run 41100032910 used source head
`4a5da9fb0b9709e40d1117244c1faf82bc60a27f`, with fresh build exit `0` and
binary SHA256
`3cbfebef37ec88b34d3655be8f0b97af7b14b34db9fb00dee8dc2747ea8d25eb`.
Probe, nested run, lane, and gate exits are all `1`. The uploaded artifact now
contains `gate/run/probe.log`, `results.json`, `ports.json`, the readiness
JSON, full Go `server.log`, and full Rust `rust-server-before-restart.log`.
The Go Frontend/History/Worker peers ran with PostgreSQL and Rust Matching;
there was no Go Matching. The SDK, restart, and durable-recovery probes still
did not run (`acceptanceEligible:false`).

The final direct `ListTaskListPartitions` call targeted `127.0.0.1:29438`.
It started at `2026-10-30T17:42:36.618Z`, completed at
`2026-10-30T17:42:48.625Z`, and returned no response: gRPC code 4,
`Deadline exceeded after 10.006s,name resolution: 0.001s,Waiting for LB pick`.
Transport fields report channel state `1` (`CONNECTING`), TCP connect
`connected:true` in 0 ms, and an independent HTTP/2 probe with
`socketConnected:true`, `remoteSettingsReceived:false`, `ETIMEDOUT` after
2 seconds. No proxy environment variables were present, no `NO_PROXY` setting
was configured, and the client credentials were insecure. This proves that
the TCP connect succeeded but no HTTP/2 SETTINGS response arrived in that
probe window. It does not prove whether the listener accepted or dispatched
the connection, whether a TLS path was selected, or whether this direct RPC
entered the handler.

Go's Ringpop logs show peer membership addition at 17:39:30.996Z
(History), 17:39:31.091Z (Frontend), and 17:39:31.114Z (Worker), followed by
removal at 17:39:44.325Z, 17:39:44.526Z, and 17:39:44.570Z respectively.
Those removals preceded the final direct probe by about 2 minutes 52 seconds
and preceded the later Go shutdown records at 17:43:08Z; they are not the
gate teardown. Rust logs show listener binding, Ringpop startup, initial
membership refresh, and admission opening. Its bounded Ringpop log contains
eight inbound PING messages, but does not record SWIM response writes or
outbound probe outcomes. The Go removal is real, but the evidence still does
not identify its SWIM/TChannel cause.

Source inspection found why the previous request trace did not correlate:
the timestamped `matching_grpc_transport` instrumentation was added to the
legacy `Matching` protobuf implementation in `grpc_matching.rs`, while
`serve_with_listener_tls_or_no_identity` registers
`NativeMatchingService` through `NativeMatchingApi` when the native runtime is
installed. The captured `Matching ListTaskListPartitions stage:` lines come
from `matching_service_native.rs` and are bounded but have neither timestamps
nor request identity. The missing transport records therefore cannot be read
as proof that no handler ran. I added opt-in, 64-request-bounded
`matching_native_grpc_transport` enter/complete markers in the actual native
handler. This change is diagnostic only and leaves the existing legacy trace,
readiness assertions, SDK assertions, and Matching behavior intact.

The next step is a fresh uncached package-unset Rust CI run with Clippy then
workspace tests. Only if it passes should the serial PostgreSQL-only gate be
dispatched at the exact immutable diagnosis commit. Its next artifact should
be evaluated by correlating each native handler timestamp to the direct probe
window. A handler entry and completion would move investigation to gRPC
response delivery; no entry with a TCP connect but no HTTP/2 SETTINGS would
focus the next bounded trace at Tonic accept/HTTP2 dispatch. Neither current
run is SDK or recovery evidence.
