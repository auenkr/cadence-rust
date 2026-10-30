# Matching startup stall: harness stream plumbing

Date: 2026-10-30  
Scope: read-only source and retained-artifact diagnosis of PostgreSQL gate run
41034380907. No local service, build, or runtime was started.

## Conclusion

The main harness path used by run 41034380907 does not leave Rust stdout or
stderr connected to an undrained pipe. Python launches the Rust Matching
process through Node's `startRust` with stdout and stderr both directed to the
same open regular log file (`tests/matching-service-compat.cjs`). Python runs
Node with stdout directed to `probe.log` and stderr redirected to stdout
(`tests/run-go-service-baseline.py`). The Go baseline is likewise directed to
`server.log`. Therefore ordinary pipe-buffer saturation in these Rust/Node/Go
streams does not explain the missing Matching admission completion or the
shutdown timeout in this run.

The reviewer concern about synchronous `eprintln!` while holding a lifecycle
mutex remains possible only if the file write itself blocks (for example,
filesystem I/O stall), or if a distinct stderr pipe is used by another
launcher. The observed plumbing and retained output do not show that condition.
No speculative source change is warranted from this evidence.

There is a separate stdout pipe in `run-go-service-baseline.py`'s optional
shard-distributor fixture startup: it reads one readiness line from
`stdout=PIPE`, then does not continue draining stdout. The fixture also writes
JSON responses to stdout. If that optional fixture emits enough responses,
its own pipe could fill and stall the fixture. However run 41034380907's
command has no `--shard-distributor` flag, and this pipe is not the Rust
process's output stream. It cannot account for this observed run.

## Retained evidence

Artifact `matching-production-postgres` from run 41034380907 is 68,234 bytes.
Its files include:

| File | Bytes |
|---|---:|
| `gate/run/server.log` (Go Frontend/History/Worker) | 3,005,273 |
| `gate/run/rust-matching/rust-server-before-restart.log` | 2,462 |
| `gate/run/probe.log` (Node probe stdout and stderr) | 1,240 |

The Rust log ends after `Matching startup: opening admission`; it records eight
Ringpop PING handlers and no later admission-success marker. The probe records
Go Frontend cancellation and a direct gRPC
`Deadline exceeded` against the Rust listener. Its `matching-shutdown.json`
records SIGTERM followed by a 20-second timeout and SIGKILL (`exitCode: null`,
`childSignal: SIGKILL`). These show a startup/admission boundary stall, but do
not identify a blocked stderr write. The 3 MB Go service log is independently
file-backed and does not share the Rust log descriptor.

## Discriminating evidence if repeated

For a recurrence, retain the exact Rust and probe logs plus the SIGTERM marker,
shutdown result and byte sizes. At the stall, record the Rust process's fd 1
and 2 targets. If either points to a pipe, capture its reader/drain state; if
both are regular files, pipe saturation is ruled out for that process. To
attribute a lifecycle-mutex stall to logging, capture a blocked `write(2, ...)`
and the thread stack showing the lifecycle mutex held by the writer (or the
lock owner). A log ending at the start marker alone is insufficient: the
existing log already places the last visible boundary, but does not establish
that stderr backpressure caused it.

Next mixed-run boundary: distinguish the admission-opening call from
successful admission publication and the subsequent server-ready transition.
Retain those stage diagnostics with the Rust log; no source instrumentation is
proposed here because the currently assigned startup diagnostic stage is
already intended to locate that boundary.
