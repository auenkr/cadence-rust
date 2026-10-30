# Matching gate harness review: run 41094318642

Review scope: Python/Node process orchestration, readiness ordering, deadlines,
signals, output capture, Frontend/direct readiness probe sequencing, and port
reservation. The reviewed run artifact is from commit
`6c73d9aa447171d4eeda37d71dd0a955d0996a9b`; it was downloaded into a unique
empty directory at `/tmp/matching-harness-41094318642.AHIDmU` (path recorded
case-sensitively in `/tmp/matching-harness-41094318642.path`). The concurrent
`tests/matching-service-compat.cjs` edit was left untouched.

## Finding

No harness defect that explains this run was established, so no harness code
was changed. The run failed in the Rust Matching readiness check: Go Frontend
returned `Call cancelled`, while the direct request to the primary Matching
gRPC endpoint timed out after 10.005 seconds with `Waiting for LB pick`.
Matching's startup log records the gRPC and TChannel listeners, successful
Ringpop joins to the Go peers, membership publication, and admission opening.
That evidence does not establish why the gRPC call could not be picked or that
the harness stopped Matching prematurely.

## Ordering and cleanup evidence

- `tests/run-go-service-baseline.py` starts the Go server, waits for Frontend's
  TCP listener and the Go Worker `worker started` log marker, then runs the
  Node compatibility probe. The artifact records both conditions satisfied.
  History has no separate readiness marker in this harness; Rust's observed
  Ringpop joins to History and the other Go peers show that those endpoints
  were available during this run.
- Go services stay alive during the entire Node probe. The artifact records
  `goServicesAliveAfterProbe: true`; only after the probe returned does the
  Python `finally` block terminate Go services and wait for exit.
- Rust Matching's logs are written to a dedicated file descriptor opened by
  Node; Go's server log is written directly to a file by Python. Probe output
  is redirected to `probe.log`, and Python closes that file before it reads
  it. The failure and Rust shutdown escalation are present in the artifact.
- Following the failed readiness probe, Node sent SIGTERM to Rust Matching.
  `matching-shutdown.json` records SIGTERM at
  `1793379880110000000` ns and SIGKILL at
  `1793379900115000000` ns, about 20 seconds later. This was cleanup after the
  readiness failure, not the cause of the failed Frontend/direct probes. The
  Python runner then shut down Go services; their log records orderly shutdown
  beginning at `2026-10-30T17:05:00.170Z`.
- The harness selects listener ports below the host ephemeral source-port
  range, writes them to `ports.json`, then closes the reservation sockets
  before starting services. That leaves a theoretical bind race with another
  local process, but the recorded run contains no evidence of a collision:
  Go listeners bound their selected ports, and Matching logged its configured
  primary gRPC/TChannel listeners. It is not a supported explanation for this
  failure.

## Run result and limits

Artifact `matching-production-postgres` records head SHA
`6c73d9aa447171d4eeda37d71dd0a955d0996a9b`; `run.exit`, `lane.exit`, and
`gate.exit` are all `1`. Run URL:
`https://github.com/auenkr/cadence-rust/actions/runs/41094318642`. Go's log
places the Frontend, History and Worker startup in `2026-10-30T17:01:13Z` and
the shutdown at `2026-10-30T17:05:00.170Z`; the matching shutdown record gives
the more precise SIGTERM/SIGKILL times above. The probe failed before SDK scenarios began, so this
run is not compatibility or recovery evidence. The logs do not timestamp each
readiness attempt and do not identify the gRPC load-balancer state beyond the
final error. This review therefore cannot distinguish a Matching transport,
registration, or service-side readiness issue from a transient connection
condition, and makes no source-wire conclusion.

No timeouts were extended, no assertions were relaxed, and no runtime/build,
Cargo, Go, test, formatting, or CI command was run for this review.
