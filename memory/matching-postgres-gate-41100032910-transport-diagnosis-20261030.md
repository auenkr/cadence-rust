# PostgreSQL Matching gate 41100032910: transport failure evidence

Inspected only `/mnt/storage/service/ci-evidence/matching-postgres-41100032910`
and source revision `4a5da9fb0b9709e40d1117244c1faf82bc60a27f`. Binary SHA256:
`3cbfebef37ec88b34d3655be8f0b97af7b14b34db9fb00dee8dc2747ea8d25eb`.

The gate failed during `waitForRustReady`, before SDK scenarios. The captured
direct `MatchingAPI.ListTaskListPartitions` probe targeted `127.0.0.1:29438`
for domain `service-compat-12f96e3e-8f4a-41c9-9431-38cd6c90084a` and task list
`primary-ready-12f96e3e-8f4a-41c9-9431-38cd6c90084a`. It started
`2026-10-30T17:42:36.618Z` and completed `17:42:48.625Z` with gRPC code 4,
`Deadline exceeded after 10.006s ... Waiting for LB pick`; no RPC response.
The independent TCP connect succeeded immediately, but the HTTP/2 probe got no
remote SETTINGS within 2s (`ETIMEDOUT`, socket connected). Go Frontend readiness
ended in `Call cancelled`.

Rust's captured server log has an un-timestamped `ListTaskListPartitions` trace
from `handler-enter` through `engine-complete`, including successful domain
resolution and both partition owner lookups. It has no
`matching_grpc_transport` timestamped event and no handler-complete record,
although this source revision enables those timestamped enter/complete records
when `CADENCE_MATCHING_TRANSPORT_DIAGNOSTICS` is set (the recorded command
environment sets it to `1`). Thus the log does not correlate the engine trace
to the final client attempt. The evidence rules out a successful gRPC response
and shows a TCP-accepting endpoint without an HTTP/2 handshake for the recorded
probe. It does not establish whether the missing HTTP/2/handler completion was
a gRPC-serving task stall or a wider process stall. The later SIGTERM also did
not complete: shutdown JSON records SIGKILL after the 20-second deadline.

SDK did not run: source `tests/matching-service-compat.cjs` calls
`waitForRustReady` before entering the workflow scenarios, and the probe
failed there. `results.json` marks `acceptanceEligible: false`.

Nested exits: build `0`, probe `1`, run `1`, lane `1`, gate `1`.
Reproduction evidence is in the artifact directory above, especially
`gate/run/rust-matching/list-task-list-partitions-readiness-before-restart.json`,
`gate/run/rust-matching/rust-server-before-restart.log`, `gate/run/probe.log`,
`gate/run/results.json`, and the `*.exit` files. No CI was dispatched or polled.
