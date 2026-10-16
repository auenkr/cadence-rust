# Current resume checkpoint (2026-10-16)

Follow root AGENTS.md. The user requires continuing the entire Go-to-Rust rewrite,
service by service: Matching -> History -> Frontend -> Worker. All roles remain
incomplete. Keep working autonomously; use tmux session cadence-rust for long
processes. After compaction save progress, commit and push. Never equate adapter
coverage or a boundary probe with complete service parity.

## Latest verified Matching RPC checkpoint

Read matching-rpc-next.md first. Opt-in Matching::with_durable_queues connects
AddTask/poll protobuf handlers to the durable registry and Go History RPC client,
without local workflow execution lookups. The production default is unchanged.
Actual mixed Go Frontend/History/Worker + Rust Matching probe PASSED with no
Go Matching process, using test-only Fx discovery and seeded Rust domain metadata.
Existing Go SDK over Go Frontend TChannel also passed. Full accumulated runner:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-final-20261016/results.json
and /tmp/cadence-matching-mixed-full.exit = 0: all 22 stages, 324 workspace tests,
strict lint. Expanded SDK (echo, activity retry, child, workflow-run/task retry,
activity timeout and signal) separately passed:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-sdk-expanded2/results.json.

This checkpoint is being committed/pushed with these notes. Matching remains
incomplete. Next: native poller history/DescribeTaskList, periodic ack maintenance,
owned queue lifecycle; then query delivery, sync matching/forwarding, rate limits,
partition/isolation/semaphores, production domain cache/config/membership and
internal Matching Thrift/TChannel. Do not move to History prematurely.

Build the Go routing harness with --rust-matching, or reuse:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-20261016/cadence-server.
Combined runner uses --go-mixed-server-bin for that test binary; --go-server-bin
is only for ordinary original Go CLI (/tmp/cadence-go-mixed/cadence-server).
Serialize heavy linking; Rust integration tests are now precompiled by the
baseline runner before services start. First two failed attempts were build
contention and an invalid discovery seed; see matching-rpc-next.md.

## Verified checkpoints

- 90e14cfac: Go-compatible raw Thrift/Snappy Matching blobs and SQLite schemas,
  leases, fenced atomic writes, recovery and Go/Rust shared-database interop.
  Full 19-stage runner passed with 303 workspace tests.
- eedeb54d9: durable queue ID allocation/read/ack/expiry/rewrite behavior,
  original-Go reader fixtures, History client and cancellation-safe dispatcher.
  Full 21-stage runner passed, including an 11-event workflow against actual Go
  History and completion through Go Frontend. Evidence:
  /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-dispatch-verified/results.json.
- bddc9003c: independent queue registry, concurrent single lease acquisition,
  ack flush/shutdown and identity-checked unload. Eleven focused Matching tests
  and strict workspace all-target clippy passed:
  /tmp/cadence-registry-unload.exit = 0.

## Latest source-domain continuation

Go CreateTasks uses each task's source DomainID for its SQL row, while fencing
TaskListInfo's target domain. Rust create_tasks_with_domains and queue
append_in_domain now preserve that distinction. Extended actual-Go SQLite
interop verifies both directions and rejected-lease rollback:
/tmp/cadence-source-domain-interop.exit = 0.
Seventeen focused persistence tests and strict workspace lint passed:
/tmp/cadence-source-domain-final.exit = 0.

Full accumulated runner PASSED all 21 stages and 321 workspace tests:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-registry-source-domain/results.json;
/tmp/cadence-registry-source-domain-full.exit = 0. Includes strict lint, original-Go
differentials, standalone SDK checks and actual-Go History delivery. Used unchanged
original Go binary /tmp/cadence-go-mixed/cadence-server. Source-domain checkpoint
is being committed/pushed with these notes.

## Known validation/resource details

Use disk-backed artifact dirs under /home/ubuntu/.cache/cadence-rust-test-artifacts;
/tmp is tmpfs and retained server binaries consume RAM. Serialize heavy links.
The dynamic-policy differential oracle outside the combined runner has a known
fixture enum mismatch and must not be represented as passing. Preserve the Go
one-minute failover cooldown; combined-runner rapid-failover checks use an explicit
test-only admin override. No PR exists for this work.
