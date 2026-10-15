# Current resume checkpoint (2026-10-15)

Follow root AGENTS.md. The user requires continuing the entire Go-to-Rust rewrite,
service by service: Matching -> History -> Frontend -> Worker. All roles remain
incomplete. Keep working autonomously; use tmux session cadence-rust for long
processes. After compaction save progress, commit and push. Never equate adapter
coverage or a boundary probe with complete service parity.

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

## Next implementation

Existing grpc_matching still delegates into the embedded engine. Independent
storage/queue/History/dispatcher/registry components are NOT wired to the listener.
Continue RPC integration and lifecycle: domain cache, owned queue registration,
configured ack timer, ownership loss/unload and cancellation. Then query delivery,
poller state, sticky lists, forwarding, partition scaling/isolation, throttling,
semaphores, membership and internal Thrift/TChannel transport parity.

Live boundary probe bypasses the Go Matching listener; it does not demonstrate
replacement of Go Matching. Need Go Frontend + Go History + Go Worker routed to
Rust Matching, exercising original SDKs and crash/restart/failover. A test-only
Go Fx PeerProvider decorator can advertise Rust Matching without changing the
original Go business logic; see matching-delivery-next.md and Go cadence/fx.go.

Read service-migration-plan.md, matching-storage-next.md, matching-delivery-next.md,
progress.md and port-map/ledger. No Go production sources changed. 29 source
files are partial, none complete; the inventory remains deliberately conservative.

## Known validation/resource details

Use disk-backed artifact dirs under /home/ubuntu/.cache/cadence-rust-test-artifacts;
/tmp is tmpfs and retained server binaries consume RAM. Serialize heavy links.
The dynamic-policy differential oracle outside the combined runner has a known
fixture enum mismatch and must not be represented as passing. Preserve the Go
one-minute failover cooldown; combined-runner rapid-failover checks use an explicit
test-only admin override. No PR exists for this work.
