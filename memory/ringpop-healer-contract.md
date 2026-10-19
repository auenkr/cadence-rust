# Ringpop v0.10.0 healer and reverse full-sync contract

Reference: `github.com/uber/ringpop-go@v0.10.0/swim` (`node.go`,
`heal_via_discover_provider.go`, `heal_partition.go`, `ping_handler.go`,
`disseminator.go`).

## Discover-provider healer

- Start after successful Bootstrap/Join and SWIM start; stop on node shutdown.
- Default period: 30 seconds. Default base probability: 3.
- Each tick first waits one full period, then attempts with probability
  `base / max(previous_discovery_host_count, reachable_member_count, 1)`.
  `CountReachableMembers` includes the local member and the divisor is retained
  at its maximum observed value until a successful Hosts read updates the saved
  host-list size.
- Refresh `Hosts()` only on a selected attempt. Failed discovery is a failed
  tick; successful reads replace the saved host-list size.
- Candidate addresses are discovery results absent from membership or whose
  status is Faulty-or-worse. Shuffle candidates. Try targets until exhausted
  or 10 failed JOIN/repair calls. On success, remove every address known on
  the remote side from the remaining candidate list.
- `AttemptHeal` is a JOIN request with a one-second timeout, without applying
  returned membership automatically. Compare both memberships. Where one
  partition's winning change would demote a currently pingable member to
  unpingable, send that winning change as Suspect with source fields cleared
  to the opposite partition and apply the local-side changes. Otherwise merge
  remote membership locally and send the complete local membership in a PING.
- Return pingable remote membership addresses to candidate filtering.
- Removing a selected target and every address learned from a remote partition
  uses Ringpop's swap-with-last deletion, including removal of duplicate host
  entries. Stop ends future ticks and waits for an active Heal pass to return,
  matching Go's unbuffered quit handoff; each repair RPC has Go's one-second
  timeout, so shutdown does not abandon a partially applied partition merge.

## Reverse full sync

- Receiver ping handling supplies the normal receiver changes/full-sync to the
  current caller immediately.
- If receiver `issue_receiver` selected full sync, asynchronously JOIN the
  ping source with a one-second timeout and merge that JOIN membership locally.
- Bound concurrently running reverse jobs to five; omit an additional job
  when full. Shutdown owns and drains/cancels these tasks.

## Rust implementation/evidence boundary

The Matching provider owns the discovery loop and its lifecycle. The Ringpop
node owns membership repair, receiver full-sync scheduling, and reverse job
limits. Source harnesses cover reincarnation deltas, discovery/retry edges,
reverse job limits, and two live peer convergence. Builds are deferred while
the shared task-store build lane is occupied.

`rust/crates/cadence-server/tests/fixtures/ringpop_healer_go_expected.json` is a
source-derived expected trace, not captured Go evidence yet. The pinned-Go
runner `tests/run-ringpop-healer-go-oracle.py` must pass in comparison mode (or
be reviewed and updated explicitly) before this fixture can be called a Go
oracle. No Go or Rust test harness has been executed in this work slice.

## Source-only Matching provider parity audit (2026-10-19)

Scope: Go `common/peerprovider/ringpopprovider/{provider.go,factory.go,config/config.go}`
and Rust `matching_ringpop_provider.rs`, `matching_peer_provider.rs`,
`ringpop_discovery.rs`, and `config.rs`. This is a code review only; no build,
test, or live mixed-cluster validation was run.

### Provider lifecycle and member API

- Go `Start` is an atomic Initialized→Started transition, calls Ringpop
  Bootstrap, registers the listener, and writes port then service labels. Any
  error removes the listener and restores Initialized (`go/common/peerprovider/ringpopprovider/provider.go:127-169`).
  Rust serializes lifecycle operations and records Started only after JOIN,
  SWIM start/readiness, task creation, and label publication
  (`rust/crates/cadence-server/src/matching_ringpop_provider.rs:206-345`).
  Bootstrap failure is retryable in both by source intent, but Rust partial
  failures after JOIN/SWIM/task creation have no explicit rollback path before
  `Lifecycle::Started`; fault injection is still required to close this gap.
- Go `Stop` is idempotent only for a started provider, removes listener then
  destroys Ringpop, and does not wait (`provider.go:285-298`). Rust stop sends
  the stop signal, awaits monitor and healer, then awaits node destruction
  (`matching_ringpop_provider.rs:368-408`). This is stronger ownership, with
  the healer pass timeout contract described above; mixed-service shutdown
  behavior remains unvalidated.
- Go delivers ring-change callbacks while holding the subscriber read lock and
  rejects duplicate subscriber names (`provider.go:172-196,300-312`). Rust
  exposes a broadcast receiver and ignores the service argument
  (`matching_ringpop_provider.rs:351-359`). That is an internal Rust API shape,
  not yet demonstrated to preserve Cadence consumer duplicate-subscription,
  delivery ordering, or slow-handler behavior.
- Go `GetMembers` uses Ringpop's reachable-member selector, matching Alive or
  Suspect, then maps identity, valid gRPC/TChannel labels, and legacy TChannel
  address-port fallback (`provider.go:202-260`). Rust filters the published
  membership snapshot by exact `serviceName` and Alive/Suspect; identity falls
  back to address and malformed labels are omitted
  (`matching_ringpop_provider.rs:144-203`,
  `matching_peer_provider.rs:17-64`). Rust snapshot initialization and event
  publication differ structurally from Ringpop `GetReachableMembers`; compare
  status edge cases and snapshot/event races in a future behavioral test.
- Go exposes `WhoAmI`, resolving identity from the identity label with address
  fallback, and includes this provider's port map (`provider.go:262-283`). No
  corresponding `WhoAmI` operation is present on the Rust Matching provider
  trait/provider surface in the audited files. Confirm whether call sites need
  this parity surface before considering provider API complete.
- Go `SelfEvict` directly calls Ringpop (`provider.go:198-200`); Rust delegates
  to node self-eviction (`matching_ringpop_provider.rs:361-366`). Source-level
  path exists; externally observed event/leave behavior is unverified.

### Configuration and discovery modes

- Mode names, case folding, required Name, default 10-second join duration,
  missing file/hosts, and rejected custom mode align in source
  (`go/common/peerprovider/ringpopprovider/config/config.go:52-123`,
  `rust/crates/cadence-server/src/config.rs:40-115`). Go's `IsEmpty`
  convenience predicate has no counterpart, but its use by Matching config
  loading was not assessed here.
- Hosts discovery preserves configured vector/order/duplicates in both
  (`go/common/peerprovider/ringpopprovider/factory.go:173-177`,
  `ringpop_discovery.rs:40-57`).
- JSON file discovery is re-read per `Hosts()` call and Rust includes explicit
  Go `encoding/json` compatibility handling for null, malformed UTF-8, syntax
  precedence, and target-type errors (`ringpop_discovery.rs:59-113`); file
  permission/read errors and diagnostic parity have not been compared.
- DNS providers deduplicate full host:port seeds, cache successful LookupHost
  answers per hostname during one call, retry missing answers for another port,
  and skip not-found results in both (`factory.go:30-86`, Rust
  `ringpop_discovery.rs:405-452`). Both deduplicate via unordered collections,
  so result ordering is not a stable parity guarantee. Rust's Linux resolver
  intentionally matches Go's cgo NSS route, but Go can select pure Go under
  netgo/GODEBUG; this environment-dependent divergence is documented in Rust
  (`ringpop_discovery.rs:272-278`).
- DNS-SRV splits the seed into first label/service and remaining domain, looks
  up TCP SRV, resolves each target, skips not-found addresses, and caches by
  service only (including the Go cache-key quirk for different domains)
  (`factory.go:117-166`, Rust `ringpop_discovery.rs:454-512`). Rust implements
  priority and weighted record selection; order is randomized within priority
  groups, so exact result order is not deterministic in Go either.
- Remaining source audit items: compare Go `net.SplitHostPort` with Rust
  `split_host_port` for empty/invalid ports and malformed bracketed IPv6;
  compare Go DNS/SRV error wrapping and resolver behavior against Rust's
  resolver/error mapping; validate actual resolver behavior in deployment.
  These are unverified, not claimed defects.

### Reproducible pinned-Go healer runner

`tests/run-ringpop-healer-go-oracle.py` now prints SHA-256 for the pinned Go
sources, harness, and fixture before execution. The pin is the module cache path
`github.com/uber/ringpop-go@v0.10.0`; source hashes recorded at this audit:

| Input | SHA-256 |
| --- | --- |
| `swim/heal_partition.go` | `e1b3994d288212863bd09062a1ce067a1e92b3efbb54afdd1ae9576996445bab` |
| `swim/heal_via_discover_provider.go` | `d5f9784482ad5f41ccbf99d8acc0c70fec43afed99a35b201a7a4ac02505a19b` |
| `swim/node.go` | `9537bc1b19c797eb44c4036e825ff68f4b9bf506d2df43c6726dde7edf74b913` |
| `swim/ping_handler.go` | `14c1fd00425c9341e5f926842756665a22b2430704e4b9cf1425552dce6d1691` |
| `swim/disseminator.go` | `6c872d3eb0f6c9f2eedd8a13c349644802d42063b2e866e4d88be1fc350cbc0d` |
| `tests/ringpop-healer-go_test.go` | `9122281be1b59110ece1ddf00c54d9074f71588edd7e71ae47d11ca13da96364` |
| `ringpop_healer_go_expected.json` | `4b058d8def737d614315fcb5672298806497ffbc2390b88aca49af319962bc27` |

The JSON is the checked-in **expected** trace, derived from source and not yet
captured Go output. Default runner mode compares actual output to that expected
trace. `--update` replaces the expected fixture using the just-captured pinned
Go run; it is an explicit review/update action and has not been executed here.
