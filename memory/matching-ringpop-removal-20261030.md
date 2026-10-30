# Matching Ringpop removal investigation — 2026-10-30

## Scope and source revision

Investigated the production Ringpop lane in retained run
https://github.com/auenkr/cadence-rust/actions/runs/40808511931 and the source
at reviewed commit `377fb2f8eea691f6740b895aaa4583ac6963b2eb`. The working branch
contains later unrelated Matching source integration changes; this investigation
did not restore or rely on the cancelled `3386857f` WIP. The existing `64fc`
direct-primary diagnostic is retained and was not duplicated.

## What the retained run proves

- `gate/run/rust-matching.yaml` seeds Rust from Go Frontend `29869`, History
  `16389`, Worker `29474`, plus reserved Matching `12973` and a second unused
  port. Go Frontend config has the same service seeds. Rust listens on
  `127.0.0.1:12973` for Ringpop/TChannel and `127.0.0.1:14329` for Matching
  gRPC.
- At `02:51:19.562`, Go Worker observed a Ringpop change adding `12973`; its
  Matching resolver refreshed to `[12973; tchannel=12973, grpc=14329]`. History
  and Frontend independently observed the same entry at `02:51:19.572`.
- At `02:51:38.612`, History received `HostsRemoved=[12973]` and refreshed its
  Matching hash ring to `[]`. Worker did the same at `02:51:38.654`, Frontend at
  `02:51:38.815`. The Frontend readiness call then timed out/cancelled; later
  `DescribeTaskList` calls report `Not enough hosts to serve the request`.
- Rust's retained log only confirms both listeners. The probe did not reach SDK
  workflow, restart, or recovery assertions. The 19-second add-to-remove window
  is consistent with a SWIM reachability transition but does not itself prove
  the failed probe or its transport error.

## Source path and elimination of alternatives

- `go/common/peerprovider/ringpopprovider/provider.go:GetMembers` asks Ringpop
  for reachable members, filters by exact `serviceName`, and includes only
  `swim.Alive` or `swim.Suspect`. It maps the advertised `tchannel` and `grpc`
  labels into host info. `HandleEvent` forwards Ringpop `ServersRemoved` as
  membership `HostsRemoved`.
- `go/common/membership/hashring.go:Refresh` rebuilds the consistent hash ring
  from `PeerProvider.GetMembers`; `diffMembers` emits `HostsRemoved`; the empty
  hash ring causes `LookupRaw` to return `ErrInsufficientHosts` and signal a
  refresh. So the failed Frontend readiness call is a Matching routing failure
  caused by the Go Membership provider returning no reachable Matching member.
- `go/common/rpc/direct_peer_chooser.go` manages cached YARPC transport peers;
  `UpdatePeers` and `removePeer` only release cached RPC connections after
  membership changes. It does not edit Ringpop membership or the hashring.
  Thus this is not independent transport-peer cache pruning.
- There is no address/namespace mismatch in the retained entry: the seeded host
  address `127.0.0.1:12973` and labels `tchannel=12973`, `grpc=14329` match the
  Rust listener addresses and Go's selected service name is Matching. The
  Frontend routing call is to `DescribeTaskList`, not a domain lookup. Go
  registered the workflow domain successfully before the entry was removed;
  domain cache/startup is therefore not the cause of this error.
- Rust `MatchingRingpopProvider::start` performs configured JOIN, starts SWIM,
  marks the node ready, and publishes `serviceName` and port labels. The matching
  shared TChannel listener is bound before bootstrap and serves both Ringpop
  JSON methods and Matching Thrift. `handle_ping_with_full_sync` validates app
  and readiness, applies gossip, and returns Ringpop's PING response. The code
  path supports the expected protocol, but the retained run has no inbound PING
  request trace, so correct on-wire response behavior in that run remains
  unproven.

## Diagnosis and remaining uncertainty

The concrete blocker is Go Ringpop reachability eviction of the only Matching
member, followed by the expected empty Go Matching hash ring and Frontend routing
failure. This excludes domain-cache startup and direct-peer-chooser cleanup.
The underlying cause of the eviction is not established from run 40808511931:
the Go removal events don't include the SWIM probe error, while the Rust log has
no PING request/response evidence. Do not claim that the seed disappeared or
that Go queried a different ring. A subsequent gate must distinguish failure to
reach the Rust TChannel endpoint from Rust rejecting/mishandling Go PINGs and
from a response-side issue.

## Focused instrumentation added

`ringpop_provider.rs` now logs only the first eight successfully handled inbound
Ringpop PINGs, including source address, accepted membership changes, and the
local membership status snapshot. This is bounded and does not alter JOIN,
discovery, SWIM, readiness, SDK, restart, recovery, or no-Go-Matching behavior.
Together with the already-integrated direct-primary probe, the next production
gate can tell whether Go PINGs reached the Rust process and what membership
Rust held when replying. No Cargo, Go, formatter, runtime, or mixed-cluster
validation was run locally. Parent owns CI dispatch and the active gate.

## Next gate interpretation

- No Rust `received PING` lines before Go removes the member: investigate Go's
  advertised address / TChannel reachability and Rust listener behavior.
- Rust PING lines with Go membership transitions: inspect the incoming request,
  accepted change summary, and TChannel response path against Ringpop's wire
  behavior; do not weaken the readiness gate.
- Rust PING lines with no Go removal but failed frontend readiness: inspect
  Go's contemporaneous `HostsRemoved`/resolver state and the direct-primary
  result. Keep the SDK, restart/recovery, production discovery, and no-Go-Matching
  assertions enabled.
