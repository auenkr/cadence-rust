# Ringpop production provider: bounded implementation plan

Research checkpoint: 2026-10-18. This document plans the missing production
provider; it does not claim Rust Ringpop/SWIM is implemented. The pinned source
is `github.com/uber/ringpop-go v0.10.0`, resolved locally at
`/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0` and required by
`go/go.mod`.

## Current boundary

The Rust repository already has (see `ringpop-wire-contract.md`,
`matching-peer-lifecycle.md`, and `ringpop-tchannel-next.md`): wire structs,
transport-independent Matching peer lifecycle, and an injectable `ringpop`
TChannel JSON boundary. The latter is not a provider or SWIM responder.
`main.rs` currently starts frontend and Matching TChannel services separately
(`rust/crates/cadence-server/src/main.rs:437,488,601`); `tchannel.rs` exposes
`serve_ringpop` but main does not call it. Matching membership is currently
constructed from static/test-oriented hosts (`matching_membership.rs` and
`matching_peer_provider.rs`), not a production Ringpop-backed resolver.

## Go behavior that the implementation must preserve

### Construction, bootstrap and join

- Cadence selects Ringpop through Fx: `go/cmd/server/cadence/fx.go:27,44`;
  `ringpopfx.New` reads `ringpop` config, uses service full name, and passes
  gRPC/TChannel ports (`go/common/peerprovider/ringpopprovider/ringpopfx/ringpopfx.go:16-58`).
- `ringpopprovider.New` validates config, builds the discovery provider,
  passes the existing TChannel channel to `ringpop.New`, and optionally replaces
  advertised IP while retaining the listening port
  (`go/common/peerprovider/ringpopprovider/provider.go:69-105,318-337`).
- Config default `maxJoinDuration` is 10 s in Cadence, with required name and
  exactly one supported mode: static hosts, JSON file, DNS, DNS-SRV. `custom`
  is explicitly rejected. See
  `go/common/peerprovider/ringpopprovider/config/config.go:34-128` and
  `factory.go:168-184`. DNS not-found yields empty discovery and single-node
  bootstrap (`factory.go:45-81,119-163`).
- Ringpop requires a listening channel, adds its own advertised address to the
  discovered hosts, forms a singleton when that is the only host, otherwise
  retries join attempts until JoinSize (defaults to 3, limited by available
  potential nodes) or MaxJoinDuration. Defaults and retry/group selection are
  in `swim/join_sender.go:39-56,118-170,282-360,482-500`; bootstrap and ready /
  gossip ordering are `swim/node.go:393-456`. Do not approximate this as a
  one-shot connection or consider the node ready before successful bootstrap.
- Join JSON fields, validation, membership snapshot and checksum are in
  `swim/join_sender.go:58-64,439-480` and `swim/join_handler.go:24-77`.
  Request validates app and rejects self-join. Response's full membership is
  produced by `disseminator.MembershipAsChanges`.

### Membership, checksums and propagation

- Ping request/response and error behavior: `swim/ping_sender.go:35-41,54-99`,
  `swim/ping_handler.go:30-80`. The receiver requires Ready, checks app,
  applies changes, then returns its own changes/checksum; checksum mismatch
  with no pending deltas triggers full sync. Reverse full sync is attempted
  (`disseminator.go:150-180`).
- Ping-req requests are in `swim/ping_request_sender.go:35-41,66-101`, handler
  and response in `swim/ping_request_handler.go:26-76`. Helper nodes update
  incoming changes, ping the target, and report `pingStatus`, target, and
  changes. The origin applies response changes. A direct ping timeout followed
  by indirect probes precedes suspicion (`node.go:511-553`; helper selection /
  result handling in `ping_request_sender.go:104-190`).
- Membership checksum is not a generic hash: member address + status + decimal
  incarnation + optional `#labels` + signed FarmHash32-label XOR. Label input
  is big-endian key length/key/value length/value and XOR is order-independent
  (`swim/member.go:114-196`; aggregate checksum in `swim/memberlist.go:85-140`).
  Exact algorithm and legacy zero-label behavior are mandatory for convergence.
- Delta dissemination records one latest change per address, limits retries
  based on cluster size, bumps successful outgoing piggyback counts, receiver
  bumps immediately, filters changes already originated by sender, and sends a
  full membership snapshot when checksums differ and no deltas remain
  (`swim/disseminator.go:62-98,107-180,184-225`). Preserve outbound
  tombstone-to-faulty normalization and inbound reversal
  (`swim/member.go:299-332`).
- Merge precedence and incarnation comparisons are in
  `swim/member.go:211-292`; do not implement status as an unconditional last
  writer-wins map. Local refutation/incarnation increments, label changes,
  and state-dependent merge behavior need Go differential vectors.

### Probe loop, timers, labels, events and shutdown

- `pingNextMember` serializes probes; it direct-pings, then indirect-pings, and
  marks suspect only when at least one helper result makes failure conclusive
  (`swim/node.go:496-553`). Gossip protocol period/rate scheduling is in
  `swim/gossip.go:62-174`.
- Incoming changes are recorded for dissemination and schedule/cancel state
  transitions (`swim/node.go:465-487`). Defaults: suspect 5 s, faulty 24 h,
  tombstone 1 min; timers and transition callbacks are in
  `swim/node.go:82-90`, `swim/state_transitions.go:59-164`. Reaping, self-evict,
  and leave flow are in `state_transitions.go`, `self_evict.go`, and
  `admin_handlers.go`. These must be ported before claiming lifecycle parity.
- Label defaults and bounds come from `swim/labels.go`; internal identity is
  `__identity`. Cadence sets public `serviceName`, `tchannel`, and `grpc`
  labels only after bootstrap (`provider.go:150-169`). Provider maps events to
  `ChangedEvent` (`provider.go:172-195`), filters reachable Alive/Suspect peers
  by exact role and extracts ports with address fallback
  (`provider.go:202-259`), and computes WhoAmI identity from `__identity`
  (`provider.go:262-279`). Label mutation is a membership update; avoid setting
  labels after gossip without disseminating the changed local member.
- Provider lifecycle uses initialized/started/stopped idempotence, installs
  listener after bootstrap, removes it on startup error/stop, and destroys
  Ringpop on stop (`provider.go:127-169,285-301`). Rust shutdown must stop probe,
  healer and transition tasks, reject new calls, then detach handler/state
  cleanly; avoid detached Tokio tasks or a partially ready provider.

## Smallest correct implementable next slice

Implement **hosts-mode bootstrap and join with a production handler, but do
not yet advertise a live multi-node provider**. This is the smallest useful
slice because it adds genuine lifecycle and protocol behavior beyond the
existing injectable boundary while keeping the unsupported ping/gossip gap
explicit. It should:

1. Add Rust config validation for `name`, `bootstrapMode=hosts`, nonempty
   `bootstrapHosts`, `maxJoinDuration` default 10 seconds, and optional
   `broadcastAddress`; reject unimplemented modes with clear errors (do not
   silently treat DNS/file/custom as hosts).
2. Derive the Ringpop advertised `host:port` from the already-bound TChannel
   listener, respecting configured broadcast IP. Ensure service channel is
   `ringpop`, and add a real join endpoint handler to the existing
   `ringpop_tchannel` boundary.
3. Support singleton bootstrap and joining a Go v0.10.0 seed with exact app,
   source, incarnation, timeout, labels, response membership and checksum JSON
   contracts. If the Rust node cannot safely maintain the returned membership
   yet, keep it explicitly in a bootstrap-only state and never report Ready or
   expose it as a usable provider. Do not accept ping/ping-req as successful
   SWIM behavior until their state semantics exist.
4. Establish Go differential coverage using the pinned Go sources: a Go test
   fixture starts `ringpop.New`/`Bootstrap` with `statichosts`, then the existing
   Go JSON TChannel client calls the Rust join endpoint; also test singleton,
   self join, app mismatch, unknown service, malformed request, bootstrap
   timeout, and exact serialized response against Go-generated JSON. Preserve
   seeds and randomized inputs in fixtures. Existing
   `tests/run-ringpop-tchannel-compat.py` is transport baseline; extend it or
   add `tests/run-ringpop-provider-join-compat.py` rather than using the echo
   probe as evidence of provider behavior.

This slice is **not production peer discovery** because Ringpop joining a seed
expects eventual ping/gossip and its own member state to be registered. Its
acceptance criteria must call it an endpoint/bootstrap research slice only;
the next implementation slice must atomically add memberlist merge, checksum,
dissemination and direct ping before exposing Ready or wiring Matching to it.
If two-way integration is required as the first production capability, skip
this smaller slice and implement that dependency group together: join + member
merge/checksum + ping endpoint/sender + disseminator, then add ping-req/failure
detection before making failure/liveness claims.

## Full production sequence and TChannel/main wiring

1. **Provider shell and listener:** introduce a Rust Ringpop provider owning
   config, local advertised address, labels, membership snapshot, subscriptions,
   readiness and task shutdown. Register `RingpopHandler` on `ringpop` over the
   Cadence TChannel listener (preferred shared listener if it supports service
   dispatch; otherwise bind the configured/dedicated Ringpop TChannel address).
   Current `serve_ringpop` binds its own address; main must explicitly spawn it
   or add an equivalent multi-service serving API. Order listener bind before
   bootstrap so remote join is reachable; do not advertise an ephemeral
   address. Wire startup errors and shutdown into the main task lifecycle.
2. **Bootstrap/discovery:** port config defaults/validation and hosts, file,
   DNS, DNS-SRV providers, their dedup/resolution/error rules, self inclusion,
   JoinSize and retries/backoff/deadline. Keep `custom` rejected as Go does.
   Match Go's single-node semantics. Add broadcast address resolution while
   retaining port.
3. **Join/memberlist:** implement join sender and server, app/self validation,
   readiness, incarnation, member merge/preference/refutation, labels and
   checksums. Validate Go's full-membership join response and checksum.
4. **Gossip/probe:** implement change queue/propagation and exact piggyback
   counters, direct ping, endpoint response exchange, checksum reconciliation
   and full sync. Then implement indirect ping request and helper selection,
   including inconclusive errors.
5. **Failure/lifecycle:** timers for alive/suspect/faulty/leave/tombstone,
   local self-refutation, self-evict and graceful leave, bounded task
   cancellation. Match Go's timer defaults and generation checks.
6. **Cadence integration:** publish labels and map ring-change events to
   provider subscriptions. Adapt service filtering/port parsing/identity and
   use the provider in Matching membership resolution. Start it before Matching
   consumes membership; stop it after consumers quiesce. Matching must never
   use a partially initialized provider.
7. **Parity gate:** only after all supported discovery modes and the above
   lifecycle work pass differential tests may main select this provider as
   production default. Document unsupported Go admin endpoints separately;
   don't equate three protocol endpoints with all Ringpop APIs.

## Go differential strategy

- The oracle is the pinned Go implementation, not hand-written expected values.
  Add Go helpers under `tests/` using `go/common/peerprovider/ringpopprovider`
  and Ringpop v0.10.0. Capture deterministic request/response JSON, memberlist
  checksum, state after each event, listener changes, and address/identity
  mapping. Feed identical inputs into Rust and compare normalized outputs.
- Unit vectors: every state pair and incarnation relation; label order/empty
  labels; checksum edge cases; tombstone conversion; nil vs empty arrays/maps;
  stale/same/newer updates; dissemination count and sender filtering; timer
  state transitions with controllable clock.
- Process-level interoperability: Go nodes join Rust and Rust nodes join Go;
  verify both sides' membership and checksums, bidirectional ping, ping-req,
  loss/failure detection, leave, rejoin/refutation, and eventual tombstone
  removal. Run 1, 2, and 3+ nodes, mixed Go/Rust versions, IPv4/IPv6 host-port,
  broadcast IP, and start/shutdown races. Keep long runners in `tmux` session
  `cadence-rust`, distinct named window, and set `CARGO_BUILD_JOBS=1`.
- Existing wire, TChannel, and Matching compatibility runners provide baseline
  evidence only. Their passing results do not establish SWIM membership parity.

## Source index

All Ringpop paths below are relative to
`/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0/`:

- `swim/node.go`: defaults 82-114; channel/subchannel setup and initial member
  231-282; lifecycle 312-360; bootstrap 393-456; change timers 465-487; probe
  loop 496-553.
- `swim/join_sender.go`: defaults and request 39-65; bootstrap candidate
  selection/retry 118-170, 189-360; wire call 439-480; top-level 482 onward.
- `swim/join_handler.go`: response and validation 24-77.
- `swim/{ping_sender,ping_handler,ping_request_sender,ping_request_handler}.go`:
  respective direct and indirect request/response behavior.
- `swim/member.go`: checksum label encoding 114-196; merge precedence
  211-292; state reachability and Change wire normalization 294-377.
- `swim/memberlist.go`: aggregate checksum 85-140, member updates and local
  incarnation handling elsewhere in the same file.
- `swim/disseminator.go`: change propagation and full sync 62-225; reverse
  full sync 256-300.
- `swim/gossip.go`: protocol scheduling 62-174; `swim/state_transitions.go`:
  suspect/faulty/tombstone timers 59-216.
- Cadence Go provider/config/Fx source locations listed above.

No builds or tests were run for this research-only round. No Rust code was
changed.
