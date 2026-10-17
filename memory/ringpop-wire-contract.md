# Ringpop TChannel wire contract (research artifact, 2026-10-17)

Scope: exact wire-level contract for Ringpop's SWIM endpoints used by Cadence production
membership (`/protocol/join`, `/protocol/ping`, `/protocol/ping-req`) and the supporting
TChannel/JSON encoding, membership states, incarnation/checksum/labels, errors, and
bootstrap behavior. This is an audit artifact for implementing a Rust provider that can
interoperate with existing Go Ringpop nodes. It is not a claim of production parity.

## Sources (pinned dependencies)

- Go module: `github.com/uber/ringpop-go v0.10.0`
  (`/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0`)
- Go module: `github.com/uber/tchannel-go v1.34.4`
  (`/home/ubuntu/go/pkg/mod/github.com/uber/tchannel-go@v1.34.4`)
- Cadence Go provider:
  `go/common/peerprovider/ringpopprovider/{provider.go,factory.go,config/config.go}`
- Cadence discovery doc: `memory/matching-discovery-contract.md`
- Rust TChannel implementation already present:
  `rust/crates/cadence-server/src/tchannel.rs`

## Verified evidence vs unresolved questions

Verified from source:
- Protocol method names and registration: `ringpop-go/swim/handlers.go:63-82`.
- Request/response Go structs and JSON tags: `swim/join_handler.go:27-32`,
  `swim/join_sender.go:58-64`, `swim/ping_sender.go:35-41`,
  `swim/ping_request_handler.go:26-30`, `swim/ping_request_sender.go:35-41`,
  `swim/member.go:62-67,299-310`.
- Membership states: `swim/member.go:36-51`.
- TChannel JSON arg2/arg3 encoding and error response shape:
  `tchannel-go/json/handler.go:126-177`, `tchannel-go/json/call.go:62-91`.
- Ringpop service/channel name: `ringpop-go/ringpop.go:188-191`
  (`GetSubChannel("ringpop", tchannel.Isolated)`), and `swim/node.go:277-280`
  (`node.service = node.channel.ServiceName()`).
- Cadence labels: `go/common/peerprovider/ringpopprovider/provider.go:60-64,202-259`
  (`roleKey = "serviceName"`; `__identity` via `rpmembership.IdentityLabelKey`).
- Bootstrap discovery modes and validation:
  `go/common/peerprovider/ringpopprovider/config/config.go:34-128`,
  `go/common/peerprovider/ringpopprovider/factory.go:168-184`.

Unresolved / not verified by this artifact:
- Exact TChannel init frame header layout beyond the Rust implementation's existing
  parser; the Rust code already implements this and is treated as the transport baseline.
- Whether Cadence's Rust server should expose the Ringpop service on the same TCP listener
  as frontend/matching or a dedicated listener; existing Rust `tchannel.rs` services are
  `cadence-frontend`/`cadence-matching`, so a `ringpop` service registration path must be
  added (unresolved wiring).
- Exact `Ringpop.WhoAmI()` address format and broadcast address resolver behavior in
  all environments (K8s broadcast IP) beyond the Go source paths listed.
- Full SWIM algorithm parity (gossip, state transitions, disseminator, full sync, checksums)
  is out of scope for this wire-contract artifact; only endpoint wire shapes are specified.
- Compatibility with older Ringpop versions is mentioned in Go (`validateOutgoing`,
  `validateIncoming`) but not fully enumerated here.

## TChannel service/channel routing

- Ringpop creates an isolated subchannel named `ringpop` on the Cadence TChannel channel:
  `ringpop-go/ringpop.go:188-191`:
  `rp.subChannel = rp.channel.GetSubChannel("ringpop", tchannel.Isolated)`.
- The SWIM node stores this as its channel and sets:
  `node.service = node.channel.ServiceName()` (`swim/node.go:277-280`).
  Therefore the TChannel service name for all three protocol endpoints is `ringpop`.
- Handlers are registered on that subchannel (`swim/handlers.go:63-82`):
  `/protocol/join`, `/protocol/ping`, `/protocol/ping-req`.
- A Rust interop implementation must accept TChannel calls whose service name is `ringpop`
  and method names are exactly those paths. The existing Rust TChannel server currently
  routes only `cadence-frontend` and `cadence-matching` (`rust/.../tchannel.rs:370-374`);
  a `ringpop` route must be added for provider interop.
- Peer addressing uses `host:port` strings (Ringpop advertised address). Cadence's Go provider
  uses the TChannel channel's address as the Ringpop address, with optional broadcast IP override
  (`go/common/peerprovider/ringpopprovider/provider.go` New, `broadcastAddrResolver`).

## Args and header encoding

TChannel JSON format (`tchannel-go/json`):

- Frame-level transport is TChannel v2 as implemented in `rust/crates/cadence-server/src/tchannel.rs`.
- `arg2` is a JSON object used for headers. In Ringpop calls it is written as a
  `map[string]string` (`tchannel-go/json/call.go:62-76` on client side and
  `tchannel-go/json/handler.go:128-133` on server side).
  - Ringpop's `json.CallPeer` passes `nil` headers in practice, so arg2 is typically `{}` or null;
    Go's JSON decoder accepts both into `map[string]string`.
- `arg3` is the JSON encoding of the request/response struct (`tchannel-go/json/call.go:69-70,86-91`).
- Client writes arg2/arg3 with `json.NewEncoder(...).Encode`, which appends a trailing newline;
  server reads with `json.NewDecoder(...).Decode` and requires the stream to be fully consumed
  (`tchannel-go/arguments.go:94-109`). Rust must tolerate/emit standard JSON; trailing whitespace is acceptable.
- Format flag in TChannel call request is `tchannel.JSON` (JSON format).
- Response:
  - arg2 is `ctx.ResponseHeaders()` (a `map[string]string`), usually `{}`.
  - arg3 is the response struct, or, on handler error, an object
    `{"type":"error","message":"..."}` with the TChannel application-error flag set
    (`tchannel-go/json/handler.go:155-170`).
- System errors are TChannel ERROR frames, not application JSON.

## JSON request/response contracts

### `/protocol/join`

Go source:
- Request struct: `swim/join_sender.go:58-64`
- Response struct: `swim/join_handler.go:27-32`
- Handler: `swim/join_handler.go:84-95` -> `handleJoin` (`:52-77`)

Request fields:
- `app`: string, required (validated against node app; mismatch returns error).
- `source`: string, required (validated; joining self returns error).
- `incarnationNumber`: int64.
- `timeout`: time.Duration, serialized as int64 nanoseconds.
- `labels`: map[string]string, `omitempty`.

Response fields:
- `app`: string.
- `coordinator`: string (remote node address).
- `membership`: array of `Change`.
- `membershipChecksum`: uint32.

### `/protocol/ping`

Go source:
- Struct: `swim/ping_sender.go:35-41` (same struct for request and response)
- Handler: `swim/ping_handler.go:30-80`

Fields:
- `changes`: array of `Change`.
- `checksum`: uint32.
- `source`: string.
- `sourceIncarnationNumber`: int64.
- `app`: string.

Notes:
- If `app` is empty and the node requires app in ping, handler returns error
  `"Pinged ringpop requires app name"`.
- If `app` is nonempty and differs, handler returns
  `"Pinged ringpop has a different app name"`.
- Node not ready returns `ErrNodeNotReady` (`"node is not ready to handle requests"`).

### `/protocol/ping-req`

Go source:
- Request struct: `swim/ping_request_sender.go:35-41`
- Response struct: `swim/ping_request_handler.go:26-30`
- Handler: `swim/ping_request_handler.go:32-76`

Request fields:
- `source`: string.
- `sourceIncarnationNumber`: int64.
- `target`: string.
- `checksum`: uint32.
- `changes`: array of `Change`.

Response fields:
- `pingStatus`: bool (JSON key is `pingStatus`, not `ok`).
- `target`: string.
- `changes`: array of `Change`.

### `Change` (membership delta)

Go source: `swim/member.go:299-310`

Fields:
- `source`: string.
- `sourceIncarnationNumber`: int64.
- `address`: string.
- `incarnationNumber`: int64.
- `status`: string, one of `alive`, `suspect`, `faulty`, `leave`, `tombstone`.
- `tombstone`: bool, `omitempty`.
- `labels`: map[string]string, `omitempty`.
- `timestamp`: integer Unix timestamp seconds (`util.Timestamp`; `util/util.go:270-289`).

Outgoing normalization: `Tombstone` status is sent as `faulty` with `tombstone: true`
(`validateOutgoing`, `swim/member.go:326-332`). Incoming normalization reverses this
(`validateIncoming`, `swim/member.go:315-320`).

### `Member` (internal Go type; not directly a protocol struct, but defines labels)

Go source: `swim/member.go:62-67`

Fields:
- `address`: string.
- `status`: string.
- `incarnationNumber`: int64.
- `labels`: map[string]string, `omitempty`.

Membership states (`swim/member.go:36-51`):
- `alive`
- `suspect`
- `faulty`
- `leave`
- `tombstone`

Reachability (`swim/member.go:294-296`): `alive` or `suspect`.

### Labels

- Label map type: `LabelMap map[string]string` (`swim/member.go:71`).
- Internal labels use prefix `__` (`swim/labels.go:39,142`).
- Identity label key: `__identity` (`ringpop-go/membership/interface.go:6`).
- Cadence role label key: `serviceName` (`go/.../ringpopprovider/provider.go:64`).
- Cadence port labels: `tchannel`, `grpc` (`go/common/membership/hostinfo.go:32-38`).
- A typical Cadence matching node's labels:
  - `__identity`: stable host identity (optional; defaults to address if absent).
  - `serviceName`: `matching` (or frontend/history/worker).
  - `tchannel`: `"7935"` (stringified port).
  - `grpc`: `"7835"` (stringified port).

Label limits (`swim/labels.go:22-26` defaults):
- KeySize 32, ValueSize 128, Count 5 (public labels; internal labels ignored).

## Sample JSON (derived from Go structs)

### Join request

```json
{
  "app": "cadence",
  "source": "127.0.0.1:7933",
  "incarnationNumber": 1729123456,
  "timeout": 1000000000,
  "labels": {
    "__identity": "host-a",
    "serviceName": "matching",
    "tchannel": "7933",
    "grpc": "7833"
  }
}
```

### Join response

```json
{
  "app": "cadence",
  "coordinator": "127.0.0.1:7934",
  "membership": [
    {
      "source": "127.0.0.1:7934",
      "sourceIncarnationNumber": 1729123400,
      "address": "127.0.0.1:7934",
      "incarnationNumber": 1729123400,
      "status": "alive",
      "labels": {
        "__identity": "host-b",
        "serviceName": "matching",
        "tchannel": "7934",
        "grpc": "7834"
      },
      "timestamp": 1729123400
    }
  ],
  "membershipChecksum": 3735928559
}
```

### Ping request

```json
{
  "changes": [
    {
      "source": "127.0.0.1:7933",
      "sourceIncarnationNumber": 1729123456,
      "address": "127.0.0.1:7933",
      "incarnationNumber": 1729123456,
      "status": "alive",
      "labels": {
        "serviceName": "matching",
        "tchannel": "7933",
        "grpc": "7833"
      },
      "timestamp": 1729123456
    }
  ],
  "checksum": 3735928559,
  "source": "127.0.0.1:7933",
  "sourceIncarnationNumber": 1729123456,
  "app": "cadence"
}
```

### Ping response

Same shape as ping request (struct `ping` is reused).

### Ping-req request

```json
{
  "source": "127.0.0.1:7933",
  "sourceIncarnationNumber": 1729123456,
  "target": "127.0.0.1:7935",
  "checksum": 3735928559,
  "changes": []
}
```

### Ping-req response

```json
{
  "pingStatus": true,
  "target": "127.0.0.1:7935",
  "changes": []
}
```

### TChannel application error response (handler failure)

```json
{
  "type": "error",
  "message": "node is not ready to handle requests"
}
```

This is returned with the TChannel application-error flag set; arg2 contains response headers (usually `{}`).

## Errors

- Join:
  - `"A node tried joining a cluster by attempting to join itself. ..."` when source == local address.
  - `"A node tried joining a different app cluster. ..."` when app mismatch.
- Ping:
  - `"node is not ready to handle requests"` (`ErrNodeNotReady`).
  - `"Pinged ringpop requires app name"` when app empty and `RequiresAppInPing`.
  - `"Pinged ringpop has a different app name"` when app mismatch.
- Ping-req:
  - `"node is not ready to handle requests"` (`ErrNodeNotReady`).
- Transport:
  - Unknown method or service -> TChannel system error frames.
  - Malformed JSON -> TChannel system error / connection failure.
- Bootstrap:
  - Missing channel: `"channel required"`.
  - No discover provider: `"no discover provider"`.
  - Join timeout: `"join duration of ... exceeded max ..."`.
  - Node destroyed during join: `"node destroyed while attempting to join cluster"`.

## Bootstrap behavior

- Ringpop `Bootstrap(opts)` (`swim/node.go:420-457`):
  - Requires non-nil channel.
  - Sets discover provider.
  - Adds local address to bootstrap hosts if absent.
  - If only local node present -> single-node cluster, no joins.
  - Joins until `JoinSize` (default 3, capped by potential nodes) successful.
  - Retries with exponential delayer until `MaxJoinDuration` (ringpop default 120s;
    Cadence config default 10s, dev 30s).
  - On success: starts gossip and healer (unless `Stopped`), sets `ready=true`.
- Cadence Go provider `Start` (`provider.go:127-169`):
  - Calls `ringpop.Bootstrap`.
  - Adds Ringpop as listener for ring changes.
  - Sets port labels (`tchannel`, `grpc`) and `serviceName` label.
- Discovery providers (`factory.go:168-184`, `config/config.go`):
  - `hosts`: static list.
  - `file`: JSON file (`discovery/jsonfile`).
  - `dns`: resolves each host, no-record -> single-node bootstrap.
  - `dns-srv`: resolves SRV targets then A records.
  - `custom`: explicitly rejected.
  - Empty/unknown mode rejected.
- Single-node bootstrap is possible when DNS returns no records or only local node is present.

## Checksum and incarnation semantics

- Incarnation: int64, per-member `incarnationNumber` (`swim/member.go:65`).
- Membership checksum: uint32 over member address/status/incarnation/labels
  (`swim/member.go:114-121,139-197`); label checksum is XOR of FarmHash32 per label
  and is order-independent.
- `joinResponse.membershipChecksum` and `ping*.checksum` carry this value.
- Changes include `source`/`sourceIncarnationNumber` for the origin member and
  `address`/`incarnationNumber` for the subject member.

## Bounded implementation/test plan for Rust provider

1. Transport reuse:
   - Add a `ringpop` service route to the Rust TChannel server
     (`rust/crates/cadence-server/src/tchannel.rs`) in addition to frontend/matching.
   - Reuse existing frame/arg readers/writers; arg2/arg3 JSON encoding via serde.
2. Data model:
   - Define serde structs matching the field names/types above, including `#[serde(rename_all = ...)]`
     or explicit `#[serde(rename = "sourceIncarnationNumber")]`, `pingStatus`, etc.
   - Implement `omitempty` equivalents with `#[serde(skip_serializing_if = "Option::is_none")]`
     or `skip_serializing_if = "Vec::is_empty"` / map empty; verify exact Go behavior for null vs `{}`.
3. SWIM node state:
   - Implement enough SWIM to exchange these messages and update membership.
   - Start with single-node + two-node interop with Go Ringpop.
4. Labels:
   - Publish `__identity`, `serviceName`, `tchannel`, `grpc` labels.
   - Filter peers by `serviceName == requested service` and status Alive/Suspect,
     matching `provider.go:GetMembers`.
5. Bootstrap:
   - Implement hosts mode first; file/DNS/DNS-SRV only if claiming those modes.
   - Honor max join duration and single-node bootstrap.
6. Tests:
   - Unit: serde round-trip of sample JSON; verify field names and omitempty.
   - Interop: Rust node joins Go Ringpop node via TChannel; both observe each other;
     membership checksums converge; ping/ping-req succeed.
   - Remove node -> tombstone/leave propagates; stale cached endpoint eviction at caller layer.
   - Invalid config and unknown bootstrap mode return startup errors.

## Non-overlap note

This artifact intentionally does not modify lifecycle, HTTP listener, or maintenance code.
It only documents the missing Ringpop provider wire contract. Existing lifecycle code
(candidate commits `d8776890a`, `56fd25c16`, `3a79b10e4`) is transport-independent and
remains unchanged.
