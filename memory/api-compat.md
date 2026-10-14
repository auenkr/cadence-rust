# API compatibility

The Rust server must be a drop-in replacement for SDK clients.

## Compatibility surface (to be verified, not yet automated)

- **Thrift IDL**: source of truth is the `go/idls` submodule plus generated
  code in `go/.gen/go/...`. Rust types in `rust/crates/cadence-api` must
  match field IDs, requiredness, and service method signatures.
- **Persistence schemas**: `go/schema/` versions must remain readable by
  either implementation; no schema rewrites during the port.
- **Behavioral checks**: `go/host/` integration tests and SDK sample
  workflows are the acceptance suite. Rust must pass the same scenarios.

## Current status

- `cadence-api` ports all 32 Thrift enums (Go-identical discriminants and
  `String()` output, int wire encoding) plus core/domain/service structs
  with Go-identical JSON names (`[]byte` as base64). Covered by
  `crates/cadence-api/tests/go_compat.rs` (Go-JSON fixtures) and unit
  tests mirroring Go table tests.
- Transport: HTTP JSON API serving `cadence-api` shapes
  (`rust/crates/cadence-server/src/handlers.rs`), tested by
  `crates/cadence-server/tests/e2e.rs`. Thrift-binary framing and gRPC
  (what the Go/Java SDKs speak) are NOT yet implemented — required
  before SDKs work unmodified.
- Next step: Thrift transport for the served shapes, then SDK-driven
  scenarios from `go/host/`.
