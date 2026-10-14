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

- Only a stub workspace exists (`API_COMPAT_VERSION = "v1-thrift"`).
- Next step: generate/port Thrift types into `cadence-api` and add
  round-trip tests against `go/.gen` fixtures.
