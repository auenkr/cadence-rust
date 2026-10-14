# Rust Cadence implementation

Work-in-progress Rust rewrite of the Cadence server. Constraints:

- No API breaking changes: wire protocol (Thrift) and persistence schemas
  must stay compatible with existing Cadence SDKs and data.
- No program-logic changes: port behavior faithfully from `../go/`.

## Crates

- `crates/cadence-api` — shared API types mirroring the Thrift IDL
  (`go/idls`, `go/.gen`). Compatibility home base.
- `crates/cadence-server` — server binary (stub; services land here).

## Commands

```bash
cargo check --workspace
cargo test --workspace
```
