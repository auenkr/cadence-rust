# Versioned CQL frame wiring (2026-10-27)

Branch: `t3/cql-versioned-frame-wiring`

Commits pushed:

- `87999f30b` — wire version-aware frame headers through the vendored driver connection.
- `852a06747` — add vendored frame tests to `rust-ci.yml` by direct manifest-path invocation.

## Changes

- `SerializedRequest::make` remains the default V4 API; `make_with_version` serializes V2/V3/V4 header layouts.
- `read_response_frame` remains the default V4 API; `read_response_frame_with_version` reads and validates the selected layout asynchronously.
- Vendored Scylla `ConnectionConfig` and `HostConnectionConfig` carry `CqlProtocolVersion`, initialized to V4. The live request serializer, response reader, and stream allocator receive that value.
- V2 stream allocation is bounded to `0..=127`; `-1` remains reserved for server events.
- Direct tests cover versioned request serialization, async response reading, V3 custom-payload flag rejection, and V2 stream allocation.
- Request/response codec modules were not edited. No public option selects V2/V3 and no persistence rejection was relaxed; protocol acceptance remains V4-only until codec and persistence work is complete.

## CI evidence

The direct vendor test is now an explicit `rust-ci.yml` step:

```sh
cargo test --manifest-path rust/vendor/scylla-cql-2.0.0/Cargo.toml --lib frame::
```

No successful CI result is available yet. `gh workflow run rust-ci.yml --ref t3/cql-versioned-frame-wiring -f package=cadence-matching` was rejected with HTTP 422, `Workflow does not have 'workflow_dispatch' trigger`, even though the branch copy of the file contains that trigger. The new standalone workflow was first pushed and its push event produced a zero-job failed run: [run 40411438857](https://github.com/auenkr/cadence-rust/actions/runs/40411438857), head `87999f30b723224cbd9322395640e3fca7e05a0e`. A control dispatch against `master` started run [40411653709](https://github.com/auenkr/cadence-rust/actions/runs/40411653709) on unrelated master SHA `fd11426423139263c9b0175b075fb1457c8cd7c3`; it was canceled and is not validation evidence.

The Rust changes therefore remain uncompiled and unformatted by CI pending a runner invocation against this branch. No Cargo, Go, or rustfmt command was run locally.
