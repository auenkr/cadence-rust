# cadence-rust

Monorepo for the Cadence workflow engine: original Go implementation plus an
in-progress Rust rewrite.

## Layout

- `go/` — original Cadence Go codebase (moved verbatim from the repo root).
  See `go/README.md`, `go/CONTRIBUTING.md`, and `go/AGENTS.md`.
  The `idls` git submodule now lives at `go/idls`.
- `rust/` — new Rust implementation of Cadence. Must stay API-compatible
  with the Cadence SDKs; no program-logic changes vs the Go behavior.
- `memory/` — findings, decisions, and progress log for agents working on
  this repo. Read `memory/README.md` before starting a task.

## Quick start

Go server (unchanged workflow, now under `go/`):

```bash
cd go
git submodule update --init --recursive
make bins
```

Rust workspace:

```bash
cargo test --manifest-path rust/Cargo.toml
```

## License

Apache 2.0 — see `go/LICENSE` (covers the Go codebase; the Rust rewrite
is under the same license).
