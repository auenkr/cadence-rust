# Project goals

Rewrite the Cadence server (Go, under `go/`) in Rust (under `rust/`).

## Hard constraints

1. **No API breaking changes.** The Rust server must stay compatible with
   existing Cadence SDKs (Go/Java clients): Thrift service definitions,
   request/response shapes, and error semantics must match.
2. **No program-logic changes.** Port behavior faithfully; do not redesign
   workflows, persistence semantics, or service coordination.
3. **Single monorepo.** `go/` = reference implementation, `rust/` = rewrite,
   `memory/` = this working memory.

## Non-goals (for now)

- New features or protocol redesigns.
- Deleting or forking `go/`; it remains the behavioral reference.
