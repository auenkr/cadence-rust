# Progress log

- **2026-10-14 — Monorepo move.** Moved the entire Go codebase from repo
  root into `go/` via `git mv` (2945 tracked files, renames staged).
  Restored `.gitmodules` (→ `go/idls`) and `.github/` to root (required
  by git/GitHub). Scaffolded `rust/` Cargo workspace (`cadence-api`,
  `cadence-server`) and `memory/` docs. Verified: `cargo test`
  in `rust/`, Go smoke compile of `go/common/types/...`.
  Follow-ups: fix CI working directories to `go/`, refresh `go/AGENTS.md`
  layout paths, start Thrift type port in `cadence-api`.
