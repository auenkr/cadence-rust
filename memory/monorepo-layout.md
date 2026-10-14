# Monorepo layout

Since the 2026-10-14 move commit, the repo root holds three directories:

- `go/` — entire pre-existing Cadence Go codebase, moved verbatim with
  `git mv` (history preserved as renames). Includes `go.mod`, `go.work`,
  `Makefile`, `Dockerfile`, `config/`, `schema/`, `service/`, `common/`,
  `host/`, `.gen/`, `proto/`, `tools/`, docs, and Go dot-configs.
- `rust/` — Cargo workspace (`cadence-api`, `cadence-server` stubs).
- `memory/` — this directory.

## Exceptions (kept at root, required by git/GitHub)

- `.gitmodules` — submodule metadata must live at repo root; it points at
  `go/idls` (was `idls` before the move).
- `.github/` — GitHub Actions only reads workflows from root `.github/`.

## Known follow-ups from the move

- CI workflows under `.github/workflows/` still use root-relative paths
  (`make ...`, `docker compose -f docker/...`). They now need a `go/`
  prefix or `working-directory: go`. CI is expected broken until fixed.
- `go/AGENTS.md` "Repository layout" section still describes the old
  root-relative paths; mentally prefix with `go/` (or update the doc).
- `go/.gitattributes` (` .gen linguist-generated=true`) moved with the code;
  root has no gitattributes. Fine unless new generated dirs appear at root.
