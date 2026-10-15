# memory/

Working memory for agents on the cadence-rust monorepo. Read these before
starting a task; update `progress.md` when something substantial lands.

- `project-goals.md` — what the Rust rewrite must (and must not) do.
- `monorepo-layout.md` — where things live after the Go→`go/` move.
- `api-compat.md` — how SDK/wire compatibility is defined and checked.
- `progress.md` — chronological log of substantial changes.
- `migration-inventory.md` — concrete RPC adapters and remaining migration scope.
- `service-map.md` — service definitions, listeners and implementation boundaries.
- `resume.md` — latest runtime, validation boundaries, lessons and next actions.
- `port-ledger-summary.md` — Go source symbol coverage and review backlog.
- `port-map.json` — reviewed Go-to-Rust mappings; partial means incomplete.

Read `resume.md` first when continuing the rewrite. Passing SDK examples and
running listeners do not establish complete Go behavior. Keep unfinished changes
and validation of the running binary distinct from validation of source changes.
