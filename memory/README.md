# memory/

Working memory for agents on the cadence-rust monorepo. Read these before
starting a task; update `progress.md` when something substantial lands.

Current Matching continuation (2026-10-29): read
`matching-native-integration-20261029.md` and
`matching-native-acceptance-ledger-20261029.md` first. Configured native
production integration is implemented, with fresh workspace validation still
pending. Matching remains incomplete. Historical v7 mixed passes predate the
native migration and cannot validate this candidate. Do not start History.

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

- `matching-storage-next.md` — durable Matching storage port, validation and queue/History RPC continuation.
- `frontend-core-20261031.md` — Frontend bootstrap skeleton, shared-runtime boundary, and pending CI/parity work.
- `frontend-workflow-api-20261031.md` — Workflow handler parity work, poll/history helpers, CI evidence, and remaining integration gaps.
