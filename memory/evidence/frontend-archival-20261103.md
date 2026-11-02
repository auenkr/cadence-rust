# Frontend archival readers + domain archival state machine (2026-11-03)

Branch `frontend-archival`, head 658cafd5ec70f998c571281d4888c7b0d67a8790.
Full rust-ci (fmt, clippy -D warnings, workspace tests): https://github.com/auenkr/cadence-rust/actions/runs/41548252149 — success.
Earlier: run 41546456027 (clippy fail, fixed), 41547310682 (fmt only, fixed via format_all patch run 41547807779).

## Ported
- `cadence-common/src/archiver.rs`: Go URI parsing, filestore history/visibility archiver (farmhash filenames, tokens, query grammar), provider seams (s3store/gstorage -> "not supported" error once configured).
- `cadence-common/src/archival_state.rs`: Go archivalConfigStateMachine + register/update handling with cluster-configured gating; unit tests for the state table and errors.
- `cadence-frontend/src/archive_readers.rs`: ArchivalMetadata gating (static+dynamic status, read-enabled), GetWorkflowExecutionHistory archive fallback (History PollMutableState EntityNotExists probe), ListArchivedWorkflowExecutions with Go step order/messages; unit tests with real filestore temp dirs.
- Wiring: `build_workflow_api` installs readers + domain archival policy from service config `archival` and `domainDefaults`; `Config.archival`/`domain_defaults`.
- Register/update (`yarpc.rs`, `domain_update.rs`) use the policy when installed (legacy behaviour otherwise).

## Not proven / left out
- No Handler-level endpoint test with real readers, no mixed Go/Rust gate step for archival.
- `service-core` yarpc `handle_visibility_archived` stub still returns the "not configured" error (separate path).
- Real s3/gcloud backends, archive write path (History), exact sqlparser syntax-error text.
- Audit rows 38/39/156 stay `partial`.
