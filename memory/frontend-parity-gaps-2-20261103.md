# Frontend parity gaps, round 2 (2026-11-03)

Branch `frontend-parity-gaps-2` (from master 9d1653fa4). CI: package cadence-frontend, run https://github.com/auenkr/cadence-rust/actions/runs/41616651958 success, head b702735d3 (earlier run on visibility commit: .../41615934034 success). No full-workspace run, no mixed gate run.

DONE (unit-tested in CI only):
- Per-domain `system.readVisibilityStoreName` (default `es`): `workflow_visibility::read_store_selects_es`, `EsBinding.read_store`, `ProductionBackend::es_for_read`. First comma entry wins; `es`/`os` -> ES, anything else (db, pinot, empty) -> DB visibility (Go falls back to DB when the store is unavailable). List/Scan/Count on DB -> "Operation is not supported" like Go. Not gated end-to-end with a mixed cluster.
- JWKS refresh: Go uses keyfunc v2.1.0 `Options{}` = no RefreshInterval, no RefreshUnknownKID, so Go never refreshes. Rust already matches (fetch once at startup); added test `jwks_is_fetched_once_and_not_refreshed` with a fake JWKS HTTP server. No code change needed.

NOT DONE (still open, audit rows unchanged):
- Cluster redirection: schedule APIs are served by the yarpc/handlers path in cadence-service-core, not the workflow Handler that carries the redirector; resolver wiring needs a refactor. Strong-consistency flag (Go reads FeatureFlags.AutoforwardingEnabled from the call headers) not plumbed. gRPC remote client, active-active routing and two-cluster gate extensions not started.
- Cross-cluster task payloads (admin_replication.rs): API types for CrossClusterTaskRequest/Response and their 5 attribute types still absent; non-empty payload still returns Internal error.
