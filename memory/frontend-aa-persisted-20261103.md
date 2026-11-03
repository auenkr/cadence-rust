# Frontend: persisted active-active lookups + outbound authorization (2026-11-03)

Branch `t3/frontend-aa-persisted` (from master incl. 387b007b5).

## Done
- `HistoryNodeStore` gained `select_current_execution` / `select_active_cluster_policy` (MySQL, Postgres, Cassandra impls: Go `current_executions`, `active_cluster_selection_policy`, Cassandra `executions` rows type 1 / 11). `decode_active_cluster_selection_policy` (thriftrw / thriftrw_snappy).
- `DomainActiveClusterManager` (redirection_runtime.rs) uses them: Go `getClusterSelectionPolicy` (current-run resolve when run_id empty, EntityNotExists -> empty policy) and `GetActiveClusterSelectionPolicyForCurrentWorkflow`. Policy cache not ported (as in Go-less path).
- Outbound `cadence-authorization` RS256 JWT (`cadence-service-core::outbound_auth`, Go claims iss=internal-jwt, admin, ttl 600) on TChannel and gRPC remote clients when `clusterGroup.<x>.authorizationProvider.enable`; bad key path fails calls ("invalid private key path"). Config field `authorizationProvider` omitted on serialize when default.
- Consistency (item 3): Go never forwards a consistency header outbound; level comes from inbound `cadence-client-feature-flags` (already ported). Only `cadence-forwarding-cluster` is sent. No code needed.
- Unit tests: policy blob decode fixtures (plain + snappy + bad encoding), fake-store manager tests, JWT sign/verify tests, header tests.

## Evidence
- rust-ci full (clippy + fmt + workspace tests): https://github.com/auenkr/cadence-rust/actions/runs/41652326886 — success, head 8e65808688d2c7f496d3906adb3f6649e1fc19f8 (earlier runs failed on clippy too_many_arguments, rustfmt, cluster fixture serialization; fixed).
- Two-cluster gate (frontend-mixed-gate.yml, two_cluster=true, mysql, rust_ref=branch): https://github.com/auenkr/cadence-rust/actions/runs/41653880433 — success.
- Rust cadence-server SHA256 (fresh target dir): d8a6a57f5ed0c4f1189e3d1269b5a0b1eb30bf29e988992a2b2b43e597dfbff9

## Remaining
- No gate step added for outbound auth against an oauth-enabled cluster B, nor for persisted AA policy against real DB rows (covered by unit tests only; SQL/Cassandra queries not exercised end-to-end).
- Postgres/Cassandra two-cluster lanes not run.
