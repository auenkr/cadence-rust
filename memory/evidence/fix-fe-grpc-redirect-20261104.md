# Fix FE gRPC/redirect review findings (2026-11-04)

Branch: t3/fix-fe-grpc-redirect, head dfb1ee4c47cfc6a7be9e7c891c3803cf77130c48

Fixed: C3 (typed gRPC error details via `status_from_cadence_error`), O13 (List/Scan/Count via handler),
O1 (uuid.Parse parity), O2 (body strong consistency for Describe/GetHistory/Query), O3 (noop policy before deprecated check).
Unit tests added in validation.rs, redirection_runtime.rs, grpc.rs.

## Evidence
- rust-ci (first run, rustfmt failure only): https://github.com/auenkr/cadence-rust/actions/runs/41748216839
- rust-ci (after fmt fix): https://github.com/auenkr/cadence-rust/actions/runs/41748831300 — success
- frontend-mixed-gate (two_cluster=true): https://github.com/auenkr/cadence-rust/actions/runs/41749952992 — success
