# Matching drop-in review (gpt-6.1-sol medium) and resolution, 2026-10-31

Independent read-only review of c508c66d found the ten mixed lanes real (all eleven runs verified success at that SHA) but deployment parity gaps:

| # | Finding | Status |
|---|---|---|
| 1 | gRPC/TChannel listeners ignored `rpc.bindOnIP` | FIXED: `matching_bind_ip` in launcher.rs (bindOnIP honoured, mutual-exclusion with bindOnLocalHost errors like Go). Unset keeps wildcard bind (Go auto-detects one IP; wildcard is a superset). |
| 2 | Go global flags/env before `start` unsupported | FIXED: `--root/-r CADENCE_ROOT`, `--config/-c CADENCE_CONFIG_DIR`, `--env/-e`, `--zone/--az` are global clap args (work before or after `start`); `-s` alias for `--services`; relative config resolved under root; default `<root>/config` used when present, embedded development settings only when absent. `check-config` uses the global `--config`. |
| 3 | HTTP :8080 bound unconditionally | FIXED: Matching-only process binds the legacy HTTP API port only when `--port` is given (configured `rpc.http` unchanged). |
| 4 | Parity-audit closure not substantiated | This file + lane runs below; earlier audits in memory/matching-*.md. |
| 5 | `MATCHING_LOG_EVENTS` simulation event stream and Go Debug task diagnostics not ported | KNOWN, deliberately open: diagnostic/simulation-only output, no SDK or wire effect. |

Validation source: branch/tag validation/matching-dropin-cli, head 2dfc71c8665ec32fed4f6e9b324ecd92582bdbfa.
- rust-ci clean_target: 41167190068 success (first attempt 41167049445 failed rustfmt only)
- mysql two-node 41167839959, Cassandra 41167842482, active offer 41167845012, production TLS 41167847367: success
- production-validation: postgres 41167849637, thrift 41168475962, mysql-probes 41169072028, configstore 41169700898, shard-distributor 41170289583, adaptive 41170916634: success
Binary SHA256 values are in each lane's artifacts.
