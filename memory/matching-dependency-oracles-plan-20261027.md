# Matching dependency oracles gate (plan, 2026-10-27)

Supplemental, focused CI gate; NOT a substitute for the ten full mixed lanes.
Not yet dispatched (parent owns serial CI). Record run URL / head SHA /
conclusion here after dispatch.

- Workflow: `.github/workflows/matching-dependency-oracles.yml` (workflow_dispatch only, ubuntu-latest, 90 min, cancel-in-progress)
- Script: `tests/gates/run-matching-dependency-oracles.sh`
- Dispatch: `gh workflow run matching-dependency-oracles.yml --ref <pushed branch>` (the ref must contain both files).
- Target dir: fresh `$RUNNER_TEMP/cargo-target-dependency-oracles`, no Cargo cache.

## Oracles (both run with `--ignored --exact`, must report `1 passed`)
1. `cadence-matching` lib `matching_history_tchannel::tests::generated_go_history_server_accepts_native_tchannel_calls`
   with `CADENCE_HISTORY_PEER_BIN` = Go peer built on-runner from `go/tools/cadence-rust-history-peer` (GOTOOLCHAIN=go1.25.14).
2. `cadence-persistence` lib `matching_postgres::tests::postgres_tls_live_oracle`, one invocation per case against a
   disposable `postgres:16` (ssl=on, memory/swap 1800m, cpus 1.5, pids 2048). Server cert is localhost-only (DNS SAN, no IP SAN).

| case | host | sslmode | CA | expected |
|---|---|---|---|---|
| disable | 127.0.0.1 | disable | - | success |
| require-no-ca | 127.0.0.1 | require | none (lib/pq: no verification) | success |
| verify-ca-valid | 127.0.0.1 | verify-ca | valid CA file | success |
| verify-full-local | localhost | verify-full | valid CA file | success |
| verify-full-ip | 127.0.0.1 | verify-full | valid CA file | reject (hostname mismatch) |
| verify-ca-wrongca | 127.0.0.1 | verify-ca | wrong CA file | reject |
| require-wrongca | 127.0.0.1 | require | wrong CA file (lib/pq verifies when root cert exists) | reject |
| inline-verify-ca | 127.0.0.1 | verify-ca | inline CA (+inline client cert/key; sslinline requires them) | success |
| inline-verify-full | localhost | verify-full | inline CA | success |
| inline-wrongca | 127.0.0.1 | verify-ca | inline wrong CA | reject |

Expectations derive from `postgres_options`/`postgres_tls_connector` in
`rust/crates/cadence-persistence/src/matching_postgres.rs` (lib/pq v1.10.2 semantics).

## Evidence (artifact `matching-dependency-oracles-evidence`, retention 3 days)
source-sha.txt, toolchains.txt, every stage `*.exit`, `overall.exit`, logs,
`*-binary.sha256` (hash of the exact test executable Cargo reported via
`--message-format=json`, plus the Go peer), postgres limits/cleanup logs.
Never retained: CA/server/client keys, password (random per run, env only,
redacted from logs; the script fails if `PRIVATE KEY` or the password appears in evidence).
