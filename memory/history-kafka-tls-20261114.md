# History Kafka mTLS gate lane (2026-11-14)

Lane: `history-es-visibility.yml` with new input `kafka_tls=true` (concurrency group and artifact name include it).
`tests/gates/run-frontend-mysql-lane.sh` generates a self-signed CA, a broker cert (SAN 127.0.0.1/localhost) and a client cert with openssl, and builds PKCS12 keystore/truststore with keytool. Kafka (`apache/kafka:3.8.0`, same Docker limits) runs an `SSL` listener with `ssl.client.auth=required` (mTLS) plus a plaintext INTERNAL listener for admin/topic setup.
`run-frontend-gate.py` writes `kafka.tls` (enabled, caFile, certFile, keyFile, enableHostVerification, serverName, Go `config.TLS` schema) for Rust History and the Go Worker, and the same tls block into the async workflow queue connection.

## Runs
- 43580085106 (head 15caaa7): FAILURE, fixture only. Kafka image needs file-based SSL env (`KAFKA_SSL_*_FILENAME/_CREDENTIALS`), not `*_LOCATION`/`*_PASSWORD`; broker exited with `!1: unbound variable`.
- **TLS lane: https://github.com/auenkr/cadence-rust/actions/runs/43580954176** (head e426b374de5415209caa509e0dcfff1d26081ae2): **success**, log has `GATE SUMMARY failed: []`. Artifact shows kafka.tls enabled config, 120 ES docs indexed (Go Worker consumed Rust History's TLS Kafka writes).
- **SASL regression re-run: https://github.com/auenkr/cadence-rust/actions/runs/43580123295** (head 15caaa7, same workflow code for SASL path): **success**, `GATE SUMMARY failed: []`.
- No Rust TLS bug found; no Rust code changed, so no new unit tests. Rust binary SHA256 file was not in the uploaded artifact (not recorded).
- No local cargo/go/docker used.
