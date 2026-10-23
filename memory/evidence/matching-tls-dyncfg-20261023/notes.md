# Matching dynamic config providers and TLS audit (P0-3, P1-15)

Branch: `matching-tls-dyncfg-20261023` (from `origin/master`).
Audit source: `memory/matching-go-parity-audit-v4-20261023.md` on `origin/matching-parity-audit-v4-20261023`.
Scope: source reading against Go (`go/`, yarpc v1.88.0 from the module cache) and Rust
(`rust/crates/cadence-matching`, `rust/crates/cadence-service-core`). No cargo run locally;
verification is the GitHub `rust-ci.yml` run recorded at the end.

## P0-3: dynamic config providers

### What Go supports

`go/common/dynamicconfig/dynamicconfigfx/fx.go` `New` selects on `dynamicconfig.client`:

| `dynamicconfig.client` | Go client | Startup behavior |
|---|---|---|
| `""` (empty) | legacy `dynamicConfigClient` file client, logs `falling back to legacy file based dynamicClientConfig` | file client, or NOP if creation fails |
| `filebased` | `dynamicconfig.NewFileBasedClient` (`file_based_client.go`) | file client, NOP if creation fails |
| `configstore` | `configstore.NewConfigStoreClient` (`configstore/config_store_client.go`) | reads dynamic-config rows from the default datastore; NOP only if creation fails |
| `openfeature` | `openfeatureclient.NewOpenFeatureClient` + `RegisterProvider` on start | named provider plugin; start fails on error |
| anything else, including `nop` | NOP (`fx.go` default branch) | no-op |

The same `dynamicconfig.Collection` serves every Go service, Matching included. Matching has
no client-specific code.

### Repo and gate usage

- Every literal `client:` line in `go/config/*.yaml` is `filebased`.
- `go/docker/config_template.yaml` renders `client: {{ $dcClient }}`. Only
  `go/docker/docker-compose-unleash.yml` sets `DYNAMIC_CONFIG_CLIENT=openfeature`, with
  `OPENFEATURE_PROVIDER_NAME=unleash`.
- Gate configs (`memory/evidence/matching-gate-run3-20261023/cassandra/config-redacted/development.yaml`
  and the other `config-redacted` directories) use `client: filebased`.
- Conclusion: no repo gate or development deployment uses `configstore` or `openfeature`.

### Rust status

- `filebased` and the legacy `dynamicConfigClient` are implemented. The selection matches
  `fx.go` (`cadence-service-core/src/config.rs` `dynamic_config_provider`, exact names,
  unknown names mean NOP). The unit test `dynamic_config_provider_selection_matches_dynamicconfigfx`
  pins it.
- File behavior checked against `file_based_client.go` in this audit, with no change needed:
  - Missing or unreadable file at startup: Go `validateConfig`/`update` error, NOP. Rust
    `DynamicConfigOverlay::from_file` error, `using no-op configuration`.
  - Poll interval: Go `minPollInterval` 5s, applied when the value is below 5s, including zero.
    Rust `nanos.max(5_000_000_000)`, and 5s when empty.
  - Empty `filepath`: Go builds `root + "/" + ""`, which fails to read and yields NOP. Rust
    warns and yields NOP. Same outcome.
- `configstore`: **blocked gap, not implemented.**
  - Rust `cadence-persistence/src/dynamic_config.rs` has typed read helpers over
    `DynamicConfigValue` entries (the admin-side representation). It has no reader for the
    config-store table: no SQL (`go/common/persistence/sql/sql_config_store.go`,
    `sqlplugin/mysql/configstore.go`, `sqlplugin/postgres/configstore.go`) and no
    NoSQL/Cassandra config-store reader.
  - Faithful support would also need Go's poll loop, `validateClientConfig` (min 2s poll,
    retry attempts, fetch/update timeouts), and the update-version cache in
    `config_store_client.go`. None of this exists in Rust.
  - Rust refuses to start. Exact message in `launcher.rs` (~line 561):
    `dynamicconfig.client "configstore" (pollInterval {}ns, updateRetryAttempts {}, FetchTimeout {}ns, UpdateTimeout {}ns) is not supported by Rust Matching: ...`
    The message is the Go-side reasoning. The Go fallback to NOP only applies when creating the
    client fails, and Rust does not silently run on defaults.
- `openfeature`: **blocked gap, not implemented.**
  - Go registers one provider plugin, `unleash` (`go/common/dynamicconfig/openfeatureprovider/unleash/init.go:36`).
    Its `RegisterProvider` (`openfeatureclient/client.go:123-145`) returns
    `unknown openfeature provider: "<name>"` for an unknown name, and the start hook fails.
  - Rust has no provider plugins. Exact startup message in `launcher.rs` (~line 566):
    `dynamicconfig.client "openfeature" (providerName "<name>") is not supported by Rust Matching: ...`
  - Both sides refuse to start for an unknown provider. Only the message text differs.

## P1-15: TLS

### Go behavior (pinned)

- `go/common/config/tls.go` `ToTLSConfig`:
  - Disabled returns nil.
  - `InsecureSkipVerify = !enableHostVerification`.
  - `caFiles` plus `caFile` are appended and read; an unreadable file is an error.
  - No CA files: nil `RootCAs`, which means system roots.
  - `requireClientAuth`: `RequireAndVerifyClientCert`, with `ClientCAs = RootCAs`.
  - The client pair is loaded only when `certFile` and `keyFile` are both set. `LoadX509KeyPair`
    errors are returned.
  - Certificates are loaded once at startup. No reload path exists in `tls.go` or in Rust.
- `go/common/rpc/params.go:50-160`:
  - Inbound gRPC TLS = this service's `rpc.tls`.
  - Outbound TLS for History = History's `rpc.tls`; for Matching = Matching's `rpc.tls`.
    A missing outbound service config is skipped.
  - HTTP TLS is built only when `rpc.http.tls.enabled`. `TLSMode` is set only inside that branch.
- `go/common/rpc/factory.go:63-96`: TChannel inbound and outbound have no TLS. The factory
  calls `tchannel.NewChannelTransport(...)` and `tchannel.NewTransport(...)` without TLS options.
  `rpc.tls` applies only to the gRPC inbound (`grpc.InboundCredentials`).
- `go/common/rpc/outbounds.go:236-250`: `createDialer` applies TLS only on the gRPC path
  (`enableGRPCOutbound`, default `true`, `constants.go:1791`). The TChannel path is plaintext.
- yarpc `transport/http/inbound.go:338-350`: `TLSMode != Disabled` wraps the listener with
  `muxlistener` (`Permissive` accepts both, `Enforced` accepts TLS only). A nil config with a
  non-disabled mode is an error.

### Rust comparison

| Item | Go | Rust | Result |
|---|---|---|---|
| YAML keys (`enabled`, `certFile`, `keyFile`, `caFile`, `caFiles`, `enableHostVerification`, `requireClientAuth`, `serverName`, `sslmode`, `TLSMode`) | `config.TLS`, `config.HTTP.TLSMode` | `cadence-service-core/src/config.rs` `TlsConfig`, `HttpConfig.tls_mode` | same tags |
| gRPC inbound TLS | `rpc.tls` on the gRPC listener | `grpc_tls::server_tls(&rpc.tls)` on the gRPC listener (`launcher.rs` ~889, ~1467) | same |
| Matching-to-Matching peer TLS | Matching's `rpc.tls` | `matching_peer_tls` from `matching_rpc.tls` (`launcher.rs` ~896, membership `with_peer_tls`) | same |
| Matching-to-History gRPC TLS | History's `rpc.tls` | `history_tls` from `services["history"].rpc.tls` (`launcher.rs` ~1028-1042) | same |
| Matching-to-History TChannel | plaintext | `TChannelHistoryClient` (no TLS) | same |
| CA list and missing-file error | append `caFile`; error on read | `ca_certs` appends `caFile`; `with_context` error | same (new test) |
| Lone `certFile` | not loaded | `client_tls` loads only when both set | same (new test) |
| Server `requireClientAuth` without CA | system roots | `load_native_certs` | same |
| Client `enableHostVerification=false` | chain and name skipped, handshake signature still checked | `SkipChainVerification` checks signatures only | same (existing test) |
| Client `serverName` | SNI and name check | SNI and name check | same (existing test) |
| Min TLS version | Go default (TLS 1.2) | `with_safe_default_protocol_versions` (1.2 and 1.3) | equivalent |
| HTTP `TLSMode` disabled with TLS enabled | plaintext, CA files still read | plaintext, CA files still read (`matching_http_tls`) | same (existing test) |
| HTTP permissive and enforced | `muxlistener` | `serve_matching_http_permissive` and `bind_rustls` (`launcher.rs` ~1498-1510) | same (existing tests) |
| HTTP `TLSMode` unknown | yaml error | `bail!` at startup | same outcome |
| TChannel server TLS | none | none (`tchannel::serve_matching`) | same |
| Certificate reload | none | none | same |

### Divergence kept (not fixed)

- **gRPC `rpc.tls` enabled without `certFile`/`keyFile` (server side).** Go `ToTLSConfig`
  returns a config with no certificates and does not error. The gRPC server therefore starts,
  and each handshake fails with a crypto/tls "no certificates" error. The exact handshake
  error text was not checked by running Go. Rust `server_tls` fails at startup
  (`rpc.tls requires both certFile and keyFile for a server certificate`). Matching HTTP
  starts without a certificate, as Go does, because `NoServerCertificate` is used there.
  Replicating Go here would require tonic to serve with no identity, and tonic does not allow
  that. This is a fail-fast choice, so it is left as is and listed for a decision.

### Tests added

`rust/crates/cadence-service-core/src/grpc_tls.rs`, pinned to `tls.go`:
- `lone_client_cert_file_is_not_loaded_like_go`
- `missing_ca_file_fails_tls_load_like_go`
- `ca_files_and_ca_file_are_both_trusted`

Existing gRPC TLS tests (5) and HTTP TLS tests (6) are unchanged.

### TChannel TLS (question from the task)

Go does not apply TLS to the TChannel inbound for Matching. Evidence: `factory.go:63-75`
creates TChannel transports with no TLS option, and the yarpc TChannel transport's
`inboundTLSConfig` (`transport/tchannel/transport.go:86`) is never set by the factory. Rust
does the same. The Matching TChannel listener, ringpop, and Matching-to-History TChannel calls
are plaintext in both.

## Outcome

- P0-3: file provider at parity (checked, no code change needed). `configstore` and `openfeature`
  are blocked gaps with the exact startup messages above. Not implemented, because a faithful
  `configstore` needs persistence reads and poll logic that Rust does not have, and no repo
  config uses either provider.
- P1-15: TLS parity confirmed for gRPC, HTTP, and TChannel, except the divergence above. Three
  Go-pinned unit tests added.

## CI

Code commit tested: `cedeaa27eb9d16412420cc44aa580786503a8b72`.

| Run | Trigger | Conclusion |
|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/40010336990 | `workflow_dispatch`, `-f package=cadence-matching` | success |
| https://github.com/auenkr/cadence-rust/actions/runs/40009824619 | `workflow_dispatch`, no package (full workspace) | success |

Notes on the runs:
- The first `package=cadence-matching` dispatch (run 40009799831) was cancelled by the workflow
  concurrency group when the no-package dispatch started. It was re-run after the full run
  finished, as run 40010336990.
- The full run's test log lists the three new tests as `ok`:
  `grpc_tls::tests::lone_client_cert_file_is_not_loaded_like_go`,
  `grpc_tls::tests::missing_ca_file_fails_tls_load_like_go`,
  `grpc_tls::tests::ca_files_and_ca_file_are_both_trusted`.
- Clippy, `cargo test`, and the package-scoped run all report success. The only annotation is
  GitHub's notice that `ubuntu-latest` migrates on 2026-10-19.
- Later commits on this branch change only this notes file.
