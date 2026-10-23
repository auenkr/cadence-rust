# Matching HTTP TLS parity audit (2026-10-23)

Base: `origin/matching-grpc-tls-v2-20261022`, `3de4f1339`.
Worktree: `/mnt/storage/service/github/wt/httptls`.
Branch: `matching-http-tls-20261023`.

## Go behavior

- `go/common/config/config.go:122-157`: `rpc.tls` secures gRPC only; HTTP has its own `rpc.http.tls` and `TLSMode`. TChannel remains plaintext.
- `go/common/rpc/factory.go:113-116`: HTTP TLS is applied through YARPC inbound TLS configuration/mode. Existing Rust permissive listener performs the same TLS-record sniff and accepts plaintext/TLS on one socket.
- `go/common/config/tls.go:61-116`: disabled TLS returns nil before file access. Enabled TLS reads `caFiles` then `caFile`, validates a certificate/key only when both names are supplied, and uses host roots if no CA files were supplied. `requireClientAuth` requires and verifies the client chain. Invalid PEM CA blocks are skipped; an explicitly empty CA pool rejects clients at handshake rather than startup.
- `sslmode` is ignored by `ToTLSConfig`; `enableHostVerification` and `serverName` configure client verification, with no inbound listener effect. Rejecting these on HTTP was incompatible.

## Implementation

HTTP startup now loads the CA pool and certificate pair even when TLSMode is disabled, as Go does. Required client authentication uses WebPKI verification with explicit or native trust roots. An empty explicit CA pool still permits startup but rejects all clients. Existing YAML field renames cover every Go TLS field. Invalid modes and unreadable CA/certificate/key files keep contextual startup errors. No Go code changed and no TChannel encryption was added.

Generated-certificate network test covers permissive plaintext, enforced plaintext rejection, trusted TLS client acceptance, missing/untrusted certificate rejection in both modes, ignored inbound-only options, disabled transport file validation, and empty CA startup. Existing listener tests cover mode and certificate error handling and handshake timeout.

## Scope

This is listener-level parity evidence, not a mixed Go/Rust cluster or SDK gate. Full Matching completion is not claimed. Cryptographic library defaults and algorithm support remain those of Rustls/WebPKI rather than Go crypto/tls.
