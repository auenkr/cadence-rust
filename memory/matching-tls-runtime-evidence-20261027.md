# Matching production TLS runtime evidence retention

The final collector review identified that generated config redaction and
temporary public certificates left no independent artifact linking the tested
Matching listener to a specific certificate and verified hostname. The TLS
fixture already configured CA-backed Go clients with enableHostVerification
and localhost as the server name; no service TLS behavior changed.

The production Node probe now performs a separate TLS handshake against the
exact primary Rust gRPC endpoint after successful Go Frontend/direct readiness
RPCs, including after restarts. It requires CA verification, hostname localhost,
rejectUnauthorized=true, ALPN h2 and equality between the observed peer
certificate fingerprint and the configured public server certificate. The
baseline supplies the existing public server-cert path alongside its CA path.

matching-tls-handshakes.json retains process PID, endpoint, readiness label,
verification outcome, hostname, ALPN, CA/server SHA256 fingerprints and public
subject/issuer/SAN fields. The evidence copier retains this file whether it
lives in the run root or the nested Rust probe directory. No keys, passwords
or certificate private material are written into it. The existing config
hashes and redacted configs plus SDK/recovery results complete the evidence
chain; the handshake alone does not establish SDK or recovery success.

Local Node syntax and Python AST checks passed. No Cargo, Go or Rust formatter
ran locally. Actual handshake and runtime results require the exact-pin TLS
mixed gate; no pass is claimed here.
