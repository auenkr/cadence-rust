# Matching TLS compact evidence retention — 2026-10-27

The compact Matching gate collector now retains the safe TLS proof emitted by
`c16d25e01` when present:

- `gate/run/rust-matching/matching-tls-handshakes.json` records the live
  endpoint, effective `serverName`, verification outcome, ALPN, certificate
  and CA fingerprints, and peer subject/SAN/issuer. The probe checks the
  running endpoint certificate against the configured public certificate.
- `gate/evidence/config-redacted/rust-matching.yaml` preserves the
  redacted effective Rust Matching config identity.
- `gate/evidence/config-hashes.json` preserves hashes identifying
  the config inputs. The handshake provides the effective public hostname;
  sensitive config fields remain redacted.

The collector's existing selected-file inventory hashes each retained input
and copies files up to its existing 1 MiB limit. Missing TLS evidence remains
an inventory absence and does not change gate acceptance or claim Matching
service parity. No private key or password content is retained.

Parent review found the TLS workflow did not request the optional sanitized
bundle. It now passes `--evidence-dir out/gate/evidence`; the collector reads
the configuration snapshots and hashes from that actual output directory.
It also retains the optional secondary configuration and binary identity.

Validation: Python syntax validation and a synthetic collector fixture passed.
The fixture verifies exact copying of the handshake, sanitized configuration
and hash records, and exclusion of an adjacent private key. No Cargo or Go
command was run. Runtime validation still requires the same-pin TLS gate.
