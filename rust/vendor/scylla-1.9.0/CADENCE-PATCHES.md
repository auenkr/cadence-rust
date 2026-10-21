This is the upstream Scylla Rust driver `scylla` crate version 1.9.0, copied
from crates.io source for one narrow Cadence compatibility patch. Its upstream
Git revision is `5a8b9bd03ef04f8d8531a4b30557d5ee3f59376d` (recorded in
`.cargo_vcs_info.json`). The original license files and package metadata are
retained.

Cadence adds `TlsContext::Rustls023ServerName` and carries the configured
server name through the existing Rustls connector. This restores Go's TLS
`ServerName` behavior for SNI and certificate-name verification without
changing TLS trust or verification policy.
