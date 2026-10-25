# HTTP TLS interop setup failure — run 40165433074

Run: https://github.com/auenkr/cadence-rust/actions/runs/40165433074  
Exact source/head: `4b5d90458471c36ab8064bb69c695854ffc23de5`  
Conclusion: failure in “Build fresh Rust server”; `build.exit=101`.

The build log says Cargo could not update `rust/Cargo.lock` because the
workflow passed `--locked`. The build stopped before producing the server
binary; the Go HTTP TLS interoperability step was skipped. This is a concrete
build setup/lockfile freshness failure and provides no production TLS result.
The run did not weaken or exercise any compatibility assertion.

The narrow workflow correction removes `--locked`, consistent with the
repository's other CI build gates, and captures the resolver's
`rust/Cargo.lock` diff as `out/cargo-lock.patch` alongside the binary SHA256.
No TLS source or compatibility assertion changed. Retain run 40165433074 as a
named failure; retry the same first lane on the new immutable retry ref only
after confirming no other mixed gate is active.

Compact source, command, exit and build log are beside this README. Original
workflow and artifact ZIPs are in
`/mnt/storage/service/ci-evidence/run-40165433074/`; ZIP checksums are recorded
in `artifact-zip.sha256`.
