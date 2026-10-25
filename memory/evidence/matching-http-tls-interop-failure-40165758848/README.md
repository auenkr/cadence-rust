# Matching HTTP TLS interop retry failure — run 40165758848

Run: https://github.com/auenkr/cadence-rust/actions/runs/40165758848

Source SHA: `e787524851b06f0f9aab007ae645c5d9cf8c85e5`  
Conclusion: failure. The fresh server build passed (`build.exit=0`) and produced
binary SHA256 `ec2582601d96e73cbaf7650b67afeffe0aebeeefa2181ef772f8b12fbe444351`.
The standalone Go HTTP/Thrift interop harness failed (`interop.exit=1`) before
its TLS server assertions. It expected the Rust domain-cache NoSQL startup
message while invoking `rust/config/development.yaml`; that config selects
Cassandra, so service assembly tried `127.0.0.1:9042` and exited on connection
refused first.

The CI-generated `cargo-lock.patch` is the exact lockfile delta produced by
the successful unlocked build. It reorders `semver` in the cadence-server
dependency list and adds the `js-sys` and `wasm-bindgen` dependencies to
`chrono`. No package version, checksum, or production dependency declaration
changed. The patch is applied to `rust/Cargo.lock` in the follow-up fix.

The follow-up also makes the negative NoSQL startup assertion deterministic:
it keeps the development NoSQL datastore configuration but substitutes an
unsupported plugin name, so startup verifies the explicit plugin rejection
before any database connection. TLS assertions and production code are
unchanged. Evidence files include the original CI source SHA, commands, build
and interop exits/logs, generated lockfile patch, and built binary checksum.
