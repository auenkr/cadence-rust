# Matching HTTP TLS interop harness failure — run 40166507919

Run: https://github.com/auenkr/cadence-rust/actions/runs/40166507919  
Artifact: https://github.com/auenkr/cadence-rust/actions/runs/40166507919/artifacts/12588707611  
Artifact ZIP SHA256: `7cb3684c0be30e2e2f7bef17ffae119804ab0ec89d9e94e028db0b8844e6343f`

Source SHA: `5811c0d8bc14ff57e03004260a83843726ef1291`. The fresh locked build
passed (`build.exit=0`) and its binary SHA256 is
`ec2582601d96e73cbaf7650b67afeffe0aebeeefa2181ef772f8b12fbe444351`. The
interop harness exited 1 at `assert_embedded_start_succeeds`; no HTTP TLS
assertion ran. The embedded server log ends with `Matching shard-distributor
configuration requires Ringpop membership`.

This is a runtime startup inconsistency in the embedded development mode, not
a TLS compatibility failure. `launcher.rs` sets `embedded_development` when
there is no config file and deliberately omits production gRPC/TChannel
listeners and the Ringpop provider (around lines 534 and 1059–1104). The
embedded development YAML still has a shard-distributor namespace. Later the
launcher unconditionally tried to activate it and required the unavailable
membership (previously around line 1410). The minimal production fix skips
shard-distributor setup only in embedded development; explicit-config service
startup keeps its configured behavior.

The CI-generated `cargo-lock.patch` is empty: the prior merged lockfile fix
worked. Go TLS classifier, persistence overlay, and earlier startup assertions
passed before the embedded check. Captured source, commands, exits, build log,
binary hash, interop log, and embedded startup log are retained alongside this
summary. CI validation of the fix is pending; no mixed gate was dispatched.
