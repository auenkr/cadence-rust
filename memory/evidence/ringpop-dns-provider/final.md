# DNS/SRV provider verification — 2026-10-19

Source worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider`, branch `t3/matching-dns-provider`, base `6977c326dd09e7338ef8d662e5e2eec11828d415`.

The final source under test is the worktree diff to `rust/crates/cadence-server/src/ringpop_discovery.rs`, Cargo manifest/lock, Go oracle overlay and runner, plus `tests/fixtures/ringpop_dns_provider.json` and `tests/run-linux-system-lookup-parity.py`. Source SHA-256: `b74c67332bd301ba60f18b28834092e83006482fba7b2e78ac780b863dce4cc6`. Cargo target was exclusively `rust/target` in this worktree; `CARGO_BUILD_JOBS=1` throughout Rust checks.

All final commands exited 0:

- `cd rust && CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider/rust/target cargo test -p cadence-server --lib ringpop_discovery` — 15 passed, 0 failed. Log: `focused-final.log`.
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider/rust/target python3 tests/run-linux-system-lookup-parity.py` — actual host Go `net.LookupHost` output for localhost and a scoped-format literal compared through the Rust system lookup test; passed. Log: `linux-system-lookup-final.log`.
- `python3 tests/run-ringpop-bootstrap-provider-oracle.py` — pinned Go static/file/config/DNS/SRV fixture oracles passed, including actual DNS/SRV fake-resolver behavior. Log: `go-provider-oracle-final.log`.
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider/rust/target cargo clippy --locked -p cadence-server --all-targets -- -D warnings` — passed. Log: `clippy-final.log`.

Each command's exit status is in the matching `*-final.exit` file. `git diff --check` passed.

Scope limits: the Linux libc implementation represents Go's cgo resolver path on Linux and follows host NSS behavior. This does not prove Go pure-Go resolver selection (`netgo`, `GODEBUG=netdns=go`, or Go platform heuristics), nor non-Linux NSS parity. Hickory SRV resolver settings are initialized once and do not reload changed system resolver configuration as Go's resolver does. Production Matching lifecycle wiring, mixed Go/Rust service validation, restart/recovery and SDK workflows remain incomplete; this patch is not a Matching completion claim.
