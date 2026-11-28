# fix-cargo-lock (2026-11-28)

Bug: rust/Cargo.lock stale (`cargo build --locked` failed). Fixed by regenerating on CI.

- Added `regen_lock` input + `lock-regen` job to rust-ci.yml (`cargo update --workspace`, uploads Cargo.lock, retention 3d).
  Regen run: https://github.com/auenkr/cadence-rust/actions/runs/46118021158 (success, head 457425346). Lock diff: +1 line only.
- Added `cargo metadata --locked` check step in rust-ci clippy-test job to prevent regression.
- Validation on head 7237ef1b6cb9a5e624d34d0ff9cfcc78106385b0 (branch fix-cargo-lock):
  - rust-ci (full): https://github.com/auenkr/cadence-rust/actions/runs/46118202191 success
  - matching-production-tls-mixed: https://github.com/auenkr/cadence-rust/actions/runs/46118204536 success
  - matching-dependency-oracles: https://github.com/auenkr/cadence-rust/actions/runs/46118206863 success
