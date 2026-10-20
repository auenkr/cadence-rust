#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
vendor_tmp="$(mktemp -d "${TMPDIR:-/tmp}/cadence-mysql-vendor-tests.XXXXXX")"
trap 'rm -rf "$vendor_tmp"' EXIT

# This first command is the one allowed workspace lockfile resolution step.
# Workspace Cargo commands after it are locked and consume rust/Cargo.lock.
cargo update --manifest-path "$repo_root/rust/Cargo.toml"
cargo check --workspace --manifest-path "$repo_root/rust/Cargo.toml" --locked

# mysql_async is a patched dependency, so test its private Endpoint unit tests
# from an isolated copy instead of adding the vendored crate to the workspace.
cp -a "$repo_root/rust/vendor/mysql_async-0.37.1" "$vendor_tmp/mysql_async-0.37.1"
# The isolated vendor copy has a separate lock so private crate unit tests do
# not add the vendored package to the project workspace or change its lock.
cargo generate-lockfile --manifest-path "$vendor_tmp/mysql_async-0.37.1/Cargo.toml"
cargo test --manifest-path "$vendor_tmp/mysql_async-0.37.1/Cargo.toml" --locked \
  --no-default-features --features minimal-rust,rustls-tls,ring \
  tls_upgrade_ -- --nocapture

# This starts pinned MySQL 8.0.36, runs Rust DSN/domain timeout/cancellation
# and TLS checks, then performs Go -> Rust -> Go persistence verification.
python3 "$repo_root/tests/run-matching-mysql-interop.py"
