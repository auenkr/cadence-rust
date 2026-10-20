#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
vendor_tmp="$(mktemp -d "${TMPDIR:-/tmp}/cadence-mysql-vendor-tests.XXXXXX")"
trap 'rm -rf "$vendor_tmp"' EXIT

# This first command resolves the newly declared driver into the existing lock
# without refreshing unrelated packages. Workspace commands after it are locked.
cargo check --workspace --manifest-path "$repo_root/rust/Cargo.toml"
cargo check --workspace --manifest-path "$repo_root/rust/Cargo.toml" --locked

# mysql_async is a patched dependency, so test its private Endpoint unit tests
# from an isolated copy instead of adding the vendored crate to the workspace.
cp -a "$repo_root/rust/vendor/mysql_async-0.37.1" "$vendor_tmp/mysql_async-0.37.1"
# The repository vendors only the library source; upstream's two integration
# test targets are intentionally absent. Remove those manifest entries from
# the isolated test copy so Cargo can select its in-tree unit tests.
python3 - "$vendor_tmp/mysql_async-0.37.1/Cargo.toml" <<'PY'
from pathlib import Path
import re
import sys

manifest = Path(sys.argv[1])
text = manifest.read_text()
text, removed = re.subn(
    r'(?m)^\[\[test\]\]\nname = "(?:exports|generic)"\npath = "tests/[^\n]+"\n\n',
    "",
    text,
)
if removed != 2:
    raise SystemExit(f"expected to remove 2 missing mysql_async test targets, removed {removed}")
manifest.write_text(text)
PY
# The isolated vendor copy has a separate lock so private crate unit tests do
# not add the vendored package to the project workspace or change its lock.
cargo generate-lockfile --manifest-path "$vendor_tmp/mysql_async-0.37.1/Cargo.toml"
cargo test --manifest-path "$vendor_tmp/mysql_async-0.37.1/Cargo.toml" --locked \
  --no-default-features --features minimal-rust,rustls-tls,ring \
  tls_upgrade_ -- --nocapture

# This starts pinned MySQL 8.0.36, runs Rust DSN/domain timeout/cancellation
# and TLS checks, then performs Go -> Rust -> Go persistence verification.
python3 "$repo_root/tests/run-matching-mysql-interop.py"
