#!/usr/bin/env bash
set -euo pipefail

# Mixed-language wire gate. The ignored Rust test launches the pinned Go
# shard-manager fixture itself, so no Go shard-distributor service is needed.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}/rust"
cargo test --locked -p cadence-server --test shard_distributor_go_interop -- --ignored --nocapture
