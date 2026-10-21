#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo/go"
go test ./common/dynamicconfig -run '^TestFileBasedOracle' -count=1 -timeout=30s
