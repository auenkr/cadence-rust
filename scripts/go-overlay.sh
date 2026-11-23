#!/usr/bin/env bash
# Copy go-overlay/ files into the upstream go/ submodule and apply go.mod/go.sum patch (idempotent).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
[ -f "$ROOT/go/go.mod" ] || { echo "go/ submodule missing; run git submodule update --init --recursive" >&2; exit 1; }
cd "$ROOT/go-overlay"
find . -type f ! -name go-mod.patch -print0 | while IFS= read -r -d '' f; do
  mkdir -p "$ROOT/go/$(dirname "$f")"; cp "$f" "$ROOT/go/$f"
done
cd "$ROOT/go"
if git apply --check -R ../go-overlay/go-mod.patch 2>/dev/null; then
  echo "go-mod.patch already applied"
else
  git apply ../go-overlay/go-mod.patch
fi
