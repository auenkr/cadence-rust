#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
go_test_file="$root_dir/go/common/cache/domain_cache_oracle_test.go"
oracle_source="$root_dir/tests/matching-domain-cache-oracle/domain_cache_oracle_test.go"

if [[ -e "$go_test_file" ]]; then
  echo "refusing to overwrite existing Go test: $go_test_file" >&2
  exit 1
fi

cleanup() {
  rm -f "$go_test_file"
}
trap cleanup EXIT

cp "$oracle_source" "$go_test_file"
cd "$root_dir/go"
go test ./common/cache -run '^TestDomainCacheSuite/TestDomainCacheOracleTrace$' -count=1 -v
