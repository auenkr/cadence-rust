# Matching membership parity validation (2026-10-23)
Source SHA (tree validated, before this evidence commit): 90c3fcc22b5d0291a648fac0f0afd4fda77c53de (cc459372f + merge of origin/master 67f95cb78)
Commands (run in rust/, via /mnt/storage/service/bin/cargo-safe, shared target dir, not the clean final target):
- cargo-safe check -p cadence-service-core -p cadence-matching --all-targets  -> exit 0
- cargo-safe clippy --workspace --all-targets -- -D warnings                  -> exit 0
- cargo-safe test --workspace                                                 -> exit 0 (938 passed, 0 failed)
Golden vectors: ring_vectors_main.go.txt / ring_vectors.out (from Go hashring).
No live multi-node gate was run; see "Needs gate" in memory/matching-membership-audit-20261023.md.
