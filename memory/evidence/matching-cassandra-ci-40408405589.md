# Cassandra UUID normalization focused CI

Run: https://github.com/auenkr/cadence-rust/actions/runs/40408405589
Conclusion: success. Artifact `commit.txt` matches implementation
`ca11f306ceffaf37630e291672a1da41a525bbda`.

Downloaded `rust-ci-logs` to `/mnt/storage/service/ci-evidence/40408405589`.
Recorded `target-dir.txt` is
`/home/runner/work/cadence-rust/cadence-rust/rust/target`, not the dedicated fresh
final target. This run is focused evidence only; do not characterize it as a
fresh-target final check.

Artifact exit files: clippy=0, test=0, fmt=0,
matching-runtime-format=0. Advisory workspace-format=1. The normalization test
`matching_domain_cache::tests::cassandra_uuid_spellings_resolve_to_the_same_cached_domain`
passed; Matching library reported 725 passed, 0 failed, 3 ignored. Integration
suites also passed. No production binary hash or mixed-cluster outcome is
established by this crate run.

SHA256 of retained `test.log`:
`536a45b52ab1c7e5ad52fe3974c14889fc22374d23c20f7e6086640ce64ab8e5`.

Fresh whole-workspace candidate run 40409014196 targets the combined source
`91c481a612eac48f786f208f4c2f4193864ed966` and remained active at this check.
No local Cargo, Go or rustfmt command ran.
