# Semaphore candidate CI scope — 2026-10-30

Authoritative Actions inspection and downloaded `rust-ci-logs` artifact:
https://github.com/auenkr/cadence-rust/actions/runs/40912963402

- Conclusion success; Actions head and artifact commit both
  `f0e876398862bba9d8cc6d663a46d424474ab7ce`.
- Clippy exit0; test exit0; selected fmt exit0; advisory workspace format exit1.
- Actual test log runs **cadence-persistence**, 344 passed, 13 ignored;
  another harness reports 0 passed/1 ignored. It does not establish Matching
  runtime tests or typed transport propagation.
- Fresh-final-target step skipped, Rust cache step succeeded; recorded target
  `/home/runner/work/cadence-rust/cadence-rust/rust/target`.
- Not fresh workspace acceptance and not mixed-cluster evidence.
- SDK compile, UUID/source-domain oracles and live metric comparison skipped.

Full small artifact retained outside repo at
`/mnt/storage/service/ci-evidence/matching-semaphore-40912963402/`.
The worker reviewing this source must still validate Matching-level changes;
do not promote this focused persistence pass to complete candidate coverage.
