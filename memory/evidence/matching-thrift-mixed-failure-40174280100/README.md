# Thrift/TChannel mixed lane run 40174280100 — FAILURE (preserved, not diagnosed here)

- Run: https://github.com/auenkr/cadence-rust/actions/runs/40174280100
- Workflow: matching-production-validation.yml `lane=thrift`
- Ref `t3/matching-validation-history-ring-20261025`, head SHA
  `29bdd2b04b573309112286c57f1c347f0ba0b60c` (commit.txt)
- Conclusion: failure (job `gate`, ~07:50–07:54Z); `gate.exit` and probe failed.
- Rust Matching binary (CI target `/tmp/cargo-target-final-40174280100`) SHA256
  `f9e1735c2f7c01c8bf7fb899512f95c2ea9b0df96e8cdec382c66abd6675cd1b`
- Topology: original Go Frontend/History/Worker + Rust Matching (Thrift/TChannel), MySQL.
- Failure: probe `tests/matching-service-compat.cjs:1242`:
  `AssertionError: paced activity dispatch was not rate limited: [8,6,7]`
  (after `pacedListOwner` agree=true, 127.0.0.1:11556). The pre-restart SDK
  workflow resumed and completed (restart-sdk.log) before the failure.
- Other observations (not root-caused): rust-server-before-restart.log shows
  "Matching task list lost ownership during shutdown: range ID was 2 ... 1".
  Go frontend server.log (sha256 2ef4295f…5c7c, not committed, 939KB) has
  "Not enough hosts" PollForDecisionTask errors for system task lists.
- Status: root cause is owned by another worker; no fix verified. Thrift lane
  is NOT green. Raw artifact (incl. cadence-server binary, server.log) kept
  outside git: `/mnt/storage/service/ci-evidence/run-40174280100/`. GitHub
  artifact download hit a /tmp quota; the artifact zip is retained 3 days only.
