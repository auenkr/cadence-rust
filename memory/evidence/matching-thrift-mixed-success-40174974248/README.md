# Thrift/TChannel mixed lane run 40174974248 — SUCCESS

- Run: https://github.com/auenkr/cadence-rust/actions/runs/40174974248
- Pin: `0bef6d183716ef32ae92fd3a67a3d611a4389972` (`t3/matching-validation-thrift-pacing-20261025`), commit.txt
- Rust Matching binary SHA256 `826b59498f2dd04dea3e72aa69714b45d6fe4dc6563429dde70c4b6308eda115`
- Exits: build 0, gate 0, lane 0, run 0, probeExitCode 0.
- Topology (results.json): original Go Frontend/History/Worker, Rust Matching via
  production Ringpop; `goMatchingStarted:false`; `--matching-transport thrift
  --history-transport tchannel`, MySQL, restart + failure recovery + idle check +
  AutoConfigHint. `acceptanceEligible:false` (emitted label, preserved).
- Covered: SDK workflow/activity replay and restart recovery over Thrift
  (restart-sdk.log), backlog + task GC watermarks (taskGarbageCollection:
  backlog/main queues remainingTaskRows 0, ackLevel/observedMaxTaskId/range blocks
  recorded in results.json), pacing assertion passed, Go services alive after probe,
  MySQL container cleanup recorded.
- Hashes: results.json 61fbac06…5629, probe.log f2a319fa…9fcc, server.log 53915ffb…e565
  (server.log and cadence-server binary not committed; kept in
  `/mnt/storage/service/ci-evidence/run-40174974248/`).
- Limits: `paced-failure-trace.json` is written only on failure and is absent. The
  pacing pass does NOT explain or fix the original failure of run 40174280100
  (same Rust tree, same lane): the cause is unresolved and a flaky/timing source is
  unexcluded. Do not cite this as a fix.
- Source: `rust/` is identical to `29bdd2b04` (empty diff). Past PostgreSQL
  40173134481 / MySQL 40173683827 remain bound to `29bdd`, not relabeled.
