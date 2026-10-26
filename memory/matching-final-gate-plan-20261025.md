# Matching final gate plan (docs only, 2026-10-25)

Nothing here was dispatched. No local Cargo/Go. Base: `t3/matching-integration-20261025`.
Source inputs: `.github/workflows/*`, `tests/gates/*`, and
`memory/matching-post-tls-final-validation-plan-20261025.md` (prior evidence).

## Inventory of gate workflows (all `ubuntu-latest`, 90 min, artifacts 3 days)

| Workflow | Inputs | Topology | Backend / transport | Restart / recovery flags |
|---|---|---|---|---|
| `rust-ci.yml` | `package`, `clean_target`, `format_tls_files` | none (Clippy then tests, Go oracles, metrics compare) | n/a | n/a |
| `matching-production-validation.yml` | `lane` = postgres, mysql-probes, thrift, configstore, shard-distributor | Go Frontend/History/Worker + Rust Matching (`tests/gates/run-{postgres,mysql}-lane.sh`) | PostgreSQL (postgres lane); MySQL (all others); thrift lane = Thrift Matching + TChannel History | every lane: `--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3` |
| `matching-mixed-gate.yml` | `extra_args` (default restart + failure-recovery + idle 3) | MySQL, two Rust Matching nodes | MySQL, gRPC | restart, failure recovery, idle |
| `matching-cassandra-mixed-gate.yml` | `extra_args` (default `--matching-restart --matching-idle-check-seconds 3`) | Go services + Rust Matching, `run-cassandra-lane.sh` | Cassandra | restart, idle; lane itself adds two-node failure recovery |
| `matching-active-offer-gate.yml` | none | Go History + Rust Matching, `run-mysql-lane.sh --probe-active-offer` | MySQL | restart, failure recovery, idle (flags present; inspect emitted scenarios) |
| `matching-production-tls-mixed.yml` | none | Go Frontend/History/Worker + standalone Rust Matching, no Go Matching | MySQL, CA/hostname-verified gRPC TLS | restart and two-instance failure recovery requested; artifact assertions required |
| `matching-http-tls-interop.yml` | none | NOT the full mixed cluster: Go YARPC/Thrift client, Go Ringpop stub + Rust History fixture, `tests/run-matching-thrift-compat.py` | SQLite / embedded | timeout recovery and drain only |

Ignored: `ci-checks`, `breaking_change_reminder`, `fresh-go-deps-check`, `issue-validation`, `semantic-pr` (upstream Go, not gates).

Scripts under `tests/` used by the lanes: `run-go-service-baseline.py` (via `tests/gates/*.sh`, SDK probe `matching-service-compat.cjs`),
`run-matching-production-failure-recovery.py`, `run-matching-thrift-compat.py`, `run-matching-owner-order-oracle.py`.
Standalone and NOT referenced by any workflow: `run-matching-configured-store-validation.py`, `run-matching-cassandra-interop.py`,
`run-matching-mysql-interop.py`, `run-matching-cassandra-validation.sh`, `run-matching-mysql-timeout-validation.sh`. Do not claim them as evidence.

## Serial dispatch order on the final pin

`BRANCH` is a stable branch pinned to `EXPECTED_SHA`. After each dispatch run `gh run view RUN_ID --json headSha,conclusion,url`,
confirm `headSha == EXPECTED_SHA`, download the artifact, record binary SHA256 (`out/binary.sha256`) and exits. Run only one lane at a time
(`gh run list --status in_progress` must be empty first).

1. `gh workflow run rust-ci.yml --ref "$BRANCH" -f clean_target=true` (fresh target, Clippy `-D warnings` then workspace tests; record the advisory workspace-format exit 1 as such).
2. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=postgres`
3. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=mysql-probes`
4. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=thrift`
5. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=configstore`
6. `gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=shard-distributor`
7. `gh workflow run matching-mixed-gate.yml --ref "$BRANCH"` (MySQL two-node; defaults are already right; not in the earlier plan, add it)
8. `gh workflow run matching-cassandra-mixed-gate.yml --ref "$BRANCH"`
9. `gh workflow run matching-active-offer-gate.yml --ref "$BRANCH"` now runs
   `--probe-active-offer --matching-restart --matching-failure-recovery
   --matching-idle-check-seconds 3`; inspect the gate artifacts for the SIGTERM
   drain/restart, SIGKILL recovery, two-node owner takeover and active-offer
   scenario assertions. Its topology remains Go Frontend/History/Worker plus
   Rust Matching, with Go Matching absent.
10. `gh workflow run matching-production-tls-mixed.yml --ref "$BRANCH"` (real mixed topology; inspect CA/hostname verification, both nodes, typed Go ownership oracle, restart/recovery, SDK outcome and exact binary hash)
11. `gh workflow run matching-http-tls-interop.yml --ref "$BRANCH"` (separate HTTP/TLS fixture)

Coverage by requirement: MySQL = 3, 7, 9; PostgreSQL = 2; Cassandra = 8; Thrift/TChannel = 4 (+11 fixture); production gRPC TLS = 10; HTTP/TLS fixture = 11
(plus workspace TLS unit tests in 1); SDK workflows = every mixed lane via the SDK probe.

## Lanes lacking a restart or failure-recovery check

- `matching-active-offer-gate.yml`: current workflow already passes `--matching-restart` and `--matching-failure-recovery`. The earlier missing-flags finding is stale. Verify the active-offer scenario and recovery scenarios actually execute together; flags alone are not runtime evidence. A Luna worker is reviewing substantive coverage.
- `matching-cassandra-mixed-gate.yml`: default args omit `--matching-failure-recovery`; the notes say the lane script adds two-node recovery. Confirm in `out/gate.log` that the recovery scenario actually ran; otherwise pass `-f extra_args="--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3"`.
- `matching-http-tls-interop.yml`: fixture only, no Go History/Frontend/Worker, no process restart or failure recovery of the backend (only handshake-timeout recovery and drain). Not a recovery lane.
- `rust-ci.yml`: unit tests only.
- All five `matching-production-validation.yml` lanes and `matching-mixed-gate.yml` do pass restart and failure recovery. Thrift lane exercises Thrift transport but the Thrift-specific `run-matching-thrift-compat.py` is only run in lane 11.

## Rerun requirements on the final binary

Every prior result is bound to its named SHA (PostgreSQL 40173134481 and MySQL 40173683827 on `29bdd2b04`; Thrift 40174974248 on the pacing pin;
workspace 40168414544 on `fade09c5`). The pin is going to change because two RPC production fixes were still under review, so
rerun ALL eleven steps above on the final pin. Rebinding an older run is allowed only with an empty `git diff OLD NEW -- rust go/idls` and
must be written as an explicit inference, not a new run. Never run on a moved branch head; each run's `out/commit.txt` and `headSha`
must equal `EXPECTED_SHA`, and each mixed lane must have a fresh `binary.sha256`. Thrift original failure 40174280100 (paced dispatch
`[8,6,7]`) has no confirmed root cause; if the lane flakes again keep the failing artifact.

Keep the emitted labels (`preliminary-passed`, `acceptanceEligible:false`) unchanged. Matching remains INCOMPLETE until the parity audit rows and these runs are accepted.

## October 26 production TLS gate status

The production TLS gate is now committed (master01148d6cf) and dispatched as
40364035990. It is not acceptance evidence until actual artifacts are reviewed.
Two source-review fixes scoped Node TLS credentials to Matching only and added
CA-backed hostname-verified TLS to the Go typed ownership-error oracle. The
initial workflow422 parser rejection was fixed by setting CARGO_TARGET_DIR in
a runner step via GITHUB_ENV. Final candidate must include these gate changes
and the reviewed fatal writer lifecycle, then run all lanes on one stable SHA.

## October 26 adaptive probe correction

The proposed persisted-config partition-listing repair `ca64a5a9` was rejected:
Go `getAllPartitions` uses the filtered static write-partition dynamic property,
not persisted adaptive configuration. Do not include that repair in the final pin.
The corrected probe commit `fb5f0ccac3450d549aad16d0c6fdc785302c9b12` adds
the workflow lane named **`adaptive`** (not `adaptive-scaler`). Once reviewed and
integrated, add this serial final-pin gate after the production validation lanes:

```sh
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=adaptive
```

Require persisted adaptive config/version growth, independently Go-shaped static
partition listing, SDK workflow/activity completion on that queue, and completion
of all 24 generated load workflows. Queue completion alone does not prove direct
root/child read routing. Record exact source and binary hashes as for every mixed
lane. This dispatch instruction is a plan, not execution or acceptance evidence.
