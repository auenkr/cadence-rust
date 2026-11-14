# History restart/recovery gate (2026-11-14)

Branch `history-restartgate` (from origin/master a5b151f44). Harness only, no Rust changes.

## Coverage of existing gates
- history-soak: one mid-soak kill -9 + restart of a single History host (soak itself currently stalls, see below).
- history-swap / history-mixed-gate two_history: History kill/restart during SDK churn only.
- NOT covered before: graceful (SIGTERM) restart of every host in turn, DB outage, Matching kill under History load, repeated kill -9 of every host.

## Added
- `tests/gates/run-history-restartgate.py` (derived from run-history-soak.py: 2 Rust History + Rust Matching + Rust Frontend, Go Worker,
  tests/history-soak SDK driver running throughout). After 45 s of steady load, with a settle period after each fault:
  s1 SIGKILL each History host in turn + restart; s2 SIGTERM each History host in turn + restart; s3 `docker pause` DB container for
  RG_DB_PAUSE (30 s) then unpause; s4 SIGKILL Rust Matching, 10 s later restart. Assertions: each fault step, load still running after each,
  driver exit 0, zero failed workflows, started == completed, no panics, services alive.
- `.github/workflows/history-restartgate.yml` (ubuntu-latest, 90 min, retention-days 3, per-run concurrency group, dispatch inputs database/seconds;
  push trigger on this branch runs the 3-database matrix since dispatch needs the file on master).

## Results (head 1 of branch, run https://github.com/auenkr/cadence-rust/actions/runs/43543900094, 25 min load at 3 wf/s)
| scenario | database | run URL | result |
|---|---|---|---|
| s1-s4 (all) | mysql | https://github.com/auenkr/cadence-rust/actions/runs/43543900094 (job 132290506272) | FAIL: only 139/4500 completed, 4361 FAIL lines (driver exit 1) |
| s1-s4 (all) | cassandra | https://github.com/auenkr/cadence-rust/actions/runs/43543900094 (job 132290506510) | FAIL: 144/4500 completed |
| s1-s4 (all) | postgres | https://github.com/auenkr/cadence-rust/actions/runs/43543900094 (job 132290506530) | FAIL: 161/4500 completed; SIGTERM of both History hosts did not exit within 60 s (harness fell back to SIGKILL) |

Fault injection steps (kill/restart/pause/matching kill, restarted-listening) all ran; the failing steps are completion ones.

## Diagnosis
Identical signature to the known soak stall (memory/history-soak-20261113.md): only ~150 workflows ever complete, the rest sit at query state "init"
(`query_init_workflows` in driver STATS grows from the first fault), on all three databases, before any fault is attributable. Per the task rules this is
owned by worker `history-soakstall` (origin/history-soakstall) and was NOT fixed here. The postgres SIGTERM hang is likely a consequence (shutdown blocked on
retrying stuck tasks) but is not proven; recheck after the soak stall fix.

## Next
Rebase on master once history-soakstall is merged, re-run `git push` on this branch (or dispatch after the workflow reaches master) for mysql/postgres/cassandra and
fill in pass results. A pass needs success conclusion and `GATE SUMMARY failed: []`. Scenario pass evidence is therefore still outstanding.
