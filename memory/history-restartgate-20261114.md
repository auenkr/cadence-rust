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

## history-soak-tail follow-up (2026-11-14)

Merged this branch into history-soak-tail from master 05fb1e57c. The gate now
builds and executes release Rust binaries, as the preceding soak audit found
debug builds can saturate the runner and invalidate latency/recovery evidence.
No scenario or success criterion removed. MySQL/PostgreSQL/Cassandra runs will
be recorded below after child-completion investigation and fresh Rust tests.

| run URL | head | lane | result |
|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/43561066779 | a79cb0741 | mysql,1500s | SUCCESS,4500/4500,GATE SUMMARY failed: [] |
| https://github.com/auenkr/cadence-rust/actions/runs/43564455423 | ad45bd03e | postgres,1500s | FAILURE:4500/4500 complete; both SIGTERM stops exceeded60s |
| pending | pending | cassandra | not dispatched |

Soak predecessor now passes all4800 workflows and RSS on e2167b61a (run43558072143). Cancellation-safe cache lease fixes uncommitted child removal retained across RPC deadline cancellation; Go deferred release clears before unlock. MySQL recovery gate dispatched after soak completed.

2026-11-14T05:47Z checkpoint: MySQL run43561066779 completed fresh release build and began fault/load phase at05:35:56Z; still running. No final outcome yet; PostgreSQL/Cassandra remain undispatched so recovery gates run sequentially.

MySQL retained evidence: memory/evidence/history-soak-tail-20261114/restart-mysql/. All forced/graceful History restarts,30s DB pause,Matching restart and liveness/completion assertions passed; zero failed workflows,4500/4500 complete,p50 4109ms. Fresh binary SHA256 and source head included.

2026-11-14T06:30Z checkpoint: PostgreSQL run43564455423 completed release build and is executing its1500s fault/load phase; MySQL acceptance passed, Cassandra next. Rust implementation unchanged since e2167b61a and clean workspace run43559207202.

PostgreSQL run43564455423 did not pass: all4500 workflows complete,zero failures,
but both s2 graceful-stop assertions timed out at60s. Preserved evidence under
restart-postgres-before/. Inspection found lost shutdown notification when a
processor monitor/WRR dispatcher subscribes after stop (existing tests themselves
took60s). Go closed shutdown channels are sticky; Rust watch.send with no receivers
loses the value and late subscribe misses changed(). Fix uses send_replace plus
initial value checks, with one-second immediate-stop regression assertions.
This is confirmed static/test behavior, not yet proven the specific live timeout;
launcher stage logs and per-restart shutdown log slices now preserve diagnostics.

| run URL | head | lane | result |
|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/43568009688 |939409f21|clean History Clippy/tests|SUCCESS,903 tests in3.50s; immediate-stop tests now bounded1s|
| https://github.com/auenkr/cadence-rust/actions/runs/43568012383 |939409f21|postgres1500s rerun|FAILURE, one SIGTERM timeout;4499/4500 complete|
| https://github.com/auenkr/cadence-rust/actions/runs/43568781798 |939409f21|fresh workspace Clippy/tests|SUCCESS|

Diagnostic rerun43568012383 shows both launchers reached History shutdown complete within milliseconds; one process still failed to exit60s later. This rules out task processor stop as the whole live-shutdown cause. The sticky-watch fix remains valid (bounded tests now3.50s vs61s), but native stack capture is needed to distinguish local destructor blocking from Tokio runtime teardown. One workflow child activity timed out around DB pause (history shows ActivityTaskStarted then TimedOut, not missing child notification); preserve that as a separate recovery finding.
