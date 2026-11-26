#!/usr/bin/env python3
"""Paired Go/Rust cold-start and abrupt-service-loss recovery timings (sub-second resolution).

Per repetition and arm (alternating order, fresh MySQL each time):
  cold_fresh    compose up of the whole cluster (MySQL + schema setup + 4 services) -> first SDK completion
  kill_<svc>    `compose kill -s KILL <svc>` immediately followed by `compose start <svc>` for
                frontend, matching, history, one at a time -> first completion, then 10 consecutive fast ones
  cold_restart  SIGKILL all four Cadence services, then start them together (DB warm) -> first completion
The SDK driver (tests/frontend-gate/recovery/main.go) starts one echo workflow every 100 ms and logs
wall-clock start/end per workflow; a workflow that never completes within its deadline is "lost".
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "out" / "recovery"
COMPOSE_FILES = {"mysql": "docker-compose.split.yml", "cassandra": "docker-compose.cassandra.yml"}
PROJECT = "cadence-recovery"
SERVICES = ("history", "matching", "worker", "frontend")
STABLE_N, STABLE_MAX_LAT_NS = 10, 2_000_000_000


def sh(args, env, name, check=True, timeout=600):
    with (OUT / f"{name}.log").open("w") as s:
        rc = subprocess.run([str(a) for a in args], cwd=ROOT, env=env, stdout=s, stderr=subprocess.STDOUT, timeout=timeout).returncode
    if check and rc:
        raise RuntimeError(f"{name} exit {rc}")
    return rc


def load(path):
    rows = []
    try:
        for line in Path(path).read_text().splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    except FileNotFoundError:
        pass
    return rows


def analyse(rows, t0, fresh_filter):
    """Times in ms relative to t0 (ns). Returns first completion, first completion of a new workflow, stable."""
    ok = [r for r in rows if r["ok"]]
    after = [r for r in ok if r["end_ns"] > t0]
    first_any = min((r["end_ns"] for r in after), default=None)
    new = sorted((r for r in rows if r["start_ns"] >= t0 or not fresh_filter), key=lambda r: r["seq"])
    first_new = min((r["end_ns"] for r in new if r["ok"] and r["end_ns"] > t0), default=None)
    stable = None
    for i in range(len(new) - STABLE_N + 1):
        w = new[i:i + STABLE_N]
        if all(r["ok"] and r["end_ns"] > t0 and r["end_ns"] - r["start_ns"] < STABLE_MAX_LAT_NS for r in w):
            stable = max(r["end_ns"] for r in w)
            break
    ms = lambda v: None if v is None else (v - t0) / 1e6
    return {"first_completion_ms": ms(first_any), "first_new_completion_ms": ms(first_new), "stable_ms": ms(stable)}


def wait_stable(samples, t0, fresh_filter, limit):
    end = time.time() + limit
    while time.time() < end:
        if analyse(load(samples), t0, fresh_filter)["stable_ms"] is not None:
            return True
        time.sleep(0.5)
    return False


def first_decision_losses(tag):
    """Lost workflows whose dumped history (driver -lost-dir) never completed a decision task, or could not be read."""
    d = OUT / f"lost-{tag}"
    n = 0
    for f in d.glob("*.json") if d.is_dir() else []:
        if (d / (f.stem + ".err")).exists():
            n += 1
            continue
        try:
            evs = json.loads(f.read_text()) or []
        except ValueError:
            n += 1
            continue
        if not any(e.get("eventType") in ("DecisionTaskCompleted", 4) for e in evs):
            n += 1
    return n


def schema_version(compose, env, name):
    """Cassandra schema_version rows (both keyspaces), via cqlsh in the DB container."""
    q = "select keyspace_name, curr_version from cadence.schema_version; select keyspace_name, curr_version from cadence_visibility.schema_version;"
    r = subprocess.run(compose("exec", "-T", "cassandra", "cqlsh", "-e", q), cwd=ROOT, env=env, capture_output=True, text=True)
    out = r.stdout.strip()
    (OUT / f"schema-version-{name}.txt").write_text(out + "\n" + r.stderr)
    return out


def pct(v, p):
    s = sorted(v)
    return s[min(len(s) - 1, max(0, int(round(p / 100 * len(s) + 0.5)) - 1))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repetitions", type=int, default=5)
    ap.add_argument("--rust-services", default="frontend,matching,history")
    ap.add_argument("--scenarios", default="cold_fresh,kill_frontend,kill_matching,kill_history,cold_restart")
    ap.add_argument("--database", default="mysql", choices=("mysql", "cassandra"))
    ap.add_argument("--limit", type=int, default=120, help="seconds to wait for stable recovery per event")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    rust_services = {x for x in args.rust_services.split(",") if x}
    scenarios = args.scenarios.split(",")
    env = os.environ.copy()
    ov = OUT / "resource-limits.yaml"
    ov.write_text("services:\n" + "".join(
        f"  {n}:\n    mem_limit: 1400m\n    memswap_limit: 1400m\n    cpus: 1.0\n    pids_limit: 2048\n"
        for n in ("frontend", "matching", "history", "worker")))
    compose_file = ROOT / "tests/docker-drop-in" / COMPOSE_FILES[args.database]
    env["COMPOSE_FILE"] = os.pathsep.join((str(compose_file), str(ov)))
    for s in ("FRONTEND", "MATCHING", "HISTORY", "WORKER"):
        env[f"CADENCE_IMAGE_{s}"] = "cadence-go:ci"
    base_env = dict(env)
    compose = lambda *a: ["docker", "compose", "-p", PROJECT, *a]
    results = []
    try:
        for rep in range(args.repetitions):
            for arm in (["go", "rust"] if rep % 2 == 0 else ["rust", "go"]):
                tag = f"{rep:02d}-{arm}"
                arm_env = dict(base_env)
                if arm == "rust":
                    for s in rust_services:
                        arm_env[f"CADENCE_IMAGE_{s.upper()}"] = "cadence-rust:ci"
                sh(compose("down", "-v", "--remove-orphans"), base_env, f"down-{tag}", check=False)
                samples, stop, ready = OUT / f"samples-{tag}.jsonl", OUT / f"stop-{tag}", OUT / f"ready-{tag}"
                for p in (samples, stop, ready):
                    p.unlink(missing_ok=True)
                driver = subprocess.Popen([str(ROOT / "out/recovery-driver"), "-output", str(samples), "-stop-file", str(stop),
                                           "-ready-file", str(ready),
                                           "-lost-dir", str(OUT / f"lost-{tag}")], cwd=ROOT, stdout=(OUT / f"driver-{tag}.log").open("w"), stderr=subprocess.STDOUT)
                events = []
                baseline_sv = None
                try:
                    if "cold_fresh" not in scenarios:
                        # Untimed bring-up so kill/restart scenarios have a running cluster.
                        sh(compose("up", "-d"), arm_env, f"up-{tag}", timeout=900)
                        t_up = time.time_ns()
                        if not wait_stable(samples, t_up, False, 300):
                            raise RuntimeError("cluster never became stable after untimed bring-up")
                        time.sleep(3)
                    if args.database == "cassandra":
                        baseline_sv = schema_version(compose, arm_env, f"{tag}-baseline")
                    for sc in scenarios:
                        ev = {"scenario": sc, "arm": arm, "rep": rep}
                        if sc == "cold_fresh":
                            ev["t0_ns"] = time.time_ns()
                            sh(compose("up", "-d"), arm_env, f"up-{tag}", timeout=900)
                            fresh = False
                        elif sc == "cold_restart":
                            sh(compose("kill", "-s", "KILL", *SERVICES), arm_env, f"killall-{tag}")
                            time.sleep(2)
                            ev["t0_ns"] = time.time_ns()
                            sh(compose("start", *SERVICES), arm_env, f"startall-{tag}")
                            fresh = True
                        elif sc.startswith("kill_"):
                            svc = sc[5:]
                            ev["t0_ns"] = time.time_ns()
                            sh(compose("kill", "-s", "KILL", svc), arm_env, f"kill-{svc}-{tag}")
                            ev["killed_ns"] = time.time_ns()
                            sh(compose("start", svc), arm_env, f"start-{svc}-{tag}")
                            ev["restarted_ns"] = time.time_ns()
                            fresh = True
                        else:
                            raise RuntimeError(f"unknown scenario {sc}")
                        ev["stable_reached"] = wait_stable(samples, ev["t0_ns"], fresh, args.limit)
                        ev.update(analyse(load(samples), ev["t0_ns"], fresh))
                        if baseline_sv is not None:
                            ev["schema_version"] = schema_version(compose, arm_env, f"{tag}-{sc}")
                            ev["schema_version_intact"] = ev["schema_version"] == baseline_sv and bool(baseline_sv)
                        ev["restart_cmd_ms"] = (ev.get("restarted_ns", ev["t0_ns"]) - ev["t0_ns"]) / 1e6
                        events.append(ev)
                        time.sleep(3)
                finally:
                    stop.write_text("stop\n")
                    try:
                        driver.wait(timeout=240)
                    except subprocess.TimeoutExpired:
                        driver.kill()
                    sh(compose("logs", "--no-color"), arm_env, f"services-{tag}", check=False)
                    # Container image identities and binary hashes of the services under test.
                    ids = {}
                    for s in ("frontend", "matching", "history", "worker"):
                        cid = subprocess.run(compose("ps", "-aq", s), cwd=ROOT, env=arm_env, capture_output=True, text=True).stdout.strip()
                        if cid:
                            ids[s] = subprocess.run(["docker", "inspect", "--format", "{{.Config.Image}} {{.Image}}", cid], capture_output=True, text=True).stdout.strip()
                    (OUT / f"identities-{tag}.json").write_text(json.dumps(ids, indent=2) + "\n")
                    sh(compose("down", "-v", "--remove-orphans"), base_env, f"down-after-{tag}", check=False)
                rows = load(samples)
                lost = [r for r in rows if not r["ok"]]
                summary = {"rep": rep, "arm": arm, "workflows": len(rows), "completed": len(rows) - len(lost),
                           "lost": len(lost), "first_decision_lost": first_decision_losses(tag), "lost_samples": [{"wf": r["wf"], "err": r.get("err", "")[:200]} for r in lost[:5]],
                           "driver_exit": driver.returncode, "events": events}
                results.append(summary)
                (OUT / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                print(f"[progress] rep {rep + 1}/{args.repetitions} {arm}: " + ", ".join(
                    f"{e['scenario']}={None if e['first_completion_ms'] is None else round(e['first_completion_ms'])}ms" for e in events)
                    + f" lost={len(lost)}/{len(rows)}", flush=True)
    finally:
        sh(compose("down", "-v", "--remove-orphans"), base_env, "cleanup", check=False)

    # Aggregate
    lines = ["# Recovery timing (ms, process kill/launch -> first successful SDK workflow completion)", "",
             "| scenario | arm | n | first completion median | p95 | first NEW workflow median | stable (10 consecutive <2s) median | p95 | not stable |",
             "|---|---|---|---|---|---|---|---|---|"]
    for sc in scenarios:
        for arm in ("go", "rust"):
            evs = [e for r in results if r["arm"] == arm for e in r["events"] if e["scenario"] == sc]
            f = [e["first_completion_ms"] for e in evs if e["first_completion_ms"] is not None]
            n = [e["first_new_completion_ms"] for e in evs if e["first_new_completion_ms"] is not None]
            st = [e["stable_ms"] for e in evs if e["stable_ms"] is not None]
            fmt = lambda v, p: "n/a" if not v else (f"{statistics.median(v):.0f}" if p == 50 else f"{pct(v, 95):.0f}")
            lines.append(f"| {sc} | {arm} | {len(evs)} | {fmt(f, 50)} | {fmt(f, 95)} | {fmt(n, 50)} | {fmt(st, 50)} | {fmt(st, 95)} | {len(evs) - len(st)} |")
    lines += ["", "| arm | reps | workflows started | completed | lost |", "|---|---|---|---|---|"]
    for arm in ("go", "rust"):
        rs = [r for r in results if r["arm"] == arm]
        lines.append(f"| {arm} | {len(rs)} | {sum(r['workflows'] for r in rs)} | {sum(r['completed'] for r in rs)} | {sum(r['lost'] for r in rs)} |")
    (OUT / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    # Pass criterion: per database, Rust lost <= Go lost AND zero Rust first-decision-task losses
    # (Go-arm losses must not redden the run); the recovery-stability checks apply to the Rust arm only (Go-arm instability is reported, not gated).
    total = lambda arm, k: sum(r[k] for r in results if r["arm"] == arm)
    bad = []
    if total("rust", "lost") > total("go", "lost"):
        bad.append(f"rust lost {total('rust', 'lost')} > go lost {total('go', 'lost')}")
    if total("rust", "first_decision_lost"):
        bad.append(f"rust first-decision-task losses: {total('rust', 'first_decision_lost')}")
    if any(not e["stable_reached"] or e.get("schema_version_intact") is False for r in results if r["arm"] == "rust" for e in r["events"]):
        bad.append("rust stability/schema check failed")
    print(f"[verdict] {args.database} go_lost={total('go', 'lost')} rust_lost={total('rust', 'lost')} "
          f"rust_first_decision_lost={total('rust', 'first_decision_lost')} -> {'FAIL: ' + '; '.join(bad) if bad else 'PASS'}")
    if bad:
        sys.exit(1)

if __name__ == "__main__":
    main()
