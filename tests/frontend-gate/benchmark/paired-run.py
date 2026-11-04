#!/usr/bin/env python3
"""Run alternating, fresh-database Go/Rust frontend+matching benchmark pairs."""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import statistics
import subprocess
import threading
import time


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "out" / "paired-benchmark"
COMPOSE = ROOT / "tests/docker-drop-in/docker-compose.split.yml"
PROJECT = "cadence-fm-benchmark"
EVENTS = []


def run(args, name, env=None, timeout=None, check=True):
    log = OUT / f"{name}.log"
    command = [str(x) for x in args]
    started = time.time()
    with log.open("w") as stream:
        result = subprocess.run(command, cwd=ROOT, env=env, stdout=stream,
                                stderr=subprocess.STDOUT, timeout=timeout)
    event = {"name": name, "command": shlex.join(command),
             "exit_code": result.returncode, "log": str(log.relative_to(ROOT)),
             "started_unix": started, "elapsed_seconds": time.time() - started}
    EVENTS.append(event)
    persist_events()
    if check and result.returncode:
        raise RuntimeError(f"{name} exited {result.returncode}; see {log}")
    return result.returncode


def persist_events():
    (OUT / "commands.json").write_text(json.dumps(EVENTS, indent=2) + "\n")


def write_summary(repetitions, concurrency):
    summary = {"interpretation": "descriptive medians; no speedup claim implied",
               "runs": [], "medians": {}}
    for arm in ("go", "rust"):
        for conc in concurrency:
            rows = []
            for rep in range(repetitions):
                path = OUT / f"result-{rep:02d}-{arm}-c{conc}.json"
                row = json.loads(path.read_text())
                rows.append({"rep": rep, "arm": arm, "concurrency": conc,
                             "throughput": row["successful_workflows_per_second"],
                             "latency_ms": row["latency_ms"]})
                summary["runs"].append({"file": str(path.relative_to(ROOT)), **rows[-1]})
            summary["medians"][f"{arm}-c{conc}"] = {
                "successful_workflows_per_second": statistics.median(r["throughput"] for r in rows),
                "latency_ms": {p: statistics.median(r["latency_ms"][p] for r in rows)
                               for p in ("p50", "p95", "p99")}}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


def service_samples(stop, arm, rep, concurrency):
    totals = {}
    prior = {}
    peaks = {}
    last_time = time.monotonic()
    while not stop.is_set():
        proc = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            cwd=ROOT, capture_output=True, text=True)
        now = time.monotonic()
        delta = max(0.0, now - last_time)
        last_time = now
        if proc.returncode == 0:
            for line in proc.stdout.splitlines():
                try:
                    row = json.loads(line)
                    name = row.get("Name", "")
                    if not name.startswith(PROJECT + "-"):
                        continue
                    service = name[len(PROJECT) + 1:].split("-")[0]
                    cpu = float(row.get("CPUPerc", "0%").rstrip("%")) / 100.0
                    used = row.get("MemUsage", "0B / 0B").split(" / ")[0]
                    match = re.fullmatch(r"([0-9.]+)(B|KiB|MiB|GiB)", used)
                    if not match:
                        continue
                    amount = float(match.group(1))
                    scale = {"B": 1, "KiB": 1024, "MiB": 1024**2,
                             "GiB": 1024**3}[match.group(2)]
                    memory = amount * scale
                    totals[service] = totals.get(service, 0.0) + cpu * delta
                    peaks[service] = max(peaks.get(service, 0), memory)
                except (ValueError, KeyError, IndexError, json.JSONDecodeError):
                    pass
        stop.wait(1)
    path = OUT / f"resources-{rep:02d}-{arm}-c{concurrency}.json"
    path.write_text(json.dumps({"arm": arm, "rep": rep,
                                "concurrency": concurrency,
                                "sampled_cpu_seconds_estimate": totals,
                                "sampled_peak_rss_bytes": peaks,
                                "method": "integrated docker stats CPU percentage; maximum sampled working-set RSS"}, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--concurrency", default="1,16,64")
    args = parser.parse_args()
    if args.requests < 1 or args.warmup < 0 or args.repetitions < 1:
        parser.error("requests/repetitions must be positive and warmup nonnegative")
    concurrency = [int(value) for value in args.concurrency.split(",")]
    if not concurrency or any(value < 1 for value in concurrency):
        parser.error("concurrency must be a comma separated list of positive integers")
    OUT.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["COMPOSE_FILE"] = str(COMPOSE)
    base_env = dict(env)
    (OUT / "source.txt").write_text(
        f"head={subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip()}\n"
        f"go_tree={subprocess.check_output(['git','rev-parse','HEAD:go'], cwd=ROOT, text=True).strip()}\n"
        f"rust_tree={subprocess.check_output(['git','rev-parse','HEAD:rust'], cwd=ROOT, text=True).strip()}\n"
        f"driver_tree={subprocess.check_output(['git','rev-parse','HEAD:tests/frontend-gate/benchmark'], cwd=ROOT, text=True).strip()}\n"
        f"go_image={subprocess.check_output(['docker','image','inspect','cadence-go:ci','--format','{{.Id}}'], text=True).strip()}\n"
        f"rust_image={subprocess.check_output(['docker','image','inspect','cadence-rust:ci','--format','{{.Id}}'], text=True).strip()}\n")
    (OUT / "host.json").write_text(json.dumps({
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "github_runner": os.environ.get("ImageOS"),
        "requests": args.requests, "warmup": args.warmup,
        "repetitions": args.repetitions, "payload_bytes": args.payload_bytes,
        "concurrency": concurrency,
        "order": "alternates by repetition; each arm gets a new database",
        "database": "mysql:8.0", "history": "Go image",
        "system_worker": "Go image", "transport": "TChannel",
        "frontend_matching_rust_image": "cadence-rust:ci",
        "cpu_memory_limits": "Compose file and runner defaults; DB capped at 1.5 CPU/1800 MiB",
    }, indent=2) + "\n")
    try:
        for rep in range(args.repetitions):
            order = ["go", "rust"] if rep % 2 == 0 else ["rust", "go"]
            for arm in order:
                # Recreate the entire project, including MySQL state, for every arm.
                run(["docker", "compose", "-p", PROJECT, "down", "-v", "--remove-orphans"],
                    f"down-{rep:02d}-{arm}", env=base_env, check=False)
                arm_env = dict(base_env)
                if arm == "rust":
                    arm_env["CADENCE_IMAGE_FRONTEND"] = "cadence-rust:ci"
                    arm_env["CADENCE_IMAGE_MATCHING"] = "cadence-rust:ci"
                (OUT / f"compose-{rep:02d}-{arm}.yaml").write_text(
                    subprocess.check_output(["docker", "compose", "-p", PROJECT,
                        "config"], cwd=ROOT, env=arm_env, text=True))
                run(["docker", "compose", "-p", PROJECT, "up", "-d"],
                    f"up-{rep:02d}-{arm}", env=arm_env, timeout=600)
                ready_env = dict(arm_env)
                ready = False
                for attempt in range(60):
                    if subprocess.run(["bash", "-lc", "echo > /dev/tcp/127.0.0.1/7933"],
                                      cwd=ROOT, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL).returncode == 0:
                        ready = True
                        break
                    time.sleep(5)
                if not ready:
                    run(["docker", "compose", "-p", PROJECT, "logs", "--no-color"],
                        f"not-ready-{rep:02d}-{arm}", env=arm_env, check=False)
                    raise RuntimeError(f"frontend did not become ready for {arm} rep {rep}")
                # Assert only Frontend/Matching image identities vary; History and
                # system Worker stay on Go for both arms.
                expected = {"frontend": "cadence-rust:ci" if arm == "rust" else "cadence-go:ci",
                            "matching": "cadence-rust:ci" if arm == "rust" else "cadence-go:ci",
                            "history": "cadence-go:ci", "worker": "cadence-go:ci"}
                for service, expected_image in expected.items():
                    container = subprocess.check_output(["docker", "compose", "-p", PROJECT,
                        "ps", "-q", service], cwd=ROOT, env=arm_env, text=True).strip()
                    actual = subprocess.check_output(["docker", "inspect", "--format",
                        "{{.Config.Image}}", container], cwd=ROOT, text=True).strip()
                    if actual != expected_image:
                        raise RuntimeError(f"{arm} arm {service} uses {actual}, expected {expected_image}")
                run([str(ROOT / "out/cadence-cli"), "--address", "127.0.0.1:7833",
                     "--transport", "grpc", "--domain", "benchmark", "domain", "register",
                     "--rd", "1"], f"domain-{rep:02d}-{arm}", env=arm_env,
                    timeout=120)
                time.sleep(5)
                for conc in concurrency:
                    result = OUT / f"result-{rep:02d}-{arm}-c{conc}.json"
                    stop = threading.Event()
                    monitor = threading.Thread(target=service_samples,
                        args=(stop, arm, rep, conc), daemon=True)
                    monitor.start()
                    try:
                        run([str(ROOT / "out/cadence-benchmark"), "-endpoint", "127.0.0.1:7933",
                             "-domain", "benchmark", "-label", arm,
                             "-requests", str(args.requests), "-warmup", str(args.warmup),
                             "-concurrency", str(conc), "-payload-bytes", str(args.payload_bytes),
                             "-output", str(result)], f"run-{rep:02d}-{arm}-c{conc}",
                            env=arm_env, timeout=1800)
                    finally:
                        stop.set()
                        monitor.join(timeout=10)
                run(["docker", "compose", "-p", PROJECT, "logs", "--no-color"],
                    f"services-{rep:02d}-{arm}", env=arm_env, check=False)
                run(["docker", "compose", "-p", PROJECT, "down", "-v", "--remove-orphans"],
                    f"down-after-{rep:02d}-{arm}", env=arm_env, check=False)
        write_summary(args.repetitions, concurrency)
    finally:
        run(["docker", "compose", "-p", PROJECT, "logs", "--no-color"],
            "final-compose-logs", env=base_env, check=False)
        run(["docker", "compose", "-p", PROJECT, "down", "-v", "--remove-orphans"],
            "cleanup", env=base_env, check=False)
        persist_events()


if __name__ == "__main__":
    main()
