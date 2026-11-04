#!/usr/bin/env python3
"""Run alternating, fresh-database Go/Rust frontend+matching benchmark pairs."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "out" / "paired-benchmark"
COMPOSE = ROOT / "tests/docker-drop-in/docker-compose.split.yml"
PROJECT = "cadence-fm-benchmark"
EVENTS = []
sys.path.insert(0, str(ROOT / "tests/benchmark"))
from compare import compare, markdown
from render_report import render_html


def run(args, name, env=None, timeout=None, check=True):
    log = OUT / f"{name}.log"
    command = [str(x) for x in args]
    started = time.time()
    with log.open("w") as stream:
        try:
            status = subprocess.run(command, cwd=ROOT, env=env, stdout=stream,
                                    stderr=subprocess.STDOUT, timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            status = 124
            stream.write(f"\nBenchmark harness timeout after {timeout} seconds\n")
    event = {"name": name, "command": shlex.join(command),
             "exit_code": status, "log": str(log.relative_to(ROOT)),
             "started_unix": started, "elapsed_seconds": time.time() - started}
    EVENTS.append(event)
    persist_events()
    if check and status:
        raise RuntimeError(f"{name} exited {status}; see {log}")
    return status


def persist_events():
    (OUT / "commands.json").write_text(json.dumps(EVENTS, indent=2) + "\n")


def write_summary(repetitions, concurrency):
    metadata = {"host": json.loads((OUT / "host.json").read_text()),
                "sources": (OUT / "source.txt").read_text()}
    for conc in concurrency:
        paths = lambda kind, arm: [OUT / f"{kind}-{rep:02d}-{arm}-c{conc}.json" for rep in range(repetitions)]
        report = compare(paths("result", "go"), paths("result", "rust"),
                         metadata, metadata, paths("resources", "go"), paths("resources", "rust"))
        if report["resources"]["status"] != "comparable":
            raise RuntimeError(f"resources are not comparable: {report['resources']}")
        prefix = OUT / f"comparison-c{conc}"
        prefix.with_suffix(".json").write_text(json.dumps(report, indent=2) + "\n")
        prefix.with_suffix(".md").write_text(markdown(report))
        prefix.with_suffix(".html").write_text(render_html(report))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--concurrency", default="1,16,64")
    args = parser.parse_args()
    if args.requests < 1 or args.warmup < 0 or args.repetitions < 1 or args.payload_bytes < 0:
        parser.error("requests/repetitions must be positive and warmup nonnegative")
    concurrency = [int(value) for value in args.concurrency.split(",")]
    if not concurrency or any(value < 1 for value in concurrency):
        parser.error("concurrency must be a comma separated list of positive integers")
    OUT.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    override = OUT / "resource-limits.yaml"
    override.write_text("services:\n" + "".join(
        f"  {name}:\n    mem_limit: 1400m\n    memswap_limit: 1400m\n    cpus: 1.0\n    pids_limit: 2048\n"
        for name in ("frontend", "matching", "history", "worker")))
    env["COMPOSE_FILE"] = os.pathsep.join((str(COMPOSE), str(override)))
    for service in ("FRONTEND", "MATCHING", "HISTORY", "WORKER"):
        env[f"CADENCE_IMAGE_{service}"] = "cadence-go:ci"
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
        "cpu_memory_limits": "Each service: 1 CPU/1400 MiB, DB: 1.5 CPU/1800 MiB; see resolved compose configs",
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
                containers = []
                identities = {}
                for service, expected_image in expected.items():
                    container = subprocess.check_output(["docker", "compose", "-p", PROJECT,
                        "ps", "-q", service], cwd=ROOT, env=arm_env, text=True).strip()
                    actual = subprocess.check_output(["docker", "inspect", "--format",
                        "{{.Config.Image}}", container], cwd=ROOT, text=True).strip()
                    if actual != expected_image:
                        raise RuntimeError(f"{arm} arm {service} uses {actual}, expected {expected_image}")
                    name = subprocess.check_output(["docker", "inspect", "--format", "{{.Name}}", container], text=True).strip().lstrip("/")
                    containers.append(name)
                    identities[service] = {"container_id": container, "container_name": name, "image": actual}
                    run(["docker", "top", container, "-eo", "pid,comm,args"], f"process-{rep:02d}-{arm}-{service}")
                (OUT / f"identities-{rep:02d}-{arm}.json").write_text(json.dumps(identities, indent=2) + "\n")
                cli = [str(ROOT / "out/cadence-cli"), "--address", "127.0.0.1:7833", "--transport", "grpc", "--domain", "benchmark", "domain"]
                for attempt in range(30):
                    if run(cli + ["register", "--rd", "1"], f"domain-{rep:02d}-{arm}-{attempt}", env=arm_env, timeout=30, check=False) == 0:
                        break
                    if run(cli + ["describe"], f"domain-describe-{rep:02d}-{arm}-{attempt}", env=arm_env, timeout=30, check=False) == 0:
                        break
                    time.sleep(2)
                else:
                    raise RuntimeError(f"domain registration not ready for {arm}")
                for conc in concurrency:
                    result = OUT / f"result-{rep:02d}-{arm}-c{conc}.json"
                    prefix = OUT / f"measurement-{rep:02d}-{arm}-c{conc}"
                    start_marker, end_marker, ready_marker = [str(prefix) + suffix for suffix in (".start", ".end", ".ready")]
                    resources = OUT / f"resources-{rep:02d}-{arm}-c{conc}.json"
                    collector_command = [sys.executable, str(ROOT / "tests/benchmark/collect_resources.py"),
                        "--start-event", start_marker, "--end-event", end_marker, "--ready-event", ready_marker,
                        "--event-timeout", "1800", "--interval", "0.05", "--output", str(resources), "--containers", *containers]
                    collector_log = OUT / f"collector-{rep:02d}-{arm}-c{conc}.log"
                    collector_started = time.time()
                    collector_stream = collector_log.open("w")
                    monitor = subprocess.Popen(collector_command, cwd=ROOT, stdout=collector_stream, stderr=subprocess.STDOUT)
                    try:
                        run([str(ROOT / "out/cadence-benchmark"), "-endpoint", "127.0.0.1:7933",
                             "-domain", "benchmark", "-label", arm,
                             "-requests", str(args.requests), "-warmup", str(args.warmup),
                             "-concurrency", str(conc), "-payload-bytes", str(args.payload_bytes),
                             "-measurement-start-file", start_marker, "-measurement-end-file", end_marker,
                             "-collector-ready-file", ready_marker,
                             "-output", str(result)], f"run-{rep:02d}-{arm}-c{conc}",
                            env=arm_env, timeout=1800)
                        if monitor.wait(timeout=15) != 0:
                            raise RuntimeError(f"resource collector failed; see {collector_log}")
                    finally:
                        if monitor.poll() is None:
                            monitor.terminate()
                            try:
                                monitor.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                monitor.kill()
                                monitor.wait()
                        collector_stream.close()
                        EVENTS.append({"name": f"collector-{rep:02d}-{arm}-c{conc}", "command": shlex.join(collector_command),
                            "exit_code": monitor.returncode, "log": str(collector_log.relative_to(ROOT)),
                            "started_unix": collector_started, "elapsed_seconds": time.time() - collector_started})
                        persist_events()
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
