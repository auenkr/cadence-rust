#!/usr/bin/env python3
"""Run production Rust Matching + Go services + pinned Go shard manager."""
import argparse
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess

ROOT = Path(__file__).resolve().parents[1]
GO = ROOT / "go"
FIXTURE = Path("tests/shard-distributor-interoperability")

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--rust-production-binary", type=Path, required=True)
parser.add_argument("--artifacts-dir", type=Path, required=True)
parser.add_argument("--history-transport", choices=("grpc", "tchannel"), default="grpc")
args = parser.parse_args()
artifacts = args.artifacts_dir.resolve()
artifacts.mkdir(parents=True, exist_ok=True)

command = subprocess.Popen(
    ["go", "run", str(FIXTURE)],
    cwd=GO,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
    start_new_session=True,
)
try:
    if command.stdout is None or command.stdin is None:
        raise RuntimeError("could not open Go shard-manager fixture pipes")
    selector = selectors.DefaultSelector()
    selector.register(command.stdout, selectors.EVENT_READ)
    if not selector.select(timeout=60):
        raise TimeoutError("pinned Go shard-manager fixture did not start within 60 seconds")
    line = command.stdout.readline()
    if not line:
        raise RuntimeError("pinned Go shard-manager fixture exited during startup")
    fixture = json.loads(line)
    env = {
        **os.environ,
        "CADENCE_MATCHING_SHARD_DISTRIBUTOR_CONTROL": f"http://{fixture['control_address']}",
    }
    run = subprocess.run(
        [
            "python3",
            str(ROOT / "tests/run-go-service-baseline.py"),
            "--rust-matching",
            "--matching-restart",
            "--rust-production-binary",
            str(args.rust_production_binary.resolve()),
            "--history-transport",
            args.history_transport,
            "--matching-shard-distributor-address",
            fixture["address"],
            "--matching-shard-distributor-namespace",
            fixture["namespace"],
            "--matching-onboarding-percentage",
            "100",
            "--artifacts-dir",
            str(artifacts),
        ],
        cwd=ROOT,
        env=env,
        check=False,
    )
    raise SystemExit(run.returncode)
finally:
    if command.poll() is None and command.stdin is not None:
        try:
            command.stdin.write('{"op":"stop"}\n')
            command.stdin.flush()
        except OSError:
            pass
    try:
        command.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if command.poll() is None:
            os.killpg(command.pid, signal.SIGKILL)
        command.wait()
