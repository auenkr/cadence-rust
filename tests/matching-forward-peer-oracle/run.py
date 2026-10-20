#!/usr/bin/env python3
"""Run Rust's Parent::Remote polls against a real generated Go YARPC peer."""
import os
import json
import selectors
import signal
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GO = ROOT / "go"
PEER = Path(__file__).resolve().parent / "main.go"

peer = subprocess.Popen(
    ["go", "run", str(PEER)],
    cwd=GO,
    stdout=subprocess.PIPE,
    text=True,
    start_new_session=True,
)
try:
    assert peer.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(peer.stdout, selectors.EVENT_READ)
    if not selector.select(timeout=60):
        raise TimeoutError("Go Matching peer did not start within 60 seconds")
    line = peer.stdout.readline()
    if not line:
        raise RuntimeError("Go Matching peer exited before publishing its address")
    address = json.loads(line)["address"]
    env = {**os.environ, "CADENCE_MATCHING_GO_PEER": address}
    result = subprocess.run(
        [
            "cargo",
            "test",
            "-p",
            "cadence-server",
            "matching_dispatch::tests::matching_remote_forward_reaches_go_yarpc_peer",
            "--",
            "--exact",
            "--nocapture",
        ],
        cwd=ROOT,
        env=env,
        check=False,
    )
    raise SystemExit(result.returncode)
finally:
    if peer.poll() is None:
        os.killpg(peer.pid, signal.SIGTERM)
    try:
        peer.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if peer.poll() is None:
            os.killpg(peer.pid, signal.SIGKILL)
        peer.wait()
