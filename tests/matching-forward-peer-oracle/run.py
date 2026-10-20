#!/usr/bin/env python3
"""Run Rust's Parent::Remote polls against a real generated Go YARPC peer."""
import os
import json
import selectors
import signal
import subprocess
import tempfile
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
    with tempfile.TemporaryDirectory(prefix="cadence-matching-go-peer-") as tmp:
        log = Path(tmp) / "observations.jsonl"
        env = {
            **os.environ,
            "CADENCE_MATCHING_GO_PEER": address,
            "CADENCE_MATCHING_GO_PEER_LOG": str(log),
        }
        result = subprocess.run(
            ["cargo", "test", "-p", "cadence-server", "matching_remote_", "--", "--nocapture"],
            cwd=ROOT,
            env=env,
            check=False,
        )
        if result.returncode:
            raise SystemExit(result.returncode)
        records = [json.loads(row) for row in log.read_text().splitlines()]
        by_method = {record["method"]: record for record in records}
        required = {
            "pollDecision": {"domainId", "pollerId", "taskList", "forwardedFrom"},
            "pollActivity": {"domainId", "pollerId", "taskList", "forwardedFrom"},
            "addDecision": {"domainId", "workflowId", "taskList", "forwardedFrom"},
            "addActivity": {"domainId", "workflowId", "taskList", "forwardedFrom"},
            "query": {"domainId", "taskList", "forwardedFrom"},
        }
        missing = required.keys() - by_method.keys()
        if missing:
            raise RuntimeError(f"Rust production calls did not reach Go YARPC methods: {sorted(missing)}")
        for method, keys in required.items():
            record = by_method[method]
            if record["rpcService"] != "cadence-matching" or record["rpcCaller"] != "cadence-matching" or record["rpcEncoding"] != "proto":
                raise RuntimeError(f"Go observed incorrect YARPC headers for {method}: {record}")
            if not record["deadline"]:
                raise RuntimeError(f"Go observed no forwarded deadline for {method}: {record}")
            fields = record["fields"]
            if not fields or any(not fields.get(key) for key in keys):
                raise RuntimeError(f"Go observed incomplete typed request fields for {method}: {record}")
finally:
    if peer.poll() is None:
        os.killpg(peer.pid, signal.SIGTERM)
    try:
        peer.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if peer.poll() is None:
            os.killpg(peer.pid, signal.SIGKILL)
        peer.wait()
