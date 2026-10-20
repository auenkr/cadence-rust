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
peer = None
with tempfile.TemporaryDirectory(prefix="cadence-matching-go-peer-") as tmp:
    log = Path(tmp) / "observations.jsonl"
    peer_env = {**os.environ, "CADENCE_MATCHING_GO_PEER_LOG": str(log)}
    peer = subprocess.Popen(
        ["go", "run", str(PEER)],
        cwd=GO,
        env=peer_env,
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        if peer.stdout is None:
            raise RuntimeError("could not read the Go Matching peer startup address")
        selector = selectors.DefaultSelector()
        selector.register(peer.stdout, selectors.EVENT_READ)
        if not selector.select(timeout=60):
            raise TimeoutError("Go Matching peer did not start within 60 seconds")
        line = peer.stdout.readline()
        if not line:
            raise RuntimeError("Go Matching peer exited before publishing its address")
        address = json.loads(line)["address"]
        env = {**peer_env, "CADENCE_MATCHING_GO_PEER": address}
        result = subprocess.run(
            ["cargo", "test", "--locked", "-p", "cadence-server", "--lib", "matching_remote_", "--", "--nocapture"],
            cwd=ROOT / "rust",
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=1800,
        )
        print(result.stdout, end="")
        print(result.stderr, end="")
        if result.returncode:
            if log.exists():
                print("Go peer observations from the failed Rust run:")
                print(log.read_text(), end="")
            raise SystemExit(result.returncode)
        test_output = result.stdout + result.stderr
        expected_tests = [
            "matching_remote_routing_propagates_spectator_failure_without_ring_fallback",
            "matching_remote_adds_and_query_reach_go_yarpc_through_production_handlers",
            "matching_remote_forward_reaches_go_yarpc_peer",
        ]
        if "3 passed" not in test_output or any(name not in test_output for name in expected_tests):
            raise RuntimeError("cargo test completed without proving all three matching_remote_ tests ran and passed")
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
