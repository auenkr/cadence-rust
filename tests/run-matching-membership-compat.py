#!/usr/bin/env python3
"""Compare Matching ownership placement with the original Go Ringpop ring."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
root = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix="matching-membership-") as temporary:
    directory = Path(temporary)
    addresses = ["127.0.0.1:2100", "127.0.0.1:21001", "127.0.0.1:7935", "[::1]:7935"]
    configurations = [[], [{"address": addresses[0], "identity": ""}],
        [{"address": address, "identity": ""} for address in addresses],
        [{"address": address, "identity": f"matching-{i}"} for i, address in enumerate(addresses)],
        [{"address": address, "identity": "shared-identity"} for address in addresses]]
    keys = ["", "tasks", "/_sys/tasks/1", "domain_semaphore_0", "日本語", "x" * 1024]
    keys += [f"queue-{i}" for i in range(300)]
    fixtures = [{"hosts": hosts, "queries": [{"key": key, "count": count} for key in keys for count in [1, 2, 8]]} for hosts in configurations]
    source, output = directory / "input.json", directory / "output.json"
    source.write_text(json.dumps(fixtures))
    overlay = directory / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(root / "go/common/membership/rust_ring_test.go"): str(root / "tests/matching-membership-go_test.go")}}))
    env = {**os.environ, "CADENCE_MATCHING_RING_INPUT": str(source), "CADENCE_MATCHING_RING_OUTPUT": str(output)}
    subprocess.run(["go", "test", "-overlay", str(overlay), "./common/membership", "-run", "^TestRustMatchingMembership$", "-count=1"], cwd=root / "go", env=env, check=True)
    subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"), "-p", "cadence-common", "matching_membership"], env=env, check=True)
