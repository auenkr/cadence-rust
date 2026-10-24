#!/usr/bin/env python3
"""Run the Go Matching handler/ring oracle for the Rust partition owner test."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix="cadence-matching-owner-oracle-") as tmp:
    tmp = Path(tmp)
    output = tmp / "owner-order.json"
    overlay = tmp / "overlay.json"
    source = root / "tests/matching-owner-order-oracle/oracle_test.go"
    destination = root / "go/service/matching/handler/rust_owner_order_oracle_test.go"
    overlay.write_text(json.dumps({"Replace": {str(destination): str(source)}}))
    subprocess.run(
        ["go", "test", "-overlay", str(overlay), "./service/matching/handler",
         "-run", "^TestCadenceMatchingPartitionOwnerOrderOracle$", "-count=1"],
        cwd=root / "go",
        env={**os.environ, "CADENCE_MATCHING_OWNER_ORACLE_OUTPUT": str(output)},
        check=True,
    )
    # The workflow consumes the file in the next step; preserve it in the runner temp directory.
    persistent = Path(os.environ.get("RUNNER_TEMP", tmp.parent)) / "cadence-matching-owner-order.json"
    persistent.write_bytes(output.read_bytes())
    env_file = os.environ.get("GITHUB_ENV")
    if env_file:
        with open(env_file, "a", encoding="utf-8") as env:
            env.write(f"CADENCE_MATCHING_OWNER_ORACLE={persistent}\n")
    print(f"Go production Matching owner oracle: {persistent}")
