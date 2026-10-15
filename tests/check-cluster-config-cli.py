#!/usr/bin/env python3
"""Verify the copied Rust executable enforces the Go cluster config fixtures."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parent.parent
binary = str(Path(sys.argv[1]).resolve())
cases = json.loads((root / "tests/fixtures/cluster-config.json").read_text())
with tempfile.TemporaryDirectory(prefix="cadence-cluster-config-cli-") as directory:
    config = Path(directory) / "config.yaml"
    checked = 0
    for case in cases:
        if not case["fillDefaults"]:
            continue  # The CLI always defaults before validating.
        config.write_text(json.dumps({"clusterGroupMetadata": case["input"]}))
        result = subprocess.run([binary, "check-config", "--config", str(config)],
                                capture_output=True, text=True, timeout=10)
        errors = case["expected"]["errors"]
        if bool(result.returncode) != bool(errors):
            raise RuntimeError(f"{case['name']}: unexpected CLI status {result.returncode}: {result.stdout}{result.stderr}")
        for error in errors:
            if error not in result.stderr:
                raise RuntimeError(f"{case['name']}: missing Go validation message {error!r}: {result.stderr}")
        if case["name"] == "increment 0":
            result = subprocess.run([binary, "start", "--config", str(config), "--port", "0",
                                     "--grpc-port", "0", "--history-grpc-port", "0",
                                     "--matching-grpc-port", "0", "--tchannel-port", "0"],
                                    capture_output=True, text=True, timeout=10)
            if not result.returncode or "version increment is 0" not in result.stderr:
                raise RuntimeError("startup accepted invalid cluster version mapping")
        checked += 1
print(f"All {checked} defaulted cluster configurations have matching CLI results; invalid startup rejected")
