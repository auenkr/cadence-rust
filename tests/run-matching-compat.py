#!/usr/bin/env python3
"""Check matching primitives against the original Go service implementation."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--regenerate", action="store_true")
args = parser.parse_args()
for fixture_name, test_name, package, source in [
    ("matching-identifiers", "Identifiers", "service/matching/tasklist", "matching-go_test.go"),
    ("matching-partitions", "Partitions", "service/matching/tasklist", "matching-go_test.go"),
    ("matching-partition-wire", "PartitionWire", "service/matching/tasklist", "matching-go_test.go"),
    ("matching-ack", "Ack", "common/messaging", "matching-ack-go_test.go"),
]:
    fixture = root / f"tests/fixtures/{fixture_name}.json"
    with tempfile.TemporaryDirectory(prefix="cadence-matching-") as directory:
        overlay = Path(directory) / "overlay.json"
        output = Path(directory) / "output.json"
        overlay.write_text(json.dumps({"Replace": {
            str(root / f"go/{package}/rust_matching_test.go"): str(root / f"tests/{source}"),
        }}))
        subprocess.run(["go", "test", "-overlay", str(overlay), f"./{package}",
                        "-run", f"^TestRustMatching{test_name}$", "-count=1"], cwd=root / "go",
                       env={**os.environ, "CADENCE_MATCHING_INPUT": str(fixture),
                            "CADENCE_MATCHING_OUTPUT": str(output)}, check=True)
        actual = json.loads(output.read_text())
        if args.regenerate:
            fixture.write_text(json.dumps(actual, indent=2) + "\n")
        elif actual != json.loads(fixture.read_text()):
            raise RuntimeError("matching Go oracle fixtures drifted")
if not args.regenerate:
    subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
                    "-p", "cadence-common", "-p", "cadence-persistence", "matching_"], check=True)
print("Verified original Go matching identifier, partition-update and acknowledgement fixtures")
