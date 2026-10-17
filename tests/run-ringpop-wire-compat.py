#!/usr/bin/env python3
"""Run Ringpop JSON wire fixtures through the original Go structs."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import argparse

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--regenerate", action="store_true")
args = parser.parse_args()

fixture = root / "rust/crates/cadence-common/tests/fixtures/ringpop_wire.json"
with tempfile.TemporaryDirectory(prefix="ringpop-wire-") as directory:
    overlay = Path(directory) / "overlay.json"
    output = Path(directory) / "output.json"
    overlay.write_text(json.dumps({"Replace": {
        str(root / "go/common/membership/rust_ringpop_wire_test.go"): str(root / "tests/ringpop-wire-go_test.go"),
    }}))
    env = {**os.environ, "CADENCE_RINGPOP_WIRE_OUTPUT": str(output)}
    subprocess.run(["go", "test", "-overlay", str(overlay), "./common/membership",
                    "-run", "^TestRustRingpopWireFixtures$", "-count=1"], cwd=root / "go",
                   env=env, check=True)
    actual = json.loads(output.read_text())
    if args.regenerate:
        fixture.write_text(json.dumps(actual, indent=2) + "\n")
    elif actual != json.loads(fixture.read_text()):
        raise RuntimeError("ringpop wire Go oracle fixtures drifted")

if not args.regenerate:
    subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
                    "-p", "cadence-common", "ringpop_wire"], check=True)
print("Verified Ringpop wire fixtures")
