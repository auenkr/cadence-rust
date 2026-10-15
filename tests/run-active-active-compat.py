#!/usr/bin/env python3
"""Run active-active state fixtures through the original Go domain helpers."""
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
fixture = root / "tests/fixtures/active-active.json"
with tempfile.TemporaryDirectory(prefix="cadence-active-active-") as directory:
    overlay = Path(directory) / "overlay.json"
    output = Path(directory) / "output.json"
    overlay.write_text(json.dumps({"Replace": {
        str(root / "go/common/domain/rust_active_active_test.go"): str(root / "tests/active-active-go_test.go"),
    }}))
    subprocess.run(["go", "test", "-overlay", str(overlay), "./common/domain",
                    "-run", "^TestRustActiveActiveFixtures$", "-count=1"], cwd=root / "go",
                   env={**os.environ, "CADENCE_ACTIVE_ACTIVE_FIXTURES": str(fixture),
                        "CADENCE_ACTIVE_ACTIVE_OUTPUT": str(output)}, check=True)
    actual = json.loads(output.read_text())
    if args.regenerate:
        fixture.write_text(json.dumps(actual, indent=2) + "\n")
    elif actual != json.loads(fixture.read_text()):
        raise RuntimeError("active-active Go oracle fixtures drifted")
# Compare the actual Go mapper's zero-valued scalar wire output as well.
with tempfile.TemporaryDirectory(prefix="cadence-active-active-wire-") as directory:
    output = Path(directory) / "active.bin"
    subprocess.run(["go", "run", "../tests/active-active-wire-go/main.go", str(output)], cwd=root / "go", check=True)
    fixture_bytes = root / "rust/crates/cadence-thrift/tests/fixtures/active_clusters_zero.bin"
    if args.regenerate:
        fixture_bytes.write_bytes(output.read_bytes())
    elif fixture_bytes.read_bytes() != output.read_bytes():
        raise RuntimeError("active-active Go wire fixture drifted")
if not args.regenerate:
    subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
                    "-p", "cadence-persistence", "active_active_go_fixtures"], check=True)
if not args.regenerate:
    subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
                    "-p", "cadence-thrift", "active_active_zero_version_matches_go_mapper"], check=True)
print(f"Verified {len(actual)} original Go active-active cases and the zero-version wire fixture")
