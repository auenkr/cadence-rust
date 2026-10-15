#!/usr/bin/env python3
"""Compare typed runtime config lookups with Go's actual store and Collection."""
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
fixture = root / "tests/fixtures/dynamic-policy.json"
with tempfile.TemporaryDirectory(prefix="cadence-dynamic-policy-") as directory:
    overlay = Path(directory) / "overlay.json"
    output = Path(directory) / "output.json"
    overlay.write_text(json.dumps({"Replace": {
        str(root / "go/common/dynamicconfig/configstore/rust_dynamic_policy_test.go"): str(root / "tests/dynamic-policy-go_test.go"),
    }}))
    subprocess.run(["go", "test", "-overlay", str(overlay), "./common/dynamicconfig/configstore", "-run", "^TestRustDynamicPolicyFixtures$", "-count=1"], cwd=root / "go", env={**os.environ, "CADENCE_DYNAMIC_POLICY_FIXTURES": str(fixture), "CADENCE_DYNAMIC_POLICY_OUTPUT": str(output)}, check=True)
    actual = json.loads(output.read_text())
    if args.regenerate:
        fixture.write_text(json.dumps(actual, indent=2) + "\n")
    elif actual != json.loads(fixture.read_text()):
        raise RuntimeError("Go dynamic policy results drifted")
if not args.regenerate:
    subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"), "-p", "cadence-persistence", "dynamic_policy_go_fixtures"], check=True)
print(f"Verified {len(actual)} original Go dynamic policy cases")
