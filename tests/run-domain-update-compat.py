#!/usr/bin/env python3
"""Compare UpdateDomain controller behavior with the actual Go handler."""
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
fixture = root / "tests/fixtures/domain-update.json"
with tempfile.TemporaryDirectory(prefix="cadence-domain-update-") as directory:
    overlay = Path(directory) / "overlay.json"
    output = Path(directory) / "output.json"
    overlay.write_text(json.dumps({"Replace": {
        str(root / "go/common/domain/rust_active_active_test.go"): str(root / "tests/active-active-go_test.go"),
        str(root / "go/common/domain/rust_domain_update_test.go"): str(root / "tests/domain-update-go_test.go"),
    }}))
    subprocess.run(["go", "test", "-overlay", str(overlay), "./common/domain", "-run", "^TestRustDomainUpdateFixtures$", "-count=1"], cwd=root / "go", env={**os.environ, "CADENCE_DOMAIN_UPDATE_FIXTURES": str(fixture), "CADENCE_DOMAIN_UPDATE_OUTPUT": str(output)}, check=True)
    actual = json.loads(output.read_text())
    if args.regenerate:
        fixture.write_text(json.dumps(actual, indent=2) + "\n")
    elif actual != json.loads(fixture.read_text()):
        raise RuntimeError("Go domain update controller results drifted")
if not args.regenerate:
    subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"), "-p", "cadence-persistence", "domain_update_go_fixtures"], check=True)
print(f"Verified {len(actual)} original Go UpdateDomain controller cases")
