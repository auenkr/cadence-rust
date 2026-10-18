#!/usr/bin/env python3
"""Compare checked-in provider contract data with pinned Go implementations."""
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/ringpop_bootstrap_provider.json"

proc = subprocess.run(
    ["go", "test", "./common/peerprovider/ringpopprovider", "-run", "^TestBootstrapProviderActualOracle$", "-count=1", "-v"],
    cwd=ROOT / "go", text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
)
sys.stdout.write(proc.stdout)
if proc.returncode:
    raise SystemExit(proc.returncode)
line = next((line for line in proc.stdout.splitlines() if line.startswith("BOOTSTRAP_PROVIDER_ORACLE=")), None)
if line is None:
    raise SystemExit("actual Go provider oracle emitted no result")
actual = json.loads(line.split("=", 1)[1])
expected = json.loads(FIXTURE.read_text())
if actual != expected:
    print("Go provider output differs from checked-in fixture", file=sys.stderr)
    print(json.dumps({"expected": expected, "actual": actual}, indent=2), file=sys.stderr)
    raise SystemExit(1)
print(f"actual Go provider output matches {FIXTURE.relative_to(ROOT)}")
