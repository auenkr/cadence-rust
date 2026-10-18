#!/usr/bin/env python3
"""Generate deterministic event traces by executing the original Go taskWriter."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
TEST = ROOT / "tests/matching-writer-next-go_test.go"
FIXTURE = ROOT / "tests/fixtures/matching-writer-next.json"
p = argparse.ArgumentParser()
p.add_argument("--regenerate", action="store_true")
a = p.parse_args()
with tempfile.TemporaryDirectory(prefix="cadence-writer-next-") as tmp:
    tmp = Path(tmp)
    overlay = tmp / "overlay.json"
    output = tmp / "events.json"
    overlay.write_text(json.dumps({"Replace": {str(ROOT / "go/service/matching/tasklist/rust_writer_next_test.go"): str(TEST)}}))
    env = {**os.environ, "CADENCE_WRITER_NEXT_OUTPUT": str(output)}
    result = subprocess.run(["go", "test", "-overlay", str(overlay), "./service/matching/tasklist", "-run", "^TestRustMatchingWriterNextOracle$", "-count=1"], cwd=ROOT / "go", env=env, check=True, text=True, capture_output=True)
    print(result.stdout, end="")
    actual = json.loads(output.read_text())
    if a.regenerate:
        FIXTURE.write_text(json.dumps(actual, indent=2) + "\n")
    elif actual != json.loads(FIXTURE.read_text()):
        raise SystemExit("original Go taskWriter event trace changed; review and run --regenerate")
print(f"Verified {len(actual)} event traces from original Go taskWriter")
