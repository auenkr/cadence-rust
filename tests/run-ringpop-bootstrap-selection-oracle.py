#!/usr/bin/env python3
"""Run selection fixtures against the pinned, original Ringpop v0.10.0 code."""
import json
import argparse
from collections import Counter
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
MODULE = Path("/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0")
TEST = ROOT / "tests/ringpop-bootstrap-selection-oracle_test.go"
FIXTURE = ROOT / "tests/fixtures/ringpop_bootstrap_selection.json"

parser = argparse.ArgumentParser()
parser.add_argument("--generate", action="store_true", help="explicitly regenerate the checked fixture")
args = parser.parse_args()

with tempfile.TemporaryDirectory(prefix="ringpop-bootstrap-selection-") as temporary:
    temp = Path(temporary)
    copied = temp / "ringpop-go"
    shutil.copytree(MODULE, copied, ignore=shutil.ignore_patterns(".git"))
    for directory, _subdirs, _files in os.walk(copied):
        os.chmod(directory, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    shutil.copy2(ROOT / "go/go.mod", copied / "go.mod")
    shutil.copy2(ROOT / "go/go.sum", copied / "go.sum")
    module = copied / "go.mod"
    module.write_text(module.read_text().replace("module github.com/uber/cadence", "module github.com/uber/ringpop-go", 1))
    virtual = copied / "swim/zz_cadence_bootstrap_selection_test.go"
    overlay = temp / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(virtual): str(TEST)}}))
    output = temp / FIXTURE.name
    env = {**os.environ, "CADENCE_RINGPOP_BOOTSTRAP_SELECTION_OUTPUT": str(output)}
    subprocess.run(["go", "test", "-overlay", str(overlay), "./swim", "-run", "^(TestGenerateRingpopBootstrapSelectionFixtures|TestGoCaptureHostMatrix)$", "-count=1", "-v"], cwd=copied, env=env, check=True, timeout=180)
    generated = json.loads(output.read_text())

if args.generate:
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(generated, indent=2) + "\n")
    print(f"generated original-Go selection fixture: {FIXTURE.relative_to(ROOT)}")
    raise SystemExit(0)

data = json.loads(FIXTURE.read_text())
assert data["source"] == "github.com/uber/ringpop-go v0.10.0 swim/join_sender.go newJoinSender + SelectGroup"
assert len(data["cases"]) == 11, len(data["cases"])
assert len(generated["cases"]) == len(data["cases"])
stable_fields = ("name", "local", "seeds", "requestedTimeoutNanos", "requestedMaxJoinDurationNanos", "requestedSize", "requestedParallelism", "bootstrapHosts", "potential", "preferred", "nonPreferred", "joined", "timeoutNanos", "maxJoinDurationNanos", "size", "parallelism")
for expected, actual in zip(data["cases"], generated["cases"]):
    assert all(expected.get(field) == actual.get(field) for field in stable_fields), expected["name"]
    candidates = (expected.get("preferred") or []) + (expected.get("nonPreferred") or [])
    group = actual.get("group") or []
    assert not (Counter(group) - Counter(candidates)), (expected["name"], group)
    cap = (expected["size"] - len(expected.get("joined") or [])) * expected["parallelism"]
    assert len(group) <= cap if cap >= 0 else True, (expected["name"], group, cap)
print(f"verified {len(data['cases'])} original-Go selection fixtures without modifying checked fixture")
