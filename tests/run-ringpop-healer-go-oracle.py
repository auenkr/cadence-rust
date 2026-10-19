#!/usr/bin/env python3
"""Generate partition-healer traces from pinned ringpop-go v0.10.0."""
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
MODULE = Path("/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0")
TEST = ROOT / "tests/ringpop-healer-go_test.go"
FIXTURE = ROOT / "rust/crates/cadence-server/tests/fixtures/ringpop_healer_go_expected.json"
if not (MODULE / "swim/heal_partition.go").is_file():
    raise SystemExit(f"pinned Ringpop source not found: {MODULE}")

with tempfile.TemporaryDirectory(prefix="ringpop-healer-go-oracle-") as directory:
    temp = Path(directory)
    copied = temp / "ringpop-go"
    shutil.copytree(MODULE, copied, ignore=shutil.ignore_patterns(".git"))
    for current, _dirs, _files in os.walk(copied):
        os.chmod(current, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    shutil.copy2(ROOT / "go/go.mod", copied / "go.mod")
    shutil.copy2(ROOT / "go/go.sum", copied / "go.sum")
    module_file = copied / "go.mod"
    module_file.write_text(module_file.read_text().replace("module github.com/uber/cadence", "module github.com/uber/ringpop-go", 1))
    virtual = copied / "swim/zz_cadence_ringpop_healer_test.go"
    overlay = temp / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(virtual): str(TEST)}}))
    output = temp / "ringpop_healer_go.json"
    env = {**os.environ, "CADENCE_RINGPOP_HEALER_OUTPUT": str(output)}
    result = subprocess.run(
        ["go", "test", "-overlay", str(overlay), "./swim", "-run", "^TestGenerateRingpopHealerGoTrace$", "-count=1", "-v"],
        cwd=copied, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        check=False, timeout=240,
    )
    print(result.stdout, end="")
    if result.returncode:
        raise RuntimeError(f"pinned Ringpop healer oracle exited {result.returncode}")
    if not output.is_file():
        raise RuntimeError("Go oracle did not write a healer trace")
    data = json.loads(output.read_text())

if "--update" in sys.argv[1:]:
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(data, indent=2) + "\n")
    print(f"Updated pinned Ringpop healer expected trace: {FIXTURE}")
else:
    expected = json.loads(FIXTURE.read_text())
    if data != expected:
        print("Pinned Go trace differs from the checked-in expected trace:")
        print(json.dumps(data, indent=2))
        raise SystemExit("rerun with --update only after reviewing the changed Ringpop behavior")
    print(f"Pinned Go trace matches {FIXTURE}")
