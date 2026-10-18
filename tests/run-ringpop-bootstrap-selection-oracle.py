#!/usr/bin/env python3
"""Run selection fixtures against the pinned, original Ringpop v0.10.0 code."""
import json
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
    subprocess.run(["go", "test", "-overlay", str(overlay), "./swim", "-run", "^TestGenerateRingpopBootstrapSelectionFixtures$", "-count=1", "-v"], cwd=copied, env=env, check=True, timeout=180)
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_bytes(output.read_bytes())

data = json.loads(FIXTURE.read_text())
assert data["source"] == "github.com/uber/ringpop-go v0.10.0 swim/join_sender.go newJoinSender + SelectGroup"
assert len(data["cases"]) == 9, len(data["cases"])
print(f"verified {len(data['cases'])} original-Go selection fixtures: {FIXTURE.relative_to(ROOT)}")
