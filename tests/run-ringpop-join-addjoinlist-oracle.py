#!/usr/bin/env python3
"""Generate JOIN AddJoinList fixtures with pinned original Ringpop Go code."""
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
MODULE = Path("/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0")
TEST = ROOT / "tests/ringpop-join-addjoinlist-oracle_test.go"
FIXTURE = ROOT / "rust/crates/cadence-server/tests/fixtures/ringpop_join_addjoinlist.json"

with tempfile.TemporaryDirectory(prefix="ringpop-join-addjoinlist-") as temporary:
    temp = Path(temporary)
    copied = temp / "ringpop-go"
    shutil.copytree(MODULE, copied, ignore=shutil.ignore_patterns(".git"))
    for directory, _subdirs, _files in os.walk(copied):
        os.chmod(directory, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    shutil.copy2(ROOT / "go/go.mod", copied / "go.mod")
    shutil.copy2(ROOT / "go/go.sum", copied / "go.sum")
    module = copied / "go.mod"
    module.write_text(module.read_text().replace("module github.com/uber/cadence", "module github.com/uber/ringpop-go", 1))
    virtual = copied / "swim/zz_cadence_join_addjoinlist_test.go"
    overlay = temp / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(virtual): str(TEST)}}))
    output = temp / FIXTURE.name
    env = {**os.environ, "CADENCE_RINGPOP_JOIN_ADDJOINLIST_OUTPUT": str(output)}
    subprocess.run(["go", "test", "-overlay", str(overlay), "./swim", "-run", "^TestGenerateRingpopJoinAddJoinListFixtures$", "-count=1"], cwd=copied, env=env, check=True, timeout=180)
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_bytes(output.read_bytes())
print(f"Generated {FIXTURE.relative_to(ROOT)} from ringpop-go v0.10.0 AddJoinList")
