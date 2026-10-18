#!/usr/bin/env python3
"""Run the pinned Ringpop memberlist.Update implementation for fixtures."""
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
MODULE = Path("/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0")
TEST = ROOT / "tests/ringpop-memberlist-update-oracle_test.go"
FIXTURE = ROOT / "rust/crates/cadence-common/tests/fixtures/ringpop_memberlist_update.json"

if not (MODULE / "swim/memberlist.go").is_file():
    raise SystemExit(f"pinned Ringpop module not found: {MODULE}")

with tempfile.TemporaryDirectory(prefix="ringpop-memberlist-update-") as temporary:
    temp = Path(temporary)
    copied = temp / "ringpop-go"
    shutil.copytree(MODULE, copied, ignore=shutil.ignore_patterns(".git"))
    for directory, _subdirs, _files in os.walk(copied):
        os.chmod(directory, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    shutil.copy2(ROOT / "go/go.mod", copied / "go.mod")
    shutil.copy2(ROOT / "go/go.sum", copied / "go.sum")
    module_file = copied / "go.mod"
    module_file.write_text(module_file.read_text().replace("module github.com/uber/cadence", "module github.com/uber/ringpop-go", 1))
    virtual = copied / "swim/zz_cadence_memberlist_update_oracle_test.go"
    overlay = temp / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(virtual): str(TEST)}}))
    output = temp / FIXTURE.name
    env = {**os.environ, "CADENCE_RINGPOP_UPDATE_OUTPUT": str(output)}
    subprocess.run(["go", "test", "-overlay", str(overlay), "./swim", "-run", "^TestGenerateRingpopMemberlistUpdateFixtures$", "-count=1"], cwd=copied, env=env, check=True)
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_bytes(output.read_bytes())
print(f"Generated {FIXTURE.relative_to(ROOT)} from ringpop-go v0.10.0")
