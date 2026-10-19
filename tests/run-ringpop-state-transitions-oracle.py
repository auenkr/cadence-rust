#!/usr/bin/env python3
"""Compare the Rust transition scheduler with pinned Ringpop v0.10.0."""
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
RUST = ROOT / "rust"
MODULE = Path("/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0")
TEST = ROOT / "tests/ringpop-state-transitions-oracle_test.go"

with tempfile.TemporaryDirectory(prefix="ringpop-state-transitions-") as directory:
    temp = Path(directory)
    copied = temp / "ringpop-go"
    shutil.copytree(MODULE, copied, ignore=shutil.ignore_patterns(".git"))
    for root, _dirs, _files in os.walk(copied):
        os.chmod(root, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    shutil.copy2(ROOT / "go/go.mod", copied / "go.mod")
    shutil.copy2(ROOT / "go/go.sum", copied / "go.sum")
    go_mod = copied / "go.mod"
    go_mod.write_text(go_mod.read_text().replace("module github.com/uber/cadence", "module github.com/uber/ringpop-go", 1))
    virtual = copied / "swim/zz_cadence_state_transitions_test.go"
    overlay = temp / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(virtual): str(TEST)}}))
    oracle = temp / "oracle.json"
    env = {**os.environ, "CADENCE_RINGPOP_TRANSITIONS_ORACLE": str(oracle)}
    subprocess.run(["go", "test", "-overlay", str(overlay), "./swim", "-run", "^TestGenerateCadenceStateTransitionOracle$", "-count=1"], cwd=copied, env=env, check=True, timeout=180)
    subprocess.run(["cargo", "test", "--locked", "-p", "cadence-common", "ringpop_state_transitions", "--lib"], cwd=RUST, env={**env, "CARGO_BUILD_JOBS": "1"}, check=True, timeout=600)
print("Rust transition trace matches the pinned Ringpop v0.10.0 scheduler oracle")
