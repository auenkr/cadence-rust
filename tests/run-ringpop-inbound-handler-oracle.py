#!/usr/bin/env python3
"""Invoke the original pinned Ringpop JOIN/PING handlers and save JSON evidence."""
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
MODULE = Path("/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0")
TEST = ROOT / "tests/ringpop-inbound-handler-oracle_test.go"
FIXTURE = ROOT / "rust/crates/cadence-server/tests/fixtures/ringpop_inbound_go.json"
if not (MODULE / "swim/join_handler.go").is_file():
    raise SystemExit(f"pinned Ringpop module not found: {MODULE}")

with tempfile.TemporaryDirectory(prefix="ringpop-inbound-oracle-") as directory:
    temp = Path(directory)
    copied = temp / "ringpop-go"
    shutil.copytree(MODULE, copied, ignore=shutil.ignore_patterns(".git"))
    for current, _dirs, _files in os.walk(copied):
        os.chmod(current, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    shutil.copy2(ROOT / "go/go.mod", copied / "go.mod")
    shutil.copy2(ROOT / "go/go.sum", copied / "go.sum")
    module_file = copied / "go.mod"
    module_file.write_text(module_file.read_text().replace("module github.com/uber/cadence", "module github.com/uber/ringpop-go", 1))
    virtual = copied / "swim/zz_cadence_ringpop_inbound_handler_oracle_test.go"
    overlay = temp / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(virtual): str(TEST)}}))
    output = temp / "ringpop_inbound_handlers.json"
    env = {**os.environ, "CADENCE_RINGPOP_INBOUND_OUTPUT": str(output)}
    subprocess.run(["go", "test", "-overlay", str(overlay), "./swim", "-run", "^TestGenerateRingpopInboundHandlerFixtures$", "-count=1", "-v"], cwd=copied, env=env, check=True)
    normalized = re.sub(r"127\.0\.0\.1:\d+", "<local>", output.read_text())
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(normalized + "\n")
    print(normalized)
