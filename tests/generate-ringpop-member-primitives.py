#!/usr/bin/env python3
"""Generate member primitive fixtures by running unexported pinned Ringpop code."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import shutil
import stat

ROOT = Path(__file__).resolve().parent.parent
MODULE = Path("/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0")
TEST = ROOT / "tests/ringpop-member-primitives-oracle_test.go"
DISSEMINATOR_TEST = ROOT / "tests/ringpop-disseminator-oracle_test.go"
FIXTURE = ROOT / "rust/crates/cadence-common/tests/fixtures/ringpop_member_primitives.json"
DISSEMINATOR_FIXTURE = ROOT / "rust/crates/cadence-common/tests/fixtures/ringpop_disseminator.json"

if not (MODULE / "swim/member.go").is_file():
    raise SystemExit(f"pinned Ringpop module not found: {MODULE}")

with tempfile.TemporaryDirectory(prefix="ringpop-member-oracle-") as temp:
    temp = Path(temp)
    copied_module = temp / "ringpop-go"
    shutil.copytree(MODULE, copied_module, ignore=shutil.ignore_patterns(".git"))
    for directory, subdirs, _files in os.walk(copied_module):
        os.chmod(directory, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    shutil.copy2(ROOT / "go/go.mod", copied_module / "go.mod")
    shutil.copy2(ROOT / "go/go.sum", copied_module / "go.sum")
    module_file = copied_module / "go.mod"
    module_file.write_text(module_file.read_text().replace(
        "module github.com/uber/cadence", "module github.com/uber/ringpop-go", 1
    ))
    overlay = temp / "overlay.json"
    output = Path(temp) / "ringpop_member_primitives.json"
    virtual_test = copied_module / "swim/zz_cadence_member_primitives_oracle_test.go"
    virtual_disseminator_test = copied_module / "swim/zz_cadence_disseminator_oracle_test.go"
    overlay.write_text(json.dumps({"Replace": {str(virtual_test): str(TEST), str(virtual_disseminator_test): str(DISSEMINATOR_TEST)}}))
    env = {**os.environ, "CADENCE_RINGPOP_MEMBER_OUTPUT": str(output), "CADENCE_RINGPOP_DISSEMINATOR_OUTPUT": str(temp / "ringpop_disseminator.json")}
    subprocess.run(
        ["go", "test", "-overlay", str(overlay), "./swim",
         "-run", "^TestGenerateRingpop(MemberPrimitive|Disseminator)Fixtures$", "-count=1"],
        cwd=copied_module, env=env, check=True,
    )
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_bytes(output.read_bytes())
    DISSEMINATOR_FIXTURE.write_bytes((temp / "ringpop_disseminator.json").read_bytes())
print(f"Generated {FIXTURE.relative_to(ROOT)} and {DISSEMINATOR_FIXTURE.relative_to(ROOT)} from ringpop-go v0.10.0")
