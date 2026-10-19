#!/usr/bin/env python3
"""Go requester -> Rust production PING-REQ relay -> pinned Go target."""
import json
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parent.parent
target = Path(os.environ.get("CARGO_TARGET_DIR", root / "rust/target"))
artifact = target / "debug/examples/ringpop_ping_req_relay_probe"
with tempfile.TemporaryDirectory(prefix="ringpop-ping-req-relay-bin-") as binary_dir:
    binary = Path(binary_dir) / "ringpop_ping_req_relay_probe"
    subprocess.run(
        ["cargo", "build", "--locked", "-p", "cadence-server", "--example", "ringpop_ping_req_relay_probe"],
        cwd=root / "rust", check=True, env=os.environ.copy(), timeout=1800,
    )
    if not artifact.is_file():
        raise RuntimeError(f"cargo build did not produce {artifact}")
    shutil.copy2(artifact, binary)
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    with tempfile.TemporaryDirectory(prefix="ringpop-ping-req-relay-") as temp:
        temp = Path(temp)
        module = Path("/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0")
        copied = temp / "ringpop-go"
        shutil.copytree(module, copied, ignore=shutil.ignore_patterns(".git"))
        for current, _dirs, _files in os.walk(copied):
            os.chmod(current, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        shutil.copy2(root / "go/go.mod", copied / "go.mod")
        shutil.copy2(root / "go/go.sum", copied / "go.sum")
        mod = copied / "go.mod"
        mod.write_text(mod.read_text().replace("module github.com/uber/cadence", "module github.com/uber/ringpop-go", 1))
        virtual = copied / "swim/zz_cadence_ping_req_relay_test.go"
        overlay = temp / "overlay.json"
        overlay.write_text(json.dumps({"Replace": {str(virtual): str(root / "tests/ringpop-ping-req-relay-go_test.go")}}))
        env = {**os.environ, "RINGPOP_PING_REQ_RELAY_PROBE": str(binary.resolve())}
        subprocess.run(
            ["go", "test", "-overlay", str(overlay), "./swim", "-run", "^(TestGoRequesterRustRelayGoTarget|TestGoTChannelArgReadJSONRelayBodies)$", "-count=1", "-v"],
            cwd=copied, env=env, check=True, timeout=180,
        )
print("Go requester -> Rust RingpopNode relay -> Go Ringpop target passed")
