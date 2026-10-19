#!/usr/bin/env python3
"""Run the pinned Go Ringpop provider against Rust Matching's shared listener."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
artifacts = Path(tempfile.mkdtemp(prefix="matching-ringpop-go-peer-"))
probe_source = root / "rust/crates/cadence-server/examples/matching_ringpop_go_peer_probe.rs"
provider_source = root / "rust/crates/cadence-server/src/matching_ringpop_provider.rs"
node_source = root / "rust/crates/cadence-server/src/ringpop_provider.rs"
transport_source = root / "rust/crates/cadence-server/src/tchannel.rs"
go_source = root / "tests/go-overlay/common/peerprovider/ringpopprovider/rust_provider_lifecycle_test.go"
runner_source = Path(__file__).resolve()
hashes = artifacts / "source.sha256"
hashes.write_text(
    "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}\n"
        for path in (probe_source, provider_source, node_source, transport_source, go_source, runner_source)
    )
)
status = artifacts / "status.json"

def run_gate(name, argv, cwd, env, log_path, timeout):
    status.write_text(json.dumps({"stage": name, "state": "running"}, indent=2) + "\n")
    with log_path.open("w") as log:
        result = subprocess.run(
            argv,
            cwd=cwd,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    if result.returncode:
        status.write_text(
            json.dumps({"stage": name, "state": "failed", "exit": result.returncode}, indent=2)
            + "\n"
        )
        print(log_path.read_text(errors="replace")[-12000:])
        raise SystemExit(result.returncode)
    status.write_text(json.dumps({"stage": name, "state": "passed", "exit": 0}, indent=2) + "\n")

env = {**os.environ, "CARGO_BUILD_JOBS": "1"}
target_dir = Path(env.get("CARGO_TARGET_DIR", root / "rust/target"))
go_flags = env.get("GOFLAGS", "").strip()
env["GOFLAGS"] = f"{go_flags} -p=1".strip()
run_gate(
    "cargo-build",
    [
        "cargo",
        "build",
        "--locked",
        "-p",
        "cadence-server",
        "--example",
        "matching_ringpop_go_peer_probe",
    ],
    root / "rust",
    env,
    artifacts / "cargo-build.log",
    600,
)

overlay_test = go_source
overlay = artifacts / "overlay.json"
overlay.write_text(
    json.dumps(
        {
            "Replace": {
                str(root / "go/common/peerprovider/ringpopprovider/rust_provider_lifecycle_test.go"):
                    str(overlay_test)
            }
        }
    )
)
env = {
    **env,
    "RUST_MATCHING_RINGPOP_PROBE": str(
        target_dir / "debug/examples/matching_ringpop_go_peer_probe"
    ),
}
run_gate(
    "go-peer-lifecycle",
    [
        "go",
        "test",
        "-overlay",
        str(overlay),
        "./common/peerprovider/ringpopprovider",
        "-run",
        "^TestRustMatchingRingpopProviderGoPeerLifecycle$",
        "-count=1",
        "-timeout=180s",
        "-v",
    ],
    root / "go",
    env,
    artifacts / "go-peer-lifecycle.log",
    240,
)
status.write_text(json.dumps({"state": "passed", "exit": 0}, indent=2) + "\n")
print(f"Mixed Go/Rust Ringpop provider lifecycle probe passed; artifacts: {artifacts}")
