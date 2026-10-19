#!/usr/bin/env python3
"""Run frozen-source gates for configured SQLite Matching persistence.

This runner is intentionally explicit: each gate writes its own log and status
to the timestamped artifact directory. It does not build the server binary;
pass --server-bin for a binary built from the recorded source revision.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
FROZEN_FILES = (
    "rust/crates/cadence-persistence/src/matching_sqlite.rs",
    "rust/crates/cadence-server/src/matching_task_store.rs",
    "rust/crates/cadence-server/src/main.rs",
    "tests/run-matching-thrift-compat.py",
    "tests/run-matching-configured-store-validation.py",
    "tests/matching-sqlite-go_test.go",
    "memory/matching-persistence-production-audit-20261019.md",
)


def run(command, *, cwd, log_path, timeout=None):
    with log_path.open("w") as log:
        log.write("$ " + " ".join(map(str, command)) + "\n")
        log.flush()
        env = os.environ.copy()
        env.setdefault("CARGO_BUILD_JOBS", "1")
        result = subprocess.run(command, cwd=cwd, env=env, stdout=log,
                                stderr=subprocess.STDOUT, timeout=timeout, check=False)
    return result.returncode


def hashes():
    result = {}
    for relative in FROZEN_FILES:
        path = ROOT / relative
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-bin", type=Path,
                        help="prebuilt server binary; defaults under Cargo's target directory")
    parser.add_argument("--target-dir", type=Path,
                        help="Cargo target directory; defaults to inherited CARGO_TARGET_DIR")
    parser.add_argument("--artifacts-dir", type=Path)
    args = parser.parse_args()
    started = datetime.now(timezone.utc)
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    artifacts = (args.artifacts_dir or ROOT / "artifacts" / f"matching-configured-store-{stamp}").resolve()
    artifacts.mkdir(parents=True, exist_ok=False)
    before = hashes()
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if args.target_dir:
        os.environ["CARGO_TARGET_DIR"] = str(args.target_dir.resolve())
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "rust/target"))
    server_bin = (args.server_bin or target_dir / "debug/cadence-server").resolve()
    report = {
        "startedUtc": started.isoformat(), "head": head,
        "serverBinary": str(server_bin), "sourceSha256Before": before,
        "cargoTargetDir": str(target_dir.resolve()),
        "gates": [],
    }
    (artifacts / "source-freeze.json").write_text(json.dumps(report, indent=2) + "\n")

    gates = [
        ("sqlite-option-and-schema-tests", ["cargo", "test", "--locked", "-p", "cadence-persistence", "--lib", "matching_sqlite"]),
        ("configured-store-resolution-tests", ["cargo", "test", "--locked", "-p", "cadence-server", "--lib", "matching_task_store"]),
        ("cadence-server-build", ["cargo", "build", "--locked", "-p", "cadence-server", "--bin", "cadence-server"]),
        ("configured-two-way-go-native-thrift-probe", [sys.executable,
         str(ROOT / "tests/run-matching-thrift-compat.py"), "--server-bin", str(server_bin),
         "--artifacts-dir", str(artifacts / "go-native-thrift")]),
        ("strict-rust-lint", ["cargo", "clippy", "--locked", "-p", "cadence-persistence", "-p", "cadence-server", "--all-targets", "--", "-D", "warnings"]),
    ]
    for name, command in gates:
        log_path = artifacts / f"{name}.log"
        cwd = ROOT / "rust" if command[0] == "cargo" else ROOT
        print(f"[{name}] running; log: {log_path}", flush=True)
        code = run(command, cwd=cwd, log_path=log_path,
                   timeout=900 if name.endswith("probe") else 1800)
        report["gates"].append({"name": name, "exitCode": code, "log": str(log_path)})
        (artifacts / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        if code != 0:
            print(f"[{name}] FAILED with exit code {code}", flush=True)

    after = hashes()
    report["finishedUtc"] = datetime.now(timezone.utc).isoformat()
    report["sourceSha256After"] = after
    report["sourceUnchanged"] = before == after
    report["passed"] = report["sourceUnchanged"] and all(g["exitCode"] == 0 for g in report["gates"])
    (artifacts / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"Artifacts: {artifacts}")
    print(f"Result: {'PASS' if report['passed'] else 'FAIL'}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
