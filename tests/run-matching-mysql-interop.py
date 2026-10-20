#!/usr/bin/env python3
"""Run Go MySQL persistence -> Rust adapters -> Go MySQL persistence interop.

This opt-in runner starts a disposable pinned MySQL 8.0.36 container, loads the
repository's Go v8 schema, and executes the real Go TaskManager/DomainStore
around an ignored Rust integration test. It is never run implicitly.
"""

import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parent.parent
IMAGE = "mysql:8.0.36"
CONTAINER = f"cadence-matching-mysql-{uuid.uuid4().hex[:10]}"
GO_OVERLAY_TARGET = ROOT / "go/common/persistence/sql/rust_matching_mysql_test.go"
GO_TEST_SOURCE = ROOT / "tests/matching-mysql-go_test.go"


def run(command, *, check=True, **kwargs):
    return subprocess.run(command, check=check, text=True, **kwargs)


def free_loopback_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    run(["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    port = free_loopback_port()
    password = "cadence"
    run([
        "docker", "run", "--detach", "--rm", "--name", CONTAINER,
        "--env", f"MYSQL_ROOT_PASSWORD={password}",
        "--env", "MYSQL_DATABASE=cadence",
        "--publish", f"127.0.0.1:{port}:3306",
        IMAGE,
    ])
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            ping = run(
                ["docker", "exec", CONTAINER, "mysqladmin", "ping", "--silent", "-uroot", f"-p{password}"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if ping.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("pinned MySQL container did not become ready within 120 seconds")

        schema = ROOT / "go/schema/mysql/v8/cadence/schema.sql"
        with schema.open("r", encoding="utf-8") as source:
            run([
                "docker", "exec", "--interactive", CONTAINER,
                "mysql", "-uroot", f"-p{password}", "cadence",
            ], stdin=source)

        with tempfile.TemporaryDirectory(prefix="cadence-matching-mysql-") as directory:
            overlay = Path(directory) / "overlay.json"
            overlay.write_text(json.dumps({"Replace": {str(GO_OVERLAY_TARGET): str(GO_TEST_SOURCE)}}))
            environment = {
                **os.environ,
                "CADENCE_MYSQL_ADDR": f"127.0.0.1:{port}",
                "CADENCE_MYSQL_USER": "root",
                "CADENCE_MYSQL_PASSWORD": password,
                "CADENCE_MYSQL_DATABASE": "cadence",
            }
            go_command = [
                "go", "test", "-overlay", str(overlay), "./common/persistence/sql",
                "-run", "^TestRustMatchingMySQLInterop$", "-count=1",
            ]
            rust_command = [
                "cargo", "test", "--locked", "--manifest-path", str(ROOT / "rust/Cargo.toml"),
                "-p", "cadence-persistence", "--test", "matching_mysql_interop",
                "mysql_interop_harness", "--", "--ignored", "--exact", "--nocapture",
            ]
            mysql_unit_command = [
                "cargo", "test", "--locked", "--manifest-path", str(ROOT / "rust/Cargo.toml"),
                "-p", "cadence-persistence", "matching_mysql::tests::", "--", "--nocapture",
            ]
            mysql_timeout_command = [
                "cargo", "test", "--locked", "--manifest-path", str(ROOT / "rust/Cargo.toml"),
                "-p", "cadence-persistence", "timed_domain_query", "--", "--ignored", "--nocapture",
            ]
            mysql_tls_command = [
                "cargo", "test", "--locked", "--manifest-path", str(ROOT / "rust/Cargo.toml"),
                "-p", "cadence-persistence", "mysql_async_tls_uses_go_socket_timeout_options",
                "--", "--ignored", "--nocapture",
            ]
            run(mysql_unit_command, cwd=ROOT, env=environment)
            run(mysql_timeout_command, cwd=ROOT, env=environment)
            run(mysql_tls_command, cwd=ROOT, env=environment)
            run(go_command, cwd=ROOT / "go", env={**environment, "CADENCE_MYSQL_PHASE": "seed"})
            run(rust_command, cwd=ROOT, env=environment)
            run(go_command, cwd=ROOT / "go", env={**environment, "CADENCE_MYSQL_PHASE": "verify-rust"})
    finally:
        run(["docker", "rm", "--force", CONTAINER], check=False, stdout=subprocess.DEVNULL)

    print("Verified Go MySQL persistence -> Rust Matching adapters -> Go MySQL persistence")


if __name__ == "__main__":
    main()
