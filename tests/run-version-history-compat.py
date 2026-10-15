#!/usr/bin/env python3
"""Compare all version-history fixture outcomes with the actual Go code and Rust."""
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent.parent
fixture = root / "tests/fixtures/version-history.json"
cases = json.loads(fixture.read_text())
result = subprocess.run(["go", "run", "../tests/version-history-go/main.go"],
                        cwd=root / "go", input=fixture.read_text(), text=True,
                        stdout=subprocess.PIPE, check=True)
actual = json.loads(result.stdout)
assert len(actual) == len(cases)
for case, value in zip(cases, actual):
    if value != case["expected"]:
        raise AssertionError(f'{case["name"]}: Go returned {value!r}, expected {case["expected"]!r}')
subprocess.run(["cargo", "test", "--manifest-path", str(root / "rust/Cargo.toml"),
                "-p", "cadence-persistence", "version_history_go_fixtures"], check=True)
print(f'{len(cases)} version-history cases match the pinned Go implementation and Rust.')
