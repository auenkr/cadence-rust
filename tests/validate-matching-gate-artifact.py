#!/usr/bin/env python3
"""Validate one downloaded Matching mixed-gate artifact bundle.

This checks recorded evidence only. It does not run services or prove parity.
"""
import argparse
import json
import re
import sys
from pathlib import Path


def fail(message):
    raise ValueError(message)


def read_text(root, name):
    path = root / name
    if not path.is_file():
        fail(f"missing required evidence: {name}")
    return path.read_text().strip()


def read_json(root, name):
    try:
        return json.loads(read_text(root, name))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        fail(f"invalid JSON in {name}: {exc}")


def exit_zero(root, name):
    value = read_text(root, name)
    if not re.fullmatch(r"0+", value):
        fail(f"{name} must record exit 0, got {value!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact_dir", type=Path, help="downloaded lane artifact directory")
    parser.add_argument("--expected-sha", required=True, help="full expected source commit SHA")
    parser.add_argument("--sdk-marker", action="append", default=[],
                        help="literal emitted probe.log text proving a lane-specific SDK outcome; repeat as needed")
    parser.add_argument("--lifecycle-json", action="append", default=[], metavar="PATH::KEY=VALUE",
                        help="require an emitted key/value in JSON below gate/run (for example rust-matching/matching-shutdown.json::exitCode=0); repeat")
    args = parser.parse_args()
    root = args.artifact_dir.resolve()
    if not re.fullmatch(r"[0-9a-fA-F]{40}", args.expected_sha):
        fail("--expected-sha must be a full 40-character commit SHA")
    commit = read_text(root, "commit.txt")
    if commit.lower() != args.expected_sha.lower():
        fail(f"commit mismatch: artifact records {commit!r}, expected {args.expected_sha}")
    digest_record = read_text(root, "binary.sha256")
    match = re.fullmatch(r"([0-9a-fA-F]{64})(?:\s+.+)?", digest_record)
    if not match:
        fail("binary.sha256 must contain a 64-hex SHA256, optionally followed by a filename")
    for name in ("build.exit", "gate.exit", "gate/lane.exit", "gate/run/run.exit"):
        exit_zero(root, name)
    results = read_json(root, "gate/run/results.json")
    expected_roles = {"frontend", "history", "worker"}
    if results.get("goMatchingStarted") is not False:
        fail("results.json must explicitly record goMatchingStarted: false")
    if "matching" not in results.get("rustServices", []):
        fail("results.json rustServices must include matching")
    go_services = results.get("goServices")
    if not isinstance(go_services, list) or not expected_roles.issubset(set(go_services)):
        fail("results.json goServices must explicitly include frontend, history, and worker")
    if "matching" in go_services:
        fail("results.json goServices includes matching; Go Matching must be absent")
    topology = results.get("topology")
    if not isinstance(topology, str) or not topology.strip():
        fail("results.json lacks a recorded topology description")
    if results.get("probeExitCode") != 0:
        fail(f"results.json probeExitCode must be 0, got {results.get('probeExitCode')!r}")
    probe = read_text(root, "gate/run/probe.log")
    for marker in args.sdk_marker:
        if marker not in probe:
            fail(f"SDK outcome marker absent from gate/run/probe.log: {marker!r}")
    for requirement in args.lifecycle_json:
        if "::" not in requirement or "=" not in requirement.split("::", 1)[1]:
            fail(f"invalid --lifecycle-json {requirement!r}; expected PATH::KEY=VALUE")
        relative, expression = requirement.split("::", 1)
        key, expected = expression.split("=", 1)
        evidence = read_json(root, "gate/run/" + relative)
        actual = evidence
        for part in key.split("."):
            if not isinstance(actual, dict) or part not in actual:
                fail(f"lifecycle evidence lacks {key!r}: gate/run/{relative}")
            actual = actual[part]
        try:
            wanted = json.loads(expected)
        except json.JSONDecodeError:
            wanted = expected
        if actual != wanted:
            fail(f"lifecycle evidence {relative}:{key} is {actual!r}, expected {wanted!r}")
    print(f"PASS recorded artifact checks: {root} (commit {commit}, binary sha256 {match.group(1)})")
    if not args.sdk_marker and not args.lifecycle_json:
        print("LIMIT: no lane-specific SDK/lifecycle assertions were supplied; this validates bundle identity, exits, topology and recorded probe status only.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
