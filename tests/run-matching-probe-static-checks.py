#!/usr/bin/env python3
"""Static checks for the JavaScript compatibility probes.

The probes are plain Node scripts without a bundler or a type checker, so a
scoping mistake in a rarely executed branch only shows up in a full mixed
run. This runner parses every probe and reports undefined identifiers with
the repository's ESLint installation, and it runs the offline self-checks
that do not need a cluster.

Run from the repository root:

    python3 tests/run-matching-probe-static-checks.py
"""

import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
PROBES = [
    'tests/matching-service-compat.cjs',
    'tests/matching-mysql-gate.cjs',
    'tests/matching-thrift-blob.cjs',
    'tests/matching-mysql-gate-check.cjs',
]
SELF_CHECKS = [
    'tests/matching-mysql-gate-check.cjs',
]


def eslint_binary():
    """Return the ESLint entry point of the web workspace, if it is present."""
    for candidate in (
        Path('/home/ubuntu/github/auenkr/cadence-rust/cadence-web/node_modules/.bin/eslint'),
        ROOT / 'cadence-web/node_modules/.bin/eslint',
    ):
        if candidate.is_file():
            return candidate
    return None


def run(command, **kwargs):
    return subprocess.run(command, capture_output=True, text=True, **kwargs)


def main():
    failures = []
    for probe in PROBES:
        path = ROOT / probe
        syntax = run(['node', '--check', str(path)])
        if syntax.returncode:
            failures.append(f'{probe}: node --check failed\n{syntax.stderr}')
            continue
        print(f'{probe}: syntax ok')
    eslint = eslint_binary()
    if eslint is None:
        print('ESLint is not installed, the undefined identifier check is skipped')
    else:
        for probe in PROBES:
            result = run([
                str(eslint), '--no-eslintrc', '--no-ignore', '--env', 'node,es2022',
                '--parser-options', 'ecmaVersion:2022,sourceType:script',
                '--rule', json.dumps({'no-undef': 'error', 'no-unused-vars': ['error', {'args': 'none'}]}),
                str(ROOT / probe),
            ])
            if result.returncode:
                failures.append(f'{probe}: eslint reported problems\n{result.stdout}{result.stderr}')
            else:
                print(f'{probe}: no undefined identifiers')
    for check in SELF_CHECKS:
        result = run(['node', str(ROOT / check)], cwd=ROOT)
        if result.returncode:
            failures.append(f'{check}: failed\n{result.stdout}{result.stderr}')
        else:
            print(f'{check}: {result.stdout.strip()}')
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        raise SystemExit(1)
    print('Matching probe static checks passed')


if __name__ == '__main__':
    main()
