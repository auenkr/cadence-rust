#!/usr/bin/env python3
"""Compare isolation selection and dispatch timing with actual Go Matching."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--regenerate', action='store_true')
args = parser.parse_args()
fixture = root / 'tests/fixtures/matching-isolation.json'
with tempfile.TemporaryDirectory(prefix='cadence-isolation-') as directory:
    output = Path(directory) / 'fixtures.json'
    overlay = Path(directory) / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(root/'go/service/matching/tasklist/rust_isolation_test.go'): str(root/'tests/matching-isolation-go_test.go')}}))
    subprocess.run(['go', 'test', '-overlay', str(overlay), './service/matching/tasklist', '-run', '^TestRustMatchingIsolationFixtures$', '-count=1'], cwd=root/'go', env={**os.environ, 'CADENCE_ISOLATION_FIXTURES': str(output)}, check=True)
    actual = json.loads(output.read_text())
    if args.regenerate:
        fixture.write_text(json.dumps(actual, indent=2)+'\n')
    elif actual != json.loads(fixture.read_text()):
        raise RuntimeError('original Go isolation fixture drift')
if not args.regenerate:
    subprocess.run(['cargo', 'test', '--manifest-path', str(root/'rust/Cargo.toml'), '-p', 'cadence-common', 'task_isolation::tests'], check=True)
print(f'Verified {len(actual["selection"])} isolation decisions, {len(actual["timeouts"])} dispatch timeouts and {len(actual["rewrites"])} metadata rewrites')
