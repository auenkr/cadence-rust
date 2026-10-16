#!/usr/bin/env python3
"""Compare clock limiter operations with the original Go wrapper."""
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
fixture = root / 'tests/fixtures/clock-rate.json'
with tempfile.TemporaryDirectory(prefix='cadence-rate-') as directory:
    overlay = Path(directory) / 'overlay.json'
    output = Path(directory) / 'output.json'
    overlay.write_text(json.dumps({'Replace': {
        str(root / 'go/common/clock/rust_clock_rate_test.go'):
        str(root / 'tests/clock-rate-go_test.go')}}))
    subprocess.run(['go', 'test', '-overlay', str(overlay), './common/clock',
                    '-run', '^TestRustClockRateOperations$', '-count=1'], cwd=root / 'go',
                   env={**os.environ, 'CADENCE_CLOCK_RATE_INPUT': str(fixture),
                        'CADENCE_CLOCK_RATE_OUTPUT': str(output)}, check=True)
    actual = json.loads(output.read_text())
    if args.regenerate:
        fixture.write_text(json.dumps(actual, indent=2) + '\n')
    elif actual != json.loads(fixture.read_text()):
        raise RuntimeError('original Go clock rate fixtures drifted')
if not args.regenerate:
    subprocess.run(['cargo', 'test', '--manifest-path', str(root / 'rust/Cargo.toml'),
                    '-p', 'cadence-common', 'clock_rate'], check=True)
print(f'Verified {len(actual)} original-Go clock rate sequences')
