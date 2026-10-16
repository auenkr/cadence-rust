#!/usr/bin/env python3
"""Check poller history state against the original Go manager and cache."""
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
fixture = root / 'tests/fixtures/matching-pollers.json'
with tempfile.TemporaryDirectory(prefix='cadence-pollers-') as directory:
    output = Path(directory) / 'fixtures.json'
    overlay = Path(directory) / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(root/'go/service/matching/poller/rust_poller_test.go'): str(root/'tests/matching-poller-go_test.go')}}))
    subprocess.run(['go','test','-overlay',str(overlay),'./service/matching/poller','-run','^TestRustMatchingPollerFixtures$','-count=1'],cwd=root/'go',env={**os.environ,'CADENCE_POLLER_FIXTURES':str(output)},check=True)
    actual=json.loads(output.read_text())
    if args.regenerate: fixture.write_text(json.dumps(actual,indent=2)+'\n')
    elif actual != json.loads(fixture.read_text()): raise RuntimeError('original Go poller fixture drift')
if not args.regenerate:
    subprocess.run(['cargo','test','--manifest-path',str(root/'rust/Cargo.toml'),'-p','cadence-common','poller::tests'],check=True)
print(f'Verified {len(actual["snapshots"])} original-Go poller snapshots')
