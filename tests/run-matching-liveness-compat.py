#!/usr/bin/env python3
"""Check Matching idle liveness against the original Go implementation."""
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
fixture = root / 'tests/fixtures/matching-liveness.json'
with tempfile.TemporaryDirectory(prefix='cadence-liveness-') as directory:
    output = Path(directory) / 'output.json'
    overlay = Path(directory) / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(root/'go/service/matching/liveness/rust_liveness_test.go'):str(root/'tests/matching-liveness-go_test.go')}}))
    subprocess.run(['go','test','-overlay',str(overlay),'./service/matching/liveness','-run','^TestRustMatchingLiveness$','-count=1'],cwd=root/'go',env={**os.environ,'CADENCE_LIVENESS_OUTPUT':str(output)},check=True)
    actual=json.loads(output.read_text())
    if args.regenerate: fixture.write_text(json.dumps(actual,indent=2)+'\n')
    elif actual != json.loads(fixture.read_text()): raise RuntimeError('original Go liveness fixtures drifted')
if not args.regenerate:
    subprocess.run(['cargo','test','--manifest-path',str(root/'rust/Cargo.toml'),'-p','cadence-common','liveness::tests'],check=True)
print('Verified original-Go liveness boundaries and refresh semantics')
