#!/usr/bin/env python3
"""Compare Matching garbage collection with the original Go collector."""
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
fixture = root / 'tests/fixtures/matching-gc.json'
with tempfile.TemporaryDirectory(prefix='cadence-gc-') as directory:
    output = Path(directory) / 'output.json'
    overlay = Path(directory) / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(root/'go/service/matching/tasklist/rust_gc_test.go'): str(root/'tests/matching-gc-go_test.go')}}))
    subprocess.run(['go','test','-overlay',str(overlay),'./service/matching/tasklist','-run','^TestRustMatchingGC$','-count=1'],cwd=root/'go',env={**os.environ,'CADENCE_GC_INPUT':str(fixture),'CADENCE_GC_OUTPUT':str(output)},check=True)
    actual=json.loads(output.read_text())
    if args.regenerate: fixture.write_text(json.dumps(actual,indent=2)+'\n')
    elif actual != json.loads(fixture.read_text()): raise RuntimeError('original Go GC fixtures drifted')
if not args.regenerate:
    subprocess.run(['cargo','test','--manifest-path',str(root/'rust/Cargo.toml'),'-p','cadence-persistence','matching_gc'],check=True)
print(f'Verified {len(actual)} original-Go GC cases')
