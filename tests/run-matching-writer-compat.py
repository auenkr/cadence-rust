#!/usr/bin/env python3
"""Check native writer batching against the original Go task writer."""
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
fixture = root / 'tests/fixtures/matching-writer.json'
with tempfile.TemporaryDirectory(prefix='cadence-writer-') as directory:
    output = Path(directory) / 'output.json'
    overlay = Path(directory) / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(root/'go/service/matching/tasklist/rust_writer_test.go'):str(root/'tests/matching-writer-go_test.go')}}))
    subprocess.run(['go','test','-overlay',str(overlay),'./service/matching/tasklist','-run','^TestRustMatchingWriterBatch$','-count=1'],cwd=root/'go',env={**os.environ,'CADENCE_WRITER_OUTPUT':str(output)},check=True)
    actual=json.loads(output.read_text())
    if args.regenerate: fixture.write_text(json.dumps(actual,indent=2)+'\n')
    elif actual != json.loads(fixture.read_text()): raise RuntimeError('original Go writer batching drifted')
if not args.regenerate:
    subprocess.run(['cargo','test','--manifest-path',str(root/'rust/Cargo.toml'),'-p','cadence-server','--lib','matching_writer'],check=True)
print(f'Verified {len(actual)} original-Go writer batch cases')
