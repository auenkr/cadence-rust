#!/usr/bin/env python3
"""Run the Rust-owned SWIM probe loop against a pinned Go Ringpop target."""
import json
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parent.parent
module = Path('/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0')
target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'rust/target'))
example = target / 'debug/examples/ringpop_swim_runtime_probe'
subprocess.run([
    'cargo', 'build', '--locked', '--manifest-path', str(root / 'rust/Cargo.toml'),
    '-p', 'cadence-server', '--example', 'ringpop_swim_runtime_probe',
], cwd=root / 'rust', check=True)
if not example.is_file():
    raise RuntimeError(f'build did not produce current probe at {example}')

with tempfile.TemporaryDirectory(prefix='ringpop-swim-runtime-compat-') as directory:
    temp = Path(directory)
    copied = temp / 'ringpop-go'
    shutil.copytree(module, copied, ignore=shutil.ignore_patterns('.git'))
    for current, _dirs, files in os.walk(copied):
        os.chmod(current, stat.S_IRWXU | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        for filename in files:
            os.chmod(Path(current) / filename, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
    shutil.copy2(root / 'go/go.mod', copied / 'go.mod')
    shutil.copy2(root / 'go/go.sum', copied / 'go.sum')
    mod = copied / 'go.mod'
    mod.write_text(mod.read_text().replace('module github.com/uber/cadence', 'module github.com/uber/ringpop-go', 1))
    virtual = copied / 'swim/zz_cadence_ringpop_swim_runtime_test.go'
    overlay = temp / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(virtual): str(root / 'tests/ringpop-swim-runtime-go_test.go')}}))
    env = {**os.environ, 'RINGPOP_SWIM_RUNTIME_PROBE': str(example.resolve())}
    subprocess.run([
        'go', 'test', '-overlay', str(overlay), './swim',
        '-run', '^Test(GoSwimProbeAndTransitionOracle|GoStopWaitsForInflightProtocol|RustSwimRuntimeAgainstGo)$', '-count=1', '-v',
    ], cwd=copied, env=env, check=True, timeout=180)
print('Rust SWIM probe loop and pinned Go probe/timer traces passed')
