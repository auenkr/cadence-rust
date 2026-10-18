#!/usr/bin/env python3
"""Run Rust direct PING sender against the pinned Ringpop v0.10.0 Go node."""
import json, os, shutil, stat, subprocess, tempfile
from pathlib import Path
root = Path(__file__).resolve().parent.parent
module = Path('/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0')
example = Path(os.environ.get('RINGPOP_PING_PROBE', '/tmp/cadence-ringpop-ping-probe'))
target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'rust/target'))
if not example.exists():
    subprocess.run(['cargo', 'build', '--locked', '--manifest-path', str(root/'rust/Cargo.toml'), '-p', 'cadence-server', '--example', 'ringpop_ping_probe'], cwd=root/'rust', check=True)
    shutil.copy2(target/'debug/examples/ringpop_ping_probe', example)
with tempfile.TemporaryDirectory(prefix='ringpop-outbound-ping-') as directory:
    temp = Path(directory); copied = temp/'ringpop-go'
    shutil.copytree(module, copied, ignore=shutil.ignore_patterns('.git'))
    for current, _dirs, _files in os.walk(copied): os.chmod(current, stat.S_IRWXU|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
    shutil.copy2(root/'go/go.mod', copied/'go.mod'); shutil.copy2(root/'go/go.sum', copied/'go.sum')
    mod = copied/'go.mod'; mod.write_text(mod.read_text().replace('module github.com/uber/cadence', 'module github.com/uber/ringpop-go', 1))
    virtual = copied/'swim/zz_cadence_ringpop_outbound_ping_test.go'
    overlay = temp/'overlay.json'; overlay.write_text(json.dumps({'Replace': {str(virtual): str(root/'tests/ringpop-outbound-ping-go_test.go')}}))
    env = {**os.environ, 'RINGPOP_PING_PROBE': str(example.resolve())}
    subprocess.run(['go', 'test', '-overlay', str(overlay), './swim', '-run', '^TestRustOutboundPingAgainstGo$', '-count=1', '-v'], cwd=copied, env=env, check=True, timeout=180)
print('Rust outbound PING passed against a real pinned Go Ringpop node')
