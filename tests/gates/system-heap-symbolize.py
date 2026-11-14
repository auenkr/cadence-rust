#!/usr/bin/env python3
"""Resolve sampled System allocator stacks on the runner after the process exits."""
import collections
import json
from pathlib import Path
import subprocess
import sys

binary, directory = sys.argv[1], Path(sys.argv[2])
profiles = {}
for path in sorted(directory.glob('system-*.json')):
    data = json.loads(path.read_text())
    counts = collections.Counter()
    sizes = collections.Counter()
    for size, stack in data['samples']:
        # dladdr base applies to executable frames; shared-library PCs resolve to ??.
        stack = tuple(pc - data['base'] for pc in stack if pc >= data['base'])
        counts[stack] += 1
        sizes[stack] += size
    profiles[path.stem] = (counts, sizes)
base = profiles['system-0180'][0]
for label, (counts, sizes) in profiles.items():
    growth = {stack: count - base[stack] for stack, count in counts.items()}
    output = []
    for stack in sorted(counts, key=lambda stack: growth[stack], reverse=True)[:40]:
        output.append(f'live_samples={counts[stack]} delta_samples={growth[stack]:+d} live_requested_sample_bytes={sizes[stack]}')
        resolved = subprocess.check_output(['addr2line', '-C', '-f', '-i', '-e', binary, *[hex(pc - 1) for pc in stack]], text=True)
        output.append(resolved)
    (directory / f'{label}-stacks.txt').write_text('\n'.join(output))
