#!/usr/bin/env python3
"""Resolve System allocator stacks after exit, loading debug symbols only once."""
import collections
import json
from pathlib import Path
import re
import subprocess
import sys

binary, directory = sys.argv[1], Path(sys.argv[2])
profiles = {}
addresses = set()
for path in sorted(directory.glob('system-*.json')):
    data = json.loads(path.read_text())
    counts = collections.Counter()
    sizes = collections.Counter()
    for size, stack in data['samples']:
        stack = tuple(pc - data['base'] for pc in stack if pc > data['base'])
        counts[stack] += 1
        sizes[stack] += size
        addresses.update(pc - 1 for pc in stack)
    profiles[path.stem] = (counts, sizes)
resolved = subprocess.check_output(
    ['addr2line', '-a', '-C', '-f', '-i', '-e', binary,
     *[hex(pc) for pc in sorted(addresses)]], text=True)
symbols = {}
current = None
for line in resolved.splitlines():
    if re.fullmatch(r'0x[0-9a-fA-F]+', line):
        current = int(line, 16)
        symbols[current] = []
    elif current is not None:
        symbols[current].append(line)
base_counts, base_sizes = profiles['system-0180']
for label, (counts, sizes) in profiles.items():
    stacks = counts.keys() | base_counts.keys()
    output = []
    for order in ['sample-count', 'sample-requested-bytes']:
        output.append(f'Largest growth by {order}; samples are not unbiased byte estimates')
        def key(stack):
            count_delta = counts[stack] - base_counts[stack]
            size_delta = sizes[stack] - base_sizes[stack]
            return (count_delta, size_delta) if order == 'sample-count' else (size_delta, count_delta)
        for stack in sorted(stacks, key=key, reverse=True)[:40]:
            output.append(f'live_samples={counts[stack]} delta_samples={counts[stack] - base_counts[stack]:+d} live_requested_sample_bytes={sizes[stack]} delta_requested_sample_bytes={sizes[stack] - base_sizes[stack]:+d}')
            for pc in stack:
                output.append(f'{hex(pc - 1)}\n' + '\n'.join(symbols.get(pc - 1, ['??'])))
    (directory / f'{label}-stacks.txt').write_text('\n'.join(output))
