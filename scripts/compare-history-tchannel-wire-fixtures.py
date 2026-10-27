#!/usr/bin/env python3
"""Compare complete Thrift binary values, allowing unordered map entries."""

import base64
import json
import struct
import sys


def decode(data):
    offset = 0

    def take(size):
        nonlocal offset
        chunk = data[offset:offset + size]
        if len(chunk) != size:
            raise ValueError('truncated Thrift value')
        offset += size
        return chunk

    def number(fmt):
        return struct.unpack(fmt, take(struct.calcsize(fmt)))[0]

    def count():
        size = number('!i')
        if size < 0:
            raise ValueError('negative Thrift container size')
        return size

    def value(kind):
        if kind in (2, 3, 4, 6, 8, 10):
            # Preserve exact scalar bytes, including floating-point bit patterns.
            return take({2: 1, 3: 1, 4: 8, 6: 2, 8: 4, 10: 8}[kind]).hex()
        if kind == 11:
            return take(count()).hex()
        if kind == 12:
            fields = []
            while True:
                child = number('!B')
                if child == 0:
                    return tuple(fields)
                field_id = number('!h')
                fields.append((field_id, child, value(child)))
        if kind == 13:
            key, child = number('!B'), number('!B')
            entries = [(value(key), value(child)) for _ in range(count())]
            return key, child, tuple(sorted(entries, key=repr))
        if kind in (14, 15):
            child = number('!B')
            entries = [value(child) for _ in range(count())]
            if kind == 14:
                entries.sort(key=repr)
            return child, tuple(entries)
        raise ValueError(f'unsupported Thrift type {kind}')

    result = value(12)
    if offset != len(data):
        raise ValueError('trailing bytes after Thrift struct')
    return result


def load(path):
    with open(path, encoding='utf-8') as source:
        fixtures = json.load(source)
    result = {}
    for fixture in fixtures:
        name = fixture['name']
        if name in result:
            raise ValueError(f'duplicate fixture {name}')
        result[name] = (fixture['thrift_type'], decode(base64.b64decode(fixture['base64'], validate=True)))
    return result


if __name__ == '__main__':
    expected, observed = map(load, sys.argv[1:])
    mismatches = sorted(name for name in expected.keys() | observed.keys()
                        if expected.get(name) != observed.get(name))
    if mismatches:
        raise SystemExit(f'Go History wire fixture values differ: {mismatches}')
