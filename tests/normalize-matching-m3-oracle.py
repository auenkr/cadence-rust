#!/usr/bin/env python3
"""Canonicalize decoded M3 packets for cross-reporter semantic comparison."""

import json
import sys
from pathlib import Path


def canonical_lines(path: Path) -> list[str]:
    output: list[str] = []
    for source_line in path.read_text(encoding="utf-8").splitlines():
        if not source_line.strip():
            continue
        batch = json.loads(source_line)
        common_tags = sorted(
            batch.get("commonTags", []), key=lambda tag: (tag["name"], tag["value"])
        )
        for metric in batch.get("metrics", []):
            # Reporter self-observation differs by implementation and is not a
            # Matching metric. It is excluded from both sides of this wire test.
            if metric.get("name", "").startswith("tally.internal."):
                continue
            metric.pop("timestamp", None)
            metric["tags"] = sorted(
                metric.get("tags", []), key=lambda tag: (tag["name"], tag["value"])
            )
            output.append(
                json.dumps(
                    {"commonTags": common_tags, "metric": metric},
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
    return sorted(output)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: normalize-matching-m3-oracle.py INPUT OUTPUT")
    Path(sys.argv[2]).write_text("\n".join(canonical_lines(Path(sys.argv[1]))) + "\n")
